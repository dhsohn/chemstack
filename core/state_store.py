from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, cast
from uuid import uuid4

from . import lock_utils
from .types import RunFinalResult, RunState

logger = logging.getLogger(__name__)


STATE_FILE_NAME = "run_state.json"
REPORT_JSON_NAME = "run_report.json"
REPORT_MD_NAME = "run_report.md"
LOCK_FILE_NAME = "run.lock"


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def state_path(reaction_dir: Path) -> Path:
    return reaction_dir / STATE_FILE_NAME


def report_json_path(reaction_dir: Path) -> Path:
    return reaction_dir / REPORT_JSON_NAME


def report_md_path(reaction_dir: Path) -> Path:
    return reaction_dir / REPORT_MD_NAME


def load_state(reaction_dir: Path) -> Optional[RunState]:
    p = state_path(reaction_dir)
    if not p.exists():
        return None
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return cast(RunState, raw)


def new_state(reaction_dir: Path, selected_inp: Path, max_retries: int) -> RunState:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    ts = now_utc_iso()
    return {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(selected_inp),
        "max_retries": int(max_retries),
        "status": "created",
        "started_at": ts,
        "updated_at": ts,
        "attempts": [],
        "final_result": None,
    }


def _atomic_write_text(path: Path, payload: str) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid4().hex[:8]}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(tmp_path), str(path))
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


atomic_write_text = _atomic_write_text


def save_state(reaction_dir: Path, state: RunState) -> Path:
    state["updated_at"] = now_utc_iso()
    p = state_path(reaction_dir)
    _atomic_write_text(p, json.dumps(state, ensure_ascii=True, indent=2))
    logger.debug("State saved: %s", p)
    return p


def finalize_state(
    reaction_dir: Path,
    state: RunState,
    *,
    status: str,
    final_result: RunFinalResult,
) -> None:
    state["status"] = status
    state["final_result"] = final_result
    save_state(reaction_dir, state)


def _build_report_payload(state: RunState) -> Dict[str, Any]:
    attempts = state.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
    return {
        "run_id": state.get("run_id"),
        "reaction_dir": state.get("reaction_dir"),
        "selected_inp": state.get("selected_inp"),
        "status": state.get("status"),
        "started_at": state.get("started_at"),
        "updated_at": state.get("updated_at"),
        "attempt_count": len(attempts),
        "max_retries": state.get("max_retries"),
        "attempts": attempts,
        "final_result": state.get("final_result"),
    }


def _render_report_markdown(report_payload: Dict[str, Any]) -> str:
    lines = [
        "# ORCA Run Report",
        "",
        f"- run_id: `{report_payload['run_id']}`",
        f"- reaction_dir: `{report_payload['reaction_dir']}`",
        f"- selected_inp: `{report_payload['selected_inp']}`",
        f"- status: `{report_payload['status']}`",
        f"- started_at_utc: `{report_payload['started_at']}`",
        f"- updated_at_utc: `{report_payload['updated_at']}`",
        f"- attempt_count: `{report_payload['attempt_count']}`",
        f"- max_retries: `{report_payload['max_retries']}`",
        "",
        "## Attempts",
        "",
        "| # | inp | out | return_code | analyzer_status |",
        "|---:|---|---|---|---|",
    ]
    attempts = report_payload["attempts"] or []
    if attempts:
        for item in attempts:
            lines.append(
                "| {index} | `{inp}` | `{out}` | `{rc}` | `{status}` |".format(
                    index=item.get("index"),
                    inp=item.get("inp_path"),
                    out=item.get("out_path"),
                    rc=item.get("return_code"),
                    status=item.get("analyzer_status"),
                )
            )
    else:
        lines.append("| - | - | - | - | - |")

    lines.extend(["", "## Final Result", ""])
    final_result = report_payload.get("final_result")
    if isinstance(final_result, dict):
        for key in [
            "status",
            "analyzer_status",
            "reason",
            "completed_at",
            "last_out_path",
        ]:
            if key in final_result:
                lines.append(f"- {key}: `{final_result[key]}`")
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def write_report_files(reaction_dir: Path, state: RunState) -> Dict[str, str]:
    report_payload = _build_report_payload(state)
    json_path = report_json_path(reaction_dir)
    md_path = report_md_path(reaction_dir)
    _atomic_write_text(json_path, json.dumps(report_payload, ensure_ascii=True, indent=2))
    _atomic_write_text(md_path, _render_report_markdown(report_payload))
    return {"report_json": str(json_path), "report_md": str(md_path)}

def _run_lock_active_error(lock_pid: int, lock_info: Dict[str, Any], lock_path: Path) -> RuntimeError:
    started_at = lock_info.get("started_at")
    started = started_at if isinstance(started_at, str) and started_at else "unknown"
    return RuntimeError(
        "Another orca_auto instance is already running in this directory "
        f"(pid={lock_pid}, started_at={started}). Lock file: {lock_path}"
    )


def _run_lock_unreadable_error(lock_path: Path) -> RuntimeError:
    return RuntimeError(
        f"Lock file exists but owner PID is unreadable. Remove manually: {lock_path}"
    )


def _run_lock_stale_remove_error(lock_pid: int, lock_path: Path, exc: OSError) -> RuntimeError:
    return RuntimeError(
        f"Detected stale lock but failed to remove it (pid={lock_pid}). "
        f"Lock file: {lock_path}. error={exc}"
    )


@contextmanager
def acquire_run_lock(reaction_dir: Path) -> Iterator[None]:
    lock_path = reaction_dir / LOCK_FILE_NAME
    lock_payload = {"pid": os.getpid(), "started_at": now_utc_iso()}
    current_start_ticks = lock_utils.current_process_start_ticks()
    if current_start_ticks is not None:
        lock_payload["process_start_ticks"] = current_start_ticks

    with lock_utils.acquire_file_lock(
        lock_path=lock_path,
        lock_payload_obj=lock_payload,
        parse_lock_info_fn=lock_utils.parse_lock_info,
        is_process_alive_fn=lock_utils.is_process_alive,
        process_start_ticks_fn=lock_utils.process_start_ticks,
        logger=logger,
        acquired_log_template="Lock acquired: %s",
        released_log_template="Lock released: %s",
        stale_pid_reuse_log_template=(
            "Stale lock detected due PID reuse (pid=%d, expected_ticks=%d, observed_ticks=%d): %s"
        ),
        stale_lock_log_template="Stale lock detected (pid=%d), removing: %s",
        active_lock_error_builder=_run_lock_active_error,
        unreadable_lock_error_builder=_run_lock_unreadable_error,
        stale_remove_error_builder=_run_lock_stale_remove_error,
    ):
        yield
