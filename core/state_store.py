from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional
from uuid import uuid4

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
    return raw


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


def _parse_lock_info(lock_path: Path) -> Dict[str, Any]:
    pid: Optional[int] = None
    started_at: Optional[str] = None
    process_start_ticks: Optional[int] = None
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return {"pid": None, "started_at": None, "process_start_ticks": None}
    if not raw:
        return {"pid": None, "started_at": None, "process_start_ticks": None}

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, dict):
        raw_pid = parsed.get("pid")
        if isinstance(raw_pid, int) and raw_pid > 0:
            pid = raw_pid
        elif isinstance(raw_pid, str):
            try:
                parsed_pid = int(raw_pid.strip())
                if parsed_pid > 0:
                    pid = parsed_pid
            except ValueError:
                pid = None
        raw_started_at = parsed.get("started_at")
        if isinstance(raw_started_at, str) and raw_started_at.strip():
            started_at = raw_started_at
        raw_ticks = parsed.get("process_start_ticks")
        if isinstance(raw_ticks, int) and raw_ticks > 0:
            process_start_ticks = raw_ticks
        elif isinstance(raw_ticks, str):
            try:
                parsed_ticks = int(raw_ticks.strip())
                if parsed_ticks > 0:
                    process_start_ticks = parsed_ticks
            except ValueError:
                process_start_ticks = None
        return {"pid": pid, "started_at": started_at, "process_start_ticks": process_start_ticks}

    # Backward-compatible fallback: legacy lock file contained only a pid line.
    first_line = raw.splitlines()[0].strip()
    try:
        parsed_pid = int(first_line)
        if parsed_pid > 0:
            pid = parsed_pid
    except ValueError:
        pid = None
    return {"pid": pid, "started_at": None, "process_start_ticks": None}


def _is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _process_start_ticks(pid: int) -> Optional[int]:
    if pid <= 0:
        return None
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        raw = stat_path.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        return None
    if not raw:
        return None

    right_paren = raw.rfind(")")
    if right_paren < 0:
        return None
    fields_after_comm = raw[right_paren + 2 :].split()
    # /proc/<pid>/stat field 22 is starttime. After dropping pid+comm, it is index 19.
    if len(fields_after_comm) <= 19:
        return None
    try:
        value = int(fields_after_comm[19])
    except ValueError:
        return None
    return value if value > 0 else None


def _current_process_start_ticks() -> Optional[int]:
    return _process_start_ticks(os.getpid())


@contextmanager
def acquire_run_lock(reaction_dir: Path) -> Iterator[None]:
    lock_path = reaction_dir / LOCK_FILE_NAME
    lock_payload = {"pid": os.getpid(), "started_at": now_utc_iso()}
    current_start_ticks = _current_process_start_ticks()
    if current_start_ticks is not None:
        lock_payload["process_start_ticks"] = current_start_ticks

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(lock_payload, ensure_ascii=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            logger.debug("Lock acquired: %s (pid=%d)", lock_path, os.getpid())
            break
        except FileExistsError:
            lock_info = _parse_lock_info(lock_path)
            lock_pid = lock_info.get("pid")
            started_at = lock_info.get("started_at")
            lock_start_ticks = lock_info.get("process_start_ticks")

            if isinstance(lock_pid, int):
                alive = _is_process_alive(lock_pid)
                if alive and isinstance(lock_start_ticks, int):
                    observed_ticks = _process_start_ticks(lock_pid)
                    if observed_ticks is not None and observed_ticks != lock_start_ticks:
                        alive = False
                        logger.info(
                            "Stale lock detected due PID reuse (pid=%d, expected_ticks=%d, observed_ticks=%d): %s",
                            lock_pid,
                            lock_start_ticks,
                            observed_ticks,
                            lock_path,
                        )

                if alive:
                    started = started_at if isinstance(started_at, str) and started_at else "unknown"
                    raise RuntimeError(
                        "Another orca_auto instance is already running in this directory "
                        f"(pid={lock_pid}, started_at={started}). Lock file: {lock_path}"
                    )
                logger.info("Stale lock detected (pid=%d), removing: %s", lock_pid, lock_path)
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise RuntimeError(
                        f"Detected stale lock but failed to remove it (pid={lock_pid}). "
                        f"Lock file: {lock_path}. error={exc}"
                    )
                continue

            raise RuntimeError(
                f"Lock file exists but owner PID is unreadable. Remove manually: {lock_path}"
            )
    try:
        yield
    finally:
        try:
            lock_path.unlink()
            logger.debug("Lock released: %s", lock_path)
        except OSError:
            pass
