from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, cast

from core.persistence_utils import (
    atomic_write_json,
    atomic_write_text as _atomic_write_text,
    now_utc_iso as _now_utc_iso,
    timestamped_token,
)
from core.types import RunFinalResult, RunState

logger = logging.getLogger(__name__)


STATE_FILE_NAME = "run_state.json"
REPORT_JSON_NAME = "run_report.json"
REPORT_MD_NAME = "run_report.md"
ORGANIZED_REF_NAME = "organized_ref.json"


def now_utc_iso() -> str:
    return _now_utc_iso()


def state_path(reaction_dir: Path) -> Path:
    return reaction_dir / STATE_FILE_NAME


def report_json_path(reaction_dir: Path) -> Path:
    return reaction_dir / REPORT_JSON_NAME


def report_md_path(reaction_dir: Path) -> Path:
    return reaction_dir / REPORT_MD_NAME


def organized_ref_path(reaction_dir: Path) -> Path:
    return reaction_dir / ORGANIZED_REF_NAME


def _load_json_dict(path: Path) -> Dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def load_state(reaction_dir: Path) -> Optional[RunState]:
    raw = _load_json_dict(state_path(reaction_dir))
    return cast(RunState, raw) if raw is not None else None


def load_report_json(reaction_dir: Path) -> Dict[str, Any] | None:
    return _load_json_dict(report_json_path(reaction_dir))


def load_organized_ref(reaction_dir: Path) -> Dict[str, Any] | None:
    return _load_json_dict(organized_ref_path(reaction_dir))


def new_state(reaction_dir: Path, selected_inp: Path, max_retries: int) -> RunState:
    run_id = timestamped_token("run")
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


atomic_write_text = _atomic_write_text


def write_state(reaction_dir: Path, state: RunState) -> Path:
    state["updated_at"] = now_utc_iso()
    path = state_path(reaction_dir)
    atomic_write_json(path, state, ensure_ascii=True, indent=2)
    logger.debug("State saved: %s", path)
    return path


def save_state(reaction_dir: Path, state: RunState) -> Path:
    return write_state(reaction_dir, state)


def finalize_state(
    reaction_dir: Path,
    state: RunState,
    *,
    status: str,
    final_result: RunFinalResult,
) -> None:
    state["status"] = status
    state["final_result"] = final_result
    write_state(reaction_dir, state)


def _build_report_payload(state: RunState) -> Dict[str, Any]:
    attempts = state.get("attempts")
    if not isinstance(attempts, list):
        attempts = []
    return {
        "job_id": state.get("job_id"),
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


def write_report_json(reaction_dir: Path, report_payload: Dict[str, Any]) -> Path:
    path = report_json_path(reaction_dir)
    atomic_write_json(path, report_payload, ensure_ascii=True, indent=2)
    return path


def write_report_md(reaction_dir: Path, markdown: str) -> Path:
    path = report_md_path(reaction_dir)
    _atomic_write_text(path, markdown)
    return path


def write_report_files(reaction_dir: Path, state: RunState) -> Dict[str, str]:
    report_payload = _build_report_payload(state)
    json_path = write_report_json(reaction_dir, report_payload)
    md_path = write_report_md(reaction_dir, _render_report_markdown(report_payload))
    return {"report_json": str(json_path), "report_md": str(md_path)}


def write_organized_ref(reaction_dir: Path, payload: Dict[str, Any]) -> Path:
    path = organized_ref_path(reaction_dir)
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path
