from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def coerce_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def recovery_reason(state: dict[str, Any] | None) -> str:
    base_state = coerce_mapping(state)
    return str(base_state.get("recovery_reason") or base_state.get("reason") or "").strip()


def recovery_count(state: dict[str, Any] | None) -> int:
    base_state = coerce_mapping(state)
    return int(base_state.get("recovery_count", 0) or 0)


def created_at(state: dict[str, Any] | None) -> str:
    base_state = coerce_mapping(state)
    return str(base_state.get("created_at", "")).strip()


def load_matching_state(
    job_dir: Path,
    *,
    load_state_fn: Callable[[Path], dict[str, Any] | None],
    state_matches_job_fn: Callable[..., bool],
    match_kwargs: dict[str, Any],
) -> dict[str, Any]:
    state = load_state_fn(job_dir) or {}
    if state_matches_job_fn(state, **match_kwargs):
        return state
    return {}


def mark_terminal_status(
    queue_root: str | Path,
    queue_id: str,
    *,
    status: str,
    reason: str,
    metadata_update: dict[str, Any] | None,
    mark_completed_fn: Callable[..., Any],
    mark_cancelled_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
) -> None:
    if status == "completed":
        mark_completed_fn(str(queue_root), queue_id, metadata_update=metadata_update)
        return
    if status == "cancelled":
        mark_cancelled_fn(
            str(queue_root),
            queue_id,
            error=reason,
            metadata_update=metadata_update,
        )
        return
    mark_failed_fn(
        str(queue_root),
        queue_id,
        error=reason,
        metadata_update=metadata_update,
    )


def write_result_artifacts(
    job_dir_text: str,
    *,
    state_payload: dict[str, Any],
    report_payload: dict[str, Any],
    report_lines: list[str],
    write_state_fn: Callable[[Path, dict[str, Any]], Any],
    write_report_json_fn: Callable[[Path, dict[str, Any]], Any],
    write_report_md_lines_fn: Callable[[Path, list[str]], Any],
) -> None:
    if not job_dir_text.strip():
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    write_state_fn(job_dir, state_payload)
    write_report_json_fn(job_dir, report_payload)
    write_report_md_lines_fn(job_dir, report_lines)
