from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.queue import execution as _queue_execution

from .runner import XtbRunResult


@dataclass(frozen=True)
class TerminalSummary:
    queue_id: str
    job_id: str
    status: str
    reason: str
    organized_output_dir: str = ""
    metadata_update: dict[str, Any] = field(default_factory=dict)


def print_terminal_summary(summary: TerminalSummary) -> None:
    if summary.organized_output_dir:
        print(f"organized_output_dir: {summary.organized_output_dir}")
    print(f"queue_id: {summary.queue_id}")
    print(f"job_id: {summary.job_id}")
    print(f"status: {summary.status}")
    print(f"reason: {summary.reason}")


def terminal_status(
    state: dict[str, Any], report: dict[str, Any], refreshed: Any, rc: int | None
) -> str:
    queue_status_value = (
        getattr(getattr(refreshed, "status", None), "value", None)
        if refreshed is not None
        else None
    )
    queue_status = str(queue_status_value).strip().lower()
    status = str(report.get("status") or state.get("status") or queue_status).strip().lower()
    if not status:
        return "completed" if rc == 0 else "failed"
    if status not in {"completed", "failed", "cancelled"} and rc is not None:
        return "completed" if rc == 0 else "failed"
    return status


def terminal_reason(
    state: dict[str, Any],
    report: dict[str, Any],
    refreshed: Any,
    *,
    status: str,
    rc: int | None,
) -> str:
    reason = str(
        report.get("reason") or state.get("reason") or getattr(refreshed, "error", "")
    ).strip()
    if reason:
        return reason
    if status == "completed":
        return "completed"
    if status == "cancelled":
        return "cancel_requested"
    if rc is not None:
        return f"worker_exit_code_{rc}"
    return "unknown"


def terminal_metadata_update(
    state: dict[str, Any], report: dict[str, Any], entry: Any
) -> dict[str, Any]:
    metadata_update: dict[str, Any] = {}
    job_type = str(
        report.get("job_type") or state.get("job_type") or entry.metadata.get("job_type", "")
    ).strip()
    if job_type:
        metadata_update["job_type"] = job_type
    candidate_count_raw = report.get("candidate_count")
    if candidate_count_raw is None:
        candidate_count_raw = state.get("candidate_count")
    if candidate_count_raw is not None:
        try:
            metadata_update["candidate_count"] = int(candidate_count_raw)
        except (TypeError, ValueError):
            pass
    return metadata_update


def load_terminal_summary(
    queue_root: Path, entry: Any, *, rc: int | None = None, deps: Any
) -> TerminalSummary:
    job_dir = deps._job_dir(entry)
    state = deps.load_state(job_dir) or {}
    report = deps.load_report_json(job_dir) or {}
    organized_ref = deps.load_organized_ref(job_dir) or {}
    refreshed = deps._queue_entry_by_id(queue_root, entry.queue_id)

    status = terminal_status(state, report, refreshed, rc)
    reason = terminal_reason(state, report, refreshed, status=status, rc=rc)
    organized_output_dir = str(
        organized_ref.get("organized_output_dir")
        or report.get("organized_output_dir")
        or state.get("organized_output_dir")
        or ""
    ).strip()

    return TerminalSummary(
        queue_id=entry.queue_id,
        job_id=entry.task_id,
        status=status,
        reason=reason,
        organized_output_dir=organized_output_dir,
        metadata_update=terminal_metadata_update(state, report, entry),
    )


def ensure_terminal_queue_status(
    queue_root: Path, entry: Any, summary: TerminalSummary, *, deps: Any
) -> None:
    refreshed = deps._queue_entry_by_id(queue_root, entry.queue_id)
    current_status = str(getattr(getattr(refreshed, "status", None), "value", "")).strip().lower()
    if current_status in {"completed", "failed", "cancelled"}:
        return

    metadata_update = summary.metadata_update or None
    _queue_execution.mark_terminal_status(
        queue_root,
        entry.queue_id,
        status=summary.status,
        reason=summary.reason,
        metadata_update=metadata_update,
        mark_completed_fn=deps.mark_completed,
        mark_cancelled_fn=deps.mark_cancelled,
        mark_failed_fn=deps.mark_failed,
    )


def finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    auto_organize: bool,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    outcome_cls: type,
    deps: Any,
) -> Any:
    deps._write_execution_artifacts(
        entry,
        result,
        previous_state=previous_state,
        resumed=resumed,
    )
    final_selected_xyz = (
        Path(str(result.selected_input_xyz)).expanduser().resolve()
        if str(result.selected_input_xyz).strip()
        else deps._selected_xyz(entry)
    )

    metadata_update = {
        "candidate_count": result.candidate_count,
        "job_type": result.job_type,
    }
    _queue_execution.mark_terminal_status(
        queue_root,
        entry.queue_id,
        status=result.status,
        reason=result.reason,
        metadata_update=metadata_update,
        mark_completed_fn=deps.mark_completed,
        mark_cancelled_fn=deps.mark_cancelled,
        mark_failed_fn=deps.mark_failed,
    )

    deps.upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status=result.status,
        job_dir=deps._job_dir(entry),
        job_type=result.job_type,
        selected_input_xyz=str(final_selected_xyz),
        reaction_key=result.reaction_key,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )

    del auto_organize
    organized_target = ""

    deps.notify_job_finished(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        status=result.status,
        reason=result.reason,
        job_type=result.job_type,
        reaction_key=result.reaction_key,
        job_dir=deps._job_dir(entry),
        selected_xyz=final_selected_xyz,
        candidate_count=result.candidate_count,
        organized_output_dir=Path(organized_target) if organized_target else None,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )

    outcome = outcome_cls(result=result, organized_output_dir=organized_target)
    if emit_output:
        print_terminal_summary(
            TerminalSummary(
                queue_id=entry.queue_id,
                job_id=entry.task_id,
                status=result.status,
                reason=result.reason,
                organized_output_dir=organized_target,
            )
        )
    return outcome
