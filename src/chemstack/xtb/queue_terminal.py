from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution

from .runner import XtbRunResult


def _dependency(deps: Any | None, explicit: Any, name: str) -> Any:
    if explicit is not None:
        return explicit
    if deps is not None:
        return getattr(deps, name)
    raise TypeError(f"missing required dependency: {name}")


@dataclass(frozen=True)
class TerminalSummary:
    queue_id: str
    job_id: str
    status: str
    reason: str
    organized_output_dir: str = ""
    metadata_update: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class XtbTerminalDependencies:
    write_execution_artifacts: Callable[..., Any]
    selected_xyz: Callable[[Any], Path]
    job_dir: Callable[[Any], Path]
    mark_completed: Callable[..., Any]
    mark_cancelled: Callable[..., Any]
    mark_failed: Callable[..., Any]
    upsert_job_record: Callable[..., Any]
    notify_job_finished: Callable[..., Any]


@dataclass(frozen=True)
class XtbTerminalFinalizationRequest:
    cfg: Any
    queue_root: Path
    entry: Any
    result: XtbRunResult
    emit_output: bool
    outcome_cls: type
    previous_state: dict[str, Any] | None = None
    resumed: bool = False


@dataclass(frozen=True)
class _XtbTerminalPaths:
    job_dir: Path
    selected_xyz: Path


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
    queue_root: Path,
    entry: Any,
    *,
    rc: int | None = None,
    deps: Any | None = None,
    job_dir_fn: Callable[[Any], Path] | None = None,
    load_state_fn: Callable[[Path], dict[str, Any] | None] | None = None,
    load_report_json_fn: Callable[[Path], dict[str, Any] | None] | None = None,
    load_organized_ref_fn: Callable[[Path], dict[str, Any] | None] | None = None,
    queue_entry_by_id_fn: Callable[[Path, str], Any | None] | None = None,
) -> TerminalSummary:
    job_dir_resolver = _dependency(deps, job_dir_fn, "_job_dir")
    load_state = _dependency(deps, load_state_fn, "load_state")
    load_report_json = _dependency(deps, load_report_json_fn, "load_report_json")
    load_organized_ref = _dependency(deps, load_organized_ref_fn, "load_organized_ref")
    queue_entry_by_id = _dependency(deps, queue_entry_by_id_fn, "_queue_entry_by_id")
    job_dir = job_dir_resolver(entry)
    state = load_state(job_dir) or {}
    report = load_report_json(job_dir) or {}
    organized_ref = load_organized_ref(job_dir) or {}
    refreshed = queue_entry_by_id(queue_root, entry.queue_id)

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
    queue_root: Path,
    entry: Any,
    summary: TerminalSummary,
    *,
    deps: Any | None = None,
    queue_entry_by_id_fn: Callable[[Path, str], Any | None] | None = None,
    mark_completed_fn: Callable[..., Any] | None = None,
    mark_cancelled_fn: Callable[..., Any] | None = None,
    mark_failed_fn: Callable[..., Any] | None = None,
) -> None:
    queue_entry_by_id = _dependency(deps, queue_entry_by_id_fn, "_queue_entry_by_id")
    mark_completed = _dependency(deps, mark_completed_fn, "mark_completed")
    mark_cancelled = _dependency(deps, mark_cancelled_fn, "mark_cancelled")
    mark_failed = _dependency(deps, mark_failed_fn, "mark_failed")
    refreshed = queue_entry_by_id(queue_root, entry.queue_id)
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
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
    )


def resolve_terminal_dependencies(
    *,
    deps: Any | None = None,
    write_execution_artifacts_fn: Callable[..., Any] | None = None,
    selected_xyz_fn: Callable[[Any], Path] | None = None,
    job_dir_fn: Callable[[Any], Path] | None = None,
    mark_completed_fn: Callable[..., Any] | None = None,
    mark_cancelled_fn: Callable[..., Any] | None = None,
    mark_failed_fn: Callable[..., Any] | None = None,
    upsert_job_record_fn: Callable[..., Any] | None = None,
    notify_job_finished_fn: Callable[..., Any] | None = None,
) -> XtbTerminalDependencies:
    return XtbTerminalDependencies(
        write_execution_artifacts=_dependency(
            deps,
            write_execution_artifacts_fn,
            "_write_execution_artifacts",
        ),
        selected_xyz=_dependency(deps, selected_xyz_fn, "_selected_xyz"),
        job_dir=_dependency(deps, job_dir_fn, "_job_dir"),
        mark_completed=_dependency(deps, mark_completed_fn, "mark_completed"),
        mark_cancelled=_dependency(deps, mark_cancelled_fn, "mark_cancelled"),
        mark_failed=_dependency(deps, mark_failed_fn, "mark_failed"),
        upsert_job_record=_dependency(deps, upsert_job_record_fn, "upsert_job_record"),
        notify_job_finished=_dependency(
            deps,
            notify_job_finished_fn,
            "notify_job_finished",
        ),
    )


def _terminal_paths(
    request: XtbTerminalFinalizationRequest,
    dependencies: XtbTerminalDependencies,
) -> _XtbTerminalPaths:
    job_dir = dependencies.job_dir(request.entry)
    result_selected_xyz = str(request.result.selected_input_xyz).strip()
    selected_xyz = (
        Path(result_selected_xyz).expanduser().resolve()
        if result_selected_xyz
        else dependencies.selected_xyz(request.entry)
    )
    return _XtbTerminalPaths(job_dir=job_dir, selected_xyz=selected_xyz)


def _terminal_metadata(result: XtbRunResult) -> dict[str, Any]:
    return {
        "candidate_count": result.candidate_count,
        "job_type": result.job_type,
    }


def finalize_terminal_result(
    request: XtbTerminalFinalizationRequest,
    *,
    dependencies: XtbTerminalDependencies,
) -> Any:
    paths = _terminal_paths(request, dependencies)
    result = request.result
    entry = request.entry
    organized_target = ""

    def mark_queue_terminal() -> None:
        _queue_execution.mark_terminal_status(
            request.queue_root,
            entry.queue_id,
            status=result.status,
            reason=result.reason,
            metadata_update=_terminal_metadata(result),
            mark_completed_fn=dependencies.mark_completed,
            mark_cancelled_fn=dependencies.mark_cancelled,
            mark_failed_fn=dependencies.mark_failed,
        )

    def sync_job_record() -> str:
        dependencies.upsert_job_record(
            request.cfg,
            job_id=entry.task_id,
            status=result.status,
            job_dir=paths.job_dir,
            job_type=result.job_type,
            selected_input_xyz=str(paths.selected_xyz),
            reaction_key=result.reaction_key,
            resource_request=result.resource_request,
            resource_actual=result.resource_actual,
        )
        return organized_target

    def notify_finished(sync_result: str) -> None:
        dependencies.notify_job_finished(
            request.cfg,
            job_id=entry.task_id,
            queue_id=entry.queue_id,
            status=result.status,
            reason=result.reason,
            job_type=result.job_type,
            reaction_key=result.reaction_key,
            job_dir=paths.job_dir,
            selected_xyz=paths.selected_xyz,
            candidate_count=result.candidate_count,
            organized_output_dir=Path(sync_result) if sync_result else None,
            resource_request=result.resource_request,
            resource_actual=result.resource_actual,
        )

    def emit_terminal_summary(sync_result: str) -> None:
        print_terminal_summary(
            TerminalSummary(
                queue_id=entry.queue_id,
                job_id=entry.task_id,
                status=result.status,
                reason=result.reason,
                organized_output_dir=sync_result,
            )
        )

    return _engine_execution.sync_terminal_result(
        _engine_execution.TerminalSyncActions(
            write_artifacts=lambda: dependencies.write_execution_artifacts(
                entry,
                result,
                previous_state=request.previous_state,
                resumed=request.resumed,
            ),
            mark_queue_terminal=mark_queue_terminal,
            sync_job_record=sync_job_record,
            notify_finished=notify_finished,
            emit_output=emit_terminal_summary,
            build_outcome=lambda sync_result: request.outcome_cls(
                result=result,
                organized_output_dir=sync_result,
            ),
        ),
        emit_output=request.emit_output,
    )


def finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    outcome_cls: type,
    deps: Any | None = None,
    write_execution_artifacts_fn: Callable[..., Any] | None = None,
    selected_xyz_fn: Callable[[Any], Path] | None = None,
    job_dir_fn: Callable[[Any], Path] | None = None,
    mark_completed_fn: Callable[..., Any] | None = None,
    mark_cancelled_fn: Callable[..., Any] | None = None,
    mark_failed_fn: Callable[..., Any] | None = None,
    upsert_job_record_fn: Callable[..., Any] | None = None,
    notify_job_finished_fn: Callable[..., Any] | None = None,
) -> Any:
    return finalize_terminal_result(
        XtbTerminalFinalizationRequest(
            cfg=cfg,
            queue_root=queue_root,
            entry=entry,
            result=result,
            emit_output=emit_output,
            outcome_cls=outcome_cls,
            previous_state=previous_state,
            resumed=resumed,
        ),
        dependencies=resolve_terminal_dependencies(
            deps=deps,
            write_execution_artifacts_fn=write_execution_artifacts_fn,
            selected_xyz_fn=selected_xyz_fn,
            job_dir_fn=job_dir_fn,
            mark_completed_fn=mark_completed_fn,
            mark_cancelled_fn=mark_cancelled_fn,
            mark_failed_fn=mark_failed_fn,
            upsert_job_record_fn=upsert_job_record_fn,
            notify_job_finished_fn=notify_job_finished_fn,
        ),
    )
