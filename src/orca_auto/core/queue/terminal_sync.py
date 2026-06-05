from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TerminalSyncActions:
    write_artifacts: Callable[[], Any]
    mark_queue_terminal: Callable[[], Any]
    sync_job_record: Callable[[], Any]
    notify_finished: Callable[[Any], Any]
    build_outcome: Callable[[Any], Any]
    emit_output: Callable[[Any], Any] | None = None


def sync_terminal_result(
    actions: TerminalSyncActions,
    *,
    emit_output: bool = False,
) -> Any:
    actions.write_artifacts()
    actions.mark_queue_terminal()
    sync_result = actions.sync_job_record()
    actions.notify_finished(sync_result)
    if emit_output and actions.emit_output is not None:
        actions.emit_output(sync_result)
    return actions.build_outcome(sync_result)


def mark_result_terminal_status(
    queue_root: str | Path,
    queue_id: str,
    result: Any,
    *,
    metadata_update: dict[str, Any] | None,
    mark_terminal_status_fn: Callable[..., Any],
    mark_completed_fn: Callable[..., Any],
    mark_cancelled_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
) -> None:
    mark_terminal_status_fn(
        queue_root,
        queue_id,
        status=result.status,
        reason=result.reason,
        metadata_update=metadata_update,
        mark_completed_fn=mark_completed_fn,
        mark_cancelled_fn=mark_cancelled_fn,
        mark_failed_fn=mark_failed_fn,
    )


def mark_engine_job_running(
    cfg: Any,
    *,
    entry: Any,
    job_dir: Path,
    selected_xyz: Path,
    resource_request: dict[str, int],
    write_running_state_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    notify_job_started_fn: Callable[..., Any],
    record_fields: dict[str, Any] | None = None,
    notify_fields: dict[str, Any] | None = None,
    write_running_state_kwargs: dict[str, Any] | None = None,
) -> None:
    write_running_state_fn(cfg, entry, **dict(write_running_state_kwargs or {}))
    upsert_job_record_fn(
        cfg,
        job_id=entry.task_id,
        status="running",
        job_dir=job_dir,
        selected_input_xyz=str(selected_xyz),
        **dict(record_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )
    notify_job_started_fn(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        **dict(notify_fields or {}),
    )


def mark_recovery_pending_and_record(
    cfg: Any,
    *,
    entry: Any,
    job_dir: Path,
    selected_input_xyz: Path | str,
    reason: str,
    resource_request: dict[str, int],
    mark_recovery_pending_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    state_identity_fields: dict[str, Any] | None = None,
    record_identity_fields: dict[str, Any] | None = None,
) -> None:
    selected_input_xyz_text = str(selected_input_xyz)
    mark_recovery_pending_fn(
        job_dir,
        job_id=str(entry.task_id),
        selected_input_xyz=selected_input_xyz_text,
        **dict(state_identity_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
        reason=reason,
    )
    upsert_job_record_fn(
        cfg,
        job_id=entry.task_id,
        status="pending",
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz_text,
        **dict(record_identity_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )


__all__ = [
    "TerminalSyncActions",
    "mark_engine_job_running",
    "mark_recovery_pending_and_record",
    "mark_result_terminal_status",
    "sync_terminal_result",
]
