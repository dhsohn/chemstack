from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from chemstack.core.queue.lifecycle import (
    EngineQueueProcessLifecycleHooks,
    EngineQueueProcessReconcileHooks,
    EngineQueueProcessShutdownHooks,
    EngineQueueTerminalSideEffectHooks,
    reconcile_orphaned_process_entries,
    shutdown_running_process_job,
)


@dataclass(frozen=True)
class OrcaQueueWorkerLifecycleCallbacks:
    queue_entry_id: Callable[[Any], str]
    queue_entry_app_name: Callable[[Any], str]
    queue_entry_task_id: Callable[[Any], str | None]
    update_slot_metadata: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    mark_failed: Callable[..., Any]
    upsert_running_job_record: Callable[[Any, Any], Any]
    get_run_id_from_state: Callable[[str], str | None]
    get_cancel_requested: Callable[..., bool]
    mark_cancelled: Callable[..., Any]
    mark_completed: Callable[..., Any]
    upsert_terminal_job_record: Callable[..., Any]
    notify_terminal_job_from_state: Callable[[Any, str], bool]
    on_completed: Callable[[Any, Any], Any] | None
    queue_roots: Callable[[Any], tuple[Any, ...]]
    reconcile_stale_slots: Callable[[Any], Any]
    reconcile_orphaned_running_entries: Callable[..., Any]
    requeue_running_entry: Callable[..., Any]


def lifecycle_callbacks_from_namespace(namespace: Any) -> OrcaQueueWorkerLifecycleCallbacks:
    return OrcaQueueWorkerLifecycleCallbacks(
        queue_entry_id=namespace.queue_entry_id,
        queue_entry_app_name=namespace.queue_entry_app_name,
        queue_entry_task_id=namespace.queue_entry_task_id,
        update_slot_metadata=namespace.update_slot_metadata,
        terminate_process=namespace._terminate_process,
        mark_failed=namespace.mark_failed,
        upsert_running_job_record=namespace._upsert_running_job_record,
        get_run_id_from_state=namespace._get_run_id_from_state,
        get_cancel_requested=namespace.get_cancel_requested,
        mark_cancelled=namespace.mark_cancelled,
        mark_completed=namespace.mark_completed,
        upsert_terminal_job_record=namespace._upsert_terminal_job_record,
        notify_terminal_job_from_state=namespace._notify_terminal_job_from_state,
        on_completed=lambda worker, job: worker._auto_organize_terminal_job(job),
        queue_roots=namespace.queue_roots,
        reconcile_stale_slots=namespace.reconcile_stale_slots,
        reconcile_orphaned_running_entries=namespace.reconcile_orphaned_running_entries,
        requeue_running_entry=namespace.requeue_running_entry,
    )


def build_orca_worker_lifecycle_hooks(
    callbacks: OrcaQueueWorkerLifecycleCallbacks,
) -> EngineQueueProcessLifecycleHooks:
    return EngineQueueProcessLifecycleHooks(
        queue_entry_id_fn=callbacks.queue_entry_id,
        queue_entry_app_name_fn=callbacks.queue_entry_app_name,
        queue_entry_task_id_fn=callbacks.queue_entry_task_id,
        update_slot_metadata_fn=callbacks.update_slot_metadata,
        terminate_process_fn=callbacks.terminate_process,
        mark_failed_fn=callbacks.mark_failed,
        upsert_running_job_record_fn=callbacks.upsert_running_job_record,
        get_run_id_from_state_fn=callbacks.get_run_id_from_state,
        get_cancel_requested_fn=callbacks.get_cancel_requested,
        mark_cancelled_fn=callbacks.mark_cancelled,
        mark_completed_fn=callbacks.mark_completed,
        upsert_terminal_job_record_fn=callbacks.upsert_terminal_job_record,
        notify_terminal_job_from_state_fn=callbacks.notify_terminal_job_from_state,
        on_completed_fn=callbacks.on_completed,
        terminal_side_effect_hooks=EngineQueueTerminalSideEffectHooks(
            upsert_terminal_job_record_fn=callbacks.upsert_terminal_job_record,
            notify_terminal_job_from_state_fn=callbacks.notify_terminal_job_from_state,
        ),
    )


def reconcile_orphaned_running(
    worker: Any,
    *,
    callbacks: OrcaQueueWorkerLifecycleCallbacks,
) -> None:
    """Fix queue entries stuck as running from a previous worker crash."""
    reconcile_orphaned_process_entries(
        worker,
        hooks=EngineQueueProcessReconcileHooks(
            queue_roots_fn=callbacks.queue_roots,
            reconcile_stale_slots_fn=callbacks.reconcile_stale_slots,
            reconcile_orphaned_running_entries_fn=callbacks.reconcile_orphaned_running_entries,
            reconcile_orphaned_running_entries_kwargs={"ignore_worker_pid": True},
        ),
    )


def shutdown_running_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    callbacks: OrcaQueueWorkerLifecycleCallbacks,
) -> None:
    shutdown_running_process_job(
        worker,
        queue_id,
        job,
        hooks=EngineQueueProcessShutdownHooks(
            terminate_process_fn=callbacks.terminate_process,
            requeue_running_entry_fn=callbacks.requeue_running_entry,
        ),
    )


__all__ = [
    "OrcaQueueWorkerLifecycleCallbacks",
    "build_orca_worker_lifecycle_hooks",
    "lifecycle_callbacks_from_namespace",
    "reconcile_orphaned_running",
    "shutdown_running_job",
]
