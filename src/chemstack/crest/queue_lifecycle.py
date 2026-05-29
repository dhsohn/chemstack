from __future__ import annotations

from typing import Any, Callable

from chemstack.core.queue.types import QueueStatus


def finalize_child_exit(
    cfg: Any,
    job: Any,
    *,
    rc: int,
    shutdown_requested: bool,
    find_queue_entry_fn: Callable[[Any, str], Any | None],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    release_admission_slot_fn: Callable[[str], Any],
) -> None:
    current = find_queue_entry_fn(job.queue_root, job.entry.queue_id)
    if current is not None and getattr(current, "status", None) == QueueStatus.RUNNING:
        if shutdown_requested:
            if getattr(current, "cancel_requested", False):
                mark_cancelled_fn(job.queue_root, current.queue_id, error="cancel_requested")
            else:
                requeue_running_entry_fn(job.queue_root, current.queue_id)
                mark_recovery_pending_fn(cfg, job.entry, reason="worker_shutdown")
        elif getattr(current, "cancel_requested", False):
            mark_cancelled_fn(job.queue_root, current.queue_id, error="cancel_requested")
        else:
            mark_failed_fn(job.queue_root, current.queue_id, error=f"worker_child_exit_code={rc}")
    release_admission_slot_fn(job.admission_token)


def shutdown_running_job(
    job: Any,
    *,
    shutdown_child_process_with_grace_fn: Callable[..., Any],
    terminate_process_fn: Callable[[Any], Any],
    finalize_child_exit_fn: Callable[[Any, int], Any],
    grace_seconds: float,
    sleep_fn: Callable[[float], None],
) -> None:
    shutdown_child_process_with_grace_fn(
        job,
        terminate_process_fn=terminate_process_fn,
        finalize_child_exit_fn=lambda current_job, rc: finalize_child_exit_fn(
            current_job,
            rc,
        ),
        grace_seconds=grace_seconds,
        sleep_fn=sleep_fn,
    )


def reconcile_orphaned_running(
    cfg: Any,
    *,
    admission_root: Any,
    queue_roots_fn: Callable[[Any], tuple[Any, ...]],
    list_queue_fn: Callable[[Any], list[Any]],
    list_slots_fn: Callable[[Any], list[Any]],
    reconcile_stale_slots_fn: Callable[[Any], Any],
    reconcile_orphaned_child_queue_entries_fn: Callable[..., Any],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
) -> None:
    reconcile_orphaned_child_queue_entries_fn(
        cfg,
        admission_root=admission_root,
        queue_roots_fn=queue_roots_fn,
        list_queue_fn=list_queue_fn,
        list_slots_fn=list_slots_fn,
        reconcile_stale_slots_fn=reconcile_stale_slots_fn,
        running_status=QueueStatus.RUNNING,
        mark_cancelled_fn=mark_cancelled_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_fn=lambda cfg_obj, entry: mark_recovery_pending_fn(
            cfg_obj,
            entry,
            reason="crashed_recovery",
        ),
    )


__all__ = [
    "finalize_child_exit",
    "reconcile_orphaned_running",
    "shutdown_running_job",
]
