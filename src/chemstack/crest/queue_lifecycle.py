from __future__ import annotations

from typing import Any, Callable

from chemstack.core.queue import lifecycle as _queue_lifecycle
from chemstack.core.queue.types import QueueStatus


shutdown_running_job = _queue_lifecycle.shutdown_running_job


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
    if current is not None and _queue_lifecycle.entry_status_is(current, QueueStatus.RUNNING):
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
    _queue_lifecycle.reconcile_orphaned_running(
        cfg,
        admission_root=admission_root,
        queue_roots_fn=queue_roots_fn,
        list_queue_fn=list_queue_fn,
        list_slots_fn=list_slots_fn,
        reconcile_stale_slots_fn=reconcile_stale_slots_fn,
        mark_cancelled_fn=mark_cancelled_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_fn=mark_recovery_pending_fn,
        reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries_fn,
    )


__all__ = [
    "finalize_child_exit",
    "reconcile_orphaned_running",
    "shutdown_running_job",
]
