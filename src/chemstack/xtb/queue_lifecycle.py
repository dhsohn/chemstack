from __future__ import annotations

from typing import Any, Callable

from chemstack.core.queue import lifecycle as _queue_lifecycle


entry_status_is_running = _queue_lifecycle.entry_status_is_running
live_worker_pid_slots = _queue_lifecycle.live_worker_pid_slots
request_pending_cancellations = _queue_lifecycle.request_pending_cancellations
shutdown_running_job = _queue_lifecycle.shutdown_running_job
sync_terminal_running_entries = _queue_lifecycle.sync_terminal_running_entries


def finalize_child_exit(
    cfg: Any,
    job: Any,
    *,
    queue_entry_by_id_fn: Callable[[Any, str], Any | None],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    release_admission_slot_fn: Callable[[str], Any],
) -> None:
    current = queue_entry_by_id_fn(job.queue_root, job.entry.queue_id) or job.entry
    if current is not None and entry_status_is_running(current):
        if getattr(current, "cancel_requested", False):
            mark_cancelled_fn(str(job.queue_root), current.queue_id, error="cancel_requested")
        else:
            requeue_running_entry_fn(str(job.queue_root), current.queue_id)
            mark_recovery_pending_fn(cfg, current, reason="worker_shutdown")
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
        coerce_root_to_str=True,
        reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries_fn,
    )


__all__ = [
    "entry_status_is_running",
    "finalize_child_exit",
    "live_worker_pid_slots",
    "reconcile_orphaned_running",
    "request_pending_cancellations",
    "shutdown_running_job",
    "sync_terminal_running_entries",
]
