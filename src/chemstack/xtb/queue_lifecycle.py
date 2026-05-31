from __future__ import annotations

from typing import Any, Callable

from chemstack.core.queue import lifecycle as _queue_lifecycle
from chemstack.core.queue.internal_engine import InternalEngineSpec


entry_status_is_running = _queue_lifecycle.entry_status_is_running
live_worker_pid_slots = _queue_lifecycle.live_worker_pid_slots
shutdown_running_job = _queue_lifecycle.shutdown_running_job
sync_terminal_running_entries = _queue_lifecycle.sync_terminal_running_entries

_ENGINE_LIFECYCLE = InternalEngineSpec(
    engine="xtb",
    coerce_queue_root_to_str=True,
).lifecycle()


def finalize_child_exit(
    cfg: Any,
    job: Any,
    *,
    rc: int,
    shutdown_requested: bool,
    queue_entry_by_id_fn: Callable[[Any, str], Any | None],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    release_admission_slot_fn: Callable[[str], Any],
) -> None:
    _ENGINE_LIFECYCLE.finalize_child_exit(
        cfg,
        job,
        shutdown_requested=shutdown_requested,
        find_queue_entry_fn=queue_entry_by_id_fn,
        mark_cancelled_fn=mark_cancelled_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_fn=mark_recovery_pending_fn,
        release_admission_slot_fn=release_admission_slot_fn,
        mark_failed_fn=mark_failed_fn,
        rc=rc,
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
    _ENGINE_LIFECYCLE.reconcile_orphaned_running(
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
    "entry_status_is_running",
    "finalize_child_exit",
    "live_worker_pid_slots",
    "reconcile_orphaned_running",
    "shutdown_running_job",
    "sync_terminal_running_entries",
]
