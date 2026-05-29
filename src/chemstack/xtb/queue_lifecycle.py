from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Callable, Iterable

from chemstack.core.queue.types import QueueStatus


def entry_status_is_running(entry: Any) -> bool:
    status = getattr(entry, "status", None)
    value = getattr(status, "value", status)
    return str(value).strip().lower() == QueueStatus.RUNNING.value


def request_pending_cancellations(
    running_jobs: Iterable[tuple[str, Any]],
    *,
    get_cancel_requested_fn: Callable[[str, str], bool],
    request_job_cancellation_fn: Callable[[Any], Any],
) -> None:
    for _queue_id, job in running_jobs:
        if job.cancel_requested:
            continue
        if get_cancel_requested_fn(str(job.queue_root), job.entry.queue_id):
            request_job_cancellation_fn(job.process)
            job.cancel_requested = True


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


def sync_terminal_running_entries(
    queue_entries: Iterable[tuple[Any, Any]],
    *,
    load_terminal_summary_fn: Callable[..., Any],
    ensure_terminal_queue_status_fn: Callable[..., Any],
) -> None:
    for queue_root, entry in queue_entries:
        if not entry_status_is_running(entry):
            continue
        summary = load_terminal_summary_fn(queue_root, entry)
        if summary.status in {"completed", "failed", "cancelled"}:
            ensure_terminal_queue_status_fn(queue_root, entry, summary)


def live_worker_pid_slots(
    queue_entries: Iterable[tuple[Any, Any]],
    *,
    load_state_fn: Callable[[Any], dict[str, Any] | None],
    job_dir_fn: Callable[[Any], Any],
    pid_is_alive_fn: Callable[[int], bool],
) -> list[Any]:
    slots: list[Any] = []
    for _queue_root, entry in queue_entries:
        if not entry_status_is_running(entry):
            continue
        state = load_state_fn(job_dir_fn(entry)) or {}
        try:
            worker_job_pid = int(state.get("worker_job_pid", 0) or 0)
        except (TypeError, ValueError):
            continue
        if worker_job_pid and pid_is_alive_fn(worker_job_pid):
            slots.append(SimpleNamespace(queue_id=entry.queue_id))
    return slots


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
        mark_cancelled_fn=lambda root, queue_id, **kwargs: mark_cancelled_fn(
            str(root),
            queue_id,
            **kwargs,
        ),
        requeue_running_entry_fn=lambda root, queue_id: requeue_running_entry_fn(
            str(root),
            queue_id,
        ),
        mark_recovery_pending_fn=lambda cfg_obj, entry: mark_recovery_pending_fn(
            cfg_obj,
            entry,
            reason="crashed_recovery",
        ),
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
