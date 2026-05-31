from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Callable, Iterable

from .child_process import (
    reconcile_orphaned_child_queue_entries,
    shutdown_child_process_with_grace,
    status_matches,
)
from .types import QueueStatus


def entry_status_is(entry: Any, expected: Any) -> bool:
    return status_matches(getattr(entry, "status", None), expected)


def entry_status_is_running(entry: Any) -> bool:
    return entry_status_is(entry, QueueStatus.RUNNING)


@dataclass(frozen=True)
class ChildExitPolicy:
    shutdown_requested: bool = True
    fail_unexpected_exit: bool = False
    use_entry_fallback: bool = True
    coerce_root_to_str: bool = False
    recovery_entry_fn: Callable[[Any, Any], Any] | None = None


@dataclass(frozen=True)
class OrphanedRunningPolicy:
    coerce_root_to_str: bool = False
    recovery_reason: str = "crashed_recovery"


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


def shutdown_running_job(
    job: Any,
    *,
    terminate_process_fn: Callable[[Any], Any],
    finalize_child_exit_fn: Callable[[Any, int], Any],
    grace_seconds: float,
    sleep_fn: Callable[[float], None],
    shutdown_child_process_with_grace_fn: Callable[..., Any] = shutdown_child_process_with_grace,
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


def finalize_child_worker_exit(
    cfg: Any,
    job: Any,
    *,
    find_queue_entry_fn: Callable[[Any, str], Any | None],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    release_admission_slot_fn: Callable[[str], Any],
    mark_failed_fn: Callable[..., Any] | None = None,
    rc: int | None = None,
    shutdown_requested: bool = True,
    fail_unexpected_exit: bool = False,
    use_entry_fallback: bool = True,
    coerce_root_to_str: bool = False,
    recovery_entry_fn: Callable[[Any, Any], Any] | None = None,
) -> None:
    root = str(job.queue_root) if coerce_root_to_str else job.queue_root
    current = find_queue_entry_fn(job.queue_root, job.entry.queue_id)
    if current is None and use_entry_fallback:
        current = job.entry

    if current is not None and entry_status_is_running(current):
        queue_id = current.queue_id
        if getattr(current, "cancel_requested", False):
            mark_cancelled_fn(root, queue_id, error="cancel_requested")
        elif shutdown_requested:
            requeue_running_entry_fn(root, queue_id)
            recovery_entry = (
                recovery_entry_fn(current, job) if recovery_entry_fn is not None else current
            )
            mark_recovery_pending_fn(cfg, recovery_entry, reason="worker_shutdown")
        elif fail_unexpected_exit and mark_failed_fn is not None:
            mark_failed_fn(root, queue_id, error=f"worker_child_exit_code={int(rc or 0)}")

    release_admission_slot_fn(job.admission_token)


def finalize_child_exit_with_policy(
    cfg: Any,
    job: Any,
    *,
    policy: ChildExitPolicy,
    find_queue_entry_fn: Callable[[Any, str], Any | None],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    release_admission_slot_fn: Callable[[str], Any],
    mark_failed_fn: Callable[..., Any] | None = None,
    rc: int | None = None,
) -> None:
    finalize_child_worker_exit(
        cfg,
        job,
        find_queue_entry_fn=find_queue_entry_fn,
        mark_cancelled_fn=mark_cancelled_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_fn=mark_recovery_pending_fn,
        release_admission_slot_fn=release_admission_slot_fn,
        mark_failed_fn=mark_failed_fn,
        rc=rc,
        shutdown_requested=policy.shutdown_requested,
        fail_unexpected_exit=policy.fail_unexpected_exit,
        use_entry_fallback=policy.use_entry_fallback,
        coerce_root_to_str=policy.coerce_root_to_str,
        recovery_entry_fn=policy.recovery_entry_fn,
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
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    coerce_root_to_str: bool = False,
    recovery_reason: str = "crashed_recovery",
    reconcile_orphaned_child_queue_entries_fn: Callable[
        ..., Any
    ] = reconcile_orphaned_child_queue_entries,
) -> None:
    def _root(root: Any) -> Any:
        return str(root) if coerce_root_to_str else root

    reconcile_orphaned_child_queue_entries_fn(
        cfg,
        admission_root=admission_root,
        queue_roots_fn=queue_roots_fn,
        list_queue_fn=list_queue_fn,
        list_slots_fn=list_slots_fn,
        reconcile_stale_slots_fn=reconcile_stale_slots_fn,
        running_status=QueueStatus.RUNNING,
        mark_cancelled_fn=lambda root, queue_id, **kwargs: mark_cancelled_fn(
            _root(root),
            queue_id,
            **kwargs,
        ),
        requeue_running_entry_fn=lambda root, queue_id: requeue_running_entry_fn(
            _root(root),
            queue_id,
        ),
        mark_recovery_pending_fn=lambda cfg_obj, entry: mark_recovery_pending_fn(
            cfg_obj,
            entry,
            reason=recovery_reason,
        ),
    )


def reconcile_orphaned_running_with_policy(
    cfg: Any,
    *,
    policy: OrphanedRunningPolicy,
    admission_root: Any,
    queue_roots_fn: Callable[[Any], tuple[Any, ...]],
    list_queue_fn: Callable[[Any], list[Any]],
    list_slots_fn: Callable[[Any], list[Any]],
    reconcile_stale_slots_fn: Callable[[Any], Any],
    mark_cancelled_fn: Callable[..., Any],
    requeue_running_entry_fn: Callable[..., Any],
    mark_recovery_pending_fn: Callable[..., Any],
    reconcile_orphaned_child_queue_entries_fn: Callable[
        ..., Any
    ] = reconcile_orphaned_child_queue_entries,
) -> None:
    reconcile_orphaned_running(
        cfg,
        admission_root=admission_root,
        queue_roots_fn=queue_roots_fn,
        list_queue_fn=list_queue_fn,
        list_slots_fn=list_slots_fn,
        reconcile_stale_slots_fn=reconcile_stale_slots_fn,
        mark_cancelled_fn=mark_cancelled_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_fn=mark_recovery_pending_fn,
        coerce_root_to_str=policy.coerce_root_to_str,
        recovery_reason=policy.recovery_reason,
        reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries_fn,
    )


__all__ = [
    "ChildExitPolicy",
    "OrphanedRunningPolicy",
    "entry_status_is",
    "entry_status_is_running",
    "finalize_child_exit_with_policy",
    "finalize_child_worker_exit",
    "live_worker_pid_slots",
    "reconcile_orphaned_running",
    "reconcile_orphaned_running_with_policy",
    "request_pending_cancellations",
    "shutdown_running_job",
    "sync_terminal_running_entries",
]
