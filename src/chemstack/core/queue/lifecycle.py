from __future__ import annotations

import logging
import subprocess
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable

from . import engine_admission as _engine_admission
from .child_process import (
    reconcile_orphaned_child_queue_entries,
    shutdown_child_process_with_grace,
    status_matches,
)
from .types import QueueStatus

LOGGER = logging.getLogger(__name__)


def entry_status_is(entry: Any, expected: Any) -> bool:
    return status_matches(getattr(entry, "status", None), expected)


def entry_status_is_running(entry: Any) -> bool:
    return entry_status_is(entry, QueueStatus.RUNNING)


@dataclass(frozen=True)
class ChildExitPolicy:
    shutdown_requested: bool = True
    fail_unexpected_exit: bool = False
    use_entry_fallback: bool = True
    recovery_entry_fn: Callable[[Any, Any], Any] | None = None


@dataclass(frozen=True)
class OrphanedRunningPolicy:
    recovery_reason: str = "crashed_recovery"


@dataclass(frozen=True)
class EngineQueueTerminalSideEffectHooks:
    upsert_terminal_job_record_fn: Callable[..., Any]
    notify_terminal_job_from_state_fn: Callable[[Any, str], bool]


@dataclass(frozen=True)
class EngineQueueProcessLifecycleHooks:
    queue_entry_id_fn: Callable[[Any], str]
    queue_entry_app_name_fn: Callable[[Any], str]
    queue_entry_task_id_fn: Callable[[Any], str | None]
    update_slot_metadata_fn: Callable[..., Any]
    terminate_process_fn: Callable[[Any], Any]
    mark_failed_fn: Callable[..., Any]
    upsert_running_job_record_fn: Callable[[Any, Any], Any]
    get_run_id_from_state_fn: Callable[[str], str | None]
    get_cancel_requested_fn: Callable[..., bool]
    mark_cancelled_fn: Callable[..., Any]
    mark_completed_fn: Callable[..., Any]
    upsert_terminal_job_record_fn: Callable[..., Any]
    notify_terminal_job_from_state_fn: Callable[[Any, str], bool]
    on_completed_fn: Callable[[Any, Any], Any] | None = None
    terminal_side_effect_hooks: EngineQueueTerminalSideEffectHooks | None = None


@dataclass(frozen=True)
class EngineQueueProcessReconcileHooks:
    queue_roots_fn: Callable[[Any], tuple[Any, ...]]
    reconcile_stale_slots_fn: Callable[[Any], Any]
    reconcile_orphaned_running_entries_fn: Callable[..., Any]
    reconcile_orphaned_running_entries_kwargs: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class EngineQueueProcessShutdownHooks:
    terminate_process_fn: Callable[[Any], Any]
    requeue_running_entry_fn: Callable[..., Any]


def job_queue_root(worker: Any, job: Any) -> Path:
    return Path(getattr(job, "queue_root", worker.allowed_root)).expanduser().resolve()


def resolved_job_queue_root(worker: Any, job: Any) -> Path:
    return job_queue_root(worker, job)


def attach_started_process_metadata(
    worker: Any,
    queue_root: Any,
    entry: Any,
    *,
    process: Any,
    admission_token: str,
    hooks: EngineQueueProcessLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> bool:
    return _engine_admission.attach_started_process_metadata(
        admission_root=worker.admission_root,
        queue_root=queue_root,
        entry=entry,
        process=process,
        admission_token=admission_token,
        queue_entry_id_fn=hooks.queue_entry_id_fn,
        queue_entry_app_name_fn=hooks.queue_entry_app_name_fn,
        queue_entry_task_id_fn=hooks.queue_entry_task_id_fn,
        update_slot_metadata_fn=hooks.update_slot_metadata_fn,
        terminate_process_fn=hooks.terminate_process_fn,
        mark_entry_failed_and_release_fn=worker._mark_entry_failed_and_release,
        mark_failed_fn=hooks.mark_failed_fn,
        cfg=worker.cfg,
        upsert_running_job_record_fn=hooks.upsert_running_job_record_fn,
        logger=logger,
    )


def mark_terminal_process_queue_entry(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    rc: int,
    hooks: EngineQueueProcessLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    queue_root = job_queue_root(worker, job)
    run_id = hooks.get_run_id_from_state_fn(job.reaction_dir)
    if hooks.get_cancel_requested_fn(queue_root, queue_id):
        logger.info("Job cancelled: %s (rc=%d)", queue_id, rc)
        hooks.mark_cancelled_fn(queue_root, queue_id)
    elif rc == 0:
        logger.info("Job completed: %s (rc=%d)", queue_id, rc)
        hooks.mark_completed_fn(queue_root, queue_id, run_id=run_id)
        if hooks.on_completed_fn is not None:
            hooks.on_completed_fn(worker, job)
    else:
        logger.warning("Job failed: %s (rc=%d)", queue_id, rc)
        hooks.mark_failed_fn(
            queue_root,
            queue_id,
            error=f"exit_code={rc}",
            run_id=run_id,
        )


def run_terminal_process_side_effects(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: EngineQueueTerminalSideEffectHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    try:
        hooks.upsert_terminal_job_record_fn(
            worker.cfg,
            job.reaction_dir,
            fallback_job_id=getattr(job, "task_id", None),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to update terminal job location for %s: %s", queue_id, exc)
    try:
        hooks.notify_terminal_job_from_state_fn(worker.cfg, job.reaction_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to send terminal notification for %s: %s", queue_id, exc)


def record_terminal_process_side_effects(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: EngineQueueProcessLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    run_terminal_process_side_effects(
        worker,
        queue_id,
        job,
        hooks=hooks.terminal_side_effect_hooks
        or EngineQueueTerminalSideEffectHooks(
            upsert_terminal_job_record_fn=hooks.upsert_terminal_job_record_fn,
            notify_terminal_job_from_state_fn=hooks.notify_terminal_job_from_state_fn,
        ),
        logger=logger,
    )


def finalize_process_finished_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    rc: int,
    hooks: EngineQueueProcessLifecycleHooks,
) -> None:
    mark_terminal_process_queue_entry(worker, queue_id, job, rc=rc, hooks=hooks)
    record_terminal_process_side_effects(worker, queue_id, job, hooks=hooks)
    worker._release_admission_slot(job.admission_token)


def cancel_running_process_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: EngineQueueProcessLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    logger.info("Cancelling running job: %s", queue_id)
    hooks.terminate_process_fn(job.process)
    with suppress(subprocess.TimeoutExpired):
        job.process.wait(timeout=5)
    hooks.mark_cancelled_fn(job_queue_root(worker, job), queue_id)
    worker._release_admission_slot(job.admission_token)


def reconcile_orphaned_process_entries(
    worker: Any,
    *,
    hooks: EngineQueueProcessReconcileHooks,
) -> None:
    hooks.reconcile_stale_slots_fn(worker.admission_root)
    kwargs = dict(hooks.reconcile_orphaned_running_entries_kwargs or {})
    for queue_root in hooks.queue_roots_fn(worker.cfg):
        hooks.reconcile_orphaned_running_entries_fn(queue_root, **kwargs)


def shutdown_running_process_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: EngineQueueProcessShutdownHooks,
) -> None:
    hooks.terminate_process_fn(job.process)
    hooks.requeue_running_entry_fn(job_queue_root(worker, job), queue_id)
    worker._release_admission_slot(job.admission_token)


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
    recovery_entry_fn: Callable[[Any, Any], Any] | None = None,
) -> None:
    root = job.queue_root
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
    recovery_reason: str = "crashed_recovery",
    reconcile_orphaned_child_queue_entries_fn: Callable[
        ..., Any
    ] = reconcile_orphaned_child_queue_entries,
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
            root,
            queue_id,
            **kwargs,
        ),
        requeue_running_entry_fn=lambda root, queue_id: requeue_running_entry_fn(
            root,
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
        recovery_reason=policy.recovery_reason,
        reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries_fn,
    )


__all__ = [
    "ChildExitPolicy",
    "EngineQueueProcessLifecycleHooks",
    "EngineQueueProcessReconcileHooks",
    "EngineQueueProcessShutdownHooks",
    "EngineQueueTerminalSideEffectHooks",
    "OrphanedRunningPolicy",
    "attach_started_process_metadata",
    "cancel_running_process_job",
    "entry_status_is",
    "entry_status_is_running",
    "finalize_child_exit_with_policy",
    "finalize_child_worker_exit",
    "finalize_process_finished_job",
    "job_queue_root",
    "live_worker_pid_slots",
    "mark_terminal_process_queue_entry",
    "reconcile_orphaned_process_entries",
    "reconcile_orphaned_running",
    "reconcile_orphaned_running_with_policy",
    "record_terminal_process_side_effects",
    "request_pending_cancellations",
    "resolved_job_queue_root",
    "run_terminal_process_side_effects",
    "shutdown_running_job",
    "shutdown_running_process_job",
    "sync_terminal_running_entries",
]
