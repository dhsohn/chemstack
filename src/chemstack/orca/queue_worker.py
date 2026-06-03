"""Queue worker foreground loop for queue execution under an external supervisor.

This engine worker is launched by the unified ChemStack worker service under
systemd. Each job is spawned in a dedicated child process so locking, state
management, and signal handling remain centralized.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
    update_slot_metadata,
)
from chemstack.core.engines.orca_execution import (
    WORKER_JOB_MODULE,
    BackgroundRunJobProcess,
    build_worker_child_command,
)
from chemstack.core.engines.queue_worker import EngineQueueWorker
from chemstack.core.indexing.roots import runtime_roots_for_cfg as _runtime_roots_for_cfg
from chemstack.core.queue.engine_execution import coerce_resource_request
from chemstack.core.queue.internal_engine import (
    InternalEngineQueueModule,
    InternalEngineSpec,
)
from chemstack.core.queue.lifecycle import (
    EngineQueueProcessLifecycleHooks,
    EngineQueueProcessReconcileHooks,
    EngineQueueProcessShutdownHooks,
    EngineQueueTerminalSideEffectHooks,
    cancel_running_process_job,
    finalize_process_finished_job,
    reconcile_orphaned_process_entries,
    shutdown_running_process_job,
)
from chemstack.core.queue.lifecycle import (
    job_queue_root as _lifecycle_job_queue_root,
)
from chemstack.core.queue.types import QueueEntry
from chemstack.core.queue.worker import (
    EngineRunningJob as _RunningJob,
)
from chemstack.core.queue.worker import (
    ManagedProcess as _ManagedProcess,
)
from chemstack.core.queue.worker import (
    resolve_admission_limit as _resolve_worker_admission_limit,
)
from chemstack.core.queue.worker import (
    start_background_process,
    terminate_process_group,
)

from . import queue_worker_lifecycle as _lifecycle_helpers
from . import queue_worker_tracking as _tracking_helpers
from .attempt_reporting import (
    build_run_finished_notification,
    finished_notification_already_sent,
    mark_finished_notification_sent,
)
from .config import AppConfig, load_config
from .inp_rewriter import read_resource_request_from_input
from .input_artifacts import selected_input_artifacts
from .job_locations import (
    record_from_artifacts,
    resolve_job_metadata,
    resource_dict,
    upsert_job_record,
)
from .queue_adapter import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    queue_entry_app_name,
    queue_entry_id,
    queue_entry_metadata,
    queue_entry_reaction_dir,
    queue_entry_task_id,
    reconcile_orphaned_running_entries,
    requeue_running_entry,
)
from .queue_worker_deps import (
    OrcaQueueWorkerFacadeCallbacks,
    build_orca_runtime_facade_deps,
)
from .queue_worker_runtime_facade import build_orca_queue_worker_runtime_facade_deps
from .state import load_organized_ref, load_report_json, load_state
from .telegram_notifier import notify_run_finished_event

# Preserve historical facade attributes for external imports and monkeypatching.
_LEGACY_COMPAT_EXPORTS = (
    EngineQueueProcessReconcileHooks,
    EngineQueueProcessShutdownHooks,
    EngineQueueTerminalSideEffectHooks,
    OrcaQueueWorkerFacadeCallbacks,
    activate_reserved_slot,
    build_orca_runtime_facade_deps,
    build_run_finished_notification,
    coerce_resource_request,
    finished_notification_already_sent,
    list_slots,
    load_organized_ref,
    load_report_json,
    load_state,
    mark_cancelled,
    mark_completed,
    mark_finished_notification_sent,
    notify_run_finished_event,
    queue_entry_app_name,
    queue_entry_metadata,
    reconcile_orphaned_process_entries,
    reconcile_orphaned_running_entries,
    reconcile_stale_slots,
    record_from_artifacts,
    release_slot,
    requeue_running_entry,
    resolve_job_metadata,
    resource_dict,
    selected_input_artifacts,
    shutdown_running_process_job,
    upsert_job_record,
    update_slot_metadata,
    read_resource_request_from_input,
)

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT = 4
POLL_INTERVAL_SECONDS = 5
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0

# PID file for the daemon
WORKER_PID_FILE = "queue_worker.pid"
_ENGINE_SPEC = InternalEngineSpec(
    engine="orca",
    worker_job_module=WORKER_JOB_MODULE,
    worker_pid_file_name=WORKER_PID_FILE,
)
_ENGINE_ADMISSION = _ENGINE_SPEC.admission()


def _runtime_facade_deps() -> Any:
    return build_orca_queue_worker_runtime_facade_deps(
        sys.modules[__name__],
        time_module=time,
    )


_queue_module = InternalEngineQueueModule.create(
    spec=_ENGINE_SPEC,
    load_config=load_config,
    runtime_roots_for_cfg=lambda cfg: _runtime_roots_for_cfg(cfg, engine="orca"),
    list_queue=lambda root: list_queue(Path(root)),
    dequeue_next=lambda root: dequeue_next(root),
    poll_interval_seconds=POLL_INTERVAL_SECONDS,
    shutdown_grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
    deps=_runtime_facade_deps(),
)
_engine_runtime = _queue_module.runtime


def queue_roots(cfg: AppConfig) -> tuple[Path, ...]:
    return _queue_module.queue_roots(cfg)


def queue_entries_with_roots(cfg: AppConfig) -> list[tuple[Path, Any]]:
    return _queue_module.queue_entries_with_roots(cfg)


def _queue_worker_deps() -> Any:
    return _queue_module.queue_worker_deps()


def _admission_root_for_cfg(cfg: AppConfig) -> str:
    return _queue_module.admission_root(cfg)


def _dequeue_next_entry(cfg: AppConfig) -> tuple[Path, QueueEntry] | None:
    return _queue_module.dequeue_next_entry(cfg)


def _reserve_orca_worker_slot(root: str | Path, limit: int, **kwargs: Any) -> str | None:
    slot_kwargs = dict(kwargs)
    slot_kwargs["source"] = "queue_worker"
    slot_kwargs["app_name"] = "chemstack_orca"
    slot_kwargs["state"] = "reserved"
    return reserve_slot(
        Path(root),
        limit,
        **slot_kwargs,
    )


def _try_reserve_admission_slot(cfg: AppConfig) -> str | None:
    admission_token = _queue_module.try_reserve_admission_slot(cfg)
    if admission_token is None:
        logger.debug(
            "Queue worker admission paused: admission slots are full (admission_limit=%d)",
            int(getattr(cfg.runtime, "resolved_admission_limit", 1)),
        )
    return admission_token


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: QueueEntry,
    admission_root: Any,
    admission_token: str,
) -> BackgroundRunJobProcess:
    del admission_root
    return start_background_process(
        build_worker_child_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=queue_entry_id(entry),
            admission_token=admission_token,
        )
    )


def _terminate_process(proc: _ManagedProcess) -> None:
    """Terminate the background run process and escalate if it does not stop."""
    terminate_process_group(proc)


def _tracking_callbacks() -> _tracking_helpers.OrcaQueueWorkerTrackingCallbacks:
    return _tracking_helpers.tracking_callbacks_from_namespace(sys.modules[__name__])


def _get_run_id_from_state(reaction_dir: str) -> str | None:
    return _tracking_helpers.get_run_id_from_state(
        reaction_dir,
        callbacks=_tracking_callbacks(),
    )


def _upsert_running_job_record(cfg: AppConfig, entry: QueueEntry) -> None:
    _tracking_helpers.upsert_running_job_record(
        cfg,
        entry,
        callbacks=_tracking_callbacks(),
    )


def _tracking_metadata_from_queue_entry(
    cfg: AppConfig,
    entry: QueueEntry,
    *,
    reaction_dir: Path,
) -> tuple[str, str, str, dict[str, int], dict[str, int]]:
    return _tracking_helpers.tracking_metadata_from_queue_entry(
        cfg,
        entry,
        reaction_dir=reaction_dir,
        callbacks=_tracking_callbacks(),
    )


def _upsert_terminal_job_record(
    cfg: AppConfig,
    reaction_dir: str,
    *,
    fallback_job_id: str | None = None,
) -> None:
    _tracking_helpers.upsert_terminal_job_record(
        cfg,
        reaction_dir,
        fallback_job_id=fallback_job_id or "",
        callbacks=_tracking_callbacks(),
    )


def _notify_terminal_job_from_state(cfg: AppConfig, reaction_dir: str) -> bool:
    return _tracking_helpers.notify_terminal_job_from_state(
        cfg,
        reaction_dir,
        callbacks=_tracking_callbacks(),
    )


def _worker_admission_limit(cfg: AppConfig, fallback_max_concurrent: int) -> int:
    if getattr(cfg.runtime, "max_concurrent", None) in (None, "", 0):
        cfg.runtime.max_concurrent = fallback_max_concurrent
    return _resolve_worker_admission_limit(cfg)


def _lifecycle_callbacks() -> _lifecycle_helpers.OrcaQueueWorkerLifecycleCallbacks:
    return _lifecycle_helpers.lifecycle_callbacks_from_namespace(sys.modules[__name__])


def _orca_worker_lifecycle_hooks() -> EngineQueueProcessLifecycleHooks:
    return _lifecycle_helpers.build_orca_worker_lifecycle_hooks(_lifecycle_callbacks())


def _job_queue_root(worker: Any, job: Any) -> Path:
    return _lifecycle_job_queue_root(worker, job)


def _handle_worker_start_error(
    worker: Any,
    queue_root: Path,
    entry: Any,
    admission_token: str,
    exc: OSError,
) -> None:
    queue_id = queue_entry_id(entry)
    logger.error("Failed to start job %s: %s", queue_id, exc)
    worker._mark_entry_failed_and_release(
        queue_root,
        entry,
        admission_token,
        error=str(exc),
        mark_failed_fn=mark_failed,
    )


def _on_worker_process_started(
    worker: Any,
    queue_root: Path,
    entry: Any,
    process: BackgroundRunJobProcess,
    admission_token: str,
) -> bool:
    return _ENGINE_ADMISSION.attach_started_process_metadata(
        worker=worker,
        queue_root=queue_root,
        entry=entry,
        process=process,
        admission_token=admission_token,
        hooks=_orca_worker_lifecycle_hooks(),
    )


def _finalize_finished_job(worker: Any, queue_id: str, job: _RunningJob, *, rc: int) -> None:
    finalize_process_finished_job(
        worker,
        queue_id,
        job,
        rc=rc,
        hooks=_orca_worker_lifecycle_hooks(),
    )


def _finalize_completed_job(worker: Any, queue_id: str, job: Any, rc: int) -> None:
    _finalize_finished_job(worker, queue_id, job, rc=rc)


def _finalize_child_exit(worker: Any, job: _RunningJob, *, rc: int) -> None:
    _finalize_finished_job(worker, job.queue_id, job, rc=rc)


def _reconcile_orphaned_running(worker: Any) -> None:
    _lifecycle_helpers.reconcile_orphaned_running(
        worker,
        callbacks=_lifecycle_callbacks(),
    )


def _reconcile_worker_state(worker: Any) -> None:
    _reconcile_orphaned_running(worker)


def _shutdown_running_job(worker: Any, queue_id: str, job: Any) -> None:
    _lifecycle_helpers.shutdown_running_job(
        worker,
        queue_id,
        job,
        callbacks=_lifecycle_callbacks(),
    )


def _before_shutdown_all(_worker: Any, running_count: int) -> None:
    logger.info("Shutting down %d running job(s)...", running_count)


def _queue_worker_hooks() -> Any:
    return _queue_module.queue_worker_hooks()


def _after_orca_worker_init(worker: EngineQueueWorker) -> None:
    worker.admission_limit = _worker_admission_limit(worker.cfg, worker.max_concurrent)


def _before_orca_worker_run(worker: EngineQueueWorker) -> None:
    logger.info(
        "Queue worker started (pid=%d, max_concurrent=%d, admission_root=%s, admission_limit=%d, auto_organize=%s)",
        os.getpid(),
        worker.max_concurrent,
        worker.admission_root,
        worker.admission_limit,
        worker.auto_organize,
    )


def _after_orca_worker_run(_worker: EngineQueueWorker) -> None:
    logger.info("Queue worker stopped")


def _log_orca_worker_interrupt(_worker: EngineQueueWorker) -> None:
    logger.info("Queue worker interrupted")


def _make_orca_running_job(
    _worker: EngineQueueWorker,
    *,
    queue_root: Path,
    entry: Any,
    process: Any,
    admission_token: str,
) -> _RunningJob:
    running = _RunningJob(
        queue_id=queue_entry_id(entry),
        reaction_dir=queue_entry_reaction_dir(entry),
        task_id=queue_entry_task_id(entry) or None,
        process=process,
        admission_token=admission_token,
    )
    queue_root_attr = "queue_root"
    setattr(running, queue_root_attr, queue_root)
    return running


def _auto_organize_terminal_job(worker: EngineQueueWorker, job: _RunningJob) -> None:
    if not worker.auto_organize:
        return
    try:
        from .commands.organize import organize_reaction_dir

        result = organize_reaction_dir(
            worker.cfg,
            Path(job.reaction_dir),
            notify_summary=False,
        )
        if result.get("action") == "organized":
            target_dir = str(result.get("target_dir") or "").strip()
            if target_dir:
                logger.info("Auto-organized %s -> %s", job.reaction_dir, target_dir)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Auto-organize failed for %s: %s", job.reaction_dir, exc)


def _check_orca_cancel_requests(worker: EngineQueueWorker) -> None:
    for queue_id, job in worker._running_jobs():
        if get_cancel_requested(_job_queue_root(worker, job), queue_id):
            _cancel_orca_running_job(worker, queue_id, job)
            worker._discard_running_job(queue_id)


def _cancel_orca_running_job(worker: EngineQueueWorker, queue_id: str, job: _RunningJob) -> None:
    cancel_running_process_job(
        worker,
        queue_id,
        job,
        hooks=_orca_worker_lifecycle_hooks(),
    )


def QueueWorker(
    cfg: AppConfig,
    config_path: str,
    *,
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
    auto_organize: bool = False,
) -> EngineQueueWorker:
    configured_max = max(1, int(max_concurrent))
    if getattr(cfg.runtime, "admission_limit", None) in (None, ""):
        cfg.runtime.max_concurrent = configured_max
    worker = EngineQueueWorker(
        cfg,
        config_path=str(config_path),
        engine="orca",
        max_concurrent=configured_max,
        deps=_queue_worker_deps(),
        hooks=_queue_worker_hooks(),
        worker_pid_file_name=WORKER_PID_FILE,
        admission_root=_admission_root_for_cfg(cfg),
        auto_organize=auto_organize,
        after_init=_after_orca_worker_init,
        before_run=_before_orca_worker_run,
        after_run=_after_orca_worker_run,
        keyboard_interrupt=_log_orca_worker_interrupt,
        running_queue_id=queue_entry_id,
        running_job_factory=_make_orca_running_job,
        finalize_finished_job=_finalize_finished_job,
        reconcile_orphaned_running=_reconcile_orphaned_running,
        check_cancel_requests=_check_orca_cancel_requests,
    )
    auto_organize_attr = "_auto_organize_terminal_job"
    cancel_running_attr = "_cancel_running_job"
    setattr(worker, auto_organize_attr, lambda job: _auto_organize_terminal_job(worker, job))
    setattr(
        worker,
        cancel_running_attr,
        lambda queue_id, job: _cancel_orca_running_job(worker, queue_id, job),
    )
    return worker


def read_worker_pid(allowed_root: Path) -> int | None:
    """Read the worker PID file. Returns None if not found or stale."""
    return _queue_module.read_worker_pid(allowed_root)
