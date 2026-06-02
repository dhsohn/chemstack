"""Queue worker foreground loop for queue execution under an external supervisor.

This engine worker is launched by the unified ChemStack worker service under
systemd. Each job is spawned in a dedicated child process so locking, state
management, and signal handling remain centralized.
"""

from __future__ import annotations

import logging
import os
import time  # noqa: F401
from pathlib import Path
from typing import Any

from chemstack.core.indexing.roots import runtime_roots_for_cfg as _runtime_roots_for_cfg
from chemstack.core.queue.types import QueueEntry
from chemstack.core.queue.engine_execution import coerce_resource_request
from chemstack.core.queue.internal_engine import (
    InternalEngineQueueModule,
    InternalEngineQueueRuntime,
    InternalEngineQueueWorkerFacade,
    InternalEngineSpec,
)
from chemstack.core.queue.lifecycle import (
    EngineQueueProcessLifecycleHooks,
    EngineQueueProcessReconcileHooks,
    EngineQueueProcessShutdownHooks,
    EngineQueueTerminalSideEffectHooks,
    cancel_running_process_job,
    finalize_process_finished_job,
    job_queue_root as _lifecycle_job_queue_root,
    reconcile_orphaned_process_entries,
    shutdown_running_process_job,
)
from chemstack.core.queue.worker import (
    EngineRunningJob as _RunningJob,
    HookedPidFileChildProcessQueueWorker,
    ManagedProcess as _ManagedProcess,
    start_background_process,
    terminate_process_group,
)

from chemstack.core.admission import (
    activate_reserved_slot,  # noqa: F401
    reconcile_stale_slots,
    release_slot,  # noqa: F401
    reserve_slot,
    update_slot_metadata,
)
from .attempt_reporting import (
    build_run_finished_notification,
    finished_notification_already_sent,
    mark_finished_notification_sent,
)
from .config import AppConfig, load_config
from .input_artifacts import selected_input_artifacts
from .inp_rewriter import read_resource_request_from_input
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
    requeue_running_entry,
    reconcile_orphaned_running_entries,
)
from .runtime.worker_job import (
    WORKER_JOB_MODULE,
    BackgroundRunJobProcess,
    build_worker_child_command,
    start_background_run_job,
)
from .state import load_organized_ref, load_report_json, load_state
from .telegram_notifier import notify_run_finished_event
from .job_locations import (
    record_from_artifacts,
    resolve_job_metadata,
    resource_dict,
    upsert_job_record,
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


_engine_runtime = InternalEngineQueueRuntime.create(
    spec=_ENGINE_SPEC,
    load_config=load_config,
    runtime_roots_for_cfg=lambda cfg: _runtime_roots_for_cfg(cfg, engine="orca"),
    list_queue=lambda root: list_queue(Path(root)),
    dequeue_next=lambda root: dequeue_next(root),
)
_runtime_facade = InternalEngineQueueWorkerFacade(
    runtime=_engine_runtime,
    poll_interval_seconds=POLL_INTERVAL_SECONDS,
    shutdown_grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
    namespace=globals(),
    reserve_slot_name="_reserve_orca_worker_slot",
    on_worker_process_started_name="_on_worker_process_started",
    shutdown_running_job_name="_shutdown_running_job",
    before_shutdown_all_name="_before_shutdown_all",
)
_queue_module = InternalEngineQueueModule(runtime=_engine_runtime, facade=_runtime_facade)


def queue_roots(cfg: AppConfig) -> tuple[Path, ...]:
    return _queue_module.queue_roots(cfg)


def queue_entries_with_roots(cfg: AppConfig) -> list[tuple[Path, Any]]:
    return _queue_module.queue_entries_with_roots(cfg)


def _queue_worker_deps() -> Any:
    return _runtime_facade.queue_worker_deps()


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
    admission_token = _runtime_facade.try_reserve_admission_slot(cfg)
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


def _start_job_process(
    *,
    reaction_dir: str,
    config_path: str,
    force: bool = False,
    admission_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
) -> BackgroundRunJobProcess:
    return start_background_run_job(
        config_path=config_path,
        reaction_dir=reaction_dir,
        force=force,
        admission_token=admission_token,
        admission_app_name=admission_app_name,
        admission_task_id=admission_task_id,
    )


def _get_run_id_from_state(reaction_dir: str) -> str | None:
    """Try to read run_id from the reaction_dir's run_state.json."""
    state = load_state(Path(reaction_dir))
    if state:
        return state.get("run_id")
    return None


def _upsert_running_job_record(cfg: AppConfig, entry: QueueEntry) -> None:
    task_id = queue_entry_task_id(entry)
    if not task_id:
        return
    reaction_dir = Path(queue_entry_reaction_dir(entry)).expanduser().resolve()
    selected_input, job_type, molecule_key, requested, actual = _tracking_metadata_from_queue_entry(
        cfg,
        entry,
        reaction_dir=reaction_dir,
    )
    upsert_job_record(
        cfg,
        job_id=task_id,
        status="running",
        job_dir=reaction_dir,
        job_type=job_type,
        selected_input_xyz=selected_input,
        molecule_key=molecule_key,
        resource_request=requested,
        resource_actual=actual,
    )


def _tracking_metadata_from_queue_entry(
    cfg: AppConfig,
    entry: QueueEntry,
    *,
    reaction_dir: Path,
) -> tuple[str, str, str, dict[str, int], dict[str, int]]:
    metadata = queue_entry_metadata(entry)
    selected_inp = str(metadata.get("selected_inp") or "").strip()
    selected_xyz = str(metadata.get("selected_input_xyz") or "").strip()
    selected_input = str(
        selected_xyz
        or metadata.get("selected_input_path")
        or selected_input_artifacts(selected_inp).selected_input_path
    ).strip()
    job_type = str(metadata.get("job_type") or "").strip()
    molecule_key = str(metadata.get("molecule_key") or "").strip()
    if not job_type or not molecule_key:
        derived_job_type, derived_molecule_key = resolve_job_metadata(
            selected_inp or selected_input,
            reaction_dir,
        )
        job_type = job_type or derived_job_type
        molecule_key = molecule_key or derived_molecule_key

    requested = coerce_resource_request(metadata.get("resource_request"))
    resource_inp = selected_inp or selected_input
    if not requested and resource_inp.lower().endswith(".inp"):
        selected_inp_path = Path(resource_inp).expanduser().resolve()
        if selected_inp_path.exists():
            requested = read_resource_request_from_input(selected_inp_path)
    if not requested:
        requested = resource_dict(
            cfg.resources.max_cores_per_task,
            cfg.resources.max_memory_gb_per_task,
        )

    actual = coerce_resource_request(metadata.get("resource_actual")) or dict(requested)
    return selected_input, job_type, molecule_key, requested, actual


def _upsert_terminal_job_record(
    cfg: AppConfig,
    reaction_dir: str,
    *,
    fallback_job_id: str | None = None,
) -> None:
    job_dir = Path(reaction_dir).expanduser().resolve()
    state = load_state(job_dir)
    record = record_from_artifacts(
        job_dir=job_dir,
        state=dict(state) if state is not None else None,
        report=load_report_json(job_dir),
        organized_ref=load_organized_ref(job_dir),
        fallback_job_id=fallback_job_id or "",
    )
    if record is None:
        return
    organized_output_dir = (
        Path(record.organized_output_dir).expanduser().resolve()
        if record.organized_output_dir
        else None
    )
    upsert_job_record(
        cfg,
        job_id=record.job_id,
        status=record.status,
        job_dir=Path(record.original_run_dir).expanduser().resolve(),
        job_type=record.job_type,
        selected_input_xyz=record.selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=record.molecule_key,
        resource_request=dict(record.resource_request),
        resource_actual=dict(record.resource_actual),
    )


def _notify_terminal_job_from_state(cfg: AppConfig, reaction_dir: str) -> bool:
    if not cfg.telegram.enabled:
        return False

    job_dir = Path(reaction_dir).expanduser().resolve()
    state = load_state(job_dir)
    if not state:
        logger.warning("Skipping terminal Telegram notification; state missing for %s", job_dir)
        return False
    if finished_notification_already_sent(state):
        return False

    final_result = state.get("final_result")
    if not isinstance(final_result, dict):
        logger.warning(
            "Skipping terminal Telegram notification; final_result missing for %s",
            job_dir,
        )
        return False

    selected_inp_text = str(state.get("selected_inp") or "").strip()
    selected_inp = Path(selected_inp_text) if selected_inp_text else job_dir / "-"
    status = str(final_result.get("status") or state.get("status") or "").strip()
    notification = build_run_finished_notification(
        reaction_dir=job_dir,
        selected_inp=selected_inp,
        state=state,
        status=status,
        final_result=final_result,
    )
    sent = notify_run_finished_event(cfg.telegram, notification)
    if sent:
        mark_finished_notification_sent(job_dir, state)
        logger.info("Terminal Telegram notification sent by queue worker: %s", job_dir)
        return True

    logger.warning("Terminal Telegram notification failed in queue worker: %s", job_dir)
    return False


def _worker_admission_limit(cfg: AppConfig, fallback_max_concurrent: int) -> int:
    raw_admission_limit: object | None = getattr(cfg.runtime, "admission_limit", None)
    if raw_admission_limit is None or raw_admission_limit == "":
        return max(1, int(fallback_max_concurrent))
    try:
        if isinstance(raw_admission_limit, bool):
            normalized_limit = int(raw_admission_limit)
        elif isinstance(raw_admission_limit, (int, float, str)):
            normalized_limit = int(raw_admission_limit)
        else:
            raise TypeError("Unsupported admission_limit type")
    except (TypeError, ValueError):
        return max(1, int(fallback_max_concurrent))
    if normalized_limit < 1:
        return 1
    return normalized_limit


def _orca_worker_lifecycle_hooks() -> EngineQueueProcessLifecycleHooks:
    return EngineQueueProcessLifecycleHooks(
        queue_entry_id_fn=queue_entry_id,
        queue_entry_app_name_fn=queue_entry_app_name,
        queue_entry_task_id_fn=queue_entry_task_id,
        update_slot_metadata_fn=update_slot_metadata,
        terminate_process_fn=_terminate_process,
        mark_failed_fn=mark_failed,
        upsert_running_job_record_fn=_upsert_running_job_record,
        get_run_id_from_state_fn=_get_run_id_from_state,
        get_cancel_requested_fn=get_cancel_requested,
        mark_cancelled_fn=mark_cancelled,
        mark_completed_fn=mark_completed,
        upsert_terminal_job_record_fn=_upsert_terminal_job_record,
        notify_terminal_job_from_state_fn=_notify_terminal_job_from_state,
        on_completed_fn=lambda worker, job: worker._auto_organize_terminal_job(job),
        terminal_side_effect_hooks=EngineQueueTerminalSideEffectHooks(
            upsert_terminal_job_record_fn=_upsert_terminal_job_record,
            notify_terminal_job_from_state_fn=_notify_terminal_job_from_state,
        ),
    )


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
    """Fix queue entries stuck as running from a previous worker crash."""
    reconcile_orphaned_process_entries(
        worker,
        hooks=EngineQueueProcessReconcileHooks(
            queue_roots_fn=queue_roots,
            reconcile_stale_slots_fn=reconcile_stale_slots,
            reconcile_orphaned_running_entries_fn=reconcile_orphaned_running_entries,
            reconcile_orphaned_running_entries_kwargs={"ignore_worker_pid": True},
        ),
    )


def _reconcile_worker_state(worker: Any) -> None:
    _reconcile_orphaned_running(worker)


def _shutdown_running_job(worker: Any, queue_id: str, job: Any) -> None:
    shutdown_running_process_job(
        worker,
        queue_id,
        job,
        hooks=EngineQueueProcessShutdownHooks(
            terminate_process_fn=_terminate_process,
            requeue_running_entry_fn=requeue_running_entry,
        ),
    )


def _before_shutdown_all(_worker: Any, running_count: int) -> None:
    logger.info("Shutting down %d running job(s)...", running_count)


def _queue_worker_hooks() -> Any:
    return _runtime_facade.queue_worker_hooks()


class QueueWorker(HookedPidFileChildProcessQueueWorker):
    """Main worker loop that manages concurrent job execution."""

    worker_pid_file_name = WORKER_PID_FILE

    def __init__(
        self,
        cfg: AppConfig,
        config_path: str,
        *,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        auto_organize: bool = False,
    ) -> None:
        configured_max = max(1, int(max_concurrent))
        if getattr(cfg.runtime, "admission_limit", None) in (None, ""):
            cfg.runtime.max_concurrent = configured_max
        super().__init__(
            cfg,
            config_path=config_path,
            max_concurrent=configured_max,
            deps=_queue_worker_deps(),
            hooks=_queue_worker_hooks(),
            worker_pid_file_name=WORKER_PID_FILE,
            admission_root=_admission_root_for_cfg(cfg),
        )
        self.auto_organize = bool(auto_organize)
        self.admission_limit = _worker_admission_limit(cfg, self.max_concurrent)

    def _before_run(self) -> None:
        super()._before_run()
        logger.info(
            "Queue worker started (pid=%d, max_concurrent=%d, admission_root=%s, admission_limit=%d, auto_organize=%s)",
            os.getpid(),
            self.max_concurrent,
            self.admission_root,
            self.admission_limit,
            self.auto_organize,
        )

    def _after_run(self) -> None:
        super()._after_run()
        logger.info("Queue worker stopped")

    def _run_iteration(self) -> None:
        try:
            super()._run_iteration()
        except KeyboardInterrupt:
            logger.info("Queue worker interrupted")
            raise

    # -- Orphan reconciliation --------------------------------------------

    def _reconcile_orphaned_running(self) -> None:
        _reconcile_orphaned_running(self)

    def _running_queue_id(self, entry: Any) -> str:
        return queue_entry_id(entry)

    def _make_running_job(
        self,
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
        setattr(running, "queue_root", queue_root)
        return running

    # -- Monitoring -------------------------------------------------------

    def _finalize_finished_job(self, queue_id: str, job: _RunningJob, *, rc: int) -> None:
        _finalize_finished_job(self, queue_id, job, rc=rc)

    def _auto_organize_terminal_job(self, job: _RunningJob) -> None:
        if not self.auto_organize:
            return
        try:
            from .commands.organize import organize_reaction_dir

            result = organize_reaction_dir(
                self.cfg,
                Path(job.reaction_dir),
                notify_summary=False,
            )
            if result.get("action") == "organized":
                target_dir = str(result.get("target_dir") or "").strip()
                if target_dir:
                    logger.info("Auto-organized %s -> %s", job.reaction_dir, target_dir)
        except Exception as exc:
            logger.warning("Auto-organize failed for %s: %s", job.reaction_dir, exc)

    def _check_cancel_requests(self) -> None:
        """Check if any running jobs have been requested to cancel."""
        for queue_id, job in self._running_jobs():
            if get_cancel_requested(_job_queue_root(self, job), queue_id):
                self._cancel_running_job(queue_id, job)
                self._discard_running_job(queue_id)

    def _cancel_running_job(self, queue_id: str, job: _RunningJob) -> None:
        cancel_running_process_job(
            self,
            queue_id,
            job,
            hooks=_orca_worker_lifecycle_hooks(),
        )

    # -- Shutdown ---------------------------------------------------------


def read_worker_pid(allowed_root: Path) -> int | None:
    """Read the worker PID file. Returns None if not found or stale."""
    return _queue_module.read_worker_pid(allowed_root)
