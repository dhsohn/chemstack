"""Queue worker foreground loop for queue execution under an external supervisor.

This engine worker is launched by the unified ChemStack worker service under
systemd. Each job is spawned in a dedicated child process so locking, state
management, and signal handling remain centralized.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from pathlib import Path
from typing import Any

from chemstack.core.queue.engine_execution import coerce_resource_request
from chemstack.core.queue.worker import (
    ChildProcessQueueWorker,
    EngineRunningJob as _RunningJob,
    ManagedProcess as _ManagedProcess,
    QueueWorkerPidFileMixin,
    make_child_queue_worker_deps,
    read_worker_pid_file,
    reserve_dequeued_entry,
    reserve_queue_worker_slot,
    resolve_admission_root,
    terminate_process_group,
)

from chemstack.core.admission.orca import reconcile_stale_slots, release_slot, reserve_slot, update_slot_metadata
from .config import AppConfig
from .inp_rewriter import read_resource_request_from_input
from .queue_adapter import (
    dequeue_next,
    get_cancel_requested,
    mark_cancelled,
    mark_completed,
    mark_failed,
    queue_entry_app_name,
    queue_entry_force,
    queue_entry_id,
    queue_entry_metadata,
    queue_entry_reaction_dir,
    queue_entry_task_id,
    requeue_running_entry,
    reconcile_orphaned_running_entries,
)
from .runtime.worker_job import BackgroundRunJobProcess, start_background_run_job
from .state import load_organized_ref, load_report_json, load_state
from .job_locations import record_from_artifacts, resolve_job_metadata, resource_dict, upsert_job_record
from .types import QueueEntry

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT = 4
POLL_INTERVAL_SECONDS = 5

# PID file for the daemon
WORKER_PID_FILE = "queue_worker.pid"


def _queue_worker_deps() -> Any:
    return make_child_queue_worker_deps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time_module=time,
        release_slot_fn=release_slot,
        reserve_dequeued_entry_fn=reserve_dequeued_entry,
        admission_root_fn=_admission_root_for_cfg,
        dequeue_next_entry_fn=_dequeue_next_entry,
        start_background_job_process_fn=_start_background_job_process,
        try_reserve_admission_slot_fn=_try_reserve_admission_slot,
    )


def _admission_root_for_cfg(cfg: AppConfig) -> str:
    return resolve_admission_root(cfg)


def _dequeue_next_entry(cfg: AppConfig) -> tuple[Path, QueueEntry] | None:
    queue_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    entry = dequeue_next(queue_root)
    if entry is None:
        return None
    return queue_root, entry


def _try_reserve_admission_slot(cfg: AppConfig) -> str | None:
    admission_token = reserve_queue_worker_slot(
        cfg,
        source="queue_worker",
        app_name="chemstack_orca",
        reserve_slot_fn=lambda root, limit, **kwargs: reserve_slot(
            Path(root),
            limit,
            **kwargs,
        ),
    )
    if admission_token is None:
        logger.debug(
            "Queue worker admission paused: admission slots are full "
            "(admission_limit=%d)",
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
    del queue_root, admission_root
    return _start_job_process(
        reaction_dir=queue_entry_reaction_dir(entry),
        config_path=config_path,
        force=queue_entry_force(entry),
        admission_token=admission_token,
        admission_app_name=queue_entry_app_name(entry) or None,
        admission_task_id=queue_entry_task_id(entry) or None,
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
    selected_input = str(
        metadata.get("selected_input_xyz") or metadata.get("selected_inp") or ""
    ).strip()
    job_type = str(metadata.get("job_type") or "").strip()
    molecule_key = str(metadata.get("molecule_key") or "").strip()
    if not job_type or not molecule_key:
        derived_job_type, derived_molecule_key = resolve_job_metadata(selected_input, reaction_dir)
        job_type = job_type or derived_job_type
        molecule_key = molecule_key or derived_molecule_key

    requested = coerce_resource_request(metadata.get("resource_request"))
    if not requested and selected_input.lower().endswith(".inp"):
        selected_inp_path = Path(selected_input).expanduser().resolve()
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


class QueueWorker(QueueWorkerPidFileMixin, ChildProcessQueueWorker):
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
        )
        self.auto_organize = bool(auto_organize)
        self.allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
        self.admission_root = Path(resolve_admission_root(cfg)).expanduser().resolve()
        self.admission_limit = _worker_admission_limit(cfg, self.max_concurrent)

    def _before_run(self) -> None:
        self._write_pid_file()
        self._reconcile_orphaned_running()
        logger.info(
            "Queue worker started (pid=%d, max_concurrent=%d, admission_root=%s, admission_limit=%d, auto_organize=%s)",
            os.getpid(),
            self.max_concurrent,
            self.admission_root,
            self.admission_limit,
            self.auto_organize,
        )

    def _after_run(self) -> None:
        self._remove_pid_file()
        logger.info("Queue worker stopped")

    def _run_iteration(self) -> None:
        try:
            super()._run_iteration()
        except KeyboardInterrupt:
            logger.info("Queue worker interrupted")
            raise

    # -- Orphan reconciliation --------------------------------------------

    def _reconcile_orphaned_running(self) -> None:
        """Fix queue entries stuck as 'running' from a previous worker crash.

        On startup, any queue entry marked 'running' is orphaned (this worker
        has no subprocess tracking it).  Resolve each by checking the
        run_state.json and run.lock of the reaction directory.
        """
        reconcile_stale_slots(self.admission_root)
        reconcile_orphaned_running_entries(self.allowed_root, ignore_worker_pid=True)

    def _on_worker_process_started(
        self,
        queue_root: Path,
        entry: Any,
        *,
        process: BackgroundRunJobProcess,
        admission_token: str,
    ) -> bool:
        attached = update_slot_metadata(
            self.admission_root,
            admission_token,
            queue_id=queue_entry_id(entry),
            app_name=queue_entry_app_name(entry),
            task_id=queue_entry_task_id(entry),
        )
        if not attached:
            logger.error(
                "Failed to attach queue identity to admission slot %s for job %s",
                admission_token,
                queue_entry_id(entry),
            )
            _terminate_process(process)
            mark_failed(queue_root, queue_entry_id(entry), error="admission_slot_missing")
            release_slot(self.admission_root, admission_token)
            return False

        try:
            _upsert_running_job_record(self.cfg, entry)
        except Exception as exc:
            logger.warning(
                "Failed to update running job location for %s: %s",
                queue_entry_id(entry),
                exc,
            )
        return True

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        queue_id = queue_entry_id(entry)
        logger.error("Failed to start job %s: %s", queue_id, exc)
        mark_failed(queue_root, queue_id, error=str(exc))
        release_slot(self.admission_root, admission_token)

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
        del queue_root
        return _RunningJob(
            queue_id=queue_entry_id(entry),
            reaction_dir=queue_entry_reaction_dir(entry),
            task_id=queue_entry_task_id(entry) or None,
            process=process,
            admission_token=admission_token,
        )

    # -- Monitoring -------------------------------------------------------

    def _poll_job(self, job: _RunningJob) -> int | None:
        return job.process.poll()

    def _finalize_completed_job(self, queue_id: str, job: Any, rc: int) -> None:
        self._finalize_finished_job(queue_id, job, rc=rc)

    def _finalize_finished_job(self, queue_id: str, job: _RunningJob, *, rc: int) -> None:
        run_id = _get_run_id_from_state(job.reaction_dir)
        if get_cancel_requested(self.allowed_root, queue_id):
            logger.info("Job cancelled: %s (rc=%d)", queue_id, rc)
            mark_cancelled(self.allowed_root, queue_id)
        elif rc == 0:
            logger.info("Job completed: %s (rc=%d)", queue_id, rc)
            mark_completed(self.allowed_root, queue_id, run_id=run_id)
            self._auto_organize_terminal_job(job)
        else:
            logger.warning("Job failed: %s (rc=%d)", queue_id, rc)
            mark_failed(
                self.allowed_root,
                queue_id,
                error=f"exit_code={rc}",
                run_id=run_id,
            )
        try:
            _upsert_terminal_job_record(self.cfg, job.reaction_dir, fallback_job_id=job.task_id)
        except Exception as exc:
            logger.warning("Failed to update terminal job location for %s: %s", queue_id, exc)
        release_slot(self.admission_root, job.admission_token)

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
        for queue_id, job in list(self._running.items()):
            if get_cancel_requested(self.allowed_root, queue_id):
                self._cancel_running_job(queue_id, job)
                del self._running[queue_id]

    def _cancel_running_job(self, queue_id: str, job: _RunningJob) -> None:
        logger.info("Cancelling running job: %s", queue_id)
        _terminate_process(job.process)
        try:
            job.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        mark_cancelled(self.allowed_root, queue_id)
        release_slot(self.admission_root, job.admission_token)

    # -- Shutdown ---------------------------------------------------------

    def _shutdown_all(self) -> None:
        """Terminate all running jobs on worker shutdown."""
        if not self._running:
            return
        logger.info("Shutting down %d running job(s)...", len(self._running))
        for queue_id, job in list(self._running.items()):
            _terminate_process(job.process)
            requeue_running_entry(self.allowed_root, queue_id)
            release_slot(self.admission_root, job.admission_token)
            del self._running[queue_id]


def read_worker_pid(allowed_root: Path) -> int | None:
    """Read the worker PID file. Returns None if not found or stale."""
    return read_worker_pid_file(allowed_root, WORKER_PID_FILE)
