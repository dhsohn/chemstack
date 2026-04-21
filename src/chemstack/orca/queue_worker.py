"""Queue worker foreground loop for queue execution under an external supervisor.

Usage::

    python -m chemstack.cli queue worker --app orca

The worker is intended to run in the foreground, typically under systemd in WSL.
Each job is spawned in a dedicated child process so locking, state management,
and signal handling remain centralized.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Protocol

from .admission_store import reconcile_stale_slots, release_slot, reserve_slot, update_slot_metadata
from .config import AppConfig
from .process_tracking import read_pid_file
from .queue_store import (
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
from .state_store import load_organized_ref, load_report_json, load_state
from .tracking import record_from_artifacts, resolve_job_metadata, resource_dict, upsert_job_record
from .types import QueueEntry

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT = 4
POLL_INTERVAL_SECONDS = 5

# PID file for the daemon
WORKER_PID_FILE = "queue_worker.pid"


class _ManagedProcess(Protocol):
    pid: int

    def kill(self) -> None: ...
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


@dataclass
class _RunningJob:
    """Tracks a child process executing a queued job."""

    queue_id: str
    reaction_dir: str
    process: _ManagedProcess
    admission_token: str
    task_id: str | None = None
    started_at: float = field(default_factory=time.monotonic)


def _terminate_process(proc: _ManagedProcess) -> None:
    """Terminate the background run process and escalate if it does not stop."""
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
    except Exception:
        pass

    deadline = time.monotonic() + 10
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)

    if proc.poll() is not None:
        return
    try:
        proc.kill()
    except Exception:
        pass

    deadline = time.monotonic() + 5
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)


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
        metadata.get("selected_input_xyz")
        or metadata.get("selected_inp")
        or ""
    ).strip()
    job_type = str(metadata.get("job_type") or "").strip()
    molecule_key = str(metadata.get("molecule_key") or "").strip()
    if not job_type or not molecule_key:
        derived_job_type, derived_molecule_key = resolve_job_metadata(selected_input, reaction_dir)
        job_type = job_type or derived_job_type
        molecule_key = molecule_key or derived_molecule_key

    def _resource_caps_from_metadata(value: object) -> dict[str, int]:
        if not isinstance(value, dict):
            return {}
        result: dict[str, int] = {}
        for key, raw in value.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            try:
                result[key_text] = int(raw)
            except (TypeError, ValueError):
                continue
        return result

    requested = _resource_caps_from_metadata(metadata.get("resource_request"))
    if not requested:
        requested = resource_dict(
            cfg.resources.max_cores_per_task,
            cfg.resources.max_memory_gb_per_task,
        )

    actual = _resource_caps_from_metadata(metadata.get("resource_actual")) or dict(requested)
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
    organized_output_dir = Path(record.organized_output_dir).expanduser().resolve() if record.organized_output_dir else None
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


class QueueWorker:
    """Main worker loop that manages concurrent job execution."""

    def __init__(
        self,
        cfg: AppConfig,
        config_path: str,
        *,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        auto_organize: bool = False,
    ) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.max_concurrent = max(1, max_concurrent)
        self.auto_organize = bool(auto_organize)
        self.allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
        resolved_admission_root = getattr(cfg.runtime, "resolved_admission_root", None)
        admission_root_raw = (
            resolved_admission_root
            or getattr(cfg.runtime, "admission_root", "")
            or cfg.runtime.allowed_root
        )
        self.admission_root = Path(admission_root_raw).expanduser().resolve()
        raw_admission_limit: object | None = getattr(cfg.runtime, "admission_limit", None)
        if raw_admission_limit in {None, ""}:
            self.admission_limit = self.max_concurrent
        else:
            try:
                if isinstance(raw_admission_limit, bool):
                    normalized_limit = int(raw_admission_limit)
                elif isinstance(raw_admission_limit, (int, float, str)):
                    normalized_limit = int(raw_admission_limit)
                else:
                    raise TypeError("Unsupported admission_limit type")
                self.admission_limit = max(1, normalized_limit)
            except (TypeError, ValueError):
                self.admission_limit = self.max_concurrent
        self._running: Dict[str, _RunningJob] = {}  # queue_id → job
        self._shutdown_requested = False

    def run(self) -> int:
        """Run the worker loop. Returns 0 on clean shutdown."""
        self._install_signal_handlers()
        self._write_pid_file()
        self._reconcile_orphaned_running()
        logger.info(
            "Queue worker started (pid=%d, max_concurrent=%d, admission_root=%s, admission_limit=%d, auto_organize=%s)",
            os.getpid(), self.max_concurrent, self.admission_root, self.admission_limit, self.auto_organize,
        )

        try:
            while not self._shutdown_requested:
                self._run_iteration()
        except KeyboardInterrupt:
            logger.info("Queue worker interrupted")
        finally:
            self._shutdown_all()
            self._remove_pid_file()
            logger.info("Queue worker stopped")
        return 0

    def _run_iteration(self) -> None:
        self._check_completed_jobs()
        self._check_cancel_requests()
        self._fill_slots()
        time.sleep(POLL_INTERVAL_SECONDS)

    # -- Orphan reconciliation --------------------------------------------

    def _reconcile_orphaned_running(self) -> None:
        """Fix queue entries stuck as 'running' from a previous worker crash.

        On startup, any queue entry marked 'running' is orphaned (this worker
        has no subprocess tracking it).  Resolve each by checking the
        run_state.json and run.lock of the reaction directory.
        """
        reconcile_stale_slots(self.admission_root)
        reconcile_orphaned_running_entries(self.allowed_root, ignore_worker_pid=True)

    # -- Slot management --------------------------------------------------

    def _fill_slots(self) -> None:
        """Dequeue pending jobs until the global active-run limit is reached."""
        while len(self._running) < self.max_concurrent:
            reserved = self._reserve_next_entry()
            if reserved is None:
                break
            entry, admission_token = reserved
            self._start_job(entry, admission_token=admission_token)

    def _reserve_admission_slot(self) -> str | None:
        admission_token = reserve_slot(
            self.admission_root,
            self.admission_limit,
            source="queue_worker",
        )
        if admission_token is None:
            logger.debug(
                "Queue worker admission paused: admission slots are full (admission_limit=%d)",
                self.admission_limit,
            )
        return admission_token

    def _reserve_next_entry(self) -> tuple[QueueEntry, str] | None:
        admission_token = self._reserve_admission_slot()
        if admission_token is None:
            return None

        entry = dequeue_next(self.allowed_root)
        if entry is None:
            self._release_admission_token(admission_token)
            return None

        if not self._attach_slot_identity(admission_token, entry):
            self._fail_start(entry, admission_token, error="admission_slot_missing")
            return None
        return entry, admission_token

    def _attach_slot_identity(self, admission_token: str, entry: QueueEntry) -> bool:
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
        return attached

    def _release_admission_token(self, admission_token: str) -> None:
        release_slot(self.admission_root, admission_token)

    def _fail_start(self, entry: QueueEntry, admission_token: str, *, error: str) -> None:
        mark_failed(self.allowed_root, queue_entry_id(entry), error=error)
        self._release_admission_token(admission_token)

    def _start_job(self, entry: QueueEntry, *, admission_token: str) -> bool:
        queue_id = queue_entry_id(entry)
        reaction_dir = queue_entry_reaction_dir(entry)
        force = queue_entry_force(entry)
        app_name = queue_entry_app_name(entry)
        task_id = queue_entry_task_id(entry) or ""
        logger.info("Starting job %s: %s", queue_id, reaction_dir)
        try:
            proc = _start_job_process(
                reaction_dir=reaction_dir,
                config_path=self.config_path,
                force=force,
                admission_token=admission_token,
                admission_app_name=app_name or None,
                admission_task_id=task_id or None,
            )
        except OSError as exc:
            logger.error("Failed to start job %s: %s", queue_id, exc)
            self._fail_start(entry, admission_token, error=str(exc))
            return False

        self._register_running_job(
            entry,
            process=proc,
            admission_token=admission_token,
        )
        try:
            _upsert_running_job_record(self.cfg, entry)
        except Exception as exc:
            logger.warning("Failed to update running job location for %s: %s", queue_id, exc)
        return True

    def _register_running_job(
        self,
        entry: QueueEntry,
        *,
        process: _ManagedProcess,
        admission_token: str,
    ) -> None:
        queue_id = queue_entry_id(entry)
        reaction_dir = queue_entry_reaction_dir(entry)
        task_id = queue_entry_task_id(entry) or ""
        self._running[queue_id] = _RunningJob(
            queue_id=queue_id,
            reaction_dir=reaction_dir,
            task_id=task_id or None,
            process=process,
            admission_token=admission_token,
        )

    # -- Monitoring -------------------------------------------------------

    def _check_completed_jobs(self) -> None:
        """Poll running child processes for completion."""
        done_ids = []
        for queue_id, job in self._running.items():
            rc = job.process.poll()
            if rc is None:
                continue
            done_ids.append(queue_id)
            self._finalize_finished_job(queue_id, job, rc=rc)
        for qid in done_ids:
            del self._running[qid]

    def _finalize_finished_job(self, queue_id: str, job: _RunningJob, *, rc: int) -> None:
        run_id = _get_run_id_from_state(job.reaction_dir)
        if rc == 0:
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
        self._release_admission_token(job.admission_token)

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
        self._release_admission_token(job.admission_token)

    # -- Shutdown ---------------------------------------------------------

    def _shutdown_all(self) -> None:
        """Terminate all running jobs on worker shutdown."""
        if not self._running:
            return
        logger.info("Shutting down %d running job(s)...", len(self._running))
        for queue_id, job in self._running.items():
            self._requeue_running_job(queue_id, job)
        self._running.clear()

    def _requeue_running_job(self, queue_id: str, job: _RunningJob) -> None:
        _terminate_process(job.process)
        requeue_running_entry(self.allowed_root, queue_id)
        self._release_admission_token(job.admission_token)

    def _install_signal_handlers(self) -> None:
        def _handle_signal(signum: int, frame: object) -> None:
            logger.info("Received signal %d, shutting down...", signum)
            self._shutdown_requested = True

        try:
            signal.signal(signal.SIGTERM, _handle_signal)
            signal.signal(signal.SIGINT, _handle_signal)
        except ValueError:
            pass  # Not in main thread

    # -- PID file ---------------------------------------------------------

    def _pid_file_path(self) -> Path:
        return self.allowed_root / WORKER_PID_FILE

    def _write_pid_file(self) -> None:
        self._pid_file_path().write_text(str(os.getpid()), encoding="utf-8")

    def _remove_pid_file(self) -> None:
        try:
            self._pid_file_path().unlink()
        except OSError:
            pass


def read_worker_pid(allowed_root: Path) -> int | None:
    """Read the worker PID file. Returns None if not found or stale."""
    return read_pid_file(allowed_root / WORKER_PID_FILE)
