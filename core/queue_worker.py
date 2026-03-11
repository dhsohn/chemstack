"""Queue worker daemon — polls the queue and runs jobs up to a global active-run limit.

Usage::

    orca_auto queue worker              # foreground, default 4 total active runs
    orca_auto queue worker --daemon     # background daemon
    orca_auto queue worker --max-concurrent 2

The worker spawns each job as a subprocess (``orca_auto run-inp --foreground``)
so that existing locking, state management, and signal handling are fully reused.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

from .config import AppConfig
from .lock_utils import is_process_alive, parse_lock_info, process_start_ticks
from .queue_store import (
    dequeue_next,
    get_cancel_requested,
    mark_completed,
    mark_failed,
    reconcile_orphaned_running_entries,
    _acquire_queue_lock,
    _load_entries,
    _save_entries,
)
from .state_store import LOCK_FILE_NAME, load_state
from .statuses import QueueStatus, RunStatus
from .types import QueueEntry

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT = 4
POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 2

# PID file for the daemon
WORKER_PID_FILE = "queue_worker.pid"


@dataclass
class _RunningJob:
    """Tracks a subprocess executing a queued job."""

    queue_id: str
    reaction_dir: str
    process: subprocess.Popen
    started_at: float = field(default_factory=time.monotonic)


def _build_run_command(
    reaction_dir: str,
    config_path: str,
    *,
    force: bool = False,
) -> list[str]:
    cmd = [
        sys.executable, "-m", "core.cli",
        "--config", config_path,
        "run-inp",
        "--reaction-dir", reaction_dir,
        "--foreground",
    ]
    if force:
        cmd.append("--force")
    return cmd


def _terminate_process(proc: subprocess.Popen) -> None:
    """Send SIGTERM to the process group, escalate to SIGKILL if needed."""
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except Exception:
            pass

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def _get_run_id_from_state(reaction_dir: str) -> str | None:
    """Try to read run_id from the reaction_dir's run_state.json."""
    state = load_state(Path(reaction_dir))
    if state:
        return state.get("run_id")
    return None


def _active_lock_pid(reaction_dir: Path) -> int | None:
    lock_info = parse_lock_info(reaction_dir / LOCK_FILE_NAME)
    pid = lock_info.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return None
    if not is_process_alive(pid):
        return None

    expected_ticks = lock_info.get("process_start_ticks")
    if isinstance(expected_ticks, int) and expected_ticks > 0:
        observed_ticks = process_start_ticks(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            logger.info(
                "Ignoring stale run.lock due to PID reuse: reaction_dir=%s pid=%d expected=%d observed=%s",
                reaction_dir,
                pid,
                expected_ticks,
                observed_ticks,
            )
            return None
    return pid


def _count_active_run_locks(
    allowed_root: Path,
    *,
    exclude_reaction_dirs: set[str] | None = None,
) -> int:
    if not allowed_root.is_dir():
        return 0

    excluded = {
        str(Path(reaction_dir).expanduser().resolve())
        for reaction_dir in (exclude_reaction_dirs or set())
    }
    count = 0
    for lock_path in allowed_root.rglob(LOCK_FILE_NAME):
        reaction_dir = str(lock_path.parent.resolve())
        if reaction_dir in excluded:
            continue
        if _active_lock_pid(Path(reaction_dir)) is None:
            continue
        count += 1
    return count


class QueueWorker:
    """Main worker loop that manages concurrent job execution."""

    def __init__(
        self,
        cfg: AppConfig,
        config_path: str,
        *,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
    ) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.max_concurrent = max(1, max_concurrent)
        self.allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
        self._running: Dict[str, _RunningJob] = {}  # queue_id → job
        self._shutdown_requested = False

    def run(self) -> int:
        """Run the worker loop. Returns 0 on clean shutdown."""
        self._install_signal_handlers()
        self._write_pid_file()
        self._reconcile_orphaned_running()
        logger.info(
            "Queue worker started (pid=%d, max_concurrent=%d)",
            os.getpid(), self.max_concurrent,
        )

        try:
            while not self._shutdown_requested:
                self._check_completed_jobs()
                self._check_cancel_requests()
                self._fill_slots()
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Queue worker interrupted")
        finally:
            self._shutdown_all()
            self._remove_pid_file()
            logger.info("Queue worker stopped")
        return 0

    # -- Orphan reconciliation --------------------------------------------

    def _reconcile_orphaned_running(self) -> None:
        """Fix queue entries stuck as 'running' from a previous worker crash.

        On startup, any queue entry marked 'running' is orphaned (this worker
        has no subprocess tracking it).  Resolve each by checking the
        run_state.json and run.lock of the reaction directory.
        """
        reconcile_orphaned_running_entries(self.allowed_root, ignore_worker_pid=True)

    # -- Slot management --------------------------------------------------

    def _fill_slots(self) -> None:
        """Dequeue pending jobs until the global active-run limit is reached."""
        while len(self._running) < self.max_concurrent:
            external_active = _count_active_run_locks(
                self.allowed_root,
                exclude_reaction_dirs={job.reaction_dir for job in self._running.values()},
            )
            total_active = len(self._running) + external_active
            if total_active >= self.max_concurrent:
                logger.debug(
                    "Queue worker admission paused: total_active=%d max_concurrent=%d",
                    total_active,
                    self.max_concurrent,
                )
                break
            entry = dequeue_next(self.allowed_root)
            if entry is None:
                break
            self._start_job(entry)

    def _start_job(self, entry: QueueEntry) -> None:
        queue_id = entry["queue_id"]
        reaction_dir = entry["reaction_dir"]
        force = bool(entry.get("force", False))

        cmd = _build_run_command(
            reaction_dir,
            self.config_path,
            force=force,
        )
        logger.info("Starting job %s: %s", queue_id, reaction_dir)
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as exc:
            logger.error("Failed to start job %s: %s", queue_id, exc)
            mark_failed(self.allowed_root, queue_id, error=str(exc))
            return

        self._running[queue_id] = _RunningJob(
            queue_id=queue_id,
            reaction_dir=reaction_dir,
            process=proc,
        )

    # -- Monitoring -------------------------------------------------------

    def _check_completed_jobs(self) -> None:
        """Poll running subprocesses for completion."""
        done_ids = []
        for queue_id, job in self._running.items():
            rc = job.process.poll()
            if rc is None:
                continue
            done_ids.append(queue_id)
            run_id = _get_run_id_from_state(job.reaction_dir)
            if rc == 0:
                logger.info("Job completed: %s (rc=%d)", queue_id, rc)
                mark_completed(self.allowed_root, queue_id, run_id=run_id)
            else:
                logger.warning("Job failed: %s (rc=%d)", queue_id, rc)
                mark_failed(
                    self.allowed_root, queue_id,
                    error=f"exit_code={rc}",
                    run_id=run_id,
                )
        for qid in done_ids:
            del self._running[qid]

    def _check_cancel_requests(self) -> None:
        """Check if any running jobs have been requested to cancel."""
        for queue_id, job in list(self._running.items()):
            if get_cancel_requested(self.allowed_root, queue_id):
                logger.info("Cancelling running job: %s", queue_id)
                _terminate_process(job.process)
                try:
                    job.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                # Mark as cancelled in the queue
                from .queue_store import _now_iso
                with _acquire_queue_lock(self.allowed_root):
                    entries = _load_entries(self.allowed_root)
                    for entry in entries:
                        if entry.get("queue_id") == queue_id:
                            entry["status"] = QueueStatus.CANCELLED.value
                            entry["finished_at"] = _now_iso()
                            break
                    _save_entries(self.allowed_root, entries)
                del self._running[queue_id]

    # -- Shutdown ---------------------------------------------------------

    def _shutdown_all(self) -> None:
        """Terminate all running jobs on worker shutdown."""
        if not self._running:
            return
        logger.info("Shutting down %d running job(s)...", len(self._running))
        for queue_id, job in self._running.items():
            _terminate_process(job.process)
            # Re-mark as pending so they can be picked up again
            with _acquire_queue_lock(self.allowed_root):
                entries = _load_entries(self.allowed_root)
                for entry in entries:
                    if entry.get("queue_id") == queue_id and entry.get("status") == QueueStatus.RUNNING.value:
                        entry["status"] = QueueStatus.PENDING.value
                        entry["started_at"] = None
                        break
                _save_entries(self.allowed_root, entries)
        self._running.clear()

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
    pid_path = allowed_root / WORKER_PID_FILE
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (ValueError, OSError):
        return None

    from .lock_utils import is_process_alive
    if not is_process_alive(pid):
        try:
            pid_path.unlink()
        except OSError:
            pass
        return None
    return pid
