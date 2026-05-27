from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any

from chemstack.core.commands import queue as _shared_queue
from chemstack.core.config.engines import (
    default_crest_config_path as default_config_path,
    load_crest_config as load_config,
)
from chemstack.core.notifications.engines import (
    notify_crest_job_finished as notify_job_finished,
    notify_crest_job_started as notify_job_started,
)
from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.types import QueueStatus
from chemstack.core.queue.worker import (
    BackgroundRunningJob as _RunningJob,
    ChildProcessQueueWorker,
    QueueWorkerPidFileMixin,
    config_path_for_worker,
    dequeue_next_across_roots,
    make_child_queue_worker_deps,
    read_worker_pid_file,
    reconcile_orphaned_child_queue_entries,
    reserve_dequeued_entry,
    reserve_engine_queue_worker_slot,
    resolve_admission_root,
    shutdown_child_process_with_grace,
)
from chemstack.core.utils import now_utc_iso

from .job_locations import runtime_roots_for_cfg, upsert_job_record
from .runner import finalize_crest_job, start_crest_job
from .worker_execution import (
    WorkerExecutionDependencies,
    _mark_recovery_pending_entry,
    _molecule_key,
    _terminate_process,
    _write_execution_artifacts,
    _write_running_state,
    build_worker_execution_dependencies,
    build_worker_child_command,
    process_dequeued_entry,
)

POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0


def _queue_worker_deps() -> Any:
    return make_child_queue_worker_deps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time_module=time,
        release_slot_fn=release_slot,
        reserve_dequeued_entry_fn=reserve_dequeued_entry,
        admission_root_fn=_admission_root_for_cfg,
        dequeue_next_entry_fn=dequeue_next_entry,
        start_background_job_process_fn=_start_background_job_process,
        try_reserve_admission_slot_fn=_try_reserve_admission_slot,
    )


_queue_runtime = _shared_queue.QueueRuntime(
    load_config_fn=lambda path: load_config(path),
    runtime_roots_for_cfg_fn=lambda cfg: runtime_roots_for_cfg(cfg),
    list_queue_fn=lambda root: list_queue(root),
    dequeue_next_fn=lambda root: dequeue_next(root),
    dequeue_next_across_roots_fn=lambda roots, **kwargs: dequeue_next_across_roots(
        roots,
        **kwargs,
    ),
)


def queue_roots(cfg: Any) -> tuple[Path, ...]:
    return _queue_runtime.queue_roots(cfg)


def queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    return _queue_runtime.queue_entries_with_roots(cfg)


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return _queue_runtime.dequeue_next_entry(cfg)


def _admission_root_for_cfg(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_engine_queue_worker_slot(
        cfg,
        engine="crest",
        reserve_slot_fn=reserve_slot,
    )


def _worker_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        now_utc_iso_fn=now_utc_iso,
        get_cancel_requested_fn=get_cancel_requested,
        start_crest_job_fn=start_crest_job,
        finalize_crest_job_fn=finalize_crest_job,
        terminate_process_fn=_terminate_process,
        write_running_state_fn=_write_running_state,
        write_execution_artifacts_fn=_write_execution_artifacts,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
        upsert_job_record_fn=upsert_job_record,
        notify_job_started_fn=notify_job_started,
        notify_job_finished_fn=notify_job_finished,
    )


def _process_one(cfg: Any) -> str:
    def execute(queue_root: Path, entry: Any) -> Any:
        return process_dequeued_entry(
            cfg,
            entry,
            queue_root=queue_root,
            molecule_key_resolver=_molecule_key,
            dependencies=_worker_dependencies(),
        )

    def print_outcome(entry: Any, outcome: Any) -> None:
        print(f"queue_id: {entry.queue_id}")
        print(f"job_id: {entry.task_id}")
        print(f"status: {outcome.result.status}")
        print(f"reason: {outcome.result.reason}")

    return _queue_runtime.process_one(
        cfg,
        reserve_slot_fn=_try_reserve_admission_slot,
        admission_root_fn=_admission_root_for_cfg,
        execute_entry_fn=execute,
        after_execute_fn=print_outcome,
        release_slot_fn=release_slot,
    )


def read_worker_pid(allowed_root: Path) -> int | None:
    return read_worker_pid_file(allowed_root, WORKER_PID_FILE)


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
) -> subprocess.Popen[str]:
    del admission_root
    return subprocess.Popen(
        build_worker_child_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_token=admission_token,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )


class QueueWorker(QueueWorkerPidFileMixin, ChildProcessQueueWorker):
    worker_pid_file_name = WORKER_PID_FILE

    def __init__(
        self,
        cfg: Any,
        config_path: str,
        *,
        max_concurrent: int,
    ) -> None:
        super().__init__(
            cfg,
            config_path=str(config_path).strip() or default_config_path(),
            max_concurrent=max(1, int(max_concurrent)),
            deps=_queue_worker_deps(),
        )
        self.allowed_root = Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
        self.admission_root = Path(_admission_root_for_cfg(cfg)).expanduser().resolve()

    def _before_run(self) -> None:
        self._write_pid_file()
        self._reconcile_orphaned_running()

    def _after_run(self) -> None:
        self._remove_pid_file()

    def _reconcile_orphaned_running(self) -> None:
        reconcile_orphaned_child_queue_entries(
            self.cfg,
            admission_root=self.admission_root,
            queue_roots_fn=queue_roots,
            list_queue_fn=list_queue,
            list_slots_fn=list_slots,
            reconcile_stale_slots_fn=reconcile_stale_slots,
            running_status=QueueStatus.RUNNING,
            mark_cancelled_fn=mark_cancelled,
            requeue_running_entry_fn=requeue_running_entry,
            mark_recovery_pending_fn=lambda cfg, entry: _mark_recovery_pending_entry(
                cfg,
                entry,
                reason="crashed_recovery",
            ),
        )

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        mark_failed(queue_root, entry.queue_id, error=str(exc))
        release_slot(self.admission_root, admission_token)

    def _on_worker_process_started(
        self,
        queue_root: Path,
        entry: Any,
        *,
        process: subprocess.Popen[str],
        admission_token: str,
    ) -> bool:
        job_dir_text = str(getattr(entry, "metadata", {}).get("job_dir", "")).strip()
        attached = activate_reserved_slot(
            self.admission_root,
            admission_token,
            owner_pid=process.pid,
            source="chemstack.crest.queue_worker.child",
            queue_id=entry.queue_id,
            work_dir=job_dir_text or None,
        )
        if attached is None:
            _terminate_process(process)
            mark_failed(queue_root, entry.queue_id, error="admission_slot_missing")
            release_slot(self.admission_root, admission_token)
            return False
        return True

    def _finalize_completed_job(self, _queue_id: str, job: Any, rc: int) -> None:
        self._finalize_child_exit(job, rc=rc)

    def _finalize_child_exit(self, job: _RunningJob, *, rc: int) -> None:
        current = _find_queue_entry(job.queue_root, job.entry.queue_id)
        if current is not None and getattr(current, "status", None) == QueueStatus.RUNNING:
            if self._shutdown_requested:
                if getattr(current, "cancel_requested", False):
                    mark_cancelled(job.queue_root, current.queue_id, error="cancel_requested")
                else:
                    requeue_running_entry(job.queue_root, current.queue_id)
                    _mark_recovery_pending_entry(self.cfg, job.entry, reason="worker_shutdown")
            elif getattr(current, "cancel_requested", False):
                mark_cancelled(job.queue_root, current.queue_id, error="cancel_requested")
            else:
                mark_failed(job.queue_root, current.queue_id, error=f"worker_child_exit_code={rc}")
        release_slot(self.admission_root, job.admission_token)

    def _shutdown_running_job(self, _queue_id: str, job: Any) -> None:
        shutdown_child_process_with_grace(
            job,
            terminate_process_fn=_terminate_process,
            finalize_child_exit_fn=lambda current_job, rc: self._finalize_child_exit(
                current_job,
                rc=rc,
            ),
            grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
            sleep_fn=time.sleep,
        )


def cmd_queue_worker(args: Any) -> int:
    return _shared_queue.run_queue_worker_command(
        args,
        load_config_fn=lambda path: load_config(path),
        config_path_fn=lambda worker_args: config_path_for_worker(
            worker_args,
            default_config_path_fn=default_config_path,
        ),
        existing_pid_fn=lambda cfg: read_worker_pid(
            Path(str(cfg.runtime.allowed_root)).expanduser().resolve(),
        ),
        max_concurrent_fn=lambda cfg: max(1, int(getattr(cfg.runtime, "max_concurrent", 1))),
        worker_factory=lambda cfg, config_path, **kwargs: QueueWorker(
            cfg,
            config_path,
            **kwargs,
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.queue_runtime")
    parser.add_argument("--config", required=True)
    parser.add_argument("--auto-organize", action="store_true")
    parser.add_argument("--no-auto-organize", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
