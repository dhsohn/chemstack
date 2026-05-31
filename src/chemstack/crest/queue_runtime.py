from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any

from chemstack.core.config.engines import (
    default_shared_config_path as default_config_path,
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
    execution as _queue_execution,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.worker import (
    BackgroundRunningJob as _RunningJob,
    HookedPidFileChildProcessQueueWorker,
    config_path_for_worker,
    reconcile_orphaned_child_queue_entries,
    start_background_process,
)
from chemstack.core.queue.internal_engine import InternalEngineQueueRuntime, InternalEngineSpec
from chemstack.core.utils import now_utc_iso

from . import queue_admission as _queue_admission
from . import queue_lifecycle as _queue_lifecycle
from .job_locations import runtime_roots_for_cfg, upsert_job_record
from .runner import finalize_crest_job, start_crest_job
from .worker_execution import (
    WorkerArtifactDependencies,
    WorkerExecutionDependencies,
    WorkerQueueDependencies,
    WorkerRunnerDependencies,
    WorkerTimingDependencies,
    WorkerTrackingDependencies,
    _mark_recovery_pending_entry,
    _terminate_process,
    _write_execution_artifacts,
    _write_running_state,
    build_worker_execution_dependencies,
    build_worker_child_command,
)

POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "crest_queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0
_ENGINE_SPEC = InternalEngineSpec(
    engine="crest",
    worker_job_module="chemstack.crest.worker_execution",
    worker_pid_file_name=WORKER_PID_FILE,
)


def _queue_worker_deps() -> Any:
    return _engine_runtime.child_worker_deps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time_module=time,
        release_slot_fn=release_slot,
        start_background_job_process_fn=_start_background_job_process,
        try_reserve_admission_slot_fn=_try_reserve_admission_slot,
    )


_engine_runtime = InternalEngineQueueRuntime.create(
    spec=_ENGINE_SPEC,
    load_config=load_config,
    runtime_roots_for_cfg=runtime_roots_for_cfg,
    list_queue=lambda root: list_queue(root),
    dequeue_next=lambda root: dequeue_next(root),
)


def queue_roots(cfg: Any) -> tuple[Path, ...]:
    return _engine_runtime.queue_roots(cfg)


def queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    return _engine_runtime.queue_entries_with_roots(cfg)


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    return _engine_runtime.queue_entry_by_id(queue_root, queue_id)


def dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return _engine_runtime.dequeue_next_entry(cfg)


def _admission_root_for_cfg(cfg: Any) -> str:
    return _engine_runtime.admission_root(cfg)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return _engine_runtime.reserve_admission_slot(
        cfg,
        reserve_slot_fn=reserve_slot,
    )


def _worker_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        timing=WorkerTimingDependencies(now_utc_iso=now_utc_iso),
        queue=WorkerQueueDependencies(
            get_cancel_requested=get_cancel_requested,
            mark_completed=mark_completed,
            mark_cancelled=mark_cancelled,
            mark_failed=mark_failed,
        ),
        runner=WorkerRunnerDependencies(
            start_crest_job=start_crest_job,
            finalize_crest_job=finalize_crest_job,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=1,
        ),
        artifacts=WorkerArtifactDependencies(
            write_running_state=_write_running_state,
            write_execution_artifacts=_write_execution_artifacts,
        ),
        tracking=WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
            notify_job_finished=notify_job_finished,
        ),
    )


def read_worker_pid(allowed_root: Path) -> int | None:
    return _engine_runtime.read_worker_pid(allowed_root)


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
) -> subprocess.Popen[str]:
    return _engine_runtime.start_child_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
        start_background_process_fn=start_background_process,
        build_worker_child_command_fn=build_worker_child_command,
    )


def _reconcile_orphaned_running(worker: Any) -> None:
    _queue_lifecycle.reconcile_orphaned_running(
        worker.cfg,
        admission_root=worker.admission_root,
        queue_roots_fn=queue_roots,
        list_queue_fn=list_queue,
        list_slots_fn=list_slots,
        reconcile_stale_slots_fn=reconcile_stale_slots,
        reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries,
        mark_cancelled_fn=mark_cancelled,
        requeue_running_entry_fn=requeue_running_entry,
        mark_recovery_pending_fn=_mark_recovery_pending_entry,
    )


def _reconcile_worker_state(worker: Any) -> None:
    _reconcile_orphaned_running(worker)


def _handle_worker_start_error(
    worker: Any,
    queue_root: Path,
    entry: Any,
    admission_token: str,
    exc: OSError,
) -> None:
    _queue_admission.mark_worker_start_error(
        queue_root=queue_root,
        entry=entry,
        admission_token=admission_token,
        exc=exc,
        mark_entry_failed_and_release_fn=worker._mark_entry_failed_and_release,
        mark_failed_fn=mark_failed,
    )


def _finalize_completed_job(worker: Any, _queue_id: str, job: Any, rc: int) -> None:
    _finalize_child_exit(worker, job, rc=rc)


def _finalize_child_exit(worker: Any, job: _RunningJob, *, rc: int) -> None:
    _queue_lifecycle.finalize_child_exit(
        worker.cfg,
        job,
        rc=rc,
        shutdown_requested=worker._shutdown_requested,
        find_queue_entry_fn=_find_queue_entry,
        mark_cancelled_fn=mark_cancelled,
        requeue_running_entry_fn=requeue_running_entry,
        mark_failed_fn=mark_failed,
        mark_recovery_pending_fn=_mark_recovery_pending_entry,
        release_admission_slot_fn=worker._release_admission_slot,
    )


def _queue_worker_hooks() -> Any:
    return _engine_runtime.child_worker_hooks(
        handle_worker_start_error_fn=_handle_worker_start_error,
        finalize_completed_job_fn=_finalize_completed_job,
        finalize_child_exit_fn=_finalize_child_exit,
        reconcile_worker_state_fn=_reconcile_worker_state,
        activate_reserved_slot_fn=lambda *args, **kwargs: activate_reserved_slot(
            *args,
            **kwargs,
        ),
        terminate_process_fn=lambda process: _terminate_process(process),
        mark_failed_fn=lambda *args, **kwargs: mark_failed(*args, **kwargs),
        shutdown_grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
        sleep_fn=lambda seconds: time.sleep(seconds),
    )


class QueueWorker(HookedPidFileChildProcessQueueWorker):
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
            hooks=_queue_worker_hooks(),
            worker_pid_file_name=WORKER_PID_FILE,
            admission_root=_admission_root_for_cfg(cfg),
        )

    def _reconcile_orphaned_running(self) -> None:
        _reconcile_orphaned_running(self)

    def _finalize_child_exit(self, job: _RunningJob, *, rc: int) -> None:
        _finalize_child_exit(self, job, rc=rc)


def cmd_queue_worker(args: Any) -> int:
    return _engine_runtime.run_pidfile_worker_command(
        args,
        config_path_fn=lambda worker_args: config_path_for_worker(
            worker_args,
            default_config_path_fn=default_config_path,
        ),
        load_config_fn=load_config,
        read_worker_pid_fn=read_worker_pid,
        worker_factory=lambda cfg, config_path, **kwargs: QueueWorker(
            cfg,
            config_path,
            **kwargs,
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.queue_runtime")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
