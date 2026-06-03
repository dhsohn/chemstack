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
    engine_execution as _engine_execution,
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
from chemstack.core.queue.internal_engine import (
    InternalEngineQueueModule,
    InternalEngineQueueWorkerDeps,
    InternalEngineSpec,
    internal_engine_queue_worker_deps_from_namespace,
)
from chemstack.core.utils import now_utc_iso

from . import queue_admission as _queue_admission
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

# Keep queue_runtime.subprocess available for tests/callers that patch Popen.
_SUBPROCESS_MODULE = subprocess
POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "crest_queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0
# The queue worker dependency adapter resolves these by name from globals().
_RUNTIME_FACADE_DEPENDENCY_SYMBOLS: tuple[Any, ...] = (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
    requeue_running_entry,
    config_path_for_worker,
    reconcile_orphaned_child_queue_entries,
    start_background_process,
    _mark_recovery_pending_entry,
    build_worker_child_command,
)
_ENGINE_SPEC = InternalEngineSpec(
    engine="crest",
    worker_job_module="chemstack.crest.worker_execution",
    worker_pid_file_name=WORKER_PID_FILE,
)


def _queue_worker_deps() -> Any:
    return _queue_module.queue_worker_deps()


def _runtime_facade_deps() -> InternalEngineQueueWorkerDeps:
    return internal_engine_queue_worker_deps_from_namespace(
        globals(),
        find_queue_entry_name="_find_queue_entry",
    )


_queue_module = InternalEngineQueueModule.create(
    spec=_ENGINE_SPEC,
    load_config=load_config,
    runtime_roots_for_cfg=runtime_roots_for_cfg,
    list_queue=lambda root: list_queue(root),
    dequeue_next=lambda root: dequeue_next(root),
    poll_interval_seconds=POLL_INTERVAL_SECONDS,
    shutdown_grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
    deps=_runtime_facade_deps(),
)
_engine_runtime = _queue_module.runtime

queue_roots = _queue_module.queue_roots
queue_entries_with_roots = _queue_module.queue_entries_with_roots
_find_queue_entry = _queue_module.queue_entry_by_id
dequeue_next_entry = _queue_module.dequeue_next_entry
_admission_root_for_cfg = _queue_module.admission_root


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return _queue_module.try_reserve_admission_slot(cfg)


def _worker_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        timing=_engine_execution.build_internal_worker_timing_dependencies(
            WorkerTimingDependencies,
            now_utc_iso=now_utc_iso,
        ),
        queue=_engine_execution.build_internal_worker_queue_dependencies(
            WorkerQueueDependencies,
            get_cancel_requested=get_cancel_requested,
            mark_completed=mark_completed,
            mark_cancelled=mark_cancelled,
            mark_failed=mark_failed,
        ),
        runner=_engine_execution.build_internal_worker_process_dependencies(
            WorkerRunnerDependencies,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=1,
            start_crest_job=start_crest_job,
            finalize_crest_job=finalize_crest_job,
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


read_worker_pid = _queue_module.read_worker_pid


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
) -> Any:
    return _queue_module.start_background_job_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
    )


def _config_path_for_worker(args: Any) -> str:
    return _queue_module.config_path_for_worker(args)


def _reconcile_orphaned_running(worker: Any) -> None:
    _queue_module.reconcile_orphaned_running(worker)


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
    _queue_module.finalize_child_exit(worker, job, rc=rc)


def _queue_worker_hooks() -> Any:
    return _queue_module.queue_worker_hooks()


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
    return _queue_module.run_pidfile_worker_command(
        args,
        config_path_fn=_config_path_for_worker,
        config_path_keyword=False,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.queue_runtime")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
