from __future__ import annotations

import argparse
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.config.engines import (
    default_shared_config_path as default_config_path,
)
from chemstack.core.config.engines import (
    load_crest_config as load_config,
)
from chemstack.core.engines.crest_execution import (
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
    build_worker_child_command,
    build_worker_execution_dependencies,
)
from chemstack.core.engines.queue_worker import EngineQueueWorker
from chemstack.core.notifications.engines import (
    notify_crest_job_finished as notify_job_finished,
)
from chemstack.core.notifications.engines import (
    notify_crest_job_started as notify_job_started,
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
from chemstack.core.queue import (
    engine_execution as _engine_execution,
)
from chemstack.core.queue import (
    execution as _queue_execution,
)
from chemstack.core.queue.internal_engine import (
    InternalEngineQueueModule,
    InternalEngineQueueWorkerDeps,
    InternalEngineSpec,
)
from chemstack.core.queue.worker import (
    BackgroundRunningJob as _RunningJob,
)
from chemstack.core.queue.worker import (
    config_path_for_worker,
    reconcile_orphaned_child_queue_entries,
    start_background_process,
)
from chemstack.core.utils import now_utc_iso

from . import queue_admission as _queue_admission
from .engine import ENGINE_DEFINITION
from .job_locations import runtime_roots_for_cfg, upsert_job_record
from .runner import finalize_crest_job, start_crest_job

# Keep queue_runtime.subprocess available for tests/callers that patch Popen.
_SUBPROCESS_MODULE = subprocess
POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "crest_queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0
_ENGINE_SPEC = InternalEngineSpec(
    engine="crest",
    worker_job_module="chemstack.core.engines.crest_execution",
    worker_pid_file_name=WORKER_PID_FILE,
)


def _queue_worker_deps() -> Any:
    return _queue_module.queue_worker_deps()


def _runtime_facade_deps() -> InternalEngineQueueWorkerDeps:
    return InternalEngineQueueWorkerDeps(
        time_module=time,
        release_slot=lambda root, token: release_slot(root, token),
        reserve_slot=lambda *args, **kwargs: reserve_slot(*args, **kwargs),
        start_background_process=lambda command: start_background_process(command),
        build_worker_child_command=lambda **kwargs: build_worker_child_command(**kwargs),
        config_path_for_worker=lambda args, *, default_config_path_fn: config_path_for_worker(
            args,
            default_config_path_fn=default_config_path_fn,
        ),
        default_config_path=lambda: default_config_path(),
        activate_reserved_slot=lambda *args, **kwargs: activate_reserved_slot(*args, **kwargs),
        terminate_process=lambda process: _terminate_process(process),
        mark_failed=lambda *args, **kwargs: mark_failed(*args, **kwargs),
        handle_worker_start_error=lambda *args, **kwargs: _handle_worker_start_error(
            *args,
            **kwargs,
        ),
        finalize_completed_job=lambda *args, **kwargs: _finalize_completed_job(
            *args,
            **kwargs,
        ),
        finalize_child_exit=lambda *args, **kwargs: _finalize_child_exit(*args, **kwargs),
        reconcile_worker_state=lambda worker: _reconcile_worker_state(worker),
        list_queue=lambda root: list_queue(root),
        list_slots=lambda root: list_slots(root),
        reconcile_stale_slots=lambda root: reconcile_stale_slots(root),
        reconcile_orphaned_child_queue_entries=lambda *args, **kwargs: (
            reconcile_orphaned_child_queue_entries(*args, **kwargs)
        ),
        mark_cancelled=lambda *args, **kwargs: mark_cancelled(*args, **kwargs),
        requeue_running_entry=lambda *args, **kwargs: requeue_running_entry(*args, **kwargs),
        mark_recovery_pending=lambda *args, **kwargs: _mark_recovery_pending_entry(
            *args,
            **kwargs,
        ),
        load_config=lambda config_path: load_config(config_path),
        read_worker_pid=lambda allowed_root: _queue_module.read_worker_pid(allowed_root),
        worker_class=lambda *args, **kwargs: QueueWorker(*args, **kwargs),
        try_reserve_admission_slot=lambda cfg: _try_reserve_admission_slot(cfg),
        start_background_job_process_fn=lambda **kwargs: _start_background_job_process(**kwargs),
        find_queue_entry=lambda root, queue_id: _find_queue_entry(root, queue_id),
    )


def _queue_runtime_definition() -> Any:
    queue_functions = ENGINE_DEFINITION.queue_functions
    if queue_functions is None:
        return ENGINE_DEFINITION
    return replace(
        ENGINE_DEFINITION,
        queue_functions=replace(
            queue_functions,
            runtime_roots_for_cfg=lambda cfg: runtime_roots_for_cfg(cfg),
            list_queue=lambda root: list_queue(root),
            dequeue_next=lambda root: dequeue_next(root),
        ),
    )


_queue_module = InternalEngineQueueModule.create_from_definition(
    definition=_queue_runtime_definition(),
    spec=_ENGINE_SPEC,
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


def QueueWorker(
    cfg: Any,
    config_path: str | None = None,
    *,
    max_concurrent: int | None = None,
) -> EngineQueueWorker:
    raw_max_concurrent: Any = (
        max_concurrent if max_concurrent is not None else getattr(cfg.runtime, "max_concurrent", 1)
    )
    configured_max = max(1, int(raw_max_concurrent))
    return EngineQueueWorker(
        cfg,
        config_path=str(config_path or "").strip() or default_config_path(),
        engine="crest",
        max_concurrent=configured_max,
        deps=_queue_worker_deps(),
        hooks=_queue_worker_hooks(),
        worker_pid_file_name=WORKER_PID_FILE,
        admission_root=_admission_root_for_cfg(cfg),
        finalize_child_exit=_finalize_child_exit,
        reconcile_orphaned_running=_reconcile_orphaned_running,
    )


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
