from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any, Callable

from chemstack.core.config.engines import (
    default_shared_config_path as default_config_path,
    load_xtb_config as load_config,
)
from chemstack.core.notifications.engines import (
    notify_xtb_job_finished as notify_job_finished,
    notify_xtb_job_started as notify_job_started,
)
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.queue import (
    dequeue_next,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.worker import (
    BackgroundRunningJob,
    HookedPidFileChildProcessQueueWorker,
    ManagedProcess as _ManagedProcess,
    config_path_for_worker,
    pid_is_alive as worker_pid_is_alive,
    reconcile_orphaned_child_queue_entries,
    start_background_process,
    terminate_process_group,
)
from chemstack.core.queue.internal_engine import (
    InternalEngineQueueModule,
    InternalEngineQueueWorkerDeps,
    InternalEngineSpec,
    internal_engine_queue_worker_deps_from_namespace,
)

from . import queue_admission as _queue_admission
from . import queue_lifecycle as _queue_lifecycle
from . import worker_execution as _worker_execution
from . import worker_terminal as _worker_terminal
from .job_locations import (
    runtime_roots_for_cfg,
    upsert_job_record,
)
from .runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from .state import (
    load_organized_ref,
    load_report_json,
    load_state,
)
from . import queue_artifacts as _queue_artifacts
from . import queue_terminal as _queue_terminal

# Keep queue_runtime.subprocess available for tests/callers that patch Popen.
_SUBPROCESS_MODULE = subprocess
# Keep these module globals present for the late-bound queue worker deps factory.
_LATE_BOUND_QUEUE_WORKER_DEPS = (
    default_config_path,
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
    requeue_running_entry,
    config_path_for_worker,
    reconcile_orphaned_child_queue_entries,
    start_background_process,
)
POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_PID_FILE = "xtb_queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0
WORKER_JOB_MODULE = _worker_execution.WORKER_JOB_MODULE
_ENGINE_SPEC = InternalEngineSpec(
    engine="xtb",
    worker_job_module=WORKER_JOB_MODULE,
    worker_pid_file_name=WORKER_PID_FILE,
    coerce_queue_root_to_str=True,
    include_legacy_admission_root_arg=True,
)


def _queue_worker_deps() -> Any:
    return _runtime_facade.queue_worker_deps()


def _worker_execution_dependencies() -> _worker_execution.WorkerExecutionDependencies:
    return _worker_execution.build_worker_execution_dependencies(
        config=_worker_execution.WorkerConfigDependencies(
            load_config=load_config,
            queue_entry_by_id=_queue_entry_by_id,
        ),
        admission=_worker_execution.WorkerAdmissionDependencies(
            activate_reserved_slot=activate_reserved_slot,
            release_slot=release_slot,
        ),
        context=_worker_execution.WorkerContextDependencies(
            job_dir=_job_dir,
            selected_xyz=_selected_xyz,
            job_type=_job_type,
            reaction_key=_reaction_key,
            input_summary=_input_summary,
            entry_resource_request=_queue_artifacts.entry_resource_request,
            matching_state=_worker_execution_hooks.matching_state,
            is_recovery_pending=_worker_execution.is_recovery_pending,
        ),
        artifacts=_worker_execution.WorkerArtifactDependencies(
            write_running_state=_write_running_state,
            build_terminal_result=_build_terminal_result,
            finalize_execution_result=_finalize_execution_result,
        ),
        tracking=_worker_execution.WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
        ),
        runner=_worker_execution.WorkerRunnerDependencies(
            run_xtb_ranking_job=run_xtb_ranking_job,
            start_xtb_job=start_xtb_job,
            finalize_xtb_job=finalize_xtb_job,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        ),
        execute_queue_entry_fn=_execute_queue_entry,
    )


_RunningJob = BackgroundRunningJob
_TerminalSummary = _queue_terminal.TerminalSummary


def _runtime_facade_deps() -> InternalEngineQueueWorkerDeps:
    return internal_engine_queue_worker_deps_from_namespace(
        globals(),
        mark_recovery_pending_name="_mark_recovery_pending_state",
        find_queue_entry_name="_queue_entry_by_id",
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
_runtime_facade = _queue_module.facade

queue_roots = _queue_module.queue_roots
queue_entries_with_roots = _queue_module.queue_entries_with_roots
dequeue_next_entry = _queue_module.dequeue_next_entry
_queue_entry_by_id = _queue_module.queue_entry_by_id
_admission_root = _queue_module.admission_root


def _pid_is_alive(pid: int) -> bool:
    return worker_pid_is_alive(pid)


_worker_execution_hooks = _worker_execution.default_worker_execution_hooks()
_job_dir = _worker_execution_hooks.job_dir
_selected_xyz = _worker_execution_hooks.selected_xyz
_job_type = _worker_execution_hooks.job_type
_reaction_key = _worker_execution_hooks.reaction_key
_input_summary = _worker_execution_hooks.input_summary

_write_execution_artifacts = _worker_terminal.write_execution_artifacts
_write_running_state = _worker_terminal.write_running_state
_build_terminal_result = _worker_terminal.build_terminal_result
build_worker_child_command = _worker_execution.build_worker_child_command


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    _worker_execution._mark_recovery_pending_entry(cfg, entry, reason=reason)


def _terminate_process(proc: _ManagedProcess) -> None:
    terminate_process_group(proc)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return _runtime_facade.try_reserve_admission_slot(cfg)


def _print_terminal_summary(summary: _TerminalSummary) -> None:
    _queue_terminal.print_terminal_summary(summary)


def _load_terminal_summary(
    queue_root: Path, entry: Any, *, rc: int | None = None
) -> _TerminalSummary:
    return _queue_terminal.load_terminal_summary(
        queue_root,
        entry,
        rc=rc,
        job_dir_fn=_job_dir,
        load_state_fn=load_state,
        load_report_json_fn=load_report_json,
        load_organized_ref_fn=load_organized_ref,
        queue_entry_by_id_fn=_queue_entry_by_id,
    )


def _ensure_terminal_queue_status(queue_root: Path, entry: Any, summary: _TerminalSummary) -> None:
    _queue_terminal.ensure_terminal_queue_status(
        queue_root,
        entry,
        summary,
        queue_entry_by_id_fn=_queue_entry_by_id,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
    )


def _finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> _worker_execution.WorkerExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
        outcome_cls=_worker_execution.WorkerExecutionOutcome,
        write_execution_artifacts_fn=_write_execution_artifacts,
        selected_xyz_fn=_selected_xyz,
        job_dir_fn=_job_dir,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
        upsert_job_record_fn=upsert_job_record,
        notify_job_finished_fn=notify_job_finished,
    )


def _execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
) -> _worker_execution.WorkerExecutionOutcome:
    return _worker_execution.execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        worker_job_pid=worker_job_pid,
        emit_output=emit_output,
        dependencies=_worker_execution_dependencies(),
    )


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
) -> Any:
    return _runtime_facade.start_background_job_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
    )


def _config_path_for_worker(args: Any) -> str:
    return _runtime_facade.config_path_for_worker(args)


read_worker_pid = _queue_module.read_worker_pid


def _entry_status_is_running(entry: Any) -> bool:
    return _queue_lifecycle.entry_status_is_running(entry)


def _handle_worker_start_error(
    worker: Any,
    queue_root: Path,
    entry: Any,
    admission_token: str,
    exc: OSError,
) -> None:
    _queue_admission.finalize_worker_start_error(
        worker.cfg,
        queue_root=queue_root,
        entry=entry,
        admission_token=admission_token,
        exc=exc,
        release_admission_slot_fn=worker._release_admission_slot,
        build_terminal_result_fn=_build_terminal_result,
        finalize_execution_result_fn=_finalize_execution_result,
        job_dir_fn=_job_dir,
        selected_xyz_fn=_selected_xyz,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        input_summary_fn=_input_summary,
        entry_resource_request_fn=_queue_artifacts.entry_resource_request,
    )


def _finalize_completed_job(worker: Any, _queue_id: str, job: Any, rc: int) -> None:
    summary = _load_terminal_summary(job.queue_root, job.entry, rc=rc)
    _ensure_terminal_queue_status(job.queue_root, job.entry, summary)
    _print_terminal_summary(summary)
    worker._release_admission_slot(job.admission_token)


def _finalize_child_exit(worker: Any, job: _RunningJob, *, rc: int) -> None:
    _runtime_facade.finalize_child_exit(worker, job, rc=rc)


def _sync_terminal_running_entries(worker: Any) -> None:
    _queue_lifecycle.sync_terminal_running_entries(
        queue_entries_with_roots(worker.cfg),
        load_terminal_summary_fn=_load_terminal_summary,
        ensure_terminal_queue_status_fn=_ensure_terminal_queue_status,
    )


def _live_worker_pid_slots(worker: Any) -> list[Any]:
    return _queue_lifecycle.live_worker_pid_slots(
        queue_entries_with_roots(worker.cfg),
        load_state_fn=load_state,
        job_dir_fn=_job_dir,
        pid_is_alive_fn=_pid_is_alive,
    )


def _list_slots_preserving_live_worker_pids(
    worker: Any,
    admission_root: str | Path,
) -> list[Any]:
    return [
        *list_slots(admission_root),
        *_live_worker_pid_slots(worker),
    ]


def _reconcile_orphaned_running(worker: Any) -> None:
    _runtime_facade.reconcile_orphaned_running(
        worker,
        list_slots_fn=lambda admission_root: _list_slots_preserving_live_worker_pids(
            worker,
            admission_root,
        ),
    )


def _reconcile_worker_state(worker: Any) -> None:
    _sync_terminal_running_entries(worker)
    _reconcile_orphaned_running(worker)


def _queue_worker_hooks() -> Any:
    return _runtime_facade.queue_worker_hooks()


class QueueWorker(HookedPidFileChildProcessQueueWorker):
    worker_pid_file_name = WORKER_PID_FILE

    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(
            cfg,
            config_path=config_path,
            max_concurrent=max_concurrent,
            deps=_queue_worker_deps(),
            hooks=_queue_worker_hooks(),
            worker_pid_file_name=WORKER_PID_FILE,
            admission_root=_admission_root(cfg),
        )


def cmd_queue_worker(args: Any) -> int:
    return _runtime_facade.run_pidfile_worker_command(
        args,
        config_path_fn=_config_path_for_worker,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.xtb.queue_runtime")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
