from __future__ import annotations

import argparse
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import activate_reserved_slot, release_slot
from chemstack.core.queue import (
    list_queue,
)
from chemstack.core.queue import child_entrypoint as _child_entrypoint
from chemstack.core.queue.dependencies import dependency_group
from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.queue.engine_execution import (
    CancellableProcessExecution,
)
from chemstack.core.config.engines import load_xtb_config as load_config
from chemstack.core.notifications.engines import (
    notify_xtb_job_started as notify_job_started,
)
from chemstack.core.queue.worker import terminate_process_group

from . import queue_artifacts as _queue_artifacts
from . import worker_child as _worker_child
from .job_locations import upsert_job_record
from .runner import finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from .state import (
    is_recovery_pending,
    mark_recovery_pending,
)
from .worker_context import (
    WorkerExecutionHooks,
    XtbExecutionContext as _XtbExecutionContext,
    build_execution_context as _build_worker_execution_context,
    default_worker_execution_hooks,
    input_summary as _input_summary,
    job_dir as _job_dir,
    job_type as _job_type,
    matching_state as _matching_state,
    reaction_key as _reaction_key,
    selected_xyz as _selected_xyz,
)
from .worker_terminal import (
    WorkerExecutionOutcome,
    build_terminal_result as _build_terminal_result,
    finalize_execution_result as _finalize_execution_result,
    write_running_state as _write_running_state,
)

WORKER_JOB_MODULE = _worker_child.WORKER_JOB_MODULE
WORKER_CANCEL_SIGNAL = _worker_child.WORKER_CANCEL_SIGNAL
WORKER_SHUTDOWN_EXIT_CODE = _worker_child.WORKER_SHUTDOWN_EXIT_CODE


@dataclass(frozen=True)
class WorkerConfigDependencies:
    load_config: Callable[..., Any]
    queue_entry_by_id: Callable[[Path | str, str], Any | None]


@dataclass(frozen=True)
class WorkerAdmissionDependencies:
    activate_reserved_slot: Callable[..., Any]
    release_slot: Callable[..., Any]


@dataclass(frozen=True)
class WorkerContextDependencies:
    job_dir: Callable[[Any], Path]
    selected_xyz: Callable[[Any], Path]
    job_type: Callable[[Any], str]
    reaction_key: Callable[[Any, Path], str]
    input_summary: Callable[[Any], dict[str, Any]]
    entry_resource_request: Callable[[Any, Any], dict[str, int]]
    matching_state: Callable[..., dict[str, Any]]
    is_recovery_pending: Callable[[dict[str, Any]], bool]


@dataclass(frozen=True)
class WorkerArtifactDependencies:
    write_running_state: Callable[..., Any]
    build_terminal_result: Callable[..., Any]
    finalize_execution_result: Callable[..., Any]


@dataclass(frozen=True)
class WorkerTrackingDependencies:
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., Any]


@dataclass(frozen=True)
class WorkerRunnerDependencies:
    run_xtb_ranking_job: Callable[..., Any]
    start_xtb_job: Callable[..., Any]
    finalize_xtb_job: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    wait_for_cancellable_process: Callable[..., Any]
    sleep: Callable[[float], None]
    cancel_check_interval_seconds: float


@dataclass(frozen=True)
class WorkerExecutionDependencies:
    config: WorkerConfigDependencies
    admission: WorkerAdmissionDependencies
    context: WorkerContextDependencies
    artifacts: WorkerArtifactDependencies
    tracking: WorkerTrackingDependencies
    runner: WorkerRunnerDependencies
    execute_queue_entry: Callable[..., Any] | None = None


def build_worker_execution_dependencies_from_groups(
    *,
    config: WorkerConfigDependencies,
    admission: WorkerAdmissionDependencies,
    context: WorkerContextDependencies,
    artifacts: WorkerArtifactDependencies,
    tracking: WorkerTrackingDependencies,
    runner: WorkerRunnerDependencies,
    execute_queue_entry_fn: Callable[..., Any] | None = None,
) -> WorkerExecutionDependencies:
    return WorkerExecutionDependencies(
        config=config,
        admission=admission,
        context=context,
        artifacts=artifacts,
        tracking=tracking,
        runner=runner,
        execute_queue_entry=execute_queue_entry_fn,
    )


def _queue_entry_by_id(queue_root: Path | str, queue_id: str) -> Any | None:
    return _child_entrypoint.queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue,
    )


def _default_config_dependencies() -> WorkerConfigDependencies:
    return WorkerConfigDependencies(
        load_config=load_config,
        queue_entry_by_id=_queue_entry_by_id,
    )


def _default_admission_dependencies() -> WorkerAdmissionDependencies:
    return WorkerAdmissionDependencies(
        activate_reserved_slot=activate_reserved_slot,
        release_slot=release_slot,
    )


def _default_context_dependencies() -> WorkerContextDependencies:
    return WorkerContextDependencies(
        job_dir=_job_dir,
        selected_xyz=_selected_xyz,
        job_type=_job_type,
        reaction_key=_reaction_key,
        input_summary=_input_summary,
        entry_resource_request=_queue_artifacts.entry_resource_request,
        matching_state=_matching_state,
        is_recovery_pending=is_recovery_pending,
    )


def _default_artifact_dependencies() -> WorkerArtifactDependencies:
    return WorkerArtifactDependencies(
        write_running_state=_write_running_state,
        build_terminal_result=_build_terminal_result,
        finalize_execution_result=_finalize_execution_result,
    )


def _default_tracking_dependencies() -> WorkerTrackingDependencies:
    return WorkerTrackingDependencies(
        upsert_job_record=upsert_job_record,
        notify_job_started=notify_job_started,
    )


def _default_runner_dependencies() -> WorkerRunnerDependencies:
    return WorkerRunnerDependencies(
        run_xtb_ranking_job=run_xtb_ranking_job,
        start_xtb_job=start_xtb_job,
        finalize_xtb_job=finalize_xtb_job,
        terminate_process=terminate_process_group,
        wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
        sleep=time.sleep,
        cancel_check_interval_seconds=1,
    )


def build_worker_execution_dependencies(
    *,
    config: WorkerConfigDependencies | None = None,
    admission: WorkerAdmissionDependencies | None = None,
    context: WorkerContextDependencies | None = None,
    artifacts: WorkerArtifactDependencies | None = None,
    tracking: WorkerTrackingDependencies | None = None,
    runner: WorkerRunnerDependencies | None = None,
    execute_queue_entry_fn: Callable[..., Any] | None = None,
) -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies_from_groups(
        config=dependency_group(config, _default_config_dependencies),
        admission=dependency_group(admission, _default_admission_dependencies),
        context=dependency_group(context, _default_context_dependencies),
        artifacts=dependency_group(artifacts, _default_artifact_dependencies),
        tracking=dependency_group(tracking, _default_tracking_dependencies),
        runner=dependency_group(runner, _default_runner_dependencies),
        execute_queue_entry_fn=execute_queue_entry_fn,
    )


def default_worker_execution_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies()


def _build_execution_context(
    cfg: Any,
    entry: Any,
    *,
    dependencies: WorkerExecutionDependencies,
) -> _XtbExecutionContext:
    return _build_worker_execution_context(
        cfg,
        entry,
        context_deps=dependencies.context,
    )


def _mark_job_running(
    cfg: Any,
    context: _XtbExecutionContext,
    *,
    worker_job_pid: int | None,
    dependencies: WorkerExecutionDependencies,
) -> None:
    artifact_deps = dependencies.artifacts
    tracking_deps = dependencies.tracking
    _engine_execution.mark_engine_job_running(
        cfg,
        entry=context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        resource_request=context.resource_request,
        write_running_state_fn=artifact_deps.write_running_state,
        upsert_job_record_fn=tracking_deps.upsert_job_record,
        notify_job_started_fn=tracking_deps.notify_job_started,
        record_fields={
            "job_type": context.job_type,
            "reaction_key": context.reaction_key,
        },
        notify_fields={
            "job_type": context.job_type,
            "reaction_key": context.reaction_key,
        },
        write_running_state_kwargs={
            "worker_job_pid": worker_job_pid,
            "previous_state": context.previous_state,
            "resumed": context.resumed,
        },
    )


def _mark_recovery_pending_context(
    cfg: Any,
    context: _XtbExecutionContext,
    *,
    reason: str,
) -> None:
    _engine_execution.mark_recovery_pending_and_record(
        cfg,
        entry=context.entry,
        job_dir=context.job_dir,
        selected_input_xyz=context.selected_xyz,
        reason=reason,
        resource_request=context.resource_request,
        mark_recovery_pending_fn=mark_recovery_pending,
        upsert_job_record_fn=upsert_job_record,
        state_identity_fields={
            "job_type": context.job_type,
            "reaction_key": context.reaction_key,
            "input_summary": context.input_summary,
        },
        record_identity_fields={
            "job_type": context.job_type,
            "reaction_key": context.reaction_key,
        },
    )


def _mark_recovery_pending_entry(cfg: Any, entry: Any, *, reason: str) -> None:
    context = _build_execution_context(
        cfg,
        entry,
        dependencies=default_worker_execution_dependencies(),
    )
    _mark_recovery_pending_context(cfg, context, reason=reason)


def _cancelled_before_start_result(
    context: _XtbExecutionContext,
    *,
    dependencies: WorkerExecutionDependencies,
) -> Any:
    return dependencies.artifacts.build_terminal_result(
        context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        input_summary=context.input_summary,
        resource_request=context.resource_request,
        status="cancelled",
        reason="cancel_requested",
        exit_code=1,
    )


def _failed_result_from_exception(
    context: _XtbExecutionContext,
    exc: Exception,
    *,
    dependencies: WorkerExecutionDependencies,
) -> Any:
    return dependencies.artifacts.build_terminal_result(
        context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        input_summary=context.input_summary,
        resource_request=context.resource_request,
        status="failed",
        reason=f"runner_error:{exc}",
        exit_code=1,
    )


def _run_xtb_job_for_entry(
    cfg: Any,
    context: _XtbExecutionContext,
    _queue_root: Path,
    *,
    dependencies: WorkerExecutionDependencies,
    should_cancel: Callable[[], bool] | None,
    register_running_job: Callable[[Any | None], None] | None,
) -> Any:
    runner_deps = dependencies.runner
    try:
        if should_cancel is not None and should_cancel():
            return _cancelled_before_start_result(context, dependencies=dependencies)
        if context.job_type == "ranking":
            return runner_deps.run_xtb_ranking_job(
                cfg,
                job_dir=context.job_dir,
                should_cancel=should_cancel,
                on_running_job=register_running_job,
                terminate_process=runner_deps.terminate_process,
            )

        return _engine_execution.run_cancellable_process_execution(
            CancellableProcessExecution(
                start_job=lambda: runner_deps.start_xtb_job(
                    cfg,
                    job_dir=context.job_dir,
                    selected_input_xyz=context.selected_xyz,
                ),
                finalize_job=runner_deps.finalize_xtb_job,
                terminate_process=runner_deps.terminate_process,
                build_failure_result=lambda exc: _failed_result_from_exception(
                    context,
                    exc,
                    dependencies=dependencies,
                ),
                wait_for_cancellable_process=runner_deps.wait_for_cancellable_process,
                should_cancel=should_cancel,
                sleep=runner_deps.sleep,
                poll_interval_seconds=runner_deps.cancel_check_interval_seconds,
                check_cancel_before_poll=True,
                register_running_job=register_running_job,
            )
        )
    except Exception as exc:
        return _failed_result_from_exception(context, exc, dependencies=dependencies)


def _finalize_processed_entry(
    cfg: Any,
    context: _XtbExecutionContext,
    result: Any,
    queue_root: Path,
    *,
    emit_output: bool,
    dependencies: WorkerExecutionDependencies,
) -> Any:
    return dependencies.artifacts.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=context.entry,
        result=result,
        emit_output=emit_output,
        previous_state=context.previous_state,
        resumed=context.resumed,
    )


def execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
    dependencies: WorkerExecutionDependencies | None = None,
) -> Any:
    deps = dependencies or default_worker_execution_dependencies()
    return _engine_execution.run_engine_worker_entry(
        cfg,
        entry,
        queue_root=queue_root,
        build_context=lambda cfg_obj, entry_obj: _build_execution_context(
            cfg_obj,
            entry_obj,
            dependencies=deps,
        ),
        mark_running=lambda cfg_obj, context: _mark_job_running(
            cfg_obj,
            context,
            worker_job_pid=worker_job_pid,
            dependencies=deps,
        ),
        run_job=lambda cfg_obj, context, active_queue_root: _run_xtb_job_for_entry(
            cfg_obj,
            context,
            active_queue_root,
            dependencies=deps,
            should_cancel=should_cancel,
            register_running_job=register_running_job,
        ),
        finalize_entry=lambda cfg_obj, context, result, active_queue_root: (
            _finalize_processed_entry(
                cfg_obj,
                context,
                result,
                active_queue_root,
                emit_output=emit_output,
                dependencies=deps,
            )
        ),
        build_outcome=lambda _context, _result, outcome: outcome,
    )


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    dependencies: WorkerExecutionDependencies | None = None,
) -> int:
    deps = dependencies or default_worker_execution_dependencies()
    return _worker_child.run_worker_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
        dependencies=deps,
        execute_queue_entry_fn=execute_queue_entry,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        getpid_fn=os.getpid,
        worker_job_module=WORKER_JOB_MODULE,
    )


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str | Path,
    admission_token: str | None = None,
) -> list[str]:
    return _worker_child.build_worker_child_command(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
    )


def build_worker_job_parser() -> argparse.ArgumentParser:
    return _worker_child.build_worker_job_parser()


class _SignalController(_worker_child.SignalController):
    def __init__(self) -> None:
        super().__init__(
            cancel_signal=WORKER_CANCEL_SIGNAL,
            shutdown_exit_code=WORKER_SHUTDOWN_EXIT_CODE,
            terminate_process_fn=lambda proc: terminate_process_group(proc),
            signal_module=signal,
            os_exit_fn=lambda code: os._exit(code),
        )


def main(argv: list[str] | None = None) -> int:
    args = build_worker_job_parser().parse_args(argv)
    controller = _SignalController()
    controller.install()
    return run_worker_job(
        config_path=args.config,
        queue_root=args.queue_root,
        queue_id=args.queue_id,
        admission_root=args.admission_root,
        admission_token=str(args.admission_token).strip() or None,
        should_cancel=controller.should_cancel,
        register_running_job=controller.set_running_job,
    )


__all__ = [
    "build_worker_child_command",
    "build_worker_execution_dependencies",
    "build_worker_execution_dependencies_from_groups",
    "build_worker_job_parser",
    "WorkerAdmissionDependencies",
    "WorkerArtifactDependencies",
    "WorkerConfigDependencies",
    "WorkerContextDependencies",
    "WorkerExecutionDependencies",
    "WorkerExecutionHooks",
    "WorkerExecutionOutcome",
    "WorkerRunnerDependencies",
    "WorkerTrackingDependencies",
    "default_worker_execution_hooks",
    "default_worker_execution_dependencies",
    "execute_queue_entry",
    "main",
    "run_worker_job",
    "WORKER_CANCEL_SIGNAL",
    "WORKER_JOB_MODULE",
    "WORKER_SHUTDOWN_EXIT_CODE",
]


if __name__ == "__main__":
    raise SystemExit(main())
