from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import release_slot
from chemstack.core.queue import (
    execution as _queue_execution,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue import child_entrypoint as _child_entrypoint
from chemstack.core.queue.dependencies import build_dependency_container
from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue.worker import (
    install_shutdown_signal_handlers,
    resolve_admission_root,
    terminate_process_group,
)
from chemstack.core.config.engines import load_crest_config as load_config
from chemstack.core.notifications.engines import (
    notify_crest_job_finished as notify_job_finished,
    notify_crest_job_started as notify_job_started,
)
from chemstack.core.utils import now_utc_iso

from . import queue_artifacts as _queue_artifacts
from . import worker_child as _worker_child
from .job_locations import upsert_job_record
from .runner import CrestRunResult, finalize_crest_job, start_crest_job
from .state import mark_recovery_pending
from .worker_context import (
    ExecutionContext,
    build_execution_context as _build_execution_context,
    molecule_key as _molecule_key,
)
from .worker_terminal import (
    WorkerExecutionOutcome,
    finalize_processed_entry as _terminal_finalize_processed_entry,
    mark_job_running as _terminal_mark_job_running,
    mark_queue_terminal as _terminal_mark_queue_terminal,
    sync_job_tracking as _terminal_sync_job_tracking,
    write_execution_artifacts as _terminal_write_execution_artifacts,
    write_running_state as _terminal_write_running_state,
)

CANCEL_CHECK_INTERVAL_SECONDS = 1
is_recovery_pending = _queue_artifacts.is_recovery_pending
load_state = _queue_artifacts.load_state
state_matches_job = _queue_artifacts.state_matches_job
write_report_json = _queue_artifacts.write_report_json
write_report_md_lines = _queue_artifacts.write_report_md_lines
write_state = _queue_artifacts.write_state
WorkerShutdownRequested = _worker_child.WorkerShutdownRequested


WorkerTimingDependencies = _engine_execution.InternalWorkerTimingDependencies
WorkerQueueDependencies = _engine_execution.InternalWorkerQueueDependencies


@dataclass(frozen=True)
class WorkerRunnerDependencies(_engine_execution.InternalWorkerProcessDependencies):
    start_crest_job: Callable[..., Any]
    finalize_crest_job: Callable[..., CrestRunResult]


@dataclass(frozen=True)
class WorkerArtifactDependencies:
    write_running_state: Callable[[Any, Any], None]
    write_execution_artifacts: Callable[[Any, CrestRunResult], None]


@dataclass(frozen=True)
class WorkerTrackingDependencies:
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., bool]
    notify_job_finished: Callable[..., bool]


@dataclass(frozen=True)
class WorkerExecutionDependencies:
    timing: WorkerTimingDependencies
    queue: WorkerQueueDependencies
    runner: WorkerRunnerDependencies
    artifacts: WorkerArtifactDependencies
    tracking: WorkerTrackingDependencies


def build_worker_execution_dependencies_from_groups(
    *,
    timing: WorkerTimingDependencies,
    queue: WorkerQueueDependencies,
    runner: WorkerRunnerDependencies,
    artifacts: WorkerArtifactDependencies,
    tracking: WorkerTrackingDependencies,
) -> WorkerExecutionDependencies:
    return WorkerExecutionDependencies(
        timing=timing,
        queue=queue,
        runner=runner,
        artifacts=artifacts,
        tracking=tracking,
    )


def _build_default_runner_dependencies(
    *,
    terminate_process: Callable[[Any], Any],
    **engine_runner_dependencies: Any,
) -> WorkerRunnerDependencies:
    return _engine_execution.build_internal_worker_process_dependencies(
        WorkerRunnerDependencies,
        terminate_process=terminate_process,
        wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
        sleep=time.sleep,
        cancel_check_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        **engine_runner_dependencies,
    )


def _default_runner_dependencies() -> WorkerRunnerDependencies:
    return _build_default_runner_dependencies(
        terminate_process=_terminate_process,
        start_crest_job=start_crest_job,
        finalize_crest_job=finalize_crest_job,
    )


def _internal_worker_default_factories() -> dict[str, Callable[[], Any]]:
    return _engine_execution.build_internal_worker_default_factories(
        timing_dependencies_type=WorkerTimingDependencies,
        queue_dependencies_type=WorkerQueueDependencies,
        runner_factory=_default_runner_dependencies,
        now_utc_iso=now_utc_iso,
        get_cancel_requested=get_cancel_requested,
        mark_completed=mark_completed,
        mark_cancelled=mark_cancelled,
        mark_failed=mark_failed,
    )


def _default_artifact_dependencies() -> WorkerArtifactDependencies:
    return WorkerArtifactDependencies(
        write_running_state=_write_running_state,
        write_execution_artifacts=_write_execution_artifacts,
    )


def _default_tracking_dependencies() -> WorkerTrackingDependencies:
    return WorkerTrackingDependencies(
        upsert_job_record=upsert_job_record,
        notify_job_started=notify_job_started,
        notify_job_finished=notify_job_finished,
    )


def build_worker_execution_dependencies(
    *,
    timing: WorkerTimingDependencies | None = None,
    queue: WorkerQueueDependencies | None = None,
    runner: WorkerRunnerDependencies | None = None,
    artifacts: WorkerArtifactDependencies | None = None,
    tracking: WorkerTrackingDependencies | None = None,
) -> WorkerExecutionDependencies:
    return build_dependency_container(
        build_worker_execution_dependencies_from_groups,
        {
            "timing": timing,
            "queue": queue,
            "runner": runner,
            "artifacts": artifacts,
            "tracking": tracking,
        },
        {
            **_internal_worker_default_factories(),
            "artifacts": _default_artifact_dependencies,
            "tracking": _default_tracking_dependencies,
        },
    )


def default_worker_execution_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies()


build_worker_child_command = _worker_child.build_worker_child_command


def _write_execution_artifacts(entry: Any, result: CrestRunResult) -> None:
    _terminal_write_execution_artifacts(
        entry,
        result,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        write_state_fn=write_state,
        write_report_json_fn=write_report_json,
        write_report_md_lines_fn=write_report_md_lines,
    )


def _write_running_state(cfg: Any, entry: Any) -> None:
    _terminal_write_running_state(
        cfg,
        entry,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        is_recovery_pending_fn=is_recovery_pending,
        write_state_fn=write_state,
        now_utc_iso_fn=_queue_artifacts.depsafe_now_utc_iso,
    )


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    terminate_process_group(
        proc,
        killpg_fn=os.killpg,
        sigterm=signal.SIGTERM,
        sigkill=signal.SIGKILL,
    )


def _mark_recovery_pending_context(cfg: Any, context: ExecutionContext, *, reason: str) -> None:
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
            "mode": context.mode,
            "molecule_key": context.molecule_key,
        },
        record_identity_fields={
            "mode": context.mode,
            "molecule_key": context.molecule_key,
        },
    )


def _mark_recovery_pending_entry(cfg: Any, entry: Any, *, reason: str) -> None:
    context = _build_execution_context(
        cfg,
        entry,
        molecule_key_resolver=_molecule_key,
    )
    _mark_recovery_pending_context(cfg, context, reason=reason)


def _mark_queue_terminal(
    queue_root: str | Path,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    dependencies: WorkerExecutionDependencies,
) -> None:
    _terminal_mark_queue_terminal(
        queue_root,
        context,
        result,
        queue_deps=dependencies.queue,
    )


def _sync_job_tracking(
    cfg: Any,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    dependencies: WorkerExecutionDependencies,
) -> Path | None:
    return _terminal_sync_job_tracking(
        cfg,
        context,
        result,
        tracking_deps=dependencies.tracking,
    )


def _raise_if_shutdown_requested(
    context: ExecutionContext,
    shutdown_requested: Callable[[], bool] | None,
) -> None:
    _engine_execution.raise_if_shutdown_callback_requested(
        context,
        shutdown_exception_type=WorkerShutdownRequested,
        shutdown_requested=shutdown_requested,
    )


def _mark_job_running(
    cfg: Any,
    context: ExecutionContext,
    *,
    dependencies: WorkerExecutionDependencies,
) -> None:
    _terminal_mark_job_running(
        cfg,
        context,
        artifact_deps=dependencies.artifacts,
        tracking_deps=dependencies.tracking,
    )


def _failed_result_from_exception(
    context: ExecutionContext,
    *,
    exc: Exception,
    failure_time: str,
) -> CrestRunResult:
    return _queue_artifacts.build_terminal_result(
        context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        mode=context.mode,
        resource_request=context.resource_request,
        status="failed",
        reason=f"runner_error:{exc}",
        exit_code=1,
        now_utc_iso_fn=lambda: failure_time,
    )


def _run_crest_job_for_entry(
    cfg: Any,
    context: ExecutionContext,
    *,
    queue_root: Path,
    dependencies: WorkerExecutionDependencies,
    shutdown_requested: Callable[[], bool] | None,
) -> CrestRunResult:
    queue_deps = dependencies.queue
    runner_deps = dependencies.runner

    return _engine_execution.run_internal_worker_process_job(
        context,
        options=_engine_execution.InternalWorkerOptions(
            should_cancel=_engine_execution.queue_cancel_callback(
                queue_deps,
                queue_root,
                context.entry,
            ),
            shutdown_requested=shutdown_requested,
        ),
        process_deps=runner_deps,
        shutdown_exception_type=WorkerShutdownRequested,
        start_job=lambda: runner_deps.start_crest_job(
            cfg,
            job_dir=context.job_dir,
            selected_xyz=context.selected_xyz,
        ),
        finalize_job=runner_deps.finalize_crest_job,
        build_failure_result=lambda exc: _failed_result_from_exception(
            context,
            exc=exc,
            failure_time=dependencies.timing.now_utc_iso(),
        ),
    )


def _finalize_processed_entry(
    cfg: Any,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    queue_root: Path,
    dependencies: WorkerExecutionDependencies,
) -> Path | None:
    return _terminal_finalize_processed_entry(
        cfg,
        context,
        result,
        queue_root=queue_root,
        dependencies=dependencies,
    )


def _worker_execution_spec(
    *,
    molecule_key_resolver: Callable[[Any, Path, Path], str],
    dependencies: WorkerExecutionDependencies,
) -> _engine_execution.InternalEngineWorkerExecutionSpec:
    return _engine_execution.build_internal_engine_worker_execution_spec(
        build_context=lambda cfg_obj, entry_obj: _build_execution_context(
            cfg_obj,
            entry_obj,
            molecule_key_resolver=molecule_key_resolver,
        ),
        shutdown_exception_type=WorkerShutdownRequested,
        mark_running=lambda cfg_obj, context, _options: _mark_job_running(
            cfg_obj,
            context,
            dependencies=dependencies,
        ),
        run_job=lambda cfg_obj, context, active_queue_root, options: _run_crest_job_for_entry(
            cfg_obj,
            context,
            queue_root=active_queue_root,
            dependencies=dependencies,
            shutdown_requested=options.shutdown_requested,
        ),
        finalize_entry=lambda cfg_obj, context, result, active_queue_root, _options: (
            _finalize_processed_entry(
                cfg_obj,
                context,
                result,
                queue_root=active_queue_root,
                dependencies=dependencies,
            )
        ),
        build_outcome=lambda context, result, organized_output_dir: WorkerExecutionOutcome(
            result=result,
            job_dir=context.job_dir,
            selected_xyz=context.selected_xyz,
            molecule_key=context.molecule_key,
            organized_output_dir=organized_output_dir,
        ),
    )


def build_worker_adapter(
    *,
    molecule_key_resolver: Callable[[Any, Path, Path], str],
    dependencies: WorkerExecutionDependencies,
) -> _engine_execution.InternalEngineWorkerAdapter:
    return _engine_execution.build_internal_engine_worker_adapter_from_spec(
        _worker_execution_spec(
            molecule_key_resolver=molecule_key_resolver,
            dependencies=dependencies,
        )
    )


def process_dequeued_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None = None,
    molecule_key_resolver: Callable[[Any, Path, Path], str],
    dependencies: WorkerExecutionDependencies,
    shutdown_requested: Callable[[], bool] | None = None,
) -> WorkerExecutionOutcome:
    return _engine_execution.run_internal_engine_worker_entry_with_spec_options(
        cfg,
        entry,
        queue_root=queue_root,
        spec=_worker_execution_spec(
            molecule_key_resolver=molecule_key_resolver,
            dependencies=dependencies,
        ),
        shutdown_requested=shutdown_requested,
    )


def _admission_root_for_cfg(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    return _child_entrypoint.queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue,
    )


def run_worker_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
) -> int:
    return _worker_child.run_worker_child_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
        load_config_fn=load_config,
        find_queue_entry_fn=_find_queue_entry,
        admission_root_fn=_admission_root_for_cfg,
        release_slot_fn=release_slot,
        install_signal_handlers_fn=_worker_child.shutdown_signal_handler_installer(
            install_shutdown_signal_handlers,
        ),
        process_dequeued_entry_fn=process_dequeued_entry,
        dependencies_fn=default_worker_execution_dependencies,
        requeue_running_entry_fn=requeue_running_entry,
        mark_recovery_pending_context_fn=_mark_recovery_pending_context,
    )


def build_parser() -> argparse.ArgumentParser:
    return _worker_child.build_parser()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_worker_child_job(
        config_path=args.config,
        queue_root=args.queue_root,
        queue_id=args.queue_id,
        admission_token=str(args.admission_token).strip() or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
