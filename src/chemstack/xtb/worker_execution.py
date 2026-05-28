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
    mark_cancelled,
    mark_completed,
    mark_failed,
)
from chemstack.core.queue import child_entrypoint as _child_entrypoint
from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.queue.engine_execution import (
    EngineWorkerLifecycle,
    run_engine_worker_lifecycle,
)
from chemstack.core.config.engines import load_xtb_config as load_config
from chemstack.core.notifications.engines import (
    notify_xtb_job_finished as notify_job_finished,
    notify_xtb_job_started as notify_job_started,
)
from chemstack.core.queue.worker import terminate_process_group
from chemstack.core.utils import now_utc_iso

from . import queue_artifacts as _queue_artifacts
from . import queue_terminal as _queue_terminal
from .job_locations import reaction_key_from_job_dir, upsert_job_record
from .runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from .state import (
    is_recovery_pending,
    load_state,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)

WORKER_JOB_MODULE = "chemstack.xtb.worker_execution"
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190


@dataclass(frozen=True)
class _XtbExecutionContext:
    entry: Any
    job_dir: Path
    selected_xyz: Path
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    resource_request: dict[str, int]
    previous_state: dict[str, Any]
    resumed: bool


@dataclass(frozen=True)
class WorkerExecutionOutcome:
    result: XtbRunResult
    organized_output_dir: str = ""


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


def _job_dir(entry: Any) -> Path:
    return _engine_execution.entry_metadata_resolved_path(entry, "job_dir")


def _selected_xyz(entry: Any) -> Path:
    return _engine_execution.entry_metadata_resolved_path(entry, "selected_input_xyz")


def _job_type(entry: Any) -> str:
    value = _engine_execution.entry_metadata_text(entry, "job_type").lower()
    return value or "path_search"


def _reaction_key(entry: Any, job_dir: Path) -> str:
    value = _engine_execution.entry_metadata_text(entry, "reaction_key")
    return value or reaction_key_from_job_dir(job_dir)


def _input_summary(entry: Any) -> dict[str, Any]:
    return _engine_execution.entry_metadata_dict(entry, "input_summary")


def _matching_state(
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    job_type: str,
    reaction_key: str,
) -> dict[str, Any]:
    return _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        match_kwargs={
            "selected_input_xyz": str(selected_xyz),
            "job_type": job_type,
            "reaction_key": reaction_key,
        },
    )


def _queue_entry_by_id(queue_root: Path | str, queue_id: str) -> Any | None:
    return _child_entrypoint.queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue,
    )


def _write_running_state(
    cfg: Any,
    entry: Any,
    *,
    worker_job_pid: int | None = None,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    _queue_artifacts.write_running_state(
        cfg,
        entry,
        worker_job_pid=worker_job_pid,
        previous_state=previous_state,
        resumed=resumed,
        input_summary_fn=_input_summary,
        entry_resource_request_fn=_queue_artifacts.entry_resource_request,
        coerce_mapping_fn=_queue_execution.coerce_mapping,
        now_utc_iso_fn=now_utc_iso,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        write_state_fn=write_state,
    )


def _write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    _queue_artifacts.write_execution_artifacts(
        entry,
        result,
        previous_state=previous_state,
        resumed=resumed,
        coerce_mapping_fn=_queue_execution.coerce_mapping,
        write_state_fn=write_state,
        write_report_json_fn=write_report_json,
        write_report_md_lines_fn=write_report_md_lines,
    )


def _build_terminal_result(entry: Any, **kwargs: Any) -> XtbRunResult:
    return _queue_artifacts.build_terminal_result(
        entry,
        **kwargs,
        now_utc_iso_fn=now_utc_iso,
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
) -> WorkerExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
        outcome_cls=WorkerExecutionOutcome,
        write_execution_artifacts_fn=_write_execution_artifacts,
        selected_xyz_fn=_selected_xyz,
        job_dir_fn=_job_dir,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
        upsert_job_record_fn=upsert_job_record,
        notify_job_finished_fn=notify_job_finished,
    )


def build_worker_execution_dependencies(
    *,
    load_config_fn: Callable[..., Any],
    queue_entry_by_id_fn: Callable[[Path | str, str], Any | None],
    activate_reserved_slot_fn: Callable[..., Any],
    release_slot_fn: Callable[..., Any],
    job_dir_fn: Callable[[Any], Path],
    selected_xyz_fn: Callable[[Any], Path],
    job_type_fn: Callable[[Any], str],
    reaction_key_fn: Callable[[Any, Path], str],
    input_summary_fn: Callable[[Any], dict[str, Any]],
    entry_resource_request_fn: Callable[[Any, Any], dict[str, int]],
    matching_state_fn: Callable[..., dict[str, Any]],
    is_recovery_pending_fn: Callable[[dict[str, Any]], bool],
    write_running_state_fn: Callable[..., Any],
    build_terminal_result_fn: Callable[..., Any],
    finalize_execution_result_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    notify_job_started_fn: Callable[..., Any],
    run_xtb_ranking_job_fn: Callable[..., Any],
    start_xtb_job_fn: Callable[..., Any],
    finalize_xtb_job_fn: Callable[..., Any],
    terminate_process_fn: Callable[[Any], Any],
    wait_for_cancellable_process_fn: Callable[..., Any],
    sleep_fn: Callable[[float], None],
    cancel_check_interval_seconds: float,
    execute_queue_entry_fn: Callable[..., Any] | None = None,
) -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies_from_groups(
        config=WorkerConfigDependencies(
            load_config=load_config_fn,
            queue_entry_by_id=queue_entry_by_id_fn,
        ),
        admission=WorkerAdmissionDependencies(
            activate_reserved_slot=activate_reserved_slot_fn,
            release_slot=release_slot_fn,
        ),
        context=WorkerContextDependencies(
            job_dir=job_dir_fn,
            selected_xyz=selected_xyz_fn,
            job_type=job_type_fn,
            reaction_key=reaction_key_fn,
            input_summary=input_summary_fn,
            entry_resource_request=entry_resource_request_fn,
            matching_state=matching_state_fn,
            is_recovery_pending=is_recovery_pending_fn,
        ),
        artifacts=WorkerArtifactDependencies(
            write_running_state=write_running_state_fn,
            build_terminal_result=build_terminal_result_fn,
            finalize_execution_result=finalize_execution_result_fn,
        ),
        tracking=WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record_fn,
            notify_job_started=notify_job_started_fn,
        ),
        runner=WorkerRunnerDependencies(
            run_xtb_ranking_job=run_xtb_ranking_job_fn,
            start_xtb_job=start_xtb_job_fn,
            finalize_xtb_job=finalize_xtb_job_fn,
            terminate_process=terminate_process_fn,
            wait_for_cancellable_process=wait_for_cancellable_process_fn,
            sleep=sleep_fn,
            cancel_check_interval_seconds=cancel_check_interval_seconds,
        ),
        execute_queue_entry_fn=execute_queue_entry_fn,
    )


def default_worker_execution_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies_from_groups(
        config=WorkerConfigDependencies(
            load_config=load_config,
            queue_entry_by_id=_queue_entry_by_id,
        ),
        admission=WorkerAdmissionDependencies(
            activate_reserved_slot=activate_reserved_slot,
            release_slot=release_slot,
        ),
        context=WorkerContextDependencies(
            job_dir=_job_dir,
            selected_xyz=_selected_xyz,
            job_type=_job_type,
            reaction_key=_reaction_key,
            input_summary=_input_summary,
            entry_resource_request=_queue_artifacts.entry_resource_request,
            matching_state=_matching_state,
            is_recovery_pending=is_recovery_pending,
        ),
        artifacts=WorkerArtifactDependencies(
            write_running_state=_write_running_state,
            build_terminal_result=_build_terminal_result,
            finalize_execution_result=_finalize_execution_result,
        ),
        tracking=WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
        ),
        runner=WorkerRunnerDependencies(
            run_xtb_ranking_job=run_xtb_ranking_job,
            start_xtb_job=start_xtb_job,
            finalize_xtb_job=finalize_xtb_job,
            terminate_process=terminate_process_group,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=1,
        ),
    )


def _build_execution_context(
    cfg: Any,
    entry: Any,
    *,
    dependencies: WorkerExecutionDependencies,
) -> _XtbExecutionContext:
    context_deps = dependencies.context
    job_dir = context_deps.job_dir(entry)
    selected_xyz = context_deps.selected_xyz(entry)
    job_type = context_deps.job_type(entry)
    reaction_key = context_deps.reaction_key(entry, job_dir)
    input_summary = context_deps.input_summary(entry)
    resource_request = context_deps.entry_resource_request(cfg, entry)
    previous_state = context_deps.matching_state(
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
    )
    resumed = _engine_execution.is_resumed_state(
        previous_state,
        is_recovery_pending_fn=context_deps.is_recovery_pending,
    )
    return _XtbExecutionContext(
        entry=entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        resource_request=resource_request,
        previous_state=previous_state,
        resumed=resumed,
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

        running = runner_deps.start_xtb_job(
            cfg,
            job_dir=context.job_dir,
            selected_input_xyz=context.selected_xyz,
        )
        if register_running_job is not None:
            register_running_job(running)
        try:
            return runner_deps.wait_for_cancellable_process(
                running,
                finalize_fn=runner_deps.finalize_xtb_job,
                terminate_process_fn=runner_deps.terminate_process,
                should_cancel=should_cancel,
                sleep_fn=runner_deps.sleep,
                poll_interval_seconds=runner_deps.cancel_check_interval_seconds,
                check_cancel_before_poll=True,
            )
        finally:
            if register_running_job is not None:
                register_running_job(None)
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
    lifecycle = EngineWorkerLifecycle(
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
    return run_engine_worker_lifecycle(
        cfg,
        queue_root=queue_root,
        entry=entry,
        lifecycle=lifecycle,
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
    job = _child_entrypoint.load_child_worker_entrypoint_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=deps.config.load_config,
        find_queue_entry_fn=deps.config.queue_entry_by_id,
        admission_token=admission_token,
        admission_root_fn=lambda _cfg: admission_root,
        release_slot_fn=deps.admission.release_slot,
    )
    if job is None:
        return 1
    cfg = job.cfg
    resolved_queue_root = job.queue_root
    entry = job.entry

    if admission_token:
        if not _child_entrypoint.activate_child_worker_admission(
            job,
            admission_token,
            work_dir=deps.context.job_dir(entry),
            queue_id=entry.queue_id,
            source=WORKER_JOB_MODULE,
            activate_reserved_slot_fn=deps.admission.activate_reserved_slot,
        ):
            return 1

    try:
        if deps.execute_queue_entry is None:
            outcome = execute_queue_entry(
                cfg,
                queue_root=resolved_queue_root,
                entry=entry,
                should_cancel=should_cancel,
                register_running_job=register_running_job,
                emit_output=False,
                worker_job_pid=os.getpid(),
                dependencies=deps,
            )
        else:
            outcome = deps.execute_queue_entry(
                cfg,
                queue_root=resolved_queue_root,
                entry=entry,
                should_cancel=should_cancel,
                register_running_job=register_running_job,
                emit_output=False,
                worker_job_pid=os.getpid(),
            )
        return 0 if outcome.result.status in {"completed", "cancelled"} else 1
    finally:
        _child_entrypoint.release_child_worker_admission(
            job,
            admission_token,
            release_slot_fn=deps.admission.release_slot,
        )


def build_worker_job_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m {WORKER_JOB_MODULE}")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queue-root", required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--admission-root", required=True)
    parser.add_argument("--admission-token", default=None)
    return parser


class _SignalController:
    def __init__(self) -> None:
        self._cancel_requested = False
        self._process: Any | None = None

    def should_cancel(self) -> bool:
        return self._cancel_requested

    def set_running_job(self, value: Any | None) -> None:
        if value is None:
            self._process = None
            return
        self._process = getattr(value, "process", value)

    def install(self) -> None:
        try:
            signal.signal(WORKER_CANCEL_SIGNAL, self._handle_cancel)
            signal.signal(signal.SIGTERM, self._handle_shutdown)
            signal.signal(signal.SIGINT, self._handle_shutdown)
        except ValueError:
            pass

    def _handle_cancel(self, _signum: int, _frame: object) -> None:
        self._cancel_requested = True
        if self._process is not None:
            terminate_process_group(self._process)

    def _handle_shutdown(self, _signum: int, _frame: object) -> None:
        if self._process is not None:
            terminate_process_group(self._process)
        os._exit(WORKER_SHUTDOWN_EXIT_CODE)


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
    "build_worker_execution_dependencies",
    "build_worker_execution_dependencies_from_groups",
    "build_worker_job_parser",
    "WorkerAdmissionDependencies",
    "WorkerArtifactDependencies",
    "WorkerConfigDependencies",
    "WorkerContextDependencies",
    "WorkerExecutionDependencies",
    "WorkerExecutionOutcome",
    "WorkerRunnerDependencies",
    "WorkerTrackingDependencies",
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
