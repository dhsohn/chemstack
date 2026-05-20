from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import activate_reserved_slot, release_slot
from chemstack.core.config import engines as _config_engines
from chemstack.core.queue import (
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
)
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.queue.engine_execution import (
    EngineWorkerLifecycle,
    run_engine_worker_lifecycle,
)
from chemstack.core.queue.worker import terminate_process_group
from chemstack.core.utils import now_utc_iso

from . import queue_artifacts as _queue_artifacts
from . import queue_terminal as _queue_terminal
from .config import load_config
from .job_locations import reaction_key_from_job_dir, resource_dict, upsert_job_record
from .notifications import notify_job_finished, notify_job_started
from .runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from .state import (
    is_recovery_pending,
    load_state,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)


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

    def __getattr__(self, name: str) -> Any:
        for group in (
            self.config,
            self.admission,
            self.context,
            self.artifacts,
            self.tracking,
            self.runner,
        ):
            if hasattr(group, name):
                return getattr(group, name)
        raise AttributeError(name)


def _job_dir(entry: Any) -> Path:
    return Path(str(entry.metadata.get("job_dir", ""))).expanduser().resolve()


def _selected_xyz(entry: Any) -> Path:
    return Path(str(entry.metadata.get("selected_input_xyz", ""))).expanduser().resolve()


def _job_type(entry: Any) -> str:
    value = str(entry.metadata.get("job_type", "")).strip().lower()
    return value or "path_search"


def _reaction_key(entry: Any, job_dir: Path) -> str:
    value = str(entry.metadata.get("reaction_key", "")).strip()
    return value or reaction_key_from_job_dir(job_dir)


def _input_summary(entry: Any) -> dict[str, Any]:
    payload = entry.metadata.get("input_summary", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _resource_caps(cfg: Any) -> dict[str, int]:
    return resource_dict(cfg.resources.max_cores_per_task, cfg.resources.max_memory_gb_per_task)


def _coerce_resource_dict(value: Any) -> dict[str, int]:
    return _config_engines.positive_int_mapping(value)


def _entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _coerce_resource_dict(entry.metadata.get("resource_request")) or _resource_caps(cfg)


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
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


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
        entry_resource_request_fn=_entry_resource_request,
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
    auto_organize: bool,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> WorkerExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        auto_organize=auto_organize,
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
    return WorkerExecutionDependencies(
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
        execute_queue_entry=execute_queue_entry_fn,
    )


def default_worker_execution_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        load_config_fn=load_config,
        queue_entry_by_id_fn=_queue_entry_by_id,
        activate_reserved_slot_fn=activate_reserved_slot,
        release_slot_fn=release_slot,
        job_dir_fn=_job_dir,
        selected_xyz_fn=_selected_xyz,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        input_summary_fn=_input_summary,
        entry_resource_request_fn=_entry_resource_request,
        matching_state_fn=_matching_state,
        is_recovery_pending_fn=is_recovery_pending,
        write_running_state_fn=_write_running_state,
        build_terminal_result_fn=_build_terminal_result,
        finalize_execution_result_fn=_finalize_execution_result,
        upsert_job_record_fn=upsert_job_record,
        notify_job_started_fn=notify_job_started,
        run_xtb_ranking_job_fn=run_xtb_ranking_job,
        start_xtb_job_fn=start_xtb_job,
        finalize_xtb_job_fn=finalize_xtb_job,
        terminate_process_fn=terminate_process_group,
        wait_for_cancellable_process_fn=_queue_execution.wait_for_cancellable_process,
        sleep_fn=time.sleep,
        cancel_check_interval_seconds=1,
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
    resumed = (
        context_deps.is_recovery_pending(previous_state)
        or str(previous_state.get("status", "")).strip().lower() == "running"
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
    artifact_deps.write_running_state(
        cfg,
        context.entry,
        worker_job_pid=worker_job_pid,
        previous_state=context.previous_state,
        resumed=context.resumed,
    )
    tracking_deps.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status="running",
        job_dir=context.job_dir,
        job_type=context.job_type,
        selected_input_xyz=str(context.selected_xyz),
        reaction_key=context.reaction_key,
        resource_request=context.resource_request,
        resource_actual=context.resource_request,
    )
    tracking_deps.notify_job_started(
        cfg,
        job_id=context.entry.task_id,
        queue_id=context.entry.queue_id,
        job_dir=context.job_dir,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        selected_xyz=context.selected_xyz,
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
    auto_organize: bool,
    emit_output: bool,
    dependencies: WorkerExecutionDependencies,
) -> Any:
    return dependencies.artifacts.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=context.entry,
        result=result,
        auto_organize=auto_organize,
        emit_output=emit_output,
        previous_state=context.previous_state,
        resumed=context.resumed,
    )


def execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
    dependencies: WorkerExecutionDependencies | None = None,
) -> Any:
    del auto_organize
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
        finalize_entry=lambda cfg_obj, context, result, active_queue_root, should_organize: (
            _finalize_processed_entry(
                cfg_obj,
                context,
                result,
                active_queue_root,
                auto_organize=should_organize,
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
        auto_organize=False,
        lifecycle=lifecycle,
    )


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    dependencies: WorkerExecutionDependencies | None = None,
) -> int:
    del auto_organize
    deps = dependencies or default_worker_execution_dependencies()
    cfg = deps.config.load_config(config_path)
    resolved_queue_root = Path(queue_root).expanduser().resolve()
    entry = deps.config.queue_entry_by_id(resolved_queue_root, queue_id)
    if entry is None:
        return 1

    if admission_token:
        activated = deps.admission.activate_reserved_slot(
            admission_root,
            admission_token,
            work_dir=deps.context.job_dir(entry),
            queue_id=entry.queue_id,
            source="chemstack.xtb.worker_job",
        )
        if activated is None:
            return 1

    try:
        if deps.execute_queue_entry is None:
            outcome = execute_queue_entry(
                cfg,
                queue_root=resolved_queue_root,
                entry=entry,
                auto_organize=False,
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
                auto_organize=False,
                should_cancel=should_cancel,
                register_running_job=register_running_job,
                emit_output=False,
                worker_job_pid=os.getpid(),
            )
        return 0 if outcome.result.status in {"completed", "cancelled"} else 1
    finally:
        if admission_token:
            deps.admission.release_slot(admission_root, admission_token)


__all__ = [
    "build_worker_execution_dependencies",
    "WorkerExecutionDependencies",
    "WorkerExecutionOutcome",
    "default_worker_execution_dependencies",
    "execute_queue_entry",
    "run_worker_job",
]
