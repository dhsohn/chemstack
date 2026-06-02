from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution

from . import queue_artifacts as _queue_artifacts
from . import worker_execution as _worker_execution


@dataclass(frozen=True)
class XtbQueueWorkerExecutionFns:
    load_config: Callable[..., Any]
    queue_entry_by_id: Callable[[Path | str, str], Any | None]
    activate_reserved_slot: Callable[..., Any]
    release_slot: Callable[..., Any]
    job_dir: Callable[[Any], Path]
    selected_xyz: Callable[[Any], Path]
    job_type: Callable[[Any], str]
    reaction_key: Callable[[Any, Path], str]
    input_summary: Callable[[Any], dict[str, Any]]
    matching_state: Callable[..., dict[str, Any]]
    is_recovery_pending: Callable[[dict[str, Any]], bool]
    write_running_state: Callable[..., Any]
    build_terminal_result: Callable[..., Any]
    finalize_execution_result: Callable[..., Any]
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., Any]
    run_xtb_ranking_job: Callable[..., Any]
    start_xtb_job: Callable[..., Any]
    finalize_xtb_job: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    sleep: Callable[[float], None]
    cancel_check_interval_seconds: float
    execute_queue_entry: Callable[..., Any]


def build_worker_execution_dependencies(
    fns: XtbQueueWorkerExecutionFns,
) -> _worker_execution.WorkerExecutionDependencies:
    return _worker_execution.build_worker_execution_dependencies(
        config=_worker_execution.WorkerConfigDependencies(
            load_config=fns.load_config,
            queue_entry_by_id=fns.queue_entry_by_id,
        ),
        admission=_worker_execution.WorkerAdmissionDependencies(
            activate_reserved_slot=fns.activate_reserved_slot,
            release_slot=fns.release_slot,
        ),
        context=_worker_execution.WorkerContextDependencies(
            job_dir=fns.job_dir,
            selected_xyz=fns.selected_xyz,
            job_type=fns.job_type,
            reaction_key=fns.reaction_key,
            input_summary=fns.input_summary,
            entry_resource_request=_queue_artifacts.entry_resource_request,
            matching_state=fns.matching_state,
            is_recovery_pending=fns.is_recovery_pending,
        ),
        artifacts=_worker_execution.WorkerArtifactDependencies(
            write_running_state=fns.write_running_state,
            build_terminal_result=fns.build_terminal_result,
            finalize_execution_result=fns.finalize_execution_result,
        ),
        tracking=_worker_execution.WorkerTrackingDependencies(
            upsert_job_record=fns.upsert_job_record,
            notify_job_started=fns.notify_job_started,
        ),
        runner=_engine_execution.build_internal_worker_process_dependencies(
            _worker_execution.WorkerRunnerDependencies,
            terminate_process=fns.terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=fns.sleep,
            cancel_check_interval_seconds=fns.cancel_check_interval_seconds,
            run_xtb_ranking_job=fns.run_xtb_ranking_job,
            start_xtb_job=fns.start_xtb_job,
            finalize_xtb_job=fns.finalize_xtb_job,
        ),
        execute_queue_entry_fn=fns.execute_queue_entry,
    )


__all__ = [
    "XtbQueueWorkerExecutionFns",
    "build_worker_execution_dependencies",
]
