from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from orca_auto.core.queue.worker_execution_dependencies import (
    WorkerProcessDependencyCallbacks,
    build_worker_process_dependency_groups,
    worker_process_dependency_callback_kwargs,
    worker_process_dependency_callbacks_from_attrs,
    worker_process_dependency_callbacks_from_namespace,
)
from orca_auto.flow.engines.xtb import artifacts as _queue_artifacts
from orca_auto.flow.engines.xtb import execution as _worker_execution


@dataclass(frozen=True)
class XtbQueueRuntimeWorkerExecutionCallbacks:
    activate_reserved_slot: Callable[..., Any]
    release_slot: Callable[..., Any]
    load_config: Callable[..., Any]
    queue_entry_by_id: Callable[..., Any]
    job_dir: Callable[..., Any]
    selected_xyz: Callable[..., Any]
    job_type: Callable[..., Any]
    reaction_key: Callable[..., Any]
    input_summary: Callable[..., Any]
    entry_resource_request: Callable[..., Any]
    matching_state: Callable[..., Any]
    is_recovery_pending: Callable[..., Any]
    write_running_state: Callable[..., Any]
    build_terminal_result: Callable[..., Any]
    finalize_execution_result: Callable[..., Any]
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., Any]
    execute_queue_entry: Callable[..., Any]
    run_xtb_ranking_job: Callable[..., Any]
    start_xtb_job: Callable[..., Any]
    finalize_xtb_job: Callable[..., Any]
    terminate_process: Callable[..., Any]
    wait_for_cancellable_process: Callable[..., Any]
    sleep: Callable[..., Any]
    now_utc_iso: Callable[..., Any]
    get_cancel_requested: Callable[..., Any]
    mark_completed: Callable[..., Any]
    mark_cancelled: Callable[..., Any]
    mark_failed: Callable[..., Any]

    @property
    def process_callbacks(self) -> WorkerProcessDependencyCallbacks:
        return worker_process_dependency_callbacks_from_attrs(
            self,
            engine_runner_dependency_names=(
                "run_xtb_ranking_job",
                "start_xtb_job",
                "finalize_xtb_job",
            ),
        )


def callbacks_from_namespace(
    namespace: Mapping[str, Any],
) -> XtbQueueRuntimeWorkerExecutionCallbacks:
    process_callbacks = worker_process_dependency_callbacks_from_namespace(
        namespace,
        engine_runner_dependency_names=(
            "run_xtb_ranking_job",
            "start_xtb_job",
            "finalize_xtb_job",
        ),
    )
    return XtbQueueRuntimeWorkerExecutionCallbacks(
        **worker_process_dependency_callback_kwargs(
            process_callbacks,
            include_engine_runner_dependencies=True,
        ),
        activate_reserved_slot=namespace["activate_reserved_slot"],
        release_slot=namespace["release_slot"],
        load_config=namespace["load_config"],
        queue_entry_by_id=namespace["_queue_entry_by_id"],
        job_dir=namespace["_job_dir"],
        selected_xyz=namespace["_selected_xyz"],
        job_type=namespace["_job_type"],
        reaction_key=namespace["_reaction_key"],
        input_summary=namespace["_input_summary"],
        entry_resource_request=_queue_artifacts.entry_resource_request,
        matching_state=namespace["_worker_execution_hooks"].matching_state,
        is_recovery_pending=_worker_execution.is_recovery_pending,
        write_running_state=namespace["_write_running_state"],
        build_terminal_result=namespace["_build_terminal_result"],
        finalize_execution_result=namespace["_finalize_execution_result"],
        upsert_job_record=namespace["upsert_job_record"],
        notify_job_started=namespace["notify_job_started"],
        execute_queue_entry=namespace["_execute_queue_entry"],
    )


def build_queue_runtime_worker_execution_dependencies(
    callbacks: XtbQueueRuntimeWorkerExecutionCallbacks | Mapping[str, Any],
    *,
    cancel_check_interval_seconds: int,
) -> _worker_execution.WorkerExecutionDependencies:
    resolved_callbacks = (
        callbacks_from_namespace(callbacks) if isinstance(callbacks, Mapping) else callbacks
    )
    process_groups = build_worker_process_dependency_groups(
        resolved_callbacks.process_callbacks,
        timing_dependencies_type=_worker_execution.WorkerTimingDependencies,
        queue_dependencies_type=_worker_execution.WorkerQueueDependencies,
        runner_dependencies_type=_worker_execution.WorkerRunnerDependencies,
        cancel_check_interval_seconds=cancel_check_interval_seconds,
    )
    return _worker_execution.build_worker_execution_dependencies(
        **process_groups,
        config=_worker_execution.WorkerConfigDependencies(
            load_config=resolved_callbacks.load_config,
            queue_entry_by_id=resolved_callbacks.queue_entry_by_id,
        ),
        admission=_worker_execution.WorkerAdmissionDependencies(
            activate_reserved_slot=resolved_callbacks.activate_reserved_slot,
            release_slot=resolved_callbacks.release_slot,
        ),
        context=_worker_execution.WorkerContextDependencies(
            job_dir=resolved_callbacks.job_dir,
            selected_xyz=resolved_callbacks.selected_xyz,
            job_type=resolved_callbacks.job_type,
            reaction_key=resolved_callbacks.reaction_key,
            input_summary=resolved_callbacks.input_summary,
            entry_resource_request=resolved_callbacks.entry_resource_request,
            matching_state=resolved_callbacks.matching_state,
            is_recovery_pending=resolved_callbacks.is_recovery_pending,
        ),
        artifacts=_worker_execution.WorkerArtifactDependencies(
            write_running_state=resolved_callbacks.write_running_state,
            build_terminal_result=resolved_callbacks.build_terminal_result,
            finalize_execution_result=resolved_callbacks.finalize_execution_result,
        ),
        tracking=_worker_execution.WorkerTrackingDependencies(
            upsert_job_record=resolved_callbacks.upsert_job_record,
            notify_job_started=resolved_callbacks.notify_job_started,
        ),
        execute_queue_entry_fn=resolved_callbacks.execute_queue_entry,
    )


__all__ = [
    "XtbQueueRuntimeWorkerExecutionCallbacks",
    "build_queue_runtime_worker_execution_dependencies",
    "callbacks_from_namespace",
]
