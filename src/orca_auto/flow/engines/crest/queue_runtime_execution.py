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
from orca_auto.flow.engines.crest import execution as _worker_execution


@dataclass(frozen=True)
class CrestQueueRuntimeWorkerExecutionCallbacks:
    terminate_process: Callable[..., Any]
    wait_for_cancellable_process: Callable[..., Any]
    sleep: Callable[..., Any]
    now_utc_iso: Callable[..., Any]
    get_cancel_requested: Callable[..., Any]
    mark_completed: Callable[..., Any]
    mark_cancelled: Callable[..., Any]
    mark_failed: Callable[..., Any]
    start_crest_job: Callable[..., Any]
    finalize_crest_job: Callable[..., Any]
    write_running_state: Callable[..., Any]
    write_execution_artifacts: Callable[..., Any]
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., Any]
    notify_job_finished: Callable[..., Any]

    @property
    def process_callbacks(self) -> WorkerProcessDependencyCallbacks:
        return worker_process_dependency_callbacks_from_attrs(
            self,
            engine_runner_dependency_names=("start_crest_job", "finalize_crest_job"),
        )


def callbacks_from_namespace(
    namespace: Mapping[str, Any],
) -> CrestQueueRuntimeWorkerExecutionCallbacks:
    process_callbacks = worker_process_dependency_callbacks_from_namespace(
        namespace,
        engine_runner_dependency_names=("start_crest_job", "finalize_crest_job"),
    )
    return CrestQueueRuntimeWorkerExecutionCallbacks(
        **worker_process_dependency_callback_kwargs(
            process_callbacks,
            include_engine_runner_dependencies=True,
        ),
        write_running_state=namespace["_write_running_state"],
        write_execution_artifacts=namespace["_write_execution_artifacts"],
        upsert_job_record=namespace["upsert_job_record"],
        notify_job_started=namespace["notify_job_started"],
        notify_job_finished=namespace["notify_job_finished"],
    )


def build_queue_runtime_worker_execution_dependencies(
    callbacks: CrestQueueRuntimeWorkerExecutionCallbacks | Mapping[str, Any],
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
        artifacts=_worker_execution.WorkerArtifactDependencies(
            write_running_state=resolved_callbacks.write_running_state,
            write_execution_artifacts=resolved_callbacks.write_execution_artifacts,
        ),
        tracking=_worker_execution.WorkerTrackingDependencies(
            upsert_job_record=resolved_callbacks.upsert_job_record,
            notify_job_started=resolved_callbacks.notify_job_started,
            notify_job_finished=resolved_callbacks.notify_job_finished,
        ),
    )


build_queue_runtime_worker_dependencies = build_queue_runtime_worker_execution_dependencies


__all__ = [
    "CrestQueueRuntimeWorkerExecutionCallbacks",
    "build_queue_runtime_worker_dependencies",
    "build_queue_runtime_worker_execution_dependencies",
    "callbacks_from_namespace",
]
