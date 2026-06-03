from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from . import child_entrypoint as _child_entrypoint
from .dependencies import build_dependency_container
from .internal_worker import build_internal_worker_process_default_factories

DependencyFactory = Callable[[], Any]


@dataclass(frozen=True)
class WorkerConfigDependencies:
    load_config: Callable[..., Any]
    queue_entry_by_id: Callable[[Path | str, str], Any | None]


@dataclass(frozen=True)
class WorkerAdmissionDependencies:
    activate_reserved_slot: Callable[..., Any]
    release_slot: Callable[..., Any]


def queue_entry_by_id(
    queue_root: Path | str,
    queue_id: str,
    *,
    list_queue_fn: Callable[..., Any],
) -> Any | None:
    return _child_entrypoint.queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue_fn,
    )


def build_worker_config_dependencies(
    *,
    load_config: Callable[..., Any],
    queue_entry_by_id_fn: Callable[[Path | str, str], Any | None],
) -> WorkerConfigDependencies:
    return WorkerConfigDependencies(
        load_config=load_config,
        queue_entry_by_id=queue_entry_by_id_fn,
    )


def build_worker_admission_dependencies(
    *,
    activate_reserved_slot: Callable[..., Any],
    release_slot: Callable[..., Any],
) -> WorkerAdmissionDependencies:
    return WorkerAdmissionDependencies(
        activate_reserved_slot=activate_reserved_slot,
        release_slot=release_slot,
    )


def build_worker_execution_dependencies_from_groups(
    dependencies_type: Callable[..., Any],
    groups: Mapping[str, Any],
    *,
    execute_queue_entry_fn: Callable[..., Any] | None = None,
) -> Any:
    resolved = {name: value for name, value in groups.items() if value is not None}
    return dependencies_type(
        **resolved,
        execute_queue_entry=execute_queue_entry_fn,
    )


def build_worker_process_default_factories(
    *,
    config_factory: DependencyFactory,
    admission_factory: DependencyFactory,
    timing_dependencies_type: Callable[..., Any],
    queue_dependencies_type: Callable[..., Any],
    runner_dependencies_type: Callable[..., Any],
    terminate_process: Callable[..., Any],
    wait_for_cancellable_process: Callable[..., Any],
    sleep: Callable[..., Any],
    cancel_check_interval_seconds: float,
    now_utc_iso: Callable[..., Any],
    get_cancel_requested: Callable[..., Any],
    mark_completed: Callable[..., Any],
    mark_cancelled: Callable[..., Any],
    mark_failed: Callable[..., Any],
    engine_runner_dependencies: Mapping[str, Any],
) -> dict[str, DependencyFactory]:
    return {
        "config": config_factory,
        "admission": admission_factory,
        **build_internal_worker_process_default_factories(
            timing_dependencies_type=timing_dependencies_type,
            queue_dependencies_type=queue_dependencies_type,
            runner_dependencies_type=runner_dependencies_type,
            terminate_process=terminate_process,
            wait_for_cancellable_process=wait_for_cancellable_process,
            sleep=sleep,
            cancel_check_interval_seconds=cancel_check_interval_seconds,
            now_utc_iso=now_utc_iso,
            get_cancel_requested=get_cancel_requested,
            mark_completed=mark_completed,
            mark_cancelled=mark_cancelled,
            mark_failed=mark_failed,
            **dict(engine_runner_dependencies),
        ),
    }


def build_worker_execution_dependency_container(
    container_builder: Callable[..., Any],
    overrides: Mapping[str, Any],
    default_factories: Mapping[str, DependencyFactory],
    *,
    execute_queue_entry_fn: Callable[..., Any] | None = None,
) -> Any:
    return build_dependency_container(
        container_builder,
        overrides,
        default_factories,
        extra_fields={"execute_queue_entry_fn": execute_queue_entry_fn},
    )


__all__ = [
    "WorkerAdmissionDependencies",
    "WorkerConfigDependencies",
    "build_worker_admission_dependencies",
    "build_worker_config_dependencies",
    "build_worker_execution_dependencies_from_groups",
    "build_worker_execution_dependency_container",
    "build_worker_process_default_factories",
    "queue_entry_by_id",
]
