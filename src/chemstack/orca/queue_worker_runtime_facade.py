from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .queue_worker_deps import (
    OrcaQueueWorkerFacadeCallbacks,
    build_orca_runtime_facade_deps,
)


@dataclass(frozen=True)
class _SlotCallbacks:
    release_slot: Any
    reserve_slot: Any
    activate_reserved_slot: Any
    list_slots: Any
    reconcile_stale_slots: Any
    try_reserve_admission_slot: Any


@dataclass(frozen=True)
class _ProcessCallbacks:
    start_background_process: Any
    build_worker_child_command: Any
    terminate_process: Any
    start_background_job_process: Any


@dataclass(frozen=True)
class _QueueCallbacks:
    mark_failed: Any
    list_queue: Any
    mark_cancelled: Any
    requeue_running_entry: Any
    find_queue_entry: Any


@dataclass(frozen=True)
class _ConfigCallbacks:
    load_config: Any
    read_worker_pid: Any
    worker_class: Any


@dataclass(frozen=True)
class _WorkerLifecycleCallbacks:
    handle_worker_start_error: Any
    finalize_completed_job: Any
    finalize_child_exit: Any
    reconcile_worker_state: Any
    on_worker_process_started: Any
    shutdown_running_job: Any
    before_shutdown_all: Any


@dataclass(frozen=True)
class _NamespaceFacadeCallbackAdapter:
    namespace: Any

    def release_slot(self, root: str | Path, token: str) -> Any:
        return self.namespace.release_slot(root, token)

    def reserve_slot(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._reserve_orca_worker_slot(*args, **kwargs)

    def start_background_process(self, command: list[str]) -> Any:
        return self.namespace.start_background_process(command)

    def build_worker_child_command(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.build_worker_child_command(*args, **kwargs)

    def activate_reserved_slot(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.activate_reserved_slot(*args, **kwargs)

    def terminate_process(self, process: Any) -> Any:
        return self.namespace._terminate_process(process)

    def mark_failed(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.mark_failed(*args, **kwargs)

    def handle_worker_start_error(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._handle_worker_start_error(*args, **kwargs)

    def finalize_completed_job(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._finalize_completed_job(*args, **kwargs)

    def finalize_child_exit(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._finalize_child_exit(*args, **kwargs)

    def reconcile_worker_state(self, worker: Any) -> Any:
        return self.namespace._reconcile_worker_state(worker)

    def list_queue(self, root: Any) -> Any:
        return self.namespace.list_queue(Path(root))

    def list_slots(self, root: Any) -> Any:
        return self.namespace.list_slots(root)

    def reconcile_stale_slots(self, root: Any) -> Any:
        return self.namespace.reconcile_stale_slots(root)

    def mark_cancelled(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.mark_cancelled(*args, **kwargs)

    def requeue_running_entry(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.requeue_running_entry(*args, **kwargs)

    def try_reserve_admission_slot(self, cfg: Any) -> Any:
        return self.namespace._try_reserve_admission_slot(cfg)

    def start_background_job_process(self, **kwargs: Any) -> Any:
        return self.namespace._start_background_job_process(**kwargs)

    def find_queue_entry(self, root: Any, queue_id: str) -> Any:
        return self.namespace._queue_module.queue_entry_by_id(root, queue_id)

    def load_config(self, config_path: Any) -> Any:
        return self.namespace.load_config(config_path)

    def read_worker_pid(self, allowed_root: Path) -> Any:
        return self.namespace.read_worker_pid(allowed_root)

    def worker_class(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace.QueueWorker(*args, **kwargs)

    def on_worker_process_started(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._on_worker_process_started(*args, **kwargs)

    def shutdown_running_job(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._shutdown_running_job(*args, **kwargs)

    def before_shutdown_all(self, *args: Any, **kwargs: Any) -> Any:
        return self.namespace._before_shutdown_all(*args, **kwargs)


def _build_slot_callbacks(adapter: _NamespaceFacadeCallbackAdapter) -> _SlotCallbacks:
    return _SlotCallbacks(
        release_slot=adapter.release_slot,
        reserve_slot=adapter.reserve_slot,
        activate_reserved_slot=adapter.activate_reserved_slot,
        list_slots=adapter.list_slots,
        reconcile_stale_slots=adapter.reconcile_stale_slots,
        try_reserve_admission_slot=adapter.try_reserve_admission_slot,
    )


def _build_process_callbacks(adapter: _NamespaceFacadeCallbackAdapter) -> _ProcessCallbacks:
    return _ProcessCallbacks(
        start_background_process=adapter.start_background_process,
        build_worker_child_command=adapter.build_worker_child_command,
        terminate_process=adapter.terminate_process,
        start_background_job_process=adapter.start_background_job_process,
    )


def _build_queue_callbacks(adapter: _NamespaceFacadeCallbackAdapter) -> _QueueCallbacks:
    return _QueueCallbacks(
        mark_failed=adapter.mark_failed,
        list_queue=adapter.list_queue,
        mark_cancelled=adapter.mark_cancelled,
        requeue_running_entry=adapter.requeue_running_entry,
        find_queue_entry=adapter.find_queue_entry,
    )


def _build_config_callbacks(adapter: _NamespaceFacadeCallbackAdapter) -> _ConfigCallbacks:
    return _ConfigCallbacks(
        load_config=adapter.load_config,
        read_worker_pid=adapter.read_worker_pid,
        worker_class=adapter.worker_class,
    )


def _build_worker_lifecycle_callbacks(
    adapter: _NamespaceFacadeCallbackAdapter,
) -> _WorkerLifecycleCallbacks:
    return _WorkerLifecycleCallbacks(
        handle_worker_start_error=adapter.handle_worker_start_error,
        finalize_completed_job=adapter.finalize_completed_job,
        finalize_child_exit=adapter.finalize_child_exit,
        reconcile_worker_state=adapter.reconcile_worker_state,
        on_worker_process_started=adapter.on_worker_process_started,
        shutdown_running_job=adapter.shutdown_running_job,
        before_shutdown_all=adapter.before_shutdown_all,
    )


def _facade_callback_kwargs(*callback_groups: Any) -> dict[str, Any]:
    values: dict[str, Any] = {}
    for group in callback_groups:
        values.update(vars(group))
    return values


def _build_orca_queue_worker_facade_callbacks(namespace: Any) -> OrcaQueueWorkerFacadeCallbacks:
    adapter = _NamespaceFacadeCallbackAdapter(namespace)
    return OrcaQueueWorkerFacadeCallbacks(
        **_facade_callback_kwargs(
            _build_slot_callbacks(adapter),
            _build_process_callbacks(adapter),
            _build_queue_callbacks(adapter),
            _build_config_callbacks(adapter),
            _build_worker_lifecycle_callbacks(adapter),
        )
    )


def build_orca_queue_worker_runtime_facade_deps(
    namespace: Any,
    *,
    time_module: Any,
) -> Any:
    return build_orca_runtime_facade_deps(
        _build_orca_queue_worker_facade_callbacks(namespace),
        time_module=time_module,
    )


__all__ = ["build_orca_queue_worker_runtime_facade_deps"]
