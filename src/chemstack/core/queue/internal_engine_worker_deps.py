from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .internal_engine_runtime import InternalEngineQueueRuntime

LegacyWorkerNamespace = Mapping[str, Any]
SlotReleaser = Callable[[str | Path, str], object]
BackgroundProcessStarter = Callable[[list[str]], Any]
DefaultConfigPath = Callable[[], str]
ProcessTerminator = Callable[[Any], object]
WorkerStartErrorHandler = Callable[[Any, Path, Any, str, OSError], None]
CompletedJobFinalizer = Callable[[Any, str, Any, int], None]
WorkerStateReconciler = Callable[[Any], None]
QueueLister = Callable[[Any], list[Any]]
SlotLister = Callable[[Any], list[Any]]
StaleSlotReconciler = Callable[[Any], Any]
AdmissionSlotReserver = Callable[[Any], str | None]
QueueEntryFinder = Callable[[Any, str], Any | None]
ConfigLoader = Callable[[Any], Any]
WorkerPidReader = Callable[[Path], int | None]
WorkerProcessStartedHook = Callable[[Any, Path, Any, Any, str], bool]
ShutdownRunningJob = Callable[[Any, str, Any], Any]
BeforeShutdownAll = Callable[[Any, int], Any]


class SlotReserver(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> str | None: ...


class WorkerChildCommandBuilder(Protocol):
    def __call__(
        self,
        *,
        config_path: str,
        queue_root: str | Path,
        queue_id: str,
        admission_root: str | Path | None = None,
        admission_token: str | None = None,
    ) -> list[str]: ...


class ConfigPathForWorker(Protocol):
    def __call__(self, args: Any, *, default_config_path_fn: DefaultConfigPath) -> str: ...


class ReservedSlotActivator(Protocol):
    def __call__(
        self,
        admission_root: str | Path,
        admission_token: str,
        **metadata: Any,
    ) -> object | None: ...


class QueueStatusMarker(Protocol):
    def __call__(self, root: str | Path, queue_id: str, **kwargs: Any) -> Any: ...


class ChildExitFinalizer(Protocol):
    def __call__(self, worker: Any, job: Any, *, rc: int) -> Any: ...


class BackgroundJobProcessStarter(Protocol):
    def __call__(
        self,
        *,
        config_path: str,
        queue_root: Path,
        entry: Any,
        admission_root: str | Path,
        admission_token: str,
    ) -> Any: ...


class OrphanedChildQueueReconciler(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class RunningEntryRequeuer(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class RecoveryPendingMarker(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class WorkerFactory(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


@dataclass(frozen=True)
class _LegacyNamespaceAdapter:
    namespace: LegacyWorkerNamespace

    def lookup(self, name: str) -> Any:
        return self.namespace[name]

    def call(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return self.lookup(name)(*args, **kwargs)


def _legacy_call_fn(legacy: _LegacyNamespaceAdapter, name: str) -> Callable[..., Any]:
    return lambda *args, **kwargs: legacy.call(name, *args, **kwargs)


def _legacy_optional_call_fn(
    legacy: _LegacyNamespaceAdapter,
    name: str | None,
) -> Callable[..., Any] | None:
    if name is None:
        return None
    return _legacy_call_fn(legacy, name)


def _legacy_queue_entry_finder(
    legacy: _LegacyNamespaceAdapter,
    name: str | None,
) -> QueueEntryFinder | None:
    if name is None:
        return None
    return lambda root, queue_id: legacy.call(name, root, queue_id)


_DepsKwargs = dict[str, Any]


@dataclass(frozen=True)
class InternalEngineQueueWorkerDeps:
    time_module: Any
    release_slot: SlotReleaser
    reserve_slot: SlotReserver
    start_background_process: BackgroundProcessStarter
    build_worker_child_command: WorkerChildCommandBuilder
    config_path_for_worker: ConfigPathForWorker
    default_config_path: DefaultConfigPath
    activate_reserved_slot: ReservedSlotActivator
    terminate_process: ProcessTerminator
    mark_failed: QueueStatusMarker
    handle_worker_start_error: WorkerStartErrorHandler
    finalize_completed_job: CompletedJobFinalizer
    finalize_child_exit: ChildExitFinalizer
    reconcile_worker_state: WorkerStateReconciler
    list_queue: QueueLister
    list_slots: SlotLister
    reconcile_stale_slots: StaleSlotReconciler
    reconcile_orphaned_child_queue_entries: OrphanedChildQueueReconciler
    mark_cancelled: QueueStatusMarker
    requeue_running_entry: RunningEntryRequeuer
    mark_recovery_pending: RecoveryPendingMarker
    try_reserve_admission_slot: AdmissionSlotReserver | None = None
    start_background_job_process_fn: BackgroundJobProcessStarter | None = None
    find_queue_entry: QueueEntryFinder | None = None
    load_config: ConfigLoader | None = None
    read_worker_pid: WorkerPidReader | None = None
    worker_class: WorkerFactory | None = None
    on_worker_process_started: WorkerProcessStartedHook | None = None
    shutdown_running_job: ShutdownRunningJob | None = None
    before_shutdown_all: BeforeShutdownAll | None = None


@dataclass(frozen=True)
class InternalEngineQueueWorkerDepsResolver:
    runtime: InternalEngineQueueRuntime
    deps: InternalEngineQueueWorkerDeps | None = None
    namespace: LegacyWorkerNamespace | None = None
    time_module_name: str = "time"
    release_slot_name: str = "release_slot"
    reserve_slot_name: str = "reserve_slot"
    start_background_process_name: str = "start_background_process"
    build_worker_child_command_name: str = "build_worker_child_command"
    config_path_for_worker_name: str = "config_path_for_worker"
    default_config_path_name: str = "default_config_path"
    activate_reserved_slot_name: str = "activate_reserved_slot"
    terminate_process_name: str = "_terminate_process"
    mark_failed_name: str = "mark_failed"
    find_queue_entry_name: str | None = None
    list_queue_name: str = "list_queue"
    list_slots_name: str = "list_slots"
    reconcile_stale_slots_name: str = "reconcile_stale_slots"
    reconcile_orphaned_child_queue_entries_name: str = "reconcile_orphaned_child_queue_entries"
    mark_cancelled_name: str = "mark_cancelled"
    requeue_running_entry_name: str = "requeue_running_entry"
    mark_recovery_pending_name: str = "_mark_recovery_pending_entry"
    on_worker_process_started_name: str | None = None
    shutdown_running_job_name: str | None = None
    before_shutdown_all_name: str | None = None

    def lookup(self, name: str) -> Any:
        if self.namespace is None:
            raise KeyError(name)
        return self.namespace[name]

    def dep(self, attr: str, fallback_name: str) -> Any:
        if self.deps is not None:
            return getattr(self.deps, attr)
        return self.lookup(fallback_name)

    def optional_dep(self, attr: str, fallback_name: str | None) -> Any | None:
        if self.deps is not None:
            return getattr(self.deps, attr)
        if fallback_name is None:
            return None
        return self.lookup(fallback_name)

    def find_queue_entry(self, queue_root: Any, queue_id: str) -> Any | None:
        if self.deps is not None and self.deps.find_queue_entry is not None:
            return self.deps.find_queue_entry(queue_root, queue_id)
        if self.find_queue_entry_name and self.namespace is not None:
            return self.lookup(self.find_queue_entry_name)(queue_root, queue_id)
        return self.runtime.queue_entry_by_id(queue_root, queue_id)

    def queue_worker_deps(
        self,
        *,
        poll_interval_seconds: int,
        start_background_job_process_fn: BackgroundJobProcessStarter,
        try_reserve_admission_slot_fn: AdmissionSlotReserver,
    ) -> Any:
        if self.deps is None:
            return self.runtime.child_worker_deps_from_namespace(
                namespace=self.namespace or {},
                poll_interval_seconds=poll_interval_seconds,
                time_module=self.dep("time_module", self.time_module_name),
                release_slot_fn=self.dep("release_slot", self.release_slot_name),
            )
        return self.runtime.child_worker_deps(
            poll_interval_seconds=poll_interval_seconds,
            time_module=self.deps.time_module,
            release_slot_fn=self.deps.release_slot,
            start_background_job_process_fn=(
                self.deps.start_background_job_process_fn or start_background_job_process_fn
            ),
            try_reserve_admission_slot_fn=(
                self.deps.try_reserve_admission_slot or try_reserve_admission_slot_fn
            ),
        )

    def try_reserve_admission_slot(self, cfg: Any) -> str | None:
        reserve_slot_fn = self.dep("reserve_slot", self.reserve_slot_name)
        return self.runtime.reserve_admission_slot(cfg, reserve_slot_fn=reserve_slot_fn)

    def start_background_job_process(
        self,
        *,
        config_path: str,
        queue_root: Path,
        entry: Any,
        admission_root: str | Path,
        admission_token: str,
    ) -> Any:
        return self.runtime.start_child_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
            start_background_process_fn=self.dep(
                "start_background_process",
                self.start_background_process_name,
            ),
            build_worker_child_command_fn=self.dep(
                "build_worker_child_command",
                self.build_worker_child_command_name,
            ),
        )

    def config_path_for_worker(self, args: Any) -> str:
        return self.dep("config_path_for_worker", self.config_path_for_worker_name)(
            args,
            default_config_path_fn=self.dep("default_config_path", self.default_config_path_name),
        )


def _required_deps_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    *,
    time_module_name: str,
    release_slot_name: str,
    reserve_slot_name: str,
    start_background_process_name: str,
    build_worker_child_command_name: str,
    config_path_for_worker_name: str,
    default_config_path_name: str,
    activate_reserved_slot_name: str,
    terminate_process_name: str,
    mark_failed_name: str,
) -> _DepsKwargs:
    return {
        "time_module": legacy.lookup(time_module_name),
        "release_slot": lambda root, token: legacy.call(release_slot_name, root, token),
        "reserve_slot": _legacy_call_fn(legacy, reserve_slot_name),
        "start_background_process": lambda command: legacy.call(
            start_background_process_name,
            command,
        ),
        "build_worker_child_command": _legacy_call_fn(
            legacy,
            build_worker_child_command_name,
        ),
        "config_path_for_worker": _legacy_call_fn(legacy, config_path_for_worker_name),
        "default_config_path": lambda: legacy.call(default_config_path_name),
        "activate_reserved_slot": _legacy_call_fn(legacy, activate_reserved_slot_name),
        "terminate_process": lambda process: legacy.call(terminate_process_name, process),
        "mark_failed": _legacy_call_fn(legacy, mark_failed_name),
    }


def _lifecycle_deps_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    *,
    handle_worker_start_error_name: str,
    finalize_completed_job_name: str,
    finalize_child_exit_name: str,
    reconcile_worker_state_name: str,
) -> _DepsKwargs:
    return {
        "handle_worker_start_error": _legacy_call_fn(
            legacy,
            handle_worker_start_error_name,
        ),
        "finalize_completed_job": _legacy_call_fn(legacy, finalize_completed_job_name),
        "finalize_child_exit": _legacy_call_fn(legacy, finalize_child_exit_name),
        "reconcile_worker_state": lambda worker: legacy.call(
            reconcile_worker_state_name,
            worker,
        ),
    }


def _reconciliation_deps_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    *,
    list_queue_name: str,
    list_slots_name: str,
    reconcile_stale_slots_name: str,
    reconcile_orphaned_child_queue_entries_name: str,
    mark_cancelled_name: str,
    requeue_running_entry_name: str,
    mark_recovery_pending_name: str,
) -> _DepsKwargs:
    return {
        "list_queue": lambda root: legacy.call(list_queue_name, root),
        "list_slots": lambda root: legacy.call(list_slots_name, root),
        "reconcile_stale_slots": lambda root: legacy.call(
            reconcile_stale_slots_name,
            root,
        ),
        "reconcile_orphaned_child_queue_entries": _legacy_call_fn(
            legacy,
            reconcile_orphaned_child_queue_entries_name,
        ),
        "mark_cancelled": _legacy_call_fn(legacy, mark_cancelled_name),
        "requeue_running_entry": _legacy_call_fn(legacy, requeue_running_entry_name),
        "mark_recovery_pending": _legacy_call_fn(legacy, mark_recovery_pending_name),
    }


def _optional_deps_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    *,
    try_reserve_admission_slot_name: str,
    start_background_job_process_name: str,
    find_queue_entry_name: str | None,
    load_config_name: str,
    read_worker_pid_name: str,
    worker_class_name: str,
    on_worker_process_started_name: str | None,
    shutdown_running_job_name: str | None,
    before_shutdown_all_name: str | None,
) -> _DepsKwargs:
    return {
        "try_reserve_admission_slot": lambda cfg: legacy.call(
            try_reserve_admission_slot_name,
            cfg,
        ),
        "start_background_job_process_fn": lambda **kwargs: legacy.call(
            start_background_job_process_name,
            **kwargs,
        ),
        "find_queue_entry": _legacy_queue_entry_finder(legacy, find_queue_entry_name),
        "load_config": lambda config_path: legacy.call(load_config_name, config_path),
        "read_worker_pid": lambda allowed_root: legacy.call(
            read_worker_pid_name,
            allowed_root,
        ),
        "worker_class": _legacy_call_fn(legacy, worker_class_name),
        "on_worker_process_started": _legacy_optional_call_fn(
            legacy,
            on_worker_process_started_name,
        ),
        "shutdown_running_job": _legacy_optional_call_fn(
            legacy,
            shutdown_running_job_name,
        ),
        "before_shutdown_all": _legacy_optional_call_fn(
            legacy,
            before_shutdown_all_name,
        ),
    }


def internal_engine_queue_worker_deps_from_namespace(
    namespace: LegacyWorkerNamespace,
    *,
    time_module_name: str = "time",
    release_slot_name: str = "release_slot",
    reserve_slot_name: str = "reserve_slot",
    start_background_process_name: str = "start_background_process",
    build_worker_child_command_name: str = "build_worker_child_command",
    config_path_for_worker_name: str = "config_path_for_worker",
    default_config_path_name: str = "default_config_path",
    activate_reserved_slot_name: str = "activate_reserved_slot",
    terminate_process_name: str = "_terminate_process",
    mark_failed_name: str = "mark_failed",
    handle_worker_start_error_name: str = "_handle_worker_start_error",
    finalize_completed_job_name: str = "_finalize_completed_job",
    finalize_child_exit_name: str = "_finalize_child_exit",
    reconcile_worker_state_name: str = "_reconcile_worker_state",
    list_queue_name: str = "list_queue",
    list_slots_name: str = "list_slots",
    reconcile_stale_slots_name: str = "reconcile_stale_slots",
    reconcile_orphaned_child_queue_entries_name: str = "reconcile_orphaned_child_queue_entries",
    mark_cancelled_name: str = "mark_cancelled",
    requeue_running_entry_name: str = "requeue_running_entry",
    mark_recovery_pending_name: str = "_mark_recovery_pending_entry",
    try_reserve_admission_slot_name: str = "_try_reserve_admission_slot",
    start_background_job_process_name: str = "_start_background_job_process",
    find_queue_entry_name: str | None = None,
    load_config_name: str = "load_config",
    read_worker_pid_name: str = "read_worker_pid",
    worker_class_name: str = "QueueWorker",
    on_worker_process_started_name: str | None = None,
    shutdown_running_job_name: str | None = None,
    before_shutdown_all_name: str | None = None,
) -> InternalEngineQueueWorkerDeps:
    legacy = _LegacyNamespaceAdapter(namespace)

    return InternalEngineQueueWorkerDeps(
        **_required_deps_from_namespace(
            legacy,
            time_module_name=time_module_name,
            release_slot_name=release_slot_name,
            reserve_slot_name=reserve_slot_name,
            start_background_process_name=start_background_process_name,
            build_worker_child_command_name=build_worker_child_command_name,
            config_path_for_worker_name=config_path_for_worker_name,
            default_config_path_name=default_config_path_name,
            activate_reserved_slot_name=activate_reserved_slot_name,
            terminate_process_name=terminate_process_name,
            mark_failed_name=mark_failed_name,
        ),
        **_lifecycle_deps_from_namespace(
            legacy,
            handle_worker_start_error_name=handle_worker_start_error_name,
            finalize_completed_job_name=finalize_completed_job_name,
            finalize_child_exit_name=finalize_child_exit_name,
            reconcile_worker_state_name=reconcile_worker_state_name,
        ),
        **_reconciliation_deps_from_namespace(
            legacy,
            list_queue_name=list_queue_name,
            list_slots_name=list_slots_name,
            reconcile_stale_slots_name=reconcile_stale_slots_name,
            reconcile_orphaned_child_queue_entries_name=(
                reconcile_orphaned_child_queue_entries_name
            ),
            mark_cancelled_name=mark_cancelled_name,
            requeue_running_entry_name=requeue_running_entry_name,
            mark_recovery_pending_name=mark_recovery_pending_name,
        ),
        **_optional_deps_from_namespace(
            legacy,
            try_reserve_admission_slot_name=try_reserve_admission_slot_name,
            start_background_job_process_name=start_background_job_process_name,
            find_queue_entry_name=find_queue_entry_name,
            load_config_name=load_config_name,
            read_worker_pid_name=read_worker_pid_name,
            worker_class_name=worker_class_name,
            on_worker_process_started_name=on_worker_process_started_name,
            shutdown_running_job_name=shutdown_running_job_name,
            before_shutdown_all_name=before_shutdown_all_name,
        ),
    )


__all__ = [
    "InternalEngineQueueWorkerDeps",
    "InternalEngineQueueWorkerDepsResolver",
    "internal_engine_queue_worker_deps_from_namespace",
]
