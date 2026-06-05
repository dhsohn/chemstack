from __future__ import annotations

import time
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
class InternalEngineQueueWorkerNamespaceNames:
    time_module: str = "time"
    release_slot: str = "release_slot"
    reserve_slot: str = "reserve_slot"
    start_background_process: str = "start_background_process"
    build_worker_child_command: str = "build_worker_child_command"
    config_path_for_worker: str = "config_path_for_worker"
    default_config_path: str = "default_config_path"
    activate_reserved_slot: str = "activate_reserved_slot"
    terminate_process: str = "_terminate_process"
    mark_failed: str = "mark_failed"
    handle_worker_start_error: str = "_handle_worker_start_error"
    finalize_completed_job: str = "_finalize_completed_job"
    finalize_child_exit: str = "_finalize_child_exit"
    reconcile_worker_state: str = "_reconcile_worker_state"
    list_queue: str = "list_queue"
    list_slots: str = "list_slots"
    reconcile_stale_slots: str = "reconcile_stale_slots"
    reconcile_orphaned_child_queue_entries: str = "reconcile_orphaned_child_queue_entries"
    mark_cancelled: str = "mark_cancelled"
    requeue_running_entry: str = "requeue_running_entry"
    mark_recovery_pending: str = "_mark_recovery_pending_entry"
    try_reserve_admission_slot: str = "_try_reserve_admission_slot"
    start_background_job_process: str = "_start_background_job_process"
    find_queue_entry: str | None = None
    load_config: str = "load_config"
    read_worker_pid: str = "read_worker_pid"
    worker_class: str = "QueueWorker"
    on_worker_process_started: str | None = None
    shutdown_running_job: str | None = None
    before_shutdown_all: str | None = None

    @classmethod
    def from_legacy_names(
        cls,
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
        reconcile_orphaned_child_queue_entries_name: str = (
            "reconcile_orphaned_child_queue_entries"
        ),
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
    ) -> InternalEngineQueueWorkerNamespaceNames:
        return cls(
            time_module=time_module_name,
            release_slot=release_slot_name,
            reserve_slot=reserve_slot_name,
            start_background_process=start_background_process_name,
            build_worker_child_command=build_worker_child_command_name,
            config_path_for_worker=config_path_for_worker_name,
            default_config_path=default_config_path_name,
            activate_reserved_slot=activate_reserved_slot_name,
            terminate_process=terminate_process_name,
            mark_failed=mark_failed_name,
            handle_worker_start_error=handle_worker_start_error_name,
            finalize_completed_job=finalize_completed_job_name,
            finalize_child_exit=finalize_child_exit_name,
            reconcile_worker_state=reconcile_worker_state_name,
            list_queue=list_queue_name,
            list_slots=list_slots_name,
            reconcile_stale_slots=reconcile_stale_slots_name,
            reconcile_orphaned_child_queue_entries=(
                reconcile_orphaned_child_queue_entries_name
            ),
            mark_cancelled=mark_cancelled_name,
            requeue_running_entry=requeue_running_entry_name,
            mark_recovery_pending=mark_recovery_pending_name,
            try_reserve_admission_slot=try_reserve_admission_slot_name,
            start_background_job_process=start_background_job_process_name,
            find_queue_entry=find_queue_entry_name,
            load_config=load_config_name,
            read_worker_pid=read_worker_pid_name,
            worker_class=worker_class_name,
            on_worker_process_started=on_worker_process_started_name,
            shutdown_running_job=shutdown_running_job_name,
            before_shutdown_all=before_shutdown_all_name,
        )


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


def _noop_callback(*_args: Any, **_kwargs: Any) -> None:
    return None


def _empty_default_config_path() -> str:
    return ""


def _config_path_from_args(args: Any, *, default_config_path_fn: DefaultConfigPath) -> str:
    return str(getattr(args, "config", "") or default_config_path_fn())


_DepsKwargs = dict[str, Any]
_CallbackSupplier = Callable[[], Callable[..., Any]]


@dataclass(frozen=True)
class InternalEngineQueueWorkerFacadeCallbacks:
    release_slot: SlotReleaser
    reserve_slot: SlotReserver
    start_background_process: BackgroundProcessStarter
    build_worker_child_command: WorkerChildCommandBuilder
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
    mark_cancelled: QueueStatusMarker
    requeue_running_entry: RunningEntryRequeuer
    config_path_for_worker: ConfigPathForWorker = _config_path_from_args
    default_config_path: DefaultConfigPath = _empty_default_config_path
    reconcile_orphaned_child_queue_entries: OrphanedChildQueueReconciler = _noop_callback
    mark_recovery_pending: RecoveryPendingMarker = _noop_callback
    try_reserve_admission_slot: AdmissionSlotReserver | None = None
    start_background_job_process: BackgroundJobProcessStarter | None = None
    find_queue_entry: QueueEntryFinder | None = None
    load_config: ConfigLoader | None = None
    read_worker_pid: WorkerPidReader | None = None
    worker_class: WorkerFactory | None = None
    on_worker_process_started: WorkerProcessStartedHook | None = None
    shutdown_running_job: ShutdownRunningJob | None = None
    before_shutdown_all: BeforeShutdownAll | None = None


@dataclass(frozen=True)
class InternalEngineQueueWorkerFacadeBindings:
    """Late-bound callback suppliers for import-time runtime facades.

    Engine modules keep concrete functions with narrower signatures; the
    wrappers built from these suppliers expose the shared worker callback
    contract while still honoring monkeypatches made after deps construction.
    """

    release_slot: _CallbackSupplier
    reserve_slot: _CallbackSupplier
    start_background_process: _CallbackSupplier
    build_worker_child_command: _CallbackSupplier
    activate_reserved_slot: _CallbackSupplier
    terminate_process: _CallbackSupplier
    mark_failed: _CallbackSupplier
    handle_worker_start_error: _CallbackSupplier
    finalize_completed_job: _CallbackSupplier
    finalize_child_exit: _CallbackSupplier
    reconcile_worker_state: _CallbackSupplier
    list_queue: _CallbackSupplier
    list_slots: _CallbackSupplier
    reconcile_stale_slots: _CallbackSupplier
    mark_cancelled: _CallbackSupplier
    requeue_running_entry: _CallbackSupplier
    config_path_for_worker: _CallbackSupplier | None = None
    default_config_path: _CallbackSupplier | None = None
    reconcile_orphaned_child_queue_entries: _CallbackSupplier | None = None
    mark_recovery_pending: _CallbackSupplier | None = None
    try_reserve_admission_slot: _CallbackSupplier | None = None
    start_background_job_process: _CallbackSupplier | None = None
    find_queue_entry: _CallbackSupplier | None = None
    load_config: _CallbackSupplier | None = None
    read_worker_pid: _CallbackSupplier | None = None
    worker_class: _CallbackSupplier | None = None
    on_worker_process_started: _CallbackSupplier | None = None
    shutdown_running_job: _CallbackSupplier | None = None
    before_shutdown_all: _CallbackSupplier | None = None


def _late_optional(
    supplier: _CallbackSupplier | None,
) -> Callable[..., Any] | None:
    if supplier is None:
        return None
    return lambda *args, **kwargs: supplier()(*args, **kwargs)


def _late_config_path_for_worker(
    supplier: _CallbackSupplier | None,
) -> ConfigPathForWorker:
    if supplier is None:
        return _config_path_from_args
    callback_supplier = supplier
    return lambda args, *, default_config_path_fn: callback_supplier()(
        args,
        default_config_path_fn=default_config_path_fn,
    )


def _late_default_config_path(supplier: _CallbackSupplier | None) -> DefaultConfigPath:
    if supplier is None:
        return _empty_default_config_path
    callback_supplier = supplier
    return lambda: callback_supplier()()


def build_late_bound_internal_engine_queue_worker_facade_callbacks(
    bindings: InternalEngineQueueWorkerFacadeBindings,
) -> InternalEngineQueueWorkerFacadeCallbacks:
    return InternalEngineQueueWorkerFacadeCallbacks(
        release_slot=lambda root, token: bindings.release_slot()(root, token),
        reserve_slot=lambda *args, **kwargs: bindings.reserve_slot()(*args, **kwargs),
        start_background_process=lambda command: bindings.start_background_process()(
            command
        ),
        build_worker_child_command=lambda **kwargs: bindings.build_worker_child_command()(
            **kwargs
        ),
        config_path_for_worker=_late_config_path_for_worker(
            bindings.config_path_for_worker,
        ),
        default_config_path=_late_default_config_path(bindings.default_config_path),
        activate_reserved_slot=lambda *args, **kwargs: bindings.activate_reserved_slot()(
            *args,
            **kwargs,
        ),
        terminate_process=lambda process: bindings.terminate_process()(process),
        mark_failed=lambda *args, **kwargs: bindings.mark_failed()(*args, **kwargs),
        handle_worker_start_error=lambda worker, queue_root, entry, admission_token, exc: (
            bindings.handle_worker_start_error()(
                worker,
                queue_root,
                entry,
                admission_token,
                exc,
            )
        ),
        finalize_completed_job=lambda worker, queue_id, job, rc: (
            bindings.finalize_completed_job()(worker, queue_id, job, rc)
        ),
        finalize_child_exit=lambda worker, job, *, rc: (
            bindings.finalize_child_exit()(worker, job, rc=rc)
        ),
        reconcile_worker_state=lambda worker: bindings.reconcile_worker_state()(worker),
        list_queue=lambda root: bindings.list_queue()(root),
        list_slots=lambda root: bindings.list_slots()(root),
        reconcile_stale_slots=lambda root: bindings.reconcile_stale_slots()(root),
        reconcile_orphaned_child_queue_entries=(
            _late_optional(bindings.reconcile_orphaned_child_queue_entries)
            or _noop_callback
        ),
        mark_cancelled=lambda *args, **kwargs: bindings.mark_cancelled()(
            *args,
            **kwargs,
        ),
        requeue_running_entry=lambda *args, **kwargs: bindings.requeue_running_entry()(
            *args,
            **kwargs,
        ),
        mark_recovery_pending=(
            _late_optional(bindings.mark_recovery_pending) or _noop_callback
        ),
        try_reserve_admission_slot=_late_optional(bindings.try_reserve_admission_slot),
        start_background_job_process=_late_optional(bindings.start_background_job_process),
        find_queue_entry=_late_optional(bindings.find_queue_entry),
        load_config=_late_optional(bindings.load_config),
        read_worker_pid=_late_optional(bindings.read_worker_pid),
        worker_class=_late_optional(bindings.worker_class),
        on_worker_process_started=_late_optional(bindings.on_worker_process_started),
        shutdown_running_job=_late_optional(bindings.shutdown_running_job),
        before_shutdown_all=_late_optional(bindings.before_shutdown_all),
    )


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


def build_internal_engine_queue_worker_deps(
    callbacks: InternalEngineQueueWorkerFacadeCallbacks,
    *,
    time_module: Any = time,
) -> InternalEngineQueueWorkerDeps:
    return InternalEngineQueueWorkerDeps(
        time_module=time_module,
        release_slot=callbacks.release_slot,
        reserve_slot=callbacks.reserve_slot,
        start_background_process=callbacks.start_background_process,
        build_worker_child_command=callbacks.build_worker_child_command,
        config_path_for_worker=callbacks.config_path_for_worker,
        default_config_path=callbacks.default_config_path,
        activate_reserved_slot=callbacks.activate_reserved_slot,
        terminate_process=callbacks.terminate_process,
        mark_failed=callbacks.mark_failed,
        handle_worker_start_error=callbacks.handle_worker_start_error,
        finalize_completed_job=callbacks.finalize_completed_job,
        finalize_child_exit=callbacks.finalize_child_exit,
        reconcile_worker_state=callbacks.reconcile_worker_state,
        list_queue=callbacks.list_queue,
        list_slots=callbacks.list_slots,
        reconcile_stale_slots=callbacks.reconcile_stale_slots,
        reconcile_orphaned_child_queue_entries=(
            callbacks.reconcile_orphaned_child_queue_entries
        ),
        mark_cancelled=callbacks.mark_cancelled,
        requeue_running_entry=callbacks.requeue_running_entry,
        mark_recovery_pending=callbacks.mark_recovery_pending,
        try_reserve_admission_slot=callbacks.try_reserve_admission_slot,
        start_background_job_process_fn=callbacks.start_background_job_process,
        find_queue_entry=callbacks.find_queue_entry,
        load_config=callbacks.load_config,
        read_worker_pid=callbacks.read_worker_pid,
        worker_class=callbacks.worker_class,
        on_worker_process_started=callbacks.on_worker_process_started,
        shutdown_running_job=callbacks.shutdown_running_job,
        before_shutdown_all=callbacks.before_shutdown_all,
    )


def build_late_bound_internal_engine_queue_worker_deps(
    bindings: InternalEngineQueueWorkerFacadeBindings,
    *,
    time_module: Any = time,
) -> InternalEngineQueueWorkerDeps:
    return build_internal_engine_queue_worker_deps(
        build_late_bound_internal_engine_queue_worker_facade_callbacks(bindings),
        time_module=time_module,
    )


@dataclass(frozen=True)
class InternalEngineQueueWorkerDepsResolver:
    runtime: InternalEngineQueueRuntime
    deps: InternalEngineQueueWorkerDeps | None = None
    namespace: LegacyWorkerNamespace | None = None
    names: InternalEngineQueueWorkerNamespaceNames | None = None
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

    def namespace_names(self) -> InternalEngineQueueWorkerNamespaceNames:
        if self.names is not None:
            return self.names
        return InternalEngineQueueWorkerNamespaceNames.from_legacy_names(
            time_module_name=self.time_module_name,
            release_slot_name=self.release_slot_name,
            reserve_slot_name=self.reserve_slot_name,
            start_background_process_name=self.start_background_process_name,
            build_worker_child_command_name=self.build_worker_child_command_name,
            config_path_for_worker_name=self.config_path_for_worker_name,
            default_config_path_name=self.default_config_path_name,
            activate_reserved_slot_name=self.activate_reserved_slot_name,
            terminate_process_name=self.terminate_process_name,
            mark_failed_name=self.mark_failed_name,
            find_queue_entry_name=self.find_queue_entry_name,
            list_queue_name=self.list_queue_name,
            list_slots_name=self.list_slots_name,
            reconcile_stale_slots_name=self.reconcile_stale_slots_name,
            reconcile_orphaned_child_queue_entries_name=(
                self.reconcile_orphaned_child_queue_entries_name
            ),
            mark_cancelled_name=self.mark_cancelled_name,
            requeue_running_entry_name=self.requeue_running_entry_name,
            mark_recovery_pending_name=self.mark_recovery_pending_name,
            on_worker_process_started_name=self.on_worker_process_started_name,
            shutdown_running_job_name=self.shutdown_running_job_name,
            before_shutdown_all_name=self.before_shutdown_all_name,
        )

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
        names = self.namespace_names()
        if names.find_queue_entry and self.namespace is not None:
            return self.lookup(names.find_queue_entry)(queue_root, queue_id)
        return self.runtime.queue_entry_by_id(queue_root, queue_id)

    def queue_worker_deps(
        self,
        *,
        poll_interval_seconds: int,
        start_background_job_process_fn: BackgroundJobProcessStarter,
        try_reserve_admission_slot_fn: AdmissionSlotReserver,
    ) -> Any:
        names = self.namespace_names()
        return self.runtime.child_worker_deps(
            poll_interval_seconds=poll_interval_seconds,
            time_module=self.dep("time_module", names.time_module),
            release_slot_fn=self.dep("release_slot", names.release_slot),
            start_background_job_process_fn=(
                self.optional_dep("start_background_job_process_fn", None)
                or start_background_job_process_fn
            ),
            try_reserve_admission_slot_fn=(
                self.optional_dep("try_reserve_admission_slot", None)
                or try_reserve_admission_slot_fn
            ),
        )

    def try_reserve_admission_slot(self, cfg: Any) -> str | None:
        reserve_slot_fn = self.dep("reserve_slot", self.namespace_names().reserve_slot)
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
        names = self.namespace_names()
        return self.runtime.start_child_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
            start_background_process_fn=self.dep(
                "start_background_process",
                names.start_background_process,
            ),
            build_worker_child_command_fn=self.dep(
                "build_worker_child_command",
                names.build_worker_child_command,
            ),
        )

    def config_path_for_worker(self, args: Any) -> str:
        names = self.namespace_names()
        return self.dep("config_path_for_worker", names.config_path_for_worker)(
            args,
            default_config_path_fn=self.dep("default_config_path", names.default_config_path),
        )


def _required_callbacks_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> _DepsKwargs:
    return {
        "release_slot": lambda root, token: legacy.call(names.release_slot, root, token),
        "reserve_slot": _legacy_call_fn(legacy, names.reserve_slot),
        "start_background_process": lambda command: legacy.call(
            names.start_background_process,
            command,
        ),
        "build_worker_child_command": _legacy_call_fn(
            legacy,
            names.build_worker_child_command,
        ),
        "config_path_for_worker": _legacy_call_fn(legacy, names.config_path_for_worker),
        "default_config_path": lambda: legacy.call(names.default_config_path),
        "activate_reserved_slot": _legacy_call_fn(legacy, names.activate_reserved_slot),
        "terminate_process": lambda process: legacy.call(names.terminate_process, process),
        "mark_failed": _legacy_call_fn(legacy, names.mark_failed),
    }


def _lifecycle_callbacks_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> _DepsKwargs:
    return {
        "handle_worker_start_error": _legacy_call_fn(
            legacy,
            names.handle_worker_start_error,
        ),
        "finalize_completed_job": _legacy_call_fn(legacy, names.finalize_completed_job),
        "finalize_child_exit": _legacy_call_fn(legacy, names.finalize_child_exit),
        "reconcile_worker_state": lambda worker: legacy.call(
            names.reconcile_worker_state,
            worker,
        ),
    }


def _reconciliation_callbacks_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> _DepsKwargs:
    return {
        "list_queue": lambda root: legacy.call(names.list_queue, root),
        "list_slots": lambda root: legacy.call(names.list_slots, root),
        "reconcile_stale_slots": lambda root: legacy.call(
            names.reconcile_stale_slots,
            root,
        ),
        "reconcile_orphaned_child_queue_entries": _legacy_call_fn(
            legacy,
            names.reconcile_orphaned_child_queue_entries,
        ),
        "mark_cancelled": _legacy_call_fn(legacy, names.mark_cancelled),
        "requeue_running_entry": _legacy_call_fn(legacy, names.requeue_running_entry),
        "mark_recovery_pending": _legacy_call_fn(legacy, names.mark_recovery_pending),
    }


def _optional_callbacks_from_namespace(
    legacy: _LegacyNamespaceAdapter,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> _DepsKwargs:
    return {
        "try_reserve_admission_slot": lambda cfg: legacy.call(
            names.try_reserve_admission_slot,
            cfg,
        ),
        "start_background_job_process": lambda **kwargs: legacy.call(
            names.start_background_job_process,
            **kwargs,
        ),
        "find_queue_entry": _legacy_queue_entry_finder(legacy, names.find_queue_entry),
        "load_config": lambda config_path: legacy.call(names.load_config, config_path),
        "read_worker_pid": lambda allowed_root: legacy.call(
            names.read_worker_pid,
            allowed_root,
        ),
        "worker_class": _legacy_call_fn(legacy, names.worker_class),
        "on_worker_process_started": _legacy_optional_call_fn(
            legacy,
            names.on_worker_process_started,
        ),
        "shutdown_running_job": _legacy_optional_call_fn(
            legacy,
            names.shutdown_running_job,
        ),
        "before_shutdown_all": _legacy_optional_call_fn(
            legacy,
            names.before_shutdown_all,
        ),
    }


def internal_engine_queue_worker_callbacks_from_namespace_names(
    namespace: LegacyWorkerNamespace,
    *,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> InternalEngineQueueWorkerFacadeCallbacks:
    legacy = _LegacyNamespaceAdapter(namespace)

    return InternalEngineQueueWorkerFacadeCallbacks(
        **_required_callbacks_from_namespace(
            legacy,
            names,
        ),
        **_lifecycle_callbacks_from_namespace(
            legacy,
            names,
        ),
        **_reconciliation_callbacks_from_namespace(
            legacy,
            names,
        ),
        **_optional_callbacks_from_namespace(
            legacy,
            names,
        ),
    )


def internal_engine_queue_worker_callbacks_from_namespace(
    namespace: LegacyWorkerNamespace,
    *,
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
) -> InternalEngineQueueWorkerFacadeCallbacks:
    return internal_engine_queue_worker_callbacks_from_namespace_names(
        namespace,
        names=InternalEngineQueueWorkerNamespaceNames.from_legacy_names(
            release_slot_name=release_slot_name,
            reserve_slot_name=reserve_slot_name,
            start_background_process_name=start_background_process_name,
            build_worker_child_command_name=build_worker_child_command_name,
            config_path_for_worker_name=config_path_for_worker_name,
            default_config_path_name=default_config_path_name,
            activate_reserved_slot_name=activate_reserved_slot_name,
            terminate_process_name=terminate_process_name,
            mark_failed_name=mark_failed_name,
            handle_worker_start_error_name=handle_worker_start_error_name,
            finalize_completed_job_name=finalize_completed_job_name,
            finalize_child_exit_name=finalize_child_exit_name,
            reconcile_worker_state_name=reconcile_worker_state_name,
            list_queue_name=list_queue_name,
            list_slots_name=list_slots_name,
            reconcile_stale_slots_name=reconcile_stale_slots_name,
            reconcile_orphaned_child_queue_entries_name=(
                reconcile_orphaned_child_queue_entries_name
            ),
            mark_cancelled_name=mark_cancelled_name,
            requeue_running_entry_name=requeue_running_entry_name,
            mark_recovery_pending_name=mark_recovery_pending_name,
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


def internal_engine_queue_worker_deps_from_namespace_names(
    namespace: LegacyWorkerNamespace,
    *,
    names: InternalEngineQueueWorkerNamespaceNames,
) -> InternalEngineQueueWorkerDeps:
    legacy = _LegacyNamespaceAdapter(namespace)
    callbacks = internal_engine_queue_worker_callbacks_from_namespace_names(
        namespace,
        names=names,
    )
    return build_internal_engine_queue_worker_deps(
        callbacks,
        time_module=legacy.lookup(names.time_module),
    )


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
    return internal_engine_queue_worker_deps_from_namespace_names(
        namespace,
        names=InternalEngineQueueWorkerNamespaceNames.from_legacy_names(
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
            handle_worker_start_error_name=handle_worker_start_error_name,
            finalize_completed_job_name=finalize_completed_job_name,
            finalize_child_exit_name=finalize_child_exit_name,
            reconcile_worker_state_name=reconcile_worker_state_name,
            list_queue_name=list_queue_name,
            list_slots_name=list_slots_name,
            reconcile_stale_slots_name=reconcile_stale_slots_name,
            reconcile_orphaned_child_queue_entries_name=(
                reconcile_orphaned_child_queue_entries_name
            ),
            mark_cancelled_name=mark_cancelled_name,
            requeue_running_entry_name=requeue_running_entry_name,
            mark_recovery_pending_name=mark_recovery_pending_name,
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
    "InternalEngineQueueWorkerFacadeBindings",
    "InternalEngineQueueWorkerFacadeCallbacks",
    "InternalEngineQueueWorkerDeps",
    "InternalEngineQueueWorkerDepsResolver",
    "InternalEngineQueueWorkerNamespaceNames",
    "build_internal_engine_queue_worker_deps",
    "build_late_bound_internal_engine_queue_worker_deps",
    "build_late_bound_internal_engine_queue_worker_facade_callbacks",
    "internal_engine_queue_worker_callbacks_from_namespace",
    "internal_engine_queue_worker_callbacks_from_namespace_names",
    "internal_engine_queue_worker_deps_from_namespace",
    "internal_engine_queue_worker_deps_from_namespace_names",
]
