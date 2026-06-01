from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .internal_engine_runtime import InternalEngineQueueRuntime


@dataclass(frozen=True)
class InternalEngineQueueWorkerDeps:
    time_module: Any
    release_slot: Callable[[str | Path, str], object]
    reserve_slot: Callable[..., str | None]
    start_background_process: Callable[[list[str]], Any]
    build_worker_child_command: Callable[..., list[str]]
    config_path_for_worker: Callable[..., str]
    default_config_path: Callable[[], str]
    activate_reserved_slot: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    mark_failed: Callable[..., Any]
    handle_worker_start_error: Callable[[Any, Path, Any, str, OSError], None]
    finalize_completed_job: Callable[[Any, str, Any, int], None]
    finalize_child_exit: Callable[..., Any]
    reconcile_worker_state: Callable[[Any], None]
    list_queue: Callable[[Any], list[Any]]
    list_slots: Callable[[Any], list[Any]]
    reconcile_stale_slots: Callable[[Any], Any]
    reconcile_orphaned_child_queue_entries: Callable[..., Any]
    mark_cancelled: Callable[..., Any]
    requeue_running_entry: Callable[..., Any]
    mark_recovery_pending: Callable[..., Any]
    try_reserve_admission_slot: Callable[[Any], str | None] | None = None
    start_background_job_process_fn: Callable[..., Any] | None = None
    find_queue_entry: Callable[[Any, str], Any | None] | None = None
    load_config: Callable[[Any], Any] | None = None
    read_worker_pid: Callable[[Path], int | None] | None = None
    worker_class: Callable[..., Any] | None = None


@dataclass(frozen=True)
class InternalEngineQueueWorkerDepsResolver:
    runtime: InternalEngineQueueRuntime
    deps: InternalEngineQueueWorkerDeps | None = None
    namespace: Mapping[str, Any] | None = None
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

    def lookup(self, name: str) -> Any:
        if self.namespace is None:
            raise KeyError(name)
        return self.namespace[name]

    def dep(self, attr: str, fallback_name: str) -> Any:
        if self.deps is not None:
            return getattr(self.deps, attr)
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
        start_background_job_process_fn: Callable[..., Any],
        try_reserve_admission_slot_fn: Callable[[Any], str | None],
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


@dataclass(frozen=True)
class InternalEngineQueueWorkerLifecycleFacade:
    runtime: InternalEngineQueueRuntime
    resolver: InternalEngineQueueWorkerDepsResolver
    shutdown_grace_seconds: float

    def queue_worker_hooks(self) -> Any:
        def activate_reserved_slot_fn(*args: Any, **kwargs: Any) -> Any:
            return self.resolver.dep(
                "activate_reserved_slot",
                self.resolver.activate_reserved_slot_name,
            )(*args, **kwargs)

        def terminate_process_fn(process: Any) -> Any:
            return self.resolver.dep(
                "terminate_process",
                self.resolver.terminate_process_name,
            )(process)

        def mark_failed_fn(*args: Any, **kwargs: Any) -> Any:
            return self.resolver.dep(
                "mark_failed",
                self.resolver.mark_failed_name,
            )(*args, **kwargs)

        def sleep_fn(seconds: float) -> None:
            self.resolver.dep("time_module", self.resolver.time_module_name).sleep(seconds)

        if self.resolver.deps is not None:
            deps = self.resolver.deps
            return self.runtime.child_worker_hooks(
                engine=self.runtime.spec.engine,
                handle_worker_start_error_fn=deps.handle_worker_start_error,
                finalize_completed_job_fn=deps.finalize_completed_job,
                finalize_child_exit_fn=deps.finalize_child_exit,
                reconcile_worker_state_fn=deps.reconcile_worker_state,
                activate_reserved_slot_fn=activate_reserved_slot_fn,
                terminate_process_fn=terminate_process_fn,
                mark_failed_fn=mark_failed_fn,
                shutdown_grace_seconds=self.shutdown_grace_seconds,
                sleep_fn=sleep_fn,
            )

        return self.runtime.child_worker_hooks_from_namespace(
            namespace=self.resolver.namespace or {},
            activate_reserved_slot_fn=activate_reserved_slot_fn,
            terminate_process_fn=terminate_process_fn,
            mark_failed_fn=mark_failed_fn,
            shutdown_grace_seconds=self.shutdown_grace_seconds,
            sleep_fn=sleep_fn,
        )

    def finalize_child_exit(self, worker: Any, job: Any, *, rc: int) -> None:
        self.runtime.spec.lifecycle().finalize_child_exit(
            worker.cfg,
            job,
            rc=rc,
            shutdown_requested=worker._shutdown_requested,
            find_queue_entry_fn=self.resolver.find_queue_entry,
            mark_cancelled_fn=self.resolver.dep(
                "mark_cancelled",
                self.resolver.mark_cancelled_name,
            ),
            requeue_running_entry_fn=self.resolver.dep(
                "requeue_running_entry",
                self.resolver.requeue_running_entry_name,
            ),
            mark_failed_fn=self.resolver.dep("mark_failed", self.resolver.mark_failed_name),
            mark_recovery_pending_fn=self.resolver.dep(
                "mark_recovery_pending",
                self.resolver.mark_recovery_pending_name,
            ),
            release_admission_slot_fn=worker._release_admission_slot,
        )

    def reconcile_orphaned_running(
        self,
        worker: Any,
        *,
        list_slots_fn: Callable[[Any], list[Any]] | None = None,
    ) -> None:
        self.runtime.spec.lifecycle().reconcile_orphaned_running(
            worker.cfg,
            admission_root=worker.admission_root,
            queue_roots_fn=self.runtime.queue_roots,
            list_queue_fn=self.resolver.dep("list_queue", self.resolver.list_queue_name),
            list_slots_fn=(
                list_slots_fn or self.resolver.dep("list_slots", self.resolver.list_slots_name)
            ),
            reconcile_stale_slots_fn=self.resolver.dep(
                "reconcile_stale_slots",
                self.resolver.reconcile_stale_slots_name,
            ),
            reconcile_orphaned_child_queue_entries_fn=self.resolver.dep(
                "reconcile_orphaned_child_queue_entries",
                self.resolver.reconcile_orphaned_child_queue_entries_name,
            ),
            mark_cancelled_fn=self.resolver.dep(
                "mark_cancelled",
                self.resolver.mark_cancelled_name,
            ),
            requeue_running_entry_fn=self.resolver.dep(
                "requeue_running_entry",
                self.resolver.requeue_running_entry_name,
            ),
            mark_recovery_pending_fn=self.resolver.dep(
                "mark_recovery_pending",
                self.resolver.mark_recovery_pending_name,
            ),
        )


@dataclass(frozen=True)
class InternalEngineQueueWorkerCommandRunner:
    runtime: InternalEngineQueueRuntime
    resolver: InternalEngineQueueWorkerDepsResolver

    def run_pidfile_worker_command(
        self,
        args: Any,
        *,
        config_path_fn: Callable[[Any], str],
        config_path_keyword: bool = True,
    ) -> int:
        if self.resolver.deps is not None:

            def worker_factory(cfg: Any, config_path: str, **kwargs: Any) -> Any:
                deps = self.resolver.deps
                if deps is None or deps.worker_class is None:
                    raise ValueError("worker_class is required for queue worker command support")
                if config_path_keyword:
                    return deps.worker_class(cfg, config_path=config_path, **kwargs)
                return deps.worker_class(cfg, config_path, **kwargs)

            return self.runtime.run_pidfile_worker_command(
                args,
                config_path_fn=config_path_fn,
                load_config_fn=self.resolver.deps.load_config,
                read_worker_pid_fn=self.resolver.deps.read_worker_pid,
                worker_factory=worker_factory,
            )

        return self.runtime.run_pidfile_worker_command_from_namespace(
            args,
            namespace=self.resolver.namespace or {},
            config_path_fn=config_path_fn,
            config_path_keyword=config_path_keyword,
        )


def internal_engine_queue_worker_deps_from_namespace(
    namespace: Mapping[str, Any],
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
) -> InternalEngineQueueWorkerDeps:
    def call(name: str, *args: Any, **kwargs: Any) -> Any:
        return namespace[name](*args, **kwargs)

    return InternalEngineQueueWorkerDeps(
        time_module=namespace[time_module_name],
        release_slot=lambda root, token: call(release_slot_name, root, token),
        reserve_slot=lambda *args, **kwargs: call(reserve_slot_name, *args, **kwargs),
        start_background_process=lambda command: call(start_background_process_name, command),
        build_worker_child_command=lambda *args, **kwargs: call(
            build_worker_child_command_name,
            *args,
            **kwargs,
        ),
        config_path_for_worker=lambda *args, **kwargs: call(
            config_path_for_worker_name,
            *args,
            **kwargs,
        ),
        default_config_path=lambda: call(default_config_path_name),
        activate_reserved_slot=lambda *args, **kwargs: call(
            activate_reserved_slot_name,
            *args,
            **kwargs,
        ),
        terminate_process=lambda process: call(terminate_process_name, process),
        mark_failed=lambda *args, **kwargs: call(mark_failed_name, *args, **kwargs),
        handle_worker_start_error=lambda *args, **kwargs: call(
            handle_worker_start_error_name,
            *args,
            **kwargs,
        ),
        finalize_completed_job=lambda *args, **kwargs: call(
            finalize_completed_job_name,
            *args,
            **kwargs,
        ),
        finalize_child_exit=lambda *args, **kwargs: call(
            finalize_child_exit_name,
            *args,
            **kwargs,
        ),
        reconcile_worker_state=lambda worker: call(reconcile_worker_state_name, worker),
        list_queue=lambda root: call(list_queue_name, root),
        list_slots=lambda root: call(list_slots_name, root),
        reconcile_stale_slots=lambda root: call(reconcile_stale_slots_name, root),
        reconcile_orphaned_child_queue_entries=lambda *args, **kwargs: call(
            reconcile_orphaned_child_queue_entries_name,
            *args,
            **kwargs,
        ),
        mark_cancelled=lambda *args, **kwargs: call(mark_cancelled_name, *args, **kwargs),
        requeue_running_entry=lambda *args, **kwargs: call(
            requeue_running_entry_name,
            *args,
            **kwargs,
        ),
        mark_recovery_pending=lambda *args, **kwargs: call(
            mark_recovery_pending_name,
            *args,
            **kwargs,
        ),
        try_reserve_admission_slot=lambda cfg: call(try_reserve_admission_slot_name, cfg),
        start_background_job_process_fn=lambda **kwargs: call(
            start_background_job_process_name,
            **kwargs,
        ),
        find_queue_entry=(
            None
            if find_queue_entry_name is None
            else lambda root, queue_id: call(find_queue_entry_name, root, queue_id)
        ),
        load_config=lambda config_path: call(load_config_name, config_path),
        read_worker_pid=lambda allowed_root: call(read_worker_pid_name, allowed_root),
        worker_class=lambda *args, **kwargs: call(worker_class_name, *args, **kwargs),
    )


@dataclass(frozen=True)
class InternalEngineQueueWorkerFacade:
    runtime: InternalEngineQueueRuntime
    poll_interval_seconds: int
    shutdown_grace_seconds: float
    deps: InternalEngineQueueWorkerDeps | None = None
    namespace: Mapping[str, Any] | None = None
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

    def _resolver(self) -> InternalEngineQueueWorkerDepsResolver:
        return InternalEngineQueueWorkerDepsResolver(
            runtime=self.runtime,
            deps=self.deps,
            namespace=self.namespace,
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
        )

    def _lifecycle(self) -> InternalEngineQueueWorkerLifecycleFacade:
        return InternalEngineQueueWorkerLifecycleFacade(
            runtime=self.runtime,
            resolver=self._resolver(),
            shutdown_grace_seconds=self.shutdown_grace_seconds,
        )

    def queue_worker_deps(self) -> Any:
        return self._resolver().queue_worker_deps(
            poll_interval_seconds=self.poll_interval_seconds,
            start_background_job_process_fn=self.start_background_job_process,
            try_reserve_admission_slot_fn=self.try_reserve_admission_slot,
        )

    def try_reserve_admission_slot(self, cfg: Any) -> str | None:
        return self._resolver().try_reserve_admission_slot(cfg)

    def start_background_job_process(
        self,
        *,
        config_path: str,
        queue_root: Path,
        entry: Any,
        admission_root: str | Path,
        admission_token: str,
    ) -> Any:
        return self._resolver().start_background_job_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
        )

    def config_path_for_worker(self, args: Any) -> str:
        return self._resolver().config_path_for_worker(args)

    def queue_worker_hooks(self) -> Any:
        return self._lifecycle().queue_worker_hooks()

    def run_pidfile_worker_command(
        self,
        args: Any,
        *,
        config_path_fn: Callable[[Any], str],
        config_path_keyword: bool = True,
    ) -> int:
        return InternalEngineQueueWorkerCommandRunner(
            runtime=self.runtime,
            resolver=self._resolver(),
        ).run_pidfile_worker_command(
            args,
            config_path_fn=config_path_fn,
            config_path_keyword=config_path_keyword,
        )

    def finalize_child_exit(self, worker: Any, job: Any, *, rc: int) -> None:
        self._lifecycle().finalize_child_exit(worker, job, rc=rc)

    def reconcile_orphaned_running(
        self,
        worker: Any,
        *,
        list_slots_fn: Callable[[Any], list[Any]] | None = None,
    ) -> None:
        self._lifecycle().reconcile_orphaned_running(worker, list_slots_fn=list_slots_fn)


__all__ = [
    "InternalEngineQueueWorkerCommandRunner",
    "InternalEngineQueueWorkerDeps",
    "InternalEngineQueueWorkerDepsResolver",
    "InternalEngineQueueWorkerFacade",
    "InternalEngineQueueWorkerLifecycleFacade",
    "internal_engine_queue_worker_deps_from_namespace",
]
