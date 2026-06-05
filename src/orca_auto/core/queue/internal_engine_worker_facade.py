from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .internal_engine_runtime import InternalEngineQueueRuntime
from .internal_engine_worker_deps import (
    InternalEngineQueueWorkerDeps,
    InternalEngineQueueWorkerDepsResolver,
    InternalEngineQueueWorkerNamespaceNames,
    internal_engine_queue_worker_deps_from_namespace,
)


@dataclass(frozen=True)
class InternalEngineQueueWorkerLifecycleFacade:
    runtime: InternalEngineQueueRuntime
    resolver: InternalEngineQueueWorkerDepsResolver
    shutdown_grace_seconds: float

    def queue_worker_hooks(self) -> Any:
        names = self.resolver.namespace_names()

        def activate_reserved_slot_fn(*args: Any, **kwargs: Any) -> Any:
            return self.resolver.dep(
                "activate_reserved_slot",
                names.activate_reserved_slot,
            )(*args, **kwargs)

        def terminate_process_fn(process: Any) -> Any:
            return self.resolver.dep(
                "terminate_process",
                names.terminate_process,
            )(process)

        def mark_failed_fn(*args: Any, **kwargs: Any) -> Any:
            return self.resolver.dep(
                "mark_failed",
                names.mark_failed,
            )(*args, **kwargs)

        def sleep_fn(seconds: float) -> None:
            self.resolver.dep("time_module", names.time_module).sleep(seconds)

        return self.runtime.child_worker_hooks(
            engine=self.runtime.spec.engine,
            handle_worker_start_error_fn=self.resolver.dep(
                "handle_worker_start_error",
                names.handle_worker_start_error,
            ),
            finalize_completed_job_fn=self.resolver.dep(
                "finalize_completed_job",
                names.finalize_completed_job,
            ),
            finalize_child_exit_fn=self.resolver.dep(
                "finalize_child_exit",
                names.finalize_child_exit,
            ),
            reconcile_worker_state_fn=self.resolver.dep(
                "reconcile_worker_state",
                names.reconcile_worker_state,
            ),
            activate_reserved_slot_fn=activate_reserved_slot_fn,
            terminate_process_fn=terminate_process_fn,
            mark_failed_fn=mark_failed_fn,
            shutdown_grace_seconds=self.shutdown_grace_seconds,
            sleep_fn=sleep_fn,
            on_worker_process_started_fn=self.resolver.optional_dep(
                "on_worker_process_started",
                names.on_worker_process_started,
            ),
            shutdown_running_job_fn=self.resolver.optional_dep(
                "shutdown_running_job",
                names.shutdown_running_job,
            ),
            before_shutdown_all_fn=self.resolver.optional_dep(
                "before_shutdown_all",
                names.before_shutdown_all,
            ),
        )

    def finalize_child_exit(self, worker: Any, job: Any, *, rc: int) -> None:
        names = self.resolver.namespace_names()
        self.runtime.spec.lifecycle().finalize_child_exit(
            worker.cfg,
            job,
            rc=rc,
            shutdown_requested=worker._shutdown_requested,
            find_queue_entry_fn=self.resolver.find_queue_entry,
            mark_cancelled_fn=self.resolver.dep(
                "mark_cancelled",
                names.mark_cancelled,
            ),
            requeue_running_entry_fn=self.resolver.dep(
                "requeue_running_entry",
                names.requeue_running_entry,
            ),
            mark_failed_fn=self.resolver.dep("mark_failed", names.mark_failed),
            mark_recovery_pending_fn=self.resolver.dep(
                "mark_recovery_pending",
                names.mark_recovery_pending,
            ),
            release_admission_slot_fn=worker._release_admission_slot,
        )

    def reconcile_orphaned_running(
        self,
        worker: Any,
        *,
        list_slots_fn: Callable[[Any], list[Any]] | None = None,
    ) -> None:
        names = self.resolver.namespace_names()
        self.runtime.spec.lifecycle().reconcile_orphaned_running(
            worker.cfg,
            admission_root=worker.admission_root,
            queue_roots_fn=self.runtime.queue_roots,
            list_queue_fn=self.resolver.dep("list_queue", names.list_queue),
            list_slots_fn=(
                list_slots_fn or self.resolver.dep("list_slots", names.list_slots)
            ),
            reconcile_stale_slots_fn=self.resolver.dep(
                "reconcile_stale_slots",
                names.reconcile_stale_slots,
            ),
            reconcile_orphaned_child_queue_entries_fn=self.resolver.dep(
                "reconcile_orphaned_child_queue_entries",
                names.reconcile_orphaned_child_queue_entries,
            ),
            mark_cancelled_fn=self.resolver.dep(
                "mark_cancelled",
                names.mark_cancelled,
            ),
            requeue_running_entry_fn=self.resolver.dep(
                "requeue_running_entry",
                names.requeue_running_entry,
            ),
            mark_recovery_pending_fn=self.resolver.dep(
                "mark_recovery_pending",
                names.mark_recovery_pending,
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
        load_config_fn: Callable[[Any], Any] | None = None,
        read_worker_pid_fn: Callable[[Path], int | None] | None = None,
        existing_pid_report_fn: Callable[[int], Any] | None = None,
        max_concurrent_fn: Callable[[Any], int] | None = None,
        worker_factory: Callable[..., Any] | None = None,
    ) -> int:
        names = self.resolver.namespace_names()

        def default_worker_factory(cfg: Any, config_path: str, **kwargs: Any) -> Any:
            worker_cls = self.resolver.dep("worker_class", names.worker_class)
            if worker_cls is None:
                raise ValueError("worker_class is required for queue worker command support")
            if config_path_keyword:
                return worker_cls(cfg, config_path=config_path, **kwargs)
            return worker_cls(cfg, config_path, **kwargs)

        return self.runtime.run_pidfile_worker_command(
            args,
            config_path_fn=config_path_fn,
            load_config_fn=load_config_fn or self.resolver.dep("load_config", names.load_config),
            read_worker_pid_fn=(
                read_worker_pid_fn or self.resolver.dep("read_worker_pid", names.read_worker_pid)
            ),
            existing_pid_report_fn=existing_pid_report_fn,
            max_concurrent_fn=max_concurrent_fn,
            worker_factory=worker_factory or default_worker_factory,
        )


@dataclass(frozen=True)
class InternalEngineQueueWorkerFacade:
    runtime: InternalEngineQueueRuntime
    poll_interval_seconds: int
    shutdown_grace_seconds: float
    deps: InternalEngineQueueWorkerDeps | None = None
    namespace: Mapping[str, Any] | None = None
    namespace_names: InternalEngineQueueWorkerNamespaceNames | None = None
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

    def resolved_namespace_names(self) -> InternalEngineQueueWorkerNamespaceNames:
        if self.namespace_names is not None:
            return self.namespace_names
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

    def _resolver(self) -> InternalEngineQueueWorkerDepsResolver:
        return InternalEngineQueueWorkerDepsResolver(
            runtime=self.runtime,
            deps=self.deps,
            namespace=self.namespace,
            names=self.resolved_namespace_names(),
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
        load_config_fn: Callable[[Any], Any] | None = None,
        read_worker_pid_fn: Callable[[Path], int | None] | None = None,
        existing_pid_report_fn: Callable[[int], Any] | None = None,
        max_concurrent_fn: Callable[[Any], int] | None = None,
        worker_factory: Callable[..., Any] | None = None,
    ) -> int:
        return InternalEngineQueueWorkerCommandRunner(
            runtime=self.runtime,
            resolver=self._resolver(),
        ).run_pidfile_worker_command(
            args,
            config_path_fn=config_path_fn,
            config_path_keyword=config_path_keyword,
            load_config_fn=load_config_fn,
            read_worker_pid_fn=read_worker_pid_fn,
            existing_pid_report_fn=existing_pid_report_fn,
            max_concurrent_fn=max_concurrent_fn,
            worker_factory=worker_factory,
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
