from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from . import lifecycle as _queue_lifecycle
from .internal_engine_child import InternalEngineWorkerChild
from .internal_engine_policies import InternalEngineAdmission, InternalEngineLifecycle
from .internal_engine_status import entry_status_is_running


@dataclass(frozen=True)
class InternalEngineLifecycleModuleExports:
    shutdown_running_job: Callable[..., Any]
    finalize_child_exit: Callable[..., Any]
    reconcile_orphaned_running: Callable[..., Any]


@dataclass(frozen=True)
class InternalEngineWorkerChildModuleExports:
    worker_child: InternalEngineWorkerChild
    build_worker_child_command: Callable[..., list[str]]
    install_shutdown_signal_handlers: Callable[..., Any]
    run_worker_child_job: Callable[..., int]
    shutdown_signal_handler_installer: Callable[..., Any]
    build_parser: Callable[..., Any]


@dataclass
class InternalEngineWorkerChildModuleFacade:
    worker_child: InternalEngineWorkerChild
    WORKER_JOB_MODULE: str
    WorkerShutdownRequested: type[BaseException]
    build_worker_child_command: Callable[..., list[str]]
    run_worker_child_job: Callable[..., int]
    shutdown_signal_handler_installer: Callable[..., Any]
    build_parser: Callable[..., Any]


@dataclass(frozen=True)
class InternalEngineSpec:
    engine: str
    worker_job_module: str = ""
    worker_pid_file_name: str = ""
    include_admission_root: bool = False

    def admission(self) -> InternalEngineAdmission:
        return InternalEngineAdmission(
            engine=self.engine,
            include_admission_root=self.include_admission_root,
        )

    def lifecycle(self) -> InternalEngineLifecycle:
        return InternalEngineLifecycle()

    def lifecycle_module_exports(self) -> InternalEngineLifecycleModuleExports:
        lifecycle = self.lifecycle()
        return InternalEngineLifecycleModuleExports(
            shutdown_running_job=_queue_lifecycle.shutdown_running_job,
            finalize_child_exit=lifecycle.finalize_child_exit,
            reconcile_orphaned_running=lifecycle.reconcile_orphaned_running,
        )

    def worker_child(
        self,
        shutdown_exception_type: type[BaseException],
        *,
        entry_ready_fn: Callable[[Any], bool] = entry_status_is_running,
        process_dequeued_entry_kwargs_fn: Callable[[], Mapping[str, Any]] | None = None,
        outcome_exit_code_fn: Callable[[Any], int] | None = None,
    ) -> InternalEngineWorkerChild:
        if not self.worker_job_module:
            raise ValueError("worker_job_module is required for worker child support")
        return InternalEngineWorkerChild(
            worker_job_module=self.worker_job_module,
            include_admission_root=self.include_admission_root,
            shutdown_exception_type=shutdown_exception_type,
            entry_ready_fn=entry_ready_fn,
            process_dequeued_entry_kwargs_fn=process_dequeued_entry_kwargs_fn,
            outcome_exit_code_fn=outcome_exit_code_fn,
        )

    def worker_child_module_exports(
        self,
        shutdown_exception_type: type[BaseException],
        *,
        entry_ready_fn: Callable[[Any], bool] = entry_status_is_running,
        process_dequeued_entry_kwargs_fn: Callable[[], Mapping[str, Any]] | None = None,
        outcome_exit_code_fn: Callable[[Any], int] | None = None,
    ) -> InternalEngineWorkerChildModuleExports:
        worker_child = self.worker_child(
            shutdown_exception_type,
            entry_ready_fn=entry_ready_fn,
            process_dequeued_entry_kwargs_fn=process_dequeued_entry_kwargs_fn,
            outcome_exit_code_fn=outcome_exit_code_fn,
        )
        return InternalEngineWorkerChildModuleExports(
            worker_child=worker_child,
            build_worker_child_command=worker_child.build_worker_child_command,
            install_shutdown_signal_handlers=worker_child.install_shutdown_signal_handlers,
            run_worker_child_job=worker_child.run_worker_child_job,
            shutdown_signal_handler_installer=worker_child.shutdown_signal_handler_installer,
            build_parser=worker_child.build_parser,
        )

    def worker_child_module_facade(
        self,
        shutdown_exception_type: type[BaseException],
        *,
        entry_ready_fn: Callable[[Any], bool] = entry_status_is_running,
        process_dequeued_entry_kwargs_fn: Callable[[], Mapping[str, Any]] | None = None,
        outcome_exit_code_fn: Callable[[Any], int] | None = None,
        build_worker_child_command: Callable[..., list[str]] | None = None,
    ) -> InternalEngineWorkerChildModuleFacade:
        worker_child = self.worker_child(
            shutdown_exception_type,
            entry_ready_fn=entry_ready_fn,
            process_dequeued_entry_kwargs_fn=process_dequeued_entry_kwargs_fn,
            outcome_exit_code_fn=outcome_exit_code_fn,
        )
        return InternalEngineWorkerChildModuleFacade(
            worker_child=worker_child,
            WORKER_JOB_MODULE=self.worker_job_module,
            WorkerShutdownRequested=shutdown_exception_type,
            build_worker_child_command=build_worker_child_command
            or worker_child.build_worker_child_command,
            run_worker_child_job=worker_child.run_worker_child_job,
            shutdown_signal_handler_installer=worker_child.shutdown_signal_handler_installer,
            build_parser=worker_child.build_parser,
        )


__all__ = [
    "InternalEngineLifecycleModuleExports",
    "InternalEngineSpec",
    "InternalEngineWorkerChildModuleFacade",
    "InternalEngineWorkerChildModuleExports",
]
