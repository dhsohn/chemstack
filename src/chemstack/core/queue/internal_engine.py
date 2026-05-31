from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import child_execution as _child_execution
from . import engine_admission as _engine_admission
from . import engine_child as _engine_child
from . import lifecycle as _queue_lifecycle
from .child_process import status_matches
from .engine_runtime import EngineQueueRuntime
from .types import QueueStatus


def entry_status_is_running(entry: Any) -> bool:
    return status_matches(getattr(entry, "status", None), QueueStatus.RUNNING)


@dataclass(frozen=True)
class InternalEngineSpec:
    engine: str
    worker_job_module: str = ""
    worker_pid_file_name: str = ""
    include_admission_root: bool = False
    coerce_queue_root_to_str: bool = False
    include_legacy_admission_root_arg: bool = False

    def admission(self) -> InternalEngineAdmission:
        return InternalEngineAdmission(
            engine=self.engine,
            include_admission_root=self.include_admission_root,
        )

    def lifecycle(self) -> InternalEngineLifecycle:
        return InternalEngineLifecycle(
            coerce_queue_root_to_str=self.coerce_queue_root_to_str,
        )

    def worker_child(
        self,
        shutdown_exception_type: type[BaseException],
        *,
        entry_ready_fn: Callable[[Any], bool] = entry_status_is_running,
    ) -> InternalEngineWorkerChild:
        if not self.worker_job_module:
            raise ValueError("worker_job_module is required for worker child support")
        return InternalEngineWorkerChild(
            worker_job_module=self.worker_job_module,
            include_admission_root=self.include_admission_root,
            include_legacy_admission_root_arg=self.include_legacy_admission_root_arg,
            shutdown_exception_type=shutdown_exception_type,
            entry_ready_fn=entry_ready_fn,
        )


@dataclass(frozen=True)
class InternalEngineQueueRuntime:
    spec: InternalEngineSpec
    runtime: EngineQueueRuntime

    @classmethod
    def create(
        cls,
        *,
        spec: InternalEngineSpec,
        load_config: Callable[[Any], Any],
        runtime_roots_for_cfg: Callable[[Any], tuple[Path, ...]],
        list_queue: Callable[[str | Path], list[Any]],
        dequeue_next: Callable[[Path], Any | None],
        worker_pid_file_name: str | None = None,
    ) -> InternalEngineQueueRuntime:
        pid_file_name = worker_pid_file_name or spec.worker_pid_file_name
        if not pid_file_name:
            raise ValueError("worker_pid_file_name is required for queue runtime support")
        return cls(
            spec=spec,
            runtime=EngineQueueRuntime(
                load_config=load_config,
                runtime_roots_for_cfg=runtime_roots_for_cfg,
                list_queue=list_queue,
                dequeue_next=dequeue_next,
                worker_pid_file_name=pid_file_name,
            ),
        )

    def queue_roots(self, cfg: Any) -> tuple[Path, ...]:
        return self.runtime.queue_roots(cfg)

    def queue_entries_with_roots(self, cfg: Any) -> list[tuple[Path, Any]]:
        return self.runtime.queue_entries_with_roots(cfg)

    def dequeue_next_entry(self, cfg: Any) -> tuple[Path, Any] | None:
        return self.runtime.dequeue_next_entry(cfg)

    def queue_entry_by_id(self, queue_root: Path | str, queue_id: str) -> Any | None:
        return self.runtime.queue_entry_by_id(queue_root, queue_id)

    def admission_root(self, cfg: Any) -> str:
        return self.runtime.admission_root(cfg)

    def read_worker_pid(self, allowed_root: Path) -> int | None:
        return self.runtime.read_worker_pid(allowed_root)

    def child_worker_deps(self, **kwargs: Any) -> Any:
        return self.runtime.child_worker_deps(**kwargs)

    def max_concurrent(self, cfg: Any) -> int:
        return self.runtime.max_concurrent(cfg)

    def reserve_admission_slot(
        self,
        cfg: Any,
        *,
        reserve_slot_fn: Callable[..., str | None],
        engine: str | None = None,
    ) -> str | None:
        return self.runtime.reserve_admission_slot(
            cfg,
            engine=engine or self.spec.engine,
            reserve_slot_fn=reserve_slot_fn,
        )

    def child_worker_hooks(self, **kwargs: Any) -> Any:
        kwargs.setdefault("engine", self.spec.engine)
        return self.runtime.child_worker_hooks(**kwargs)

    def start_child_process(
        self,
        *,
        config_path: str,
        queue_root: Path,
        entry: Any,
        admission_root: str | Path,
        admission_token: str,
        start_background_process_fn: Callable[[list[str]], Any],
        build_worker_child_command_fn: Callable[..., list[str]],
        include_admission_root: bool | None = None,
    ) -> Any:
        return self.runtime.start_child_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
            start_background_process_fn=start_background_process_fn,
            build_worker_child_command_fn=build_worker_child_command_fn,
            include_admission_root=(
                self.spec.include_admission_root
                if include_admission_root is None
                else include_admission_root
            ),
        )

    def run_pidfile_worker_command(self, args: Any, **kwargs: Any) -> int:
        return self.runtime.run_pidfile_worker_command(args, **kwargs)

    def reserve_admission_slot_fn(
        self,
        reserve_slot_fn: Callable[..., str | None],
    ) -> Callable[[Any], str | None]:
        def _reserve_admission_slot(cfg: Any) -> str | None:
            return self.reserve_admission_slot(cfg, reserve_slot_fn=reserve_slot_fn)

        return _reserve_admission_slot

    def child_worker_deps_from_namespace(
        self,
        *,
        namespace: Mapping[str, Any],
        poll_interval_seconds: int,
        time_module: Any,
        release_slot_fn: Callable[[str | Path, str], object],
        start_background_job_process_name: str = "_start_background_job_process",
        try_reserve_admission_slot_name: str = "_try_reserve_admission_slot",
    ) -> Any:
        return self.child_worker_deps(
            poll_interval_seconds=poll_interval_seconds,
            time_module=time_module,
            release_slot_fn=release_slot_fn,
            start_background_job_process_fn=namespace[start_background_job_process_name],
            try_reserve_admission_slot_fn=namespace[try_reserve_admission_slot_name],
        )

    def start_background_job_process_fn(
        self,
        *,
        start_background_process_fn: Callable[[list[str]], Any],
        build_worker_child_command_fn: Callable[..., list[str]],
    ) -> Callable[..., Any]:
        def _start_background_job_process(
            *,
            config_path: str,
            queue_root: Path,
            entry: Any,
            admission_root: str | Path,
            admission_token: str,
        ) -> Any:
            return self.start_child_process(
                config_path=config_path,
                queue_root=queue_root,
                entry=entry,
                admission_root=admission_root,
                admission_token=admission_token,
                start_background_process_fn=start_background_process_fn,
                build_worker_child_command_fn=build_worker_child_command_fn,
            )

        return _start_background_job_process

    def config_path_for_worker_fn(
        self,
        *,
        config_path_for_worker_fn: Callable[..., str],
        default_config_path_fn: Callable[[], str],
    ) -> Callable[[Any], str]:
        def _config_path_for_worker(args: Any) -> str:
            return config_path_for_worker_fn(
                args,
                default_config_path_fn=default_config_path_fn,
            )

        return _config_path_for_worker

    def child_worker_hooks_from_namespace(
        self,
        *,
        namespace: Mapping[str, Any],
        activate_reserved_slot_fn: Callable[..., Any],
        terminate_process_fn: Callable[[Any], Any],
        mark_failed_fn: Callable[..., Any],
        shutdown_grace_seconds: float,
        sleep_fn: Callable[[float], None],
        handle_worker_start_error_name: str = "_handle_worker_start_error",
        finalize_completed_job_name: str = "_finalize_completed_job",
        finalize_child_exit_name: str = "_finalize_child_exit",
        reconcile_worker_state_name: str = "_reconcile_worker_state",
    ) -> Any:
        return self.child_worker_hooks(
            handle_worker_start_error_fn=namespace[handle_worker_start_error_name],
            finalize_completed_job_fn=namespace[finalize_completed_job_name],
            finalize_child_exit_fn=namespace[finalize_child_exit_name],
            reconcile_worker_state_fn=namespace[reconcile_worker_state_name],
            activate_reserved_slot_fn=activate_reserved_slot_fn,
            terminate_process_fn=terminate_process_fn,
            mark_failed_fn=mark_failed_fn,
            shutdown_grace_seconds=shutdown_grace_seconds,
            sleep_fn=sleep_fn,
        )

    def run_pidfile_worker_command_from_namespace(
        self,
        args: Any,
        *,
        namespace: Mapping[str, Any],
        config_path_fn: Callable[[Any], str],
        load_config_name: str = "load_config",
        read_worker_pid_name: str = "read_worker_pid",
        worker_class_name: str = "QueueWorker",
        config_path_keyword: bool = True,
    ) -> int:
        def worker_factory(cfg: Any, config_path: str, **kwargs: Any) -> Any:
            worker_cls = namespace[worker_class_name]
            if config_path_keyword:
                return worker_cls(cfg, config_path=config_path, **kwargs)
            return worker_cls(cfg, config_path, **kwargs)

        return self.run_pidfile_worker_command(
            args,
            config_path_fn=config_path_fn,
            load_config_fn=namespace[load_config_name],
            read_worker_pid_fn=namespace[read_worker_pid_name],
            worker_factory=worker_factory,
        )


@dataclass(frozen=True)
class InternalEngineAdmission:
    engine: str
    include_admission_root: bool = False

    @property
    def child_source(self) -> str:
        return f"chemstack.{self.engine}.queue_worker.child"

    def reserve_admission_slot(
        self,
        cfg: Any,
        *,
        reserve_slot_fn: Callable[..., str | None],
    ) -> str | None:
        return _engine_admission.reserve_engine_admission_slot(
            cfg,
            engine=self.engine,
            reserve_slot_fn=reserve_slot_fn,
        )

    def start_background_job_process(
        self,
        *,
        config_path: str,
        queue_root: Path,
        entry: Any,
        admission_root: str | Path,
        admission_token: str,
        start_background_process_fn: Callable[[list[str]], Any],
        build_worker_child_command_fn: Callable[..., list[str]],
    ) -> Any:
        return _engine_admission.start_engine_child_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
            start_background_process_fn=start_background_process_fn,
            build_worker_child_command_fn=build_worker_child_command_fn,
            include_admission_root=self.include_admission_root,
        )

    def attach_started_process(
        self,
        *,
        admission_root: str | Path,
        queue_root: Path,
        entry: Any,
        process: Any,
        admission_token: str,
        activate_reserved_slot_fn: Callable[..., Any],
        terminate_process_fn: Callable[[Any], Any],
        mark_entry_failed_and_release_fn: Callable[..., Any],
        mark_failed_fn: Callable[..., Any],
        source: str | None = None,
    ) -> bool:
        return _engine_admission.attach_started_process(
            admission_root=admission_root,
            queue_root=queue_root,
            entry=entry,
            process=process,
            admission_token=admission_token,
            activate_reserved_slot_fn=activate_reserved_slot_fn,
            terminate_process_fn=terminate_process_fn,
            mark_entry_failed_and_release_fn=mark_entry_failed_and_release_fn,
            mark_failed_fn=mark_failed_fn,
            source=source or self.child_source,
        )

    def mark_worker_start_error(
        self,
        *,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
        mark_entry_failed_and_release_fn: Callable[..., Any],
        mark_failed_fn: Callable[..., Any],
    ) -> None:
        _engine_admission.mark_worker_start_error(
            queue_root=queue_root,
            entry=entry,
            admission_token=admission_token,
            exc=exc,
            mark_entry_failed_and_release_fn=mark_entry_failed_and_release_fn,
            mark_failed_fn=mark_failed_fn,
        )

    def finalize_start_error_as_terminal_result(self, cfg: Any, **kwargs: Any) -> None:
        _engine_admission.finalize_start_error_as_terminal_result(cfg, **kwargs)


@dataclass(frozen=True)
class InternalEngineLifecycle:
    coerce_queue_root_to_str: bool = False

    @property
    def child_exit_policy(self) -> _queue_lifecycle.ChildExitPolicy:
        return _queue_lifecycle.ChildExitPolicy(
            fail_unexpected_exit=True,
            use_entry_fallback=False,
            coerce_root_to_str=self.coerce_queue_root_to_str,
            recovery_entry_fn=lambda _current, current_job: current_job.entry,
        )

    @property
    def orphaned_running_policy(self) -> _queue_lifecycle.OrphanedRunningPolicy:
        return _queue_lifecycle.OrphanedRunningPolicy(
            coerce_root_to_str=self.coerce_queue_root_to_str,
        )

    def finalize_child_exit(
        self,
        cfg: Any,
        job: Any,
        *,
        rc: int,
        shutdown_requested: bool,
        find_queue_entry_fn: Callable[[Any, str], Any | None],
        mark_cancelled_fn: Callable[..., Any],
        requeue_running_entry_fn: Callable[..., Any],
        mark_failed_fn: Callable[..., Any],
        mark_recovery_pending_fn: Callable[..., Any],
        release_admission_slot_fn: Callable[[str], Any],
    ) -> None:
        _queue_lifecycle.finalize_child_exit_with_policy(
            cfg,
            job,
            policy=_queue_lifecycle.ChildExitPolicy(
                shutdown_requested=shutdown_requested,
                fail_unexpected_exit=self.child_exit_policy.fail_unexpected_exit,
                use_entry_fallback=self.child_exit_policy.use_entry_fallback,
                coerce_root_to_str=self.child_exit_policy.coerce_root_to_str,
                recovery_entry_fn=self.child_exit_policy.recovery_entry_fn,
            ),
            find_queue_entry_fn=find_queue_entry_fn,
            mark_cancelled_fn=mark_cancelled_fn,
            requeue_running_entry_fn=requeue_running_entry_fn,
            mark_recovery_pending_fn=mark_recovery_pending_fn,
            release_admission_slot_fn=release_admission_slot_fn,
            mark_failed_fn=mark_failed_fn,
            rc=rc,
        )

    def reconcile_orphaned_running(
        self,
        cfg: Any,
        *,
        admission_root: Any,
        queue_roots_fn: Callable[[Any], tuple[Any, ...]],
        list_queue_fn: Callable[[Any], list[Any]],
        list_slots_fn: Callable[[Any], list[Any]],
        reconcile_stale_slots_fn: Callable[[Any], Any],
        reconcile_orphaned_child_queue_entries_fn: Callable[..., Any],
        mark_cancelled_fn: Callable[..., Any],
        requeue_running_entry_fn: Callable[..., Any],
        mark_recovery_pending_fn: Callable[..., Any],
    ) -> None:
        _queue_lifecycle.reconcile_orphaned_running_with_policy(
            cfg,
            policy=self.orphaned_running_policy,
            admission_root=admission_root,
            queue_roots_fn=queue_roots_fn,
            list_queue_fn=list_queue_fn,
            list_slots_fn=list_slots_fn,
            reconcile_stale_slots_fn=reconcile_stale_slots_fn,
            mark_cancelled_fn=mark_cancelled_fn,
            requeue_running_entry_fn=requeue_running_entry_fn,
            mark_recovery_pending_fn=mark_recovery_pending_fn,
            reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries_fn,
        )


@dataclass(frozen=True)
class InternalEngineWorkerChild:
    worker_job_module: str
    include_admission_root: bool
    include_legacy_admission_root_arg: bool
    shutdown_exception_type: type[BaseException]
    entry_ready_fn: Callable[[Any], bool] = entry_status_is_running

    @property
    def command_spec(self) -> _engine_child.WorkerChildCommandSpec:
        return _engine_child.WorkerChildCommandSpec(
            worker_job_module=self.worker_job_module,
            include_admission_root=self.include_admission_root,
        )

    @property
    def run_spec(self) -> _engine_child.WorkerChildRunSpec:
        return _engine_child.WorkerChildRunSpec(
            shutdown_exception_type=self.shutdown_exception_type,
            entry_ready_fn=self.entry_ready_fn,
        )

    def build_worker_child_command(
        self,
        *,
        config_path: str,
        queue_root: str | Path,
        queue_id: str,
        admission_root: str | Path | None = None,
        admission_token: str | None = None,
    ) -> list[str]:
        return _engine_child.build_engine_worker_child_command(
            spec=self.command_spec,
            config_path=config_path,
            queue_root=queue_root,
            queue_id=queue_id,
            admission_root=admission_root,
            admission_token=admission_token,
        )

    def install_shutdown_signal_handlers(
        self,
        controller: _child_execution.ChildWorkerShutdownController,
        *,
        install_signal_handlers_fn: Callable[[Callable[[], None]], Any],
    ) -> None:
        _child_execution.install_shutdown_request_handlers(
            controller,
            install_signal_handlers_fn=install_signal_handlers_fn,
        )

    def run_worker_child_job(
        self,
        *,
        config_path: str,
        queue_root: str | Path,
        queue_id: str,
        admission_token: str | None = None,
        load_config_fn: Callable[[str], Any],
        find_queue_entry_fn: Callable[[Path, str], Any | None],
        admission_root_fn: Callable[[Any], str | Path],
        release_slot_fn: Callable[[str | Path, str], Any],
        install_signal_handlers_fn: Callable[
            [_child_execution.ChildWorkerShutdownController],
            Any,
        ],
        process_dequeued_entry_fn: Callable[..., Any],
        dependencies_fn: Callable[[], Any],
        requeue_running_entry_fn: Callable[[Path, str], Any],
        mark_recovery_pending_context_fn: Callable[..., Any],
        process_dequeued_entry_kwargs: Mapping[str, Any] | None = None,
    ) -> int:
        return _engine_child.run_engine_worker_child_job(
            spec=self.run_spec,
            config_path=config_path,
            queue_root=queue_root,
            queue_id=queue_id,
            load_config_fn=load_config_fn,
            find_queue_entry_fn=find_queue_entry_fn,
            admission_root_fn=admission_root_fn,
            release_slot_fn=release_slot_fn,
            admission_token=admission_token,
            install_signal_handlers_fn=install_signal_handlers_fn,
            process_dequeued_entry_fn=process_dequeued_entry_fn,
            dependencies_fn=dependencies_fn,
            requeue_running_entry_fn=requeue_running_entry_fn,
            mark_recovery_pending_context_fn=mark_recovery_pending_context_fn,
            process_dequeued_entry_kwargs=process_dequeued_entry_kwargs,
        )

    def build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(prog=f"python -m {self.worker_job_module}")
        parser.add_argument("--config", required=True)
        parser.add_argument("--queue-root", required=True)
        parser.add_argument("--queue-id", required=True)
        if self.include_legacy_admission_root_arg:
            parser.add_argument("--admission-root", default=None)
        parser.add_argument("--admission-token", default=None)
        return parser


__all__ = [
    "InternalEngineAdmission",
    "InternalEngineLifecycle",
    "InternalEngineQueueRuntime",
    "InternalEngineSpec",
    "InternalEngineWorkerChild",
    "entry_status_is_running",
]
