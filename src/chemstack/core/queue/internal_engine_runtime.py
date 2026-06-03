from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import engine_admission as _engine_admission
from . import internal_engine_runtime_adapters as _runtime_adapters
from .engine_runtime import EngineQueueRuntime
from .internal_engine_spec import InternalEngineSpec


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

    @classmethod
    def from_runtime(
        cls,
        *,
        spec: InternalEngineSpec,
        runtime: EngineQueueRuntime,
    ) -> InternalEngineQueueRuntime:
        return cls(spec=spec, runtime=runtime)

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
        if engine is None or engine == self.spec.engine:
            return self.spec.admission().reserve_admission_slot(
                cfg,
                reserve_slot_fn=reserve_slot_fn,
            )
        return self.runtime.reserve_admission_slot(
            cfg,
            engine=engine,
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
        if include_admission_root is None:
            return self.spec.admission().start_background_job_process(
                config_path=config_path,
                queue_root=queue_root,
                entry=entry,
                admission_root=admission_root,
                admission_token=admission_token,
                start_background_process_fn=start_background_process_fn,
                build_worker_child_command_fn=build_worker_child_command_fn,
            )
        return _engine_admission.start_engine_child_process(
            config_path=config_path,
            queue_root=queue_root,
            entry=entry,
            admission_root=admission_root,
            admission_token=admission_token,
            start_background_process_fn=start_background_process_fn,
            build_worker_child_command_fn=build_worker_child_command_fn,
            include_admission_root=include_admission_root,
        )

    def run_pidfile_worker_command(self, args: Any, **kwargs: Any) -> int:
        return self.runtime.run_pidfile_worker_command(args, **kwargs)

    def reserve_admission_slot_fn(
        self,
        reserve_slot_fn: Callable[..., str | None],
    ) -> Callable[[Any], str | None]:
        return _runtime_adapters.reserve_admission_slot_fn(self, reserve_slot_fn)

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
        return _runtime_adapters.child_worker_deps_from_namespace(
            self,
            namespace=namespace,
            poll_interval_seconds=poll_interval_seconds,
            time_module=time_module,
            release_slot_fn=release_slot_fn,
            start_background_job_process_name=start_background_job_process_name,
            try_reserve_admission_slot_name=try_reserve_admission_slot_name,
        )

    def start_background_job_process_fn(
        self,
        *,
        start_background_process_fn: Callable[[list[str]], Any],
        build_worker_child_command_fn: Callable[..., list[str]],
    ) -> Callable[..., Any]:
        return _runtime_adapters.start_background_job_process_fn(
            self,
            start_background_process_fn=start_background_process_fn,
            build_worker_child_command_fn=build_worker_child_command_fn,
        )

    def config_path_for_worker_fn(
        self,
        *,
        config_path_for_worker_fn: Callable[..., str],
        default_config_path_fn: Callable[[], str],
    ) -> Callable[[Any], str]:
        return _runtime_adapters.config_path_for_worker_fn(
            config_path_for_worker_fn=config_path_for_worker_fn,
            default_config_path_fn=default_config_path_fn,
        )

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
        on_worker_process_started_fn: Callable[[Any, Path, Any, Any, str], bool] | None = None,
        shutdown_running_job_fn: Callable[[Any, str, Any], Any] | None = None,
        before_shutdown_all_fn: Callable[[Any, int], Any] | None = None,
    ) -> Any:
        return _runtime_adapters.child_worker_hooks_from_namespace(
            self,
            namespace=namespace,
            activate_reserved_slot_fn=activate_reserved_slot_fn,
            terminate_process_fn=terminate_process_fn,
            mark_failed_fn=mark_failed_fn,
            shutdown_grace_seconds=shutdown_grace_seconds,
            sleep_fn=sleep_fn,
            handle_worker_start_error_name=handle_worker_start_error_name,
            finalize_completed_job_name=finalize_completed_job_name,
            finalize_child_exit_name=finalize_child_exit_name,
            reconcile_worker_state_name=reconcile_worker_state_name,
            on_worker_process_started_fn=on_worker_process_started_fn,
            shutdown_running_job_fn=shutdown_running_job_fn,
            before_shutdown_all_fn=before_shutdown_all_fn,
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
        return _runtime_adapters.run_pidfile_worker_command_from_namespace(
            self,
            args,
            namespace=namespace,
            config_path_fn=config_path_fn,
            load_config_name=load_config_name,
            read_worker_pid_name=read_worker_pid_name,
            worker_class_name=worker_class_name,
            config_path_keyword=config_path_keyword,
        )


__all__ = ["InternalEngineQueueRuntime"]
