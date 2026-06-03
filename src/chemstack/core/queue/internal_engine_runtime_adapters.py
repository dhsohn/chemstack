from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any


def reserve_admission_slot_fn(
    runtime: Any,
    reserve_slot_fn: Callable[..., str | None],
) -> Callable[[Any], str | None]:
    def _reserve_admission_slot(cfg: Any) -> str | None:
        return runtime.reserve_admission_slot(cfg, reserve_slot_fn=reserve_slot_fn)

    return _reserve_admission_slot


def child_worker_deps_from_namespace(
    runtime: Any,
    *,
    namespace: Mapping[str, Any],
    poll_interval_seconds: int,
    time_module: Any,
    release_slot_fn: Callable[[str | Path, str], object],
    start_background_job_process_name: str = "_start_background_job_process",
    try_reserve_admission_slot_name: str = "_try_reserve_admission_slot",
) -> Any:
    return runtime.child_worker_deps(
        poll_interval_seconds=poll_interval_seconds,
        time_module=time_module,
        release_slot_fn=release_slot_fn,
        start_background_job_process_fn=namespace[start_background_job_process_name],
        try_reserve_admission_slot_fn=namespace[try_reserve_admission_slot_name],
    )


def start_background_job_process_fn(
    runtime: Any,
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
        return runtime.start_child_process(
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
    runtime: Any,
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
    return runtime.child_worker_hooks(
        handle_worker_start_error_fn=namespace[handle_worker_start_error_name],
        finalize_completed_job_fn=namespace[finalize_completed_job_name],
        finalize_child_exit_fn=namespace[finalize_child_exit_name],
        reconcile_worker_state_fn=namespace[reconcile_worker_state_name],
        activate_reserved_slot_fn=activate_reserved_slot_fn,
        terminate_process_fn=terminate_process_fn,
        mark_failed_fn=mark_failed_fn,
        shutdown_grace_seconds=shutdown_grace_seconds,
        sleep_fn=sleep_fn,
        on_worker_process_started_fn=on_worker_process_started_fn,
        shutdown_running_job_fn=shutdown_running_job_fn,
        before_shutdown_all_fn=before_shutdown_all_fn,
    )


def run_pidfile_worker_command_from_namespace(
    runtime: Any,
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

    return runtime.run_pidfile_worker_command(
        args,
        config_path_fn=config_path_fn,
        load_config_fn=namespace[load_config_name],
        read_worker_pid_fn=namespace[read_worker_pid_name],
        worker_factory=worker_factory,
    )


__all__ = [
    "child_worker_deps_from_namespace",
    "child_worker_hooks_from_namespace",
    "config_path_for_worker_fn",
    "reserve_admission_slot_fn",
    "run_pidfile_worker_command_from_namespace",
    "start_background_job_process_fn",
]
