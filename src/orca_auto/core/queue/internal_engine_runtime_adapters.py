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
    load_config_fn: Callable[[Any], Any] | None = None,
    read_worker_pid_fn: Callable[[Path], int | None] | None = None,
    existing_pid_report_fn: Callable[[int], Any] | None = None,
    max_concurrent_fn: Callable[[Any], int] | None = None,
    worker_factory: Callable[..., Any] | None = None,
) -> int:
    def default_worker_factory(cfg: Any, config_path: str, **kwargs: Any) -> Any:
        worker_cls = namespace[worker_class_name]
        if config_path_keyword:
            return worker_cls(cfg, config_path=config_path, **kwargs)
        return worker_cls(cfg, config_path, **kwargs)

    return runtime.run_pidfile_worker_command(
        args,
        config_path_fn=config_path_fn,
        load_config_fn=load_config_fn or namespace[load_config_name],
        read_worker_pid_fn=read_worker_pid_fn or namespace[read_worker_pid_name],
        existing_pid_report_fn=existing_pid_report_fn,
        max_concurrent_fn=max_concurrent_fn,
        worker_factory=worker_factory or default_worker_factory,
    )


__all__ = [
    "config_path_for_worker_fn",
    "reserve_admission_slot_fn",
    "run_pidfile_worker_command_from_namespace",
    "start_background_job_process_fn",
]
