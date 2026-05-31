from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue import engine_admission as _engine_admission


def reserve_admission_slot(
    cfg: Any,
    *,
    reserve_slot_fn: Callable[..., str | None],
) -> str | None:
    return _engine_admission.reserve_engine_admission_slot(
        cfg,
        engine="xtb",
        reserve_slot_fn=reserve_slot_fn,
    )


def start_background_job_process(
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
        include_admission_root=False,
    )


def attach_started_process(
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
    source: str = "chemstack.xtb.queue_worker.child",
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
        source=source,
    )


def finalize_worker_start_error(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    admission_token: str,
    exc: OSError,
    release_admission_slot_fn: Callable[[str], Any],
    build_terminal_result_fn: Callable[..., Any],
    finalize_execution_result_fn: Callable[..., Any],
    job_dir_fn: Callable[[Any], Path],
    selected_xyz_fn: Callable[[Any], Path],
    job_type_fn: Callable[[Any], str],
    reaction_key_fn: Callable[[Any, Path], str],
    input_summary_fn: Callable[[Any], dict[str, Any]],
    entry_resource_request_fn: Callable[[Any, Any], dict[str, int]],
) -> None:
    _engine_admission.finalize_start_error_as_terminal_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        admission_token=admission_token,
        exc=exc,
        release_admission_slot_fn=release_admission_slot_fn,
        build_terminal_result_fn=build_terminal_result_fn,
        finalize_execution_result_fn=finalize_execution_result_fn,
        job_dir_fn=job_dir_fn,
        selected_xyz_fn=selected_xyz_fn,
        job_type_fn=job_type_fn,
        reaction_key_fn=reaction_key_fn,
        input_summary_fn=input_summary_fn,
        entry_resource_request_fn=entry_resource_request_fn,
    )


__all__ = [
    "attach_started_process",
    "finalize_worker_start_error",
    "reserve_admission_slot",
    "start_background_job_process",
]
