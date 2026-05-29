from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue.worker import reserve_engine_queue_worker_slot


def reserve_admission_slot(
    cfg: Any,
    *,
    reserve_slot_fn: Callable[..., str | None],
) -> str | None:
    return reserve_engine_queue_worker_slot(
        cfg,
        engine="xtb",
        reserve_slot_fn=reserve_slot_fn,
    )


def start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str,
    admission_token: str,
    start_background_process_fn: Callable[[list[str]], Any],
    build_worker_child_command_fn: Callable[..., list[str]],
) -> Any:
    return start_background_process_fn(
        build_worker_child_command_fn(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_root=admission_root,
            admission_token=admission_token,
        )
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
    release_admission_slot_fn(admission_token)
    job_dir = job_dir_fn(entry)
    failure = build_terminal_result_fn(
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz_fn(entry),
        job_type=job_type_fn(entry),
        reaction_key=reaction_key_fn(entry, job_dir),
        input_summary=input_summary_fn(entry),
        resource_request=entry_resource_request_fn(cfg, entry),
        status="failed",
        reason=f"worker_start_error:{exc}",
    )
    finalize_execution_result_fn(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=failure,
        emit_output=True,
    )


__all__ = [
    "finalize_worker_start_error",
    "reserve_admission_slot",
    "start_background_job_process",
]
