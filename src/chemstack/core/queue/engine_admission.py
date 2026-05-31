from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from .worker import reserve_engine_queue_worker_slot


def reserve_engine_admission_slot(
    cfg: Any,
    *,
    engine: str,
    reserve_slot_fn: Callable[..., str | None],
) -> str | None:
    return reserve_engine_queue_worker_slot(
        cfg,
        engine=engine,
        reserve_slot_fn=reserve_slot_fn,
    )


def start_engine_child_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
    start_background_process_fn: Callable[[list[str]], Any],
    build_worker_child_command_fn: Callable[..., list[str]],
    include_admission_root: bool,
) -> Any:
    command_kwargs: dict[str, Any] = {
        "config_path": config_path,
        "queue_root": queue_root,
        "queue_id": entry.queue_id,
        "admission_token": admission_token,
    }
    if include_admission_root:
        command_kwargs["admission_root"] = admission_root
    return start_background_process_fn(
        build_worker_child_command_fn(**command_kwargs),
    )


def mark_worker_start_error(
    *,
    queue_root: Path,
    entry: Any,
    admission_token: str,
    exc: OSError,
    mark_entry_failed_and_release_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
) -> None:
    mark_entry_failed_and_release_fn(
        queue_root,
        entry,
        admission_token,
        error=str(exc),
        mark_failed_fn=mark_failed_fn,
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
    source: str,
) -> bool:
    metadata = getattr(entry, "metadata", {})
    getter = getattr(metadata, "get", None)
    job_dir_text = str(getter("job_dir", "") if callable(getter) else "").strip()
    attached = activate_reserved_slot_fn(
        admission_root,
        admission_token,
        owner_pid=process.pid,
        source=source,
        queue_id=entry.queue_id,
        work_dir=job_dir_text or None,
    )
    if attached is not None:
        return True

    terminate_process_fn(process)
    mark_entry_failed_and_release_fn(
        queue_root,
        entry,
        admission_token,
        error="admission_slot_missing",
        mark_failed_fn=mark_failed_fn,
    )
    return False


def finalize_start_error_as_terminal_result(
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
    "attach_started_process",
    "finalize_start_error_as_terminal_result",
    "mark_worker_start_error",
    "reserve_engine_admission_slot",
    "start_engine_child_process",
]
