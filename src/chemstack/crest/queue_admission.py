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
        engine="crest",
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
    del admission_root
    return start_background_process_fn(
        build_worker_child_command_fn(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_token=admission_token,
        )
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
    source: str = "chemstack.crest.queue_worker.child",
) -> bool:
    job_dir_text = str(getattr(entry, "metadata", {}).get("job_dir", "")).strip()
    attached = activate_reserved_slot_fn(
        admission_root,
        admission_token,
        owner_pid=process.pid,
        source=source,
        queue_id=entry.queue_id,
        work_dir=job_dir_text or None,
    )
    if attached is None:
        terminate_process_fn(process)
        mark_entry_failed_and_release_fn(
            queue_root,
            entry,
            admission_token,
            error="admission_slot_missing",
            mark_failed_fn=mark_failed_fn,
        )
        return False
    return True


__all__ = [
    "attach_started_process",
    "mark_worker_start_error",
    "reserve_admission_slot",
    "start_background_job_process",
]
