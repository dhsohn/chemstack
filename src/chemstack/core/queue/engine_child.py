from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import child_entrypoint as _child_entrypoint
from .child_entrypoint import ChildWorkerEntrypointJob
from .worker import build_background_worker_command


@dataclass(frozen=True)
class WorkerChildCommandSpec:
    worker_job_module: str
    include_admission_root: bool = True


def build_engine_worker_child_command(
    *,
    spec: WorkerChildCommandSpec,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str | Path | None = None,
    admission_token: str | None = None,
) -> list[str]:
    return build_background_worker_command(
        config_path=config_path,
        queue_root=Path(queue_root),
        queue_id=queue_id,
        worker_job_module=spec.worker_job_module,
        admission_root=admission_root,
        admission_token=admission_token,
        include_admission_root=spec.include_admission_root,
    )


def activate_child_worker_admission(
    job: ChildWorkerEntrypointJob,
    admission_token: str | None,
    *,
    work_dir: str | Path,
    queue_id: str,
    source: str,
    activate_reserved_slot_fn: Callable[..., Any],
) -> bool:
    return _child_entrypoint.activate_child_worker_admission(
        job,
        admission_token,
        work_dir=work_dir,
        queue_id=queue_id,
        source=source,
        activate_reserved_slot_fn=activate_reserved_slot_fn,
    )


def load_engine_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    load_config_fn: Callable[[str], Any],
    find_queue_entry_fn: Callable[[Path, str], Any | None],
    admission_root_fn: Callable[[Any], str | Path],
    release_slot_fn: Callable[[str | Path, str], Any],
    admission_token: str | None = None,
    entry_ready_fn: Callable[[Any], bool] | None = None,
) -> ChildWorkerEntrypointJob | None:
    return _child_entrypoint.load_child_worker_entrypoint_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=load_config_fn,
        find_queue_entry_fn=find_queue_entry_fn,
        entry_ready_fn=entry_ready_fn,
        admission_token=admission_token,
        admission_root_fn=admission_root_fn,
        release_slot_fn=release_slot_fn,
    )


def run_child_job_with_admission_scope(
    job: ChildWorkerEntrypointJob,
    admission_token: str | None,
    *,
    release_slot_fn: Callable[[str | Path, str], Any],
    run_job_fn: Callable[[ChildWorkerEntrypointJob], int],
) -> int:
    with _child_entrypoint.child_worker_admission_scope(
        job,
        admission_token,
        release_slot_fn=release_slot_fn,
    ):
        return run_job_fn(job)


def run_loaded_engine_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    load_config_fn: Callable[[str], Any],
    find_queue_entry_fn: Callable[[Path, str], Any | None],
    admission_root_fn: Callable[[Any], str | Path],
    release_slot_fn: Callable[[str | Path, str], Any],
    run_job_fn: Callable[[ChildWorkerEntrypointJob], int],
    admission_token: str | None = None,
    entry_ready_fn: Callable[[Any], bool] | None = None,
    prepare_job_fn: Callable[[ChildWorkerEntrypointJob], bool] | None = None,
    missing_exit_code: int = 1,
    prepare_failed_exit_code: int = 1,
) -> int:
    job = load_engine_child_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=load_config_fn,
        find_queue_entry_fn=find_queue_entry_fn,
        entry_ready_fn=entry_ready_fn,
        admission_token=admission_token,
        admission_root_fn=admission_root_fn,
        release_slot_fn=release_slot_fn,
    )
    if job is None:
        return missing_exit_code
    if prepare_job_fn is not None and not prepare_job_fn(job):
        return prepare_failed_exit_code
    return run_child_job_with_admission_scope(
        job,
        admission_token,
        release_slot_fn=release_slot_fn,
        run_job_fn=run_job_fn,
    )


def outcome_exit_code(
    outcome: Any,
    *,
    success_statuses: set[str] | frozenset[str] = frozenset({"completed", "cancelled"}),
) -> int:
    status = str(getattr(outcome.result, "status", "")).strip().lower()
    return 0 if status in success_statuses else 1


__all__ = [
    "WorkerChildCommandSpec",
    "activate_child_worker_admission",
    "build_engine_worker_child_command",
    "load_engine_child_job",
    "outcome_exit_code",
    "run_child_job_with_admission_scope",
    "run_loaded_engine_child_job",
]
