from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue.types import QueueEntry

from .config import AppConfig

LOGGER = logging.getLogger(__name__)


def _job_queue_root(worker: Any, job: Any) -> Path:
    return Path(getattr(job, "queue_root", worker.allowed_root)).expanduser().resolve()


@dataclass(frozen=True)
class OrcaQueueWorkerLifecycleHooks:
    queue_entry_id_fn: Callable[[Any], str]
    queue_entry_app_name_fn: Callable[[Any], str]
    queue_entry_task_id_fn: Callable[[Any], str | None]
    update_slot_metadata_fn: Callable[..., Any]
    terminate_process_fn: Callable[[Any], Any]
    mark_failed_fn: Callable[..., Any]
    upsert_running_job_record_fn: Callable[[AppConfig, QueueEntry], Any]
    get_run_id_from_state_fn: Callable[[str], str | None]
    get_cancel_requested_fn: Callable[..., bool]
    mark_cancelled_fn: Callable[..., Any]
    mark_completed_fn: Callable[..., Any]
    upsert_terminal_job_record_fn: Callable[..., Any]
    notify_terminal_job_from_state_fn: Callable[[AppConfig, str], bool]


def attach_started_orca_process(
    worker: Any,
    queue_root: Path,
    entry: Any,
    *,
    process: Any,
    admission_token: str,
    hooks: OrcaQueueWorkerLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> bool:
    queue_id = hooks.queue_entry_id_fn(entry)
    attached = hooks.update_slot_metadata_fn(
        worker.admission_root,
        admission_token,
        queue_id=queue_id,
        app_name=hooks.queue_entry_app_name_fn(entry),
        task_id=hooks.queue_entry_task_id_fn(entry),
    )
    if not attached:
        logger.error(
            "Failed to attach queue identity to admission slot %s for job %s",
            admission_token,
            queue_id,
        )
        hooks.terminate_process_fn(process)
        worker._mark_entry_failed_and_release(
            queue_root,
            entry,
            admission_token,
            error="admission_slot_missing",
            mark_failed_fn=hooks.mark_failed_fn,
        )
        return False

    try:
        hooks.upsert_running_job_record_fn(worker.cfg, entry)
    except Exception as exc:
        logger.warning("Failed to update running job location for %s: %s", queue_id, exc)
    return True


def mark_terminal_queue_entry(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    rc: int,
    hooks: OrcaQueueWorkerLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    queue_root = _job_queue_root(worker, job)
    run_id = hooks.get_run_id_from_state_fn(job.reaction_dir)
    if hooks.get_cancel_requested_fn(queue_root, queue_id):
        logger.info("Job cancelled: %s (rc=%d)", queue_id, rc)
        hooks.mark_cancelled_fn(queue_root, queue_id)
    elif rc == 0:
        logger.info("Job completed: %s (rc=%d)", queue_id, rc)
        hooks.mark_completed_fn(queue_root, queue_id, run_id=run_id)
        worker._auto_organize_terminal_job(job)
    else:
        logger.warning("Job failed: %s (rc=%d)", queue_id, rc)
        hooks.mark_failed_fn(
            queue_root,
            queue_id,
            error=f"exit_code={rc}",
            run_id=run_id,
        )


def record_terminal_job_side_effects(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: OrcaQueueWorkerLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    try:
        hooks.upsert_terminal_job_record_fn(
            worker.cfg,
            job.reaction_dir,
            fallback_job_id=job.task_id,
        )
    except Exception as exc:
        logger.warning("Failed to update terminal job location for %s: %s", queue_id, exc)
    try:
        hooks.notify_terminal_job_from_state_fn(worker.cfg, job.reaction_dir)
    except Exception as exc:
        logger.warning("Failed to send terminal notification for %s: %s", queue_id, exc)


def finalize_orca_finished_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    rc: int,
    hooks: OrcaQueueWorkerLifecycleHooks,
) -> None:
    mark_terminal_queue_entry(worker, queue_id, job, rc=rc, hooks=hooks)
    record_terminal_job_side_effects(worker, queue_id, job, hooks=hooks)
    worker._release_admission_slot(job.admission_token)


def cancel_orca_running_job(
    worker: Any,
    queue_id: str,
    job: Any,
    *,
    hooks: OrcaQueueWorkerLifecycleHooks,
    logger: logging.Logger = LOGGER,
) -> None:
    logger.info("Cancelling running job: %s", queue_id)
    hooks.terminate_process_fn(job.process)
    try:
        job.process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    hooks.mark_cancelled_fn(_job_queue_root(worker, job), queue_id)
    worker._release_admission_slot(job.admission_token)
