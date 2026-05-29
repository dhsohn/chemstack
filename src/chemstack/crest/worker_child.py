from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.queue import child_entrypoint as _child_entrypoint
from chemstack.core.queue import child_execution as _child_execution
from chemstack.core.queue.types import QueueStatus
from chemstack.core.queue.worker import build_background_worker_command

WORKER_JOB_MODULE = "chemstack.crest.worker_execution"


class WorkerShutdownRequested(RuntimeError):
    def __init__(self, context: Any):
        super().__init__("worker_shutdown")
        self.context = context


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
) -> list[str]:
    return build_background_worker_command(
        config_path=config_path,
        queue_root=Path(queue_root),
        queue_id=queue_id,
        worker_job_module=WORKER_JOB_MODULE,
        admission_token=admission_token,
        include_admission_root=False,
    )


def install_shutdown_signal_handlers(
    controller: _child_execution.ChildWorkerShutdownController,
    *,
    install_signal_handlers_fn: Callable[[Callable[[], None]], Any],
) -> None:
    _child_execution.install_shutdown_request_handlers(
        controller,
        install_signal_handlers_fn=install_signal_handlers_fn,
    )


def run_worker_child_job(
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
    molecule_key_resolver: Callable[..., str],
    requeue_running_entry_fn: Callable[[Path, str], Any],
    mark_recovery_pending_context_fn: Callable[..., Any],
) -> int:
    job = _child_entrypoint.load_child_worker_entrypoint_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=load_config_fn,
        find_queue_entry_fn=find_queue_entry_fn,
        entry_ready_fn=lambda entry: getattr(entry, "status", None) == QueueStatus.RUNNING,
        admission_token=admission_token,
        admission_root_fn=admission_root_fn,
        release_slot_fn=release_slot_fn,
    )
    if job is None:
        return 1
    cfg = job.cfg
    queue_root_path = job.queue_root
    entry = job.entry

    controller = _child_execution.ChildWorkerShutdownController()
    install_signal_handlers_fn(controller)

    with _child_entrypoint.child_worker_admission_scope(
        job,
        admission_token,
        release_slot_fn=release_slot_fn,
    ):
        try:
            process_dequeued_entry_fn(
                cfg,
                entry,
                queue_root=queue_root_path,
                molecule_key_resolver=molecule_key_resolver,
                dependencies=dependencies_fn(),
                shutdown_requested=controller.is_requested,
            )
            return 0
        except WorkerShutdownRequested as exc:
            requeue_running_entry_fn(queue_root_path, queue_id)
            mark_recovery_pending_context_fn(cfg, exc.context, reason="worker_shutdown")
            return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m {WORKER_JOB_MODULE}")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queue-root", required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--admission-token", default=None)
    return parser


__all__ = [
    "WORKER_JOB_MODULE",
    "WorkerShutdownRequested",
    "build_parser",
    "build_worker_child_command",
    "install_shutdown_signal_handlers",
    "run_worker_child_job",
]
