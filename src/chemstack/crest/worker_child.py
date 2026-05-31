from __future__ import annotations

import argparse
from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.queue import child_execution as _child_execution
from chemstack.core.queue import engine_child as _engine_child
from chemstack.core.queue.types import QueueStatus

WORKER_JOB_MODULE = "chemstack.crest.worker_execution"
_COMMAND_SPEC = _engine_child.WorkerChildCommandSpec(
    worker_job_module=WORKER_JOB_MODULE,
    include_admission_root=False,
)


class WorkerShutdownRequested(RuntimeError):
    def __init__(self, context: Any):
        super().__init__("worker_shutdown")
        self.context = context


def _entry_status_is_running(entry: Any) -> bool:
    return getattr(entry, "status", None) == QueueStatus.RUNNING


_RUN_SPEC = _engine_child.WorkerChildRunSpec(
    shutdown_exception_type=WorkerShutdownRequested,
    entry_ready_fn=_entry_status_is_running,
)


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
) -> list[str]:
    return _engine_child.build_engine_worker_child_command(
        spec=_COMMAND_SPEC,
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
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
    return _engine_child.run_engine_worker_child_job(
        spec=_RUN_SPEC,
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=load_config_fn,
        find_queue_entry_fn=find_queue_entry_fn,
        admission_root_fn=admission_root_fn,
        release_slot_fn=release_slot_fn,
        admission_token=admission_token,
        install_signal_handlers_fn=install_signal_handlers_fn,
        process_dequeued_entry_fn=process_dequeued_entry_fn,
        dependencies_fn=dependencies_fn,
        requeue_running_entry_fn=requeue_running_entry_fn,
        mark_recovery_pending_context_fn=mark_recovery_pending_context_fn,
        process_dequeued_entry_kwargs={"molecule_key_resolver": molecule_key_resolver},
    )


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
