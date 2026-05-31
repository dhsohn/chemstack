from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.queue import child_execution as _child_execution
from chemstack.core.queue.internal_engine import InternalEngineSpec

WORKER_JOB_MODULE = "chemstack.crest.worker_execution"
_ENGINE_SPEC = InternalEngineSpec(
    engine="crest",
    worker_job_module=WORKER_JOB_MODULE,
    include_admission_root=False,
)


class WorkerShutdownRequested(RuntimeError):
    def __init__(self, context: Any):
        super().__init__("worker_shutdown")
        self.context = context


_WORKER_CHILD = _ENGINE_SPEC.worker_child(WorkerShutdownRequested)

build_worker_child_command = _WORKER_CHILD.build_worker_child_command
install_shutdown_signal_handlers = _WORKER_CHILD.install_shutdown_signal_handlers
build_parser = _WORKER_CHILD.build_parser


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
    return _WORKER_CHILD.run_worker_child_job(
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


__all__ = [
    "WORKER_JOB_MODULE",
    "WorkerShutdownRequested",
    "build_parser",
    "build_worker_child_command",
    "install_shutdown_signal_handlers",
    "run_worker_child_job",
]
