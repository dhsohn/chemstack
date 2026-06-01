from __future__ import annotations

from typing import Any

from chemstack.core.queue.internal_engine import InternalEngineSpec

from .worker_context import molecule_key as _molecule_key

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


_WORKER_CHILD = _ENGINE_SPEC.worker_child(
    WorkerShutdownRequested,
    process_dequeued_entry_kwargs_fn=lambda: {"molecule_key_resolver": _molecule_key},
)

build_worker_child_command = _WORKER_CHILD.build_worker_child_command
build_worker_entrypoint = _WORKER_CHILD.entrypoint
install_shutdown_signal_handlers = _WORKER_CHILD.install_shutdown_signal_handlers
run_worker_child_job = _WORKER_CHILD.run_worker_child_job
build_parser = _WORKER_CHILD.build_parser


__all__ = [
    "WORKER_JOB_MODULE",
    "WorkerShutdownRequested",
    "build_parser",
    "build_worker_child_command",
    "build_worker_entrypoint",
    "install_shutdown_signal_handlers",
    "run_worker_child_job",
]
