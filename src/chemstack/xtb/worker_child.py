from __future__ import annotations

from chemstack.core.queue.internal_engine import (
    InternalEngineSpec,
    create_worker_shutdown_exception_type,
)

WORKER_JOB_MODULE = "chemstack.xtb.worker_execution"
_ENGINE_SPEC = InternalEngineSpec(
    engine="xtb",
    worker_job_module=WORKER_JOB_MODULE,
    include_admission_root=False,
)

WorkerShutdownRequested = create_worker_shutdown_exception_type(__name__)
_WORKER_CHILD_EXPORTS = _ENGINE_SPEC.worker_child_module_exports(WorkerShutdownRequested)
_WORKER_CHILD = _WORKER_CHILD_EXPORTS.worker_child

build_worker_child_command = _WORKER_CHILD_EXPORTS.build_worker_child_command
install_shutdown_signal_handlers = _WORKER_CHILD_EXPORTS.install_shutdown_signal_handlers
run_worker_child_job = _WORKER_CHILD_EXPORTS.run_worker_child_job
shutdown_signal_handler_installer = _WORKER_CHILD_EXPORTS.shutdown_signal_handler_installer
build_parser = _WORKER_CHILD_EXPORTS.build_parser


__all__ = [
    "WORKER_JOB_MODULE",
    "WorkerShutdownRequested",
    "build_parser",
    "build_worker_child_command",
    "install_shutdown_signal_handlers",
    "run_worker_child_job",
    "shutdown_signal_handler_installer",
]
