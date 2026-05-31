from __future__ import annotations

import argparse
import os
import signal
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

from chemstack.core.queue import child_execution as _child_execution
from chemstack.core.queue import engine_child as _engine_child

WORKER_JOB_MODULE = "chemstack.xtb.worker_execution"
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190
_COMMAND_SPEC = _engine_child.WorkerChildCommandSpec(
    worker_job_module=WORKER_JOB_MODULE,
    include_admission_root=True,
)


class WorkerChildConfigDependencies(Protocol):
    @property
    def load_config(self) -> Callable[..., Any]: ...

    @property
    def queue_entry_by_id(self) -> Callable[[Path | str, str], Any | None]: ...


class WorkerChildAdmissionDependencies(Protocol):
    @property
    def activate_reserved_slot(self) -> Callable[..., Any]: ...

    @property
    def release_slot(self) -> Callable[..., Any]: ...


class WorkerChildContextDependencies(Protocol):
    @property
    def job_dir(self) -> Callable[[Any], Path]: ...


class WorkerChildDependencies(Protocol):
    @property
    def config(self) -> WorkerChildConfigDependencies: ...

    @property
    def admission(self) -> WorkerChildAdmissionDependencies: ...

    @property
    def context(self) -> WorkerChildContextDependencies: ...

    @property
    def execute_queue_entry(self) -> Callable[..., Any] | None: ...


class SignalController(_child_execution.CancellableChildWorkerController):
    def __init__(
        self,
        *,
        cancel_signal: int = WORKER_CANCEL_SIGNAL,
        shutdown_exit_code: int = WORKER_SHUTDOWN_EXIT_CODE,
        terminate_process_fn: Callable[[Any], Any],
        signal_module: Any = signal,
        os_exit_fn: Callable[[int], Any] | None = None,
    ) -> None:
        super().__init__(
            cancel_signal=cancel_signal,
            shutdown_exit_code=shutdown_exit_code,
            terminate_process_fn=terminate_process_fn,
            signal_module=signal_module,
            os_exit_fn=os_exit_fn or (lambda code: os._exit(code)),
        )


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str | Path,
    admission_token: str | None = None,
) -> list[str]:
    return _engine_child.build_engine_worker_child_command(
        spec=_COMMAND_SPEC,
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
    )


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    dependencies: WorkerChildDependencies,
    execute_queue_entry_fn: Callable[..., Any],
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    getpid_fn: Callable[[], int] = os.getpid,
    worker_job_module: str = WORKER_JOB_MODULE,
) -> int:
    def prepare_loaded_job(job: _engine_child.ChildWorkerEntrypointJob) -> bool:
        if not admission_token:
            return True
        return _engine_child.activate_child_worker_admission(
            job,
            admission_token,
            work_dir=dependencies.context.job_dir(job.entry),
            queue_id=job.entry.queue_id,
            source=worker_job_module,
            activate_reserved_slot_fn=dependencies.admission.activate_reserved_slot,
        )

    def run_loaded_job(job: _engine_child.ChildWorkerEntrypointJob) -> int:
        cfg = job.cfg
        resolved_queue_root = job.queue_root
        entry = job.entry
        if dependencies.execute_queue_entry is None:
            outcome = execute_queue_entry_fn(
                cfg,
                queue_root=resolved_queue_root,
                entry=entry,
                should_cancel=should_cancel,
                register_running_job=register_running_job,
                emit_output=False,
                worker_job_pid=getpid_fn(),
                dependencies=dependencies,
            )
        else:
            outcome = dependencies.execute_queue_entry(
                cfg,
                queue_root=resolved_queue_root,
                entry=entry,
                should_cancel=should_cancel,
                register_running_job=register_running_job,
                emit_output=False,
                worker_job_pid=getpid_fn(),
            )
        return _engine_child.outcome_exit_code(outcome)

    return _engine_child.run_loaded_engine_child_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        load_config_fn=dependencies.config.load_config,
        find_queue_entry_fn=dependencies.config.queue_entry_by_id,
        admission_root_fn=lambda _cfg: admission_root,
        release_slot_fn=dependencies.admission.release_slot,
        admission_token=admission_token,
        prepare_job_fn=prepare_loaded_job,
        run_job_fn=run_loaded_job,
    )


def build_worker_job_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m {WORKER_JOB_MODULE}")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queue-root", required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--admission-root", required=True)
    parser.add_argument("--admission-token", default=None)
    return parser


__all__ = [
    "SignalController",
    "WORKER_CANCEL_SIGNAL",
    "WORKER_JOB_MODULE",
    "WORKER_SHUTDOWN_EXIT_CODE",
    "WorkerChildAdmissionDependencies",
    "WorkerChildConfigDependencies",
    "WorkerChildContextDependencies",
    "WorkerChildDependencies",
    "build_worker_child_command",
    "build_worker_job_parser",
    "run_worker_job",
]
