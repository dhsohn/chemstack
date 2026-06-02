from __future__ import annotations

import argparse
import subprocess
import sys
from argparse import Namespace
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Type

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue.internal_engine import InternalEngineSpec
from chemstack.core.queue.worker import (
    install_shutdown_signal_handlers,
    resolve_admission_root,
    start_background_process,
)

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME
from chemstack.core.admission import release_slot
from chemstack.core.queue.child_execution import find_queue_entry_by_id

from ..orca_runner import OrcaRunner
from ..commands.run_inp import _cmd_run_inp_execute
from ..config import load_config
from ..queue_adapter import (
    list_queue,
    queue_entry_app_name,
    queue_entry_force,
    queue_entry_reaction_dir,
    queue_entry_task_id,
    requeue_running_entry,
)

BackgroundRunJobProcess = subprocess.Popen
WORKER_JOB_MODULE = "chemstack.orca.runtime.worker_job"
_ENGINE_SPEC = InternalEngineSpec(
    engine="orca",
    worker_job_module=WORKER_JOB_MODULE,
    include_admission_root=False,
)


class WorkerShutdownRequested(RuntimeError):
    def __init__(self, context: Any):
        super().__init__("worker_shutdown")
        self.context = context


@dataclass(frozen=True)
class OrcaWorkerExecutionContext:
    entry: Any
    config_path: str
    reaction_dir: str
    force: bool
    admission_token: str | None
    admission_app_name: str | None
    admission_task_id: str | None


@dataclass(frozen=True)
class OrcaWorkerExecutionOutcome:
    exit_code: int
    reaction_dir: str
    entry: Any


def _orca_worker_outcome_exit_code(outcome: OrcaWorkerExecutionOutcome) -> int:
    return int(outcome.exit_code)


_WORKER_CHILD = _ENGINE_SPEC.worker_child(
    WorkerShutdownRequested,
    outcome_exit_code_fn=_orca_worker_outcome_exit_code,
)


def _canonical_admission_app_name(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text == CHEMSTACK_ORCA_APP_NAME:
        return CHEMSTACK_ORCA_APP_NAME
    return text


def _build_background_run_job_command(
    *,
    config_path: str,
    reaction_dir: str,
    force: bool = False,
    admission_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
) -> list[str]:
    cmd = [
        sys.executable,
        "-m",
        WORKER_JOB_MODULE,
        "--config",
        config_path,
        "--reaction-dir",
        reaction_dir,
    ]
    if force:
        cmd.append("--force")
    if admission_token:
        cmd.extend(["--admission-token", admission_token])
    canonical_app_name = _canonical_admission_app_name(admission_app_name)
    if canonical_app_name:
        cmd.extend(["--admission-app-name", canonical_app_name])
    if admission_task_id:
        cmd.extend(["--admission-task-id", admission_task_id])
    return cmd


def start_background_run_job(
    *,
    config_path: str,
    reaction_dir: str,
    force: bool = False,
    admission_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
) -> BackgroundRunJobProcess[str]:
    if runner_cls is not OrcaRunner:
        raise ValueError("start_background_run_job only supports the default OrcaRunner")
    return start_background_process(
        _build_background_run_job_command(
            config_path=config_path,
            reaction_dir=str(Path(reaction_dir)),
            force=force,
            admission_token=admission_token,
            admission_app_name=admission_app_name,
            admission_task_id=admission_task_id,
        )
    )


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
    admission_root: str | Path | None = None,
) -> list[str]:
    del admission_root
    return _WORKER_CHILD.build_worker_child_command(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
    )


def _queue_entry_by_id(queue_root: Path, queue_id: str) -> Any | None:
    return find_queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=lambda root: list_queue(Path(root)),
    )


def _build_execution_context(
    _cfg: Any,
    entry: Any,
    *,
    worker_config_path: str,
    admission_token: str | None,
) -> OrcaWorkerExecutionContext:
    return OrcaWorkerExecutionContext(
        entry=entry,
        config_path=worker_config_path,
        reaction_dir=queue_entry_reaction_dir(entry),
        force=queue_entry_force(entry),
        admission_token=admission_token,
        admission_app_name=queue_entry_app_name(entry) or None,
        admission_task_id=queue_entry_task_id(entry) or None,
    )


def _run_orca_job_for_entry(
    _cfg: Any,
    context: OrcaWorkerExecutionContext,
    _queue_root: Path,
    _options: _engine_execution.InternalWorkerOptions,
) -> int:
    return execute_run_job(
        context.config_path,
        context.reaction_dir,
        force=context.force,
        reservation_token=context.admission_token,
        admission_app_name=context.admission_app_name,
        admission_task_id=context.admission_task_id,
    )


def _worker_execution_spec(
    *,
    worker_config_path: str,
    admission_token: str | None,
) -> _engine_execution.InternalEngineWorkerExecutionSpec:
    return _engine_execution.build_internal_engine_worker_execution_spec(
        build_context=lambda cfg_obj, entry_obj: _build_execution_context(
            cfg_obj,
            entry_obj,
            worker_config_path=worker_config_path,
            admission_token=admission_token,
        ),
        shutdown_exception_type=WorkerShutdownRequested,
        mark_running=lambda _cfg, _context, _options: None,
        run_job=_run_orca_job_for_entry,
        finalize_entry=lambda _cfg, _context, result, _queue_root, _options: result,
        build_outcome=lambda context, result, _finalized: OrcaWorkerExecutionOutcome(
            exit_code=int(result),
            reaction_dir=context.reaction_dir,
            entry=context.entry,
        ),
    )


def process_dequeued_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None = None,
    worker_config_path: str,
    admission_token: str | None = None,
    dependencies: Any | None = None,
    shutdown_requested: Callable[[], bool] | None = None,
) -> OrcaWorkerExecutionOutcome:
    del dependencies
    return _engine_execution.run_internal_engine_worker_entry_with_spec_options(
        cfg,
        entry,
        queue_root=queue_root,
        spec=_worker_execution_spec(
            worker_config_path=worker_config_path,
            admission_token=admission_token,
        ),
        shutdown_requested=shutdown_requested,
    )


def _mark_recovery_pending_context(_cfg: Any, _context: Any, *, reason: str) -> None:
    del reason


def run_worker_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
) -> int:
    return _WORKER_CHILD.run_worker_child_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
        load_config_fn=load_config,
        find_queue_entry_fn=_queue_entry_by_id,
        admission_root_fn=resolve_admission_root,
        release_slot_fn=release_slot,
        install_signal_handlers_fn=_WORKER_CHILD.shutdown_signal_handler_installer(
            install_shutdown_signal_handlers,
        ),
        process_dequeued_entry_fn=process_dequeued_entry,
        dependencies_fn=lambda: None,
        requeue_running_entry_fn=requeue_running_entry,
        mark_recovery_pending_context_fn=_mark_recovery_pending_context,
        process_dequeued_entry_kwargs={
            "worker_config_path": config_path,
            "admission_token": admission_token,
        },
    )


def execute_run_job(
    config_path: str,
    reaction_dir: str,
    *,
    force: bool = False,
    reservation_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
) -> int:
    return _cmd_run_inp_execute(
        Namespace(
            config=config_path,
            reaction_dir=reaction_dir,
            force=force,
        ),
        runner_cls=runner_cls,
        reservation_token=reservation_token,
        admission_app_name=_canonical_admission_app_name(admission_app_name),
        admission_task_id=admission_task_id,
    )


def cmd_run_job(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    return execute_run_job(
        args.config,
        args.reaction_dir,
        force=bool(getattr(args, "force", False)),
        runner_cls=runner_cls,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog=f"python -m {WORKER_JOB_MODULE}")
    parser.add_argument("--config", required=True)
    parser.add_argument("--reaction-dir", default=None)
    parser.add_argument("--queue-root", default=None)
    parser.add_argument("--queue-id", default=None)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--admission-token", default=None)
    parser.add_argument("--admission-app-name", default=None)
    parser.add_argument("--admission-task-id", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    queue_root = str(args.queue_root or "").strip()
    queue_id = str(args.queue_id or "").strip()
    if queue_root or queue_id:
        if not queue_root or not queue_id:
            build_parser().error("--queue-root and --queue-id must be provided together")
        return run_worker_child_job(
            config_path=args.config,
            queue_root=queue_root,
            queue_id=queue_id,
            admission_token=str(args.admission_token).strip() or None,
        )

    reaction_dir = str(args.reaction_dir or "").strip()
    if not reaction_dir:
        build_parser().error("--reaction-dir is required unless --queue-root/--queue-id are used")
    return execute_run_job(
        args.config,
        reaction_dir,
        force=bool(args.force),
        reservation_token=str(args.admission_token).strip() or None,
        admission_app_name=_canonical_admission_app_name(
            str(args.admission_app_name).strip() or None
        ),
        admission_task_id=str(args.admission_task_id).strip() or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "BackgroundRunJobProcess",
    "OrcaWorkerExecutionContext",
    "OrcaWorkerExecutionOutcome",
    "WORKER_JOB_MODULE",
    "WorkerShutdownRequested",
    "build_parser",
    "build_worker_child_command",
    "cmd_run_job",
    "execute_run_job",
    "main",
    "process_dequeued_entry",
    "run_worker_child_job",
    "start_background_run_job",
]
