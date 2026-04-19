from __future__ import annotations

from argparse import Namespace
from typing import Any, Type

from ..orca_runner import OrcaRunner
from .run_inp import _cmd_run_inp_execute


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
        admission_app_name=admission_app_name,
        admission_task_id=admission_task_id,
    )


def cmd_run_job(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    return execute_run_job(
        args.config,
        args.reaction_dir,
        force=bool(getattr(args, "force", False)),
        runner_cls=runner_cls,
    )
