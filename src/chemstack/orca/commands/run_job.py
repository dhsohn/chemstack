from __future__ import annotations

from argparse import Namespace
from typing import Any, Type

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME, ORCA_APP_NAMES

from ..orca_runner import OrcaRunner
from .run_inp import _cmd_run_inp_execute


def _canonical_admission_app_name(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text in ORCA_APP_NAMES:
        return CHEMSTACK_ORCA_APP_NAME
    return text


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
