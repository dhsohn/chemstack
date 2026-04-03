from __future__ import annotations

from typing import Any, Type

from ..orca_runner import OrcaRunner
from .run_inp import _cmd_run_inp_execute


def cmd_run_job(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    return _cmd_run_inp_execute(args, runner_cls=runner_cls)
