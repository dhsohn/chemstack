from __future__ import annotations

from typing import Any

from chemstack.crest.commands.queue import cmd_queue_cancel as cmd_queue_cancel
from chemstack.crest.commands.run_dir import cmd_run_dir as cmd_run_dir

from . import sibling_engine as _sibling_engine

_RUN_DIR_API_NAME = "chemstack.crest.commands.run_dir.cmd_run_dir"
_CANCEL_API_NAME = "chemstack.crest.commands.queue.cmd_queue_cancel"


def submit_job_dir(
    *,
    job_dir: str,
    priority: int,
    config_path: str,
) -> dict[str, Any]:
    return _sibling_engine.submit_job_dir_direct(
        run_dir_handler=cmd_run_dir,
        api_name=_RUN_DIR_API_NAME,
        config_path=config_path,
        job_dir=job_dir,
        priority=priority,
    )


def cancel_target(
    *,
    target: str,
    config_path: str,
) -> dict[str, Any]:
    return _sibling_engine.cancel_target_direct(
        cancel_handler=cmd_queue_cancel,
        api_name=_CANCEL_API_NAME,
        config_path=config_path,
        target=target,
    )


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
