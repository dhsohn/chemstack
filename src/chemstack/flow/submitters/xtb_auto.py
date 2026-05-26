from __future__ import annotations

import sys
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CLI_MODULE, CHEMSTACK_XTB_MODULE

from . import sibling_engine as _sibling_engine
from .common import normalize_text
from .common import parse_key_value_lines as parse_key_value_lines
from .common import queue_submission_status as queue_submission_status
from .common import run_sibling_app as run_sibling_app

_SUBMIT_MODULE_NAME = CHEMSTACK_XTB_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0
_THIS_MODULE = sys.modules[__name__]


def submit_job_dir(
    *,
    job_dir: str,
    priority: int,
    config_path: str,
    executable: str = "xtb_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _sibling_engine.submit_job_dir(
        deps=_sibling_engine.submitter_deps(_THIS_MODULE),
        executable=normalize_text(executable) or "xtb_auto",
        config_path=config_path,
        repo_root=repo_root,
        module_name=_SUBMIT_MODULE_NAME,
        job_dir=job_dir,
        priority=priority,
        extra_fields=lambda parsed: {
            "job_type": parsed.get("job_type", ""),
            "reaction_key": parsed.get("reaction_key", ""),
        },
    )


def cancel_target(
    *,
    target: str,
    config_path: str,
    executable: str = "xtb_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _sibling_engine.cancel_target(
        deps=_sibling_engine.submitter_deps(_THIS_MODULE),
        executable=normalize_text(executable) or "xtb_auto",
        config_path=config_path,
        repo_root=repo_root,
        module_name=_CANCEL_MODULE_NAME,
        target=target,
        timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
    )


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
