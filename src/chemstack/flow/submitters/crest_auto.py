from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CREST_MODULE

from .common import normalize_text, parse_key_value_lines, queue_submission_status, run_sibling_app
from . import sibling_engine as _sibling_engine

_MODULE_NAME = CHEMSTACK_CREST_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class _SubmitterDeps:
    normalize_text: Any
    parse_key_value_lines: Any
    queue_submission_status: Any
    run_sibling_app: Any


def _submitter_deps() -> _SubmitterDeps:
    return _SubmitterDeps(
        normalize_text=normalize_text,
        parse_key_value_lines=parse_key_value_lines,
        queue_submission_status=queue_submission_status,
        run_sibling_app=run_sibling_app,
    )


def submit_job_dir(
    *,
    job_dir: str,
    priority: int,
    config_path: str,
    executable: str = "crest_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _sibling_engine.submit_job_dir(
        deps=_submitter_deps(),
        executable=normalize_text(executable) or "crest_auto",
        config_path=config_path,
        repo_root=repo_root,
        module_name=_MODULE_NAME,
        job_dir=job_dir,
        priority=priority,
    )


def cancel_target(
    *,
    target: str,
    config_path: str,
    executable: str = "crest_auto",
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _sibling_engine.cancel_target(
        deps=_submitter_deps(),
        executable=normalize_text(executable) or "crest_auto",
        config_path=config_path,
        repo_root=repo_root,
        module_name=_MODULE_NAME,
        target=target,
        timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
    )


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
