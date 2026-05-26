from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.utils import now_utc_iso

from ..registry import sync_workflow_registry
from ..state import load_workflow_payload, resolve_workflow_workspace, write_workflow_payload
from . import orca_cancellation as _cancellation
from . import orca_cli as _cli
from . import orca_models as _models
from . import orca_submission as _submission
from .common import (
    normalize_text as _normalize_text,
    parse_key_value_lines as _parse_key_value_lines,
    queue_submission_status as _queue_submission_status,
    run_sibling_app,
)

_ensure_submission_metadata = _models.ensure_submission_metadata
_submission_summary_state = _submission.submission_summary_state


def _submitter_deps() -> _cli.OrcaCliDeps:
    return _cli.OrcaCliDeps(
        _normalize_text=_normalize_text,
        run_sibling_app=run_sibling_app,
        parse_key_value_lines=_parse_key_value_lines,
        queue_submission_status=_queue_submission_status,
    )


def submit_reaction_dir(
    *,
    reaction_dir: str,
    priority: int,
    config_path: str,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    force: bool = False,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _cli.submit_reaction_dir(
        deps=_submitter_deps(),
        reaction_dir=reaction_dir,
        priority=priority,
        config_path=config_path,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        force=force,
        executable=executable,
        repo_root=repo_root,
    )


def cancel_target(
    *,
    target: str,
    config_path: str,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    return _cli.cancel_target(
        deps=_submitter_deps(),
        target=target,
        config_path=config_path,
        executable=executable,
        repo_root=repo_root,
    )


def _submission_deps() -> _submission.SubmissionDeps:
    return _submission.SubmissionDeps(
        normalize_text=_normalize_text,
        now_utc_iso=now_utc_iso,
        resolve_workflow_workspace=resolve_workflow_workspace,
        load_workflow_payload=load_workflow_payload,
        write_workflow_payload=write_workflow_payload,
        sync_workflow_registry=sync_workflow_registry,
        submit_reaction_dir=submit_reaction_dir,
    )


def submit_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_config: str,
    orca_executable: str = CHEMSTACK_EXECUTABLE,
    orca_repo_root: str | None = None,
    skip_submitted: bool = True,
) -> dict[str, Any]:
    return _submission.submit_reaction_ts_search_workflow(
        workflow_target=workflow_target,
        workflow_root=workflow_root,
        orca_config=orca_config,
        orca_executable=orca_executable,
        orca_repo_root=orca_repo_root,
        skip_submitted=skip_submitted,
        deps=_submission_deps(),
    )


def _cancellation_deps() -> _cancellation.CancellationDeps:
    return _cancellation.CancellationDeps(
        normalize_text=_normalize_text,
        now_utc_iso=now_utc_iso,
        resolve_workflow_workspace=resolve_workflow_workspace,
        load_workflow_payload=load_workflow_payload,
        write_workflow_payload=write_workflow_payload,
        sync_workflow_registry=sync_workflow_registry,
        cancel_target=cancel_target,
    )


def cancel_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_config: str | None = None,
    orca_executable: str = CHEMSTACK_EXECUTABLE,
    orca_repo_root: str | None = None,
) -> dict[str, Any]:
    return _cancellation.cancel_reaction_ts_search_workflow(
        workflow_target=workflow_target,
        workflow_root=workflow_root,
        orca_config=orca_config,
        orca_executable=orca_executable,
        orca_repo_root=orca_repo_root,
        deps=_cancellation_deps(),
    )


__all__ = [
    "cancel_target",
    "cancel_reaction_ts_search_workflow",
    "submit_reaction_dir",
    "submit_reaction_ts_search_workflow",
]
