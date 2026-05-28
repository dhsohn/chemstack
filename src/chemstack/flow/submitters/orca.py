from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_CLI_MODULE,
)
from chemstack.core.utils import now_utc_iso

from ..registry import sync_workflow_registry
from ..state import load_workflow_payload, resolve_workflow_workspace, write_workflow_payload
from . import orca_cancellation as _cancellation
from . import orca_submission as _submission
from . import sibling_engine
from .common import (
    normalize_text as _normalize_text,
    parse_key_value_lines as _parse_key_value_lines,
    queue_submission_status as _queue_submission_status,
    run_sibling_app,
)

_SUBMIT_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


def _submission_tail_argv(
    *,
    reaction_dir: str,
    priority: int,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    force: bool = False,
) -> list[str]:
    argv = [
        "run-dir",
        reaction_dir,
        "--priority",
        str(int(priority)),
    ]
    if force:
        argv.append("--force")
    if max_cores is not None and int(max_cores) > 0:
        argv.extend(["--max-cores", str(int(max_cores))])
    if max_memory_gb is not None and int(max_memory_gb) > 0:
        argv.extend(["--max-memory-gb", str(int(max_memory_gb))])
    return argv


def submit_reaction_dir(
    *,
    reaction_dir: str,
    priority: int,
    config_path: str,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    force: bool = False,
    repo_root: str | None = None,
) -> dict[str, Any]:
    result = run_sibling_app(
        config_path=_normalize_text(config_path),
        repo_root=_normalize_text(repo_root) or None,
        module_name=_SUBMIT_MODULE_NAME,
        tail_argv=_submission_tail_argv(
            reaction_dir=reaction_dir,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            force=force,
        ),
    )
    parsed = _parse_key_value_lines(result.stdout)
    status, reason = _queue_submission_status(
        returncode=int(result.returncode),
        parsed_stdout=parsed,
        stdout=result.stdout,
        stderr=result.stderr,
    )
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": status,
        "reason": reason,
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "reaction_dir": parsed.get("job_dir") or parsed.get("reaction_dir", reaction_dir),
        "priority": int(priority),
        "force": bool(force),
    }


def cancel_target(
    *,
    target: str,
    config_path: str,
    repo_root: str | None = None,
) -> dict[str, Any]:
    return sibling_engine.orca_cancel_target(
        normalize_text_fn=_normalize_text,
        run_sibling_app=run_sibling_app,
        target=target,
        config_path=config_path,
        repo_root=repo_root,
        module_name=_CANCEL_MODULE_NAME,
        timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
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
    orca_repo_root: str | None = None,
    skip_submitted: bool = True,
) -> dict[str, Any]:
    return _submission.submit_reaction_ts_search_workflow(
        workflow_target=workflow_target,
        workflow_root=workflow_root,
        orca_config=orca_config,
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
    orca_repo_root: str | None = None,
) -> dict[str, Any]:
    return _cancellation.cancel_reaction_ts_search_workflow(
        workflow_target=workflow_target,
        workflow_root=workflow_root,
        orca_config=orca_config,
        orca_repo_root=orca_repo_root,
        deps=_cancellation_deps(),
    )


__all__ = [
    "cancel_target",
    "cancel_reaction_ts_search_workflow",
    "submit_reaction_dir",
    "submit_reaction_ts_search_workflow",
]
