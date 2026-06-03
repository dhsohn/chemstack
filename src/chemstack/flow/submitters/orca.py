from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from typing import Any

from chemstack.core.commands.queue import display_status
from chemstack.core.utils import normalize_text as _normalize_text
from chemstack.core.utils import now_utc_iso

from ..registry import sync_workflow_registry
from ..state import load_workflow_payload, resolve_workflow_workspace, write_workflow_payload
from . import internal_engine as _internal_engine
from . import orca_cancellation as _cancellation
from . import orca_submission as _submission

_SUBMIT_API_NAME = "chemstack.orca.direct_submit"
_CANCEL_API_NAME = "chemstack.orca.direct_cancel"


def _trace_argv(*, api_name: str, config_path: str, kwargs: dict[str, Any]) -> list[str]:
    return _internal_engine.internal_call_argv(
        api_name=api_name,
        config_path=config_path,
        kwargs=kwargs,
    )


def _key_value_stdout(fields: dict[str, Any]) -> str:
    return _internal_engine._key_value_stdout(_internal_engine._text_fields(fields))


def _failure_payload(
    *,
    command_argv: list[str],
    stderr: str,
    reaction_dir: str = "",
    reason: str = "",
) -> dict[str, Any]:
    if stderr and not stderr.endswith("\n"):
        stderr += "\n"
    return _internal_engine.InternalEngineCommandResult(
        status="failed",
        reason=reason,
        returncode=1,
        command_argv=command_argv,
        stderr=stderr,
        extra_fields={
            "reaction_dir": reaction_dir,
            "priority": 0,
            "force": False,
        },
    ).to_payload()


def _queued_payload(
    *,
    command_argv: list[str],
    result: Any,
    priority: int,
    force: bool,
) -> dict[str, Any]:
    from chemstack.orca import queue_adapter

    entry = result.entry
    parsed = {
        "status": "queued",
        "job_dir": result.reaction_dir,
        "queue_id": queue_adapter.queue_entry_id(entry),
        "job_id": queue_adapter.queue_entry_task_id(entry),
        "priority": priority,
    }
    if force:
        parsed["force"] = "true"
    if result.worker_info.status:
        parsed["worker"] = result.worker_info.status
    if result.worker_info.pid is not None:
        parsed["worker_pid"] = result.worker_info.pid
    if result.worker_info.log_file:
        parsed["worker_log"] = result.worker_info.log_file
    if result.worker_info.detail:
        parsed["worker_detail"] = result.worker_info.detail
    parsed_stdout = _internal_engine._text_fields(parsed)
    return _internal_engine.InternalEngineCommandResult(
        status="submitted",
        reason="",
        returncode=0,
        command_argv=command_argv,
        stdout=_key_value_stdout(parsed_stdout),
        parsed_stdout=parsed_stdout,
        queue_id=parsed_stdout.get("queue_id", ""),
        job_id=parsed_stdout.get("job_id", ""),
        extra_fields={
            "reaction_dir": parsed_stdout.get("job_dir", _normalize_text(result.reaction_dir)),
            "priority": int(priority),
            "force": bool(force),
        },
    ).to_payload()


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
    del repo_root
    normalized_config = _normalize_text(config_path)
    command_argv = _trace_argv(
        api_name=_SUBMIT_API_NAME,
        config_path=normalized_config,
        kwargs={
            "reaction_dir": reaction_dir,
            "priority": int(priority),
            "force": bool(force),
        },
    )
    try:
        from chemstack.orca.commands import run_inp as _run_inp

        args = Namespace(
            config=normalized_config,
            path=reaction_dir,
            priority=int(priority),
            force=bool(force),
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
        )
        submission = _run_inp.submit_reaction_dir_to_queue(args)
        if submission.reason == "invalid_submission_target":
            return _failure_payload(
                command_argv=command_argv,
                reaction_dir=reaction_dir,
                stderr=submission.stderr,
                reason="invalid_submission_target",
            )
        context = submission.context
        if submission.reason == "submission_conflict":
            return _failure_payload(
                command_argv=command_argv,
                reaction_dir=str(context.reaction_dir) if context is not None else reaction_dir,
                stderr=submission.stderr,
                reason="submission_conflict",
            )
        if submission.status != "submitted" or submission.queued_result is None:
            return _failure_payload(
                command_argv=command_argv,
                reaction_dir=str(context.reaction_dir) if context is not None else reaction_dir,
                stderr=submission.stderr or "failed to submit ORCA queue entry",
                reason=submission.reason or "submission_failed",
            )
        queued = submission.queued_result
    except Exception as exc:  # noqa: BLE001
        return _failure_payload(
            command_argv=command_argv,
            reaction_dir=reaction_dir,
            stderr=f"{exc.__class__.__name__}: {exc}",
            reason="submission_failed",
        )
    return _queued_payload(
        command_argv=command_argv,
        result=queued,
        priority=int(priority),
        force=bool(force),
    )


def cancel_target(
    *,
    target: str,
    config_path: str,
    repo_root: str | None = None,
) -> dict[str, Any]:
    del repo_root
    normalized_config = _normalize_text(config_path)
    normalized_target = _normalize_text(target)
    command_argv = _trace_argv(
        api_name=_CANCEL_API_NAME,
        config_path=normalized_config,
        kwargs={"target": normalized_target},
    )
    if not normalized_target:
        return _failure_payload(command_argv=command_argv, stderr="queue cancel requires a target")

    try:
        from chemstack.orca.config import load_config
        from chemstack.orca import queue_adapter

        cfg = load_config(normalized_config)
        allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
        matched = queue_adapter.find_entry_by_target(
            queue_adapter.list_queue(allowed_root),
            normalized_target,
        )
        if matched is None:
            return _failure_payload(
                command_argv=command_argv,
                stderr=f"queue target not found: {normalized_target}",
                reason="target_not_found",
            )
        updated = queue_adapter.cancel(allowed_root, queue_adapter.queue_entry_id(matched))
        if updated is None:
            return _failure_payload(
                command_argv=command_argv,
                stderr=f"queue target already terminal: {normalized_target}",
                reason="already_terminal",
            )
        status = display_status(updated)
        parsed_stdout = _internal_engine._text_fields({
            "status": status,
            "queue_id": queue_adapter.queue_entry_id(updated),
            "job_id": queue_adapter.queue_entry_task_id(updated),
        })
    except Exception as exc:  # noqa: BLE001
        return _failure_payload(
            command_argv=command_argv,
            stderr=f"{exc.__class__.__name__}: {exc}",
            reason="cancel_failed",
        )

    return _internal_engine.InternalEngineCommandResult(
        status=status,
        reason="",
        returncode=0,
        command_argv=command_argv,
        stdout=_key_value_stdout(parsed_stdout),
        parsed_stdout=parsed_stdout,
        queue_id=parsed_stdout.get("queue_id", ""),
        job_id=parsed_stdout.get("job_id", ""),
    ).to_payload()


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
