from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_CLI_MODULE,
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
    ORCA_SUBMITTERS,
)
from chemstack.core.utils import now_utc_iso

from ..registry import sync_workflow_registry
from ..state import load_workflow_payload, resolve_workflow_workspace, write_workflow_payload
from .common import (
    normalize_text as _normalize_text,
    parse_key_value_lines as _parse_key_value_lines,
    queue_submission_status as _queue_submission_status,
    run_sibling_app,
)

_SUBMIT_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_ORCA_INTERNAL_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class _SiblingSubmitterConfig:
    config_path: str
    executable: str
    repo_root: str | None


@dataclass
class _WorkflowStageOutcome:
    bucket: str
    detail: dict[str, Any]
    stage_result: dict[str, Any]


@dataclass
class _WorkflowBuckets:
    submitted: list[dict[str, Any]] = field(default_factory=list)
    cancelled: list[dict[str, Any]] = field(default_factory=list)
    requested: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    stage_results: list[dict[str, Any]] = field(default_factory=list)

    def record(self, outcome: _WorkflowStageOutcome) -> None:
        bucket = getattr(self, outcome.bucket)
        bucket.append(outcome.detail)
        self.stage_results.append(outcome.stage_result)


@dataclass
class _CancelStageContext:
    stage: dict[str, Any]
    task: dict[str, Any]
    stage_metadata: dict[str, Any]
    enqueue_payload: dict[str, Any]
    stage_id: str
    task_status: str
    stage_status: str
    queue_id: str
    reaction_dir: str


def _mapping_payload(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _submission_is_deferred(value: dict[str, Any]) -> bool:
    return _normalize_text(value.get("status")).lower() in {
        "blocked",
        "waiting_for_slot",
        "admission_blocked",
        "admission_limit_reached",
        "deferred",
    }


def _submission_deferred_reason(value: dict[str, Any]) -> str:
    return (
        _normalize_text(value.get("reason"))
        or _normalize_text(value.get("status"))
        or "waiting_for_slot"
    )


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


def _cancel_tail_argv(*, target: str) -> list[str]:
    return [
        "queue",
        "cancel",
        target,
    ]


def _cancel_status_from_output(*, returncode: int, stdout: str) -> str:
    if returncode != 0:
        return "failed"
    text = stdout.strip()
    if text.startswith("Cancelled:"):
        return "cancelled"
    if "Cancel requested" in text:
        return "cancel_requested"
    return "cancelled"


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
    result = run_sibling_app(
        executable=_normalize_text(executable) or CHEMSTACK_EXECUTABLE,
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
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    try:
        result = run_sibling_app(
            executable=_normalize_text(executable) or CHEMSTACK_EXECUTABLE,
            config_path=_normalize_text(config_path),
            repo_root=_normalize_text(repo_root) or None,
            module_name=_CANCEL_MODULE_NAME,
            tail_argv=_cancel_tail_argv(target=target),
            timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        command_argv = list(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else [str(exc.cmd)]
        return {
            "status": "failed",
            "reason": "cancel_command_timeout",
            "returncode": 124,
            "command_argv": command_argv,
            "stdout": exc.stdout or "",
            "stderr": exc.stderr or "",
        }
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": _cancel_status_from_output(returncode=result.returncode, stdout=result.stdout),
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _ensure_submission_metadata(stage: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    metadata = task.get("metadata")
    if not isinstance(metadata, dict):
        task["metadata"] = {}
    stage_metadata = stage.get("metadata")
    if not isinstance(stage_metadata, dict):
        stage_metadata = {}
        stage["metadata"] = stage_metadata
    return stage_metadata


def _skip_submission_reason(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    skip_submitted: bool,
) -> str:
    if not skip_submitted:
        return ""
    existing_submission = task.get("submission_result")
    task_status = _normalize_text(task.get("status")).lower()
    stage_status = _normalize_text(stage.get("status")).lower()
    if (
        (isinstance(existing_submission, dict) and existing_submission.get("status") == "submitted")
        or task_status == "submitted"
        or stage_status in {"submitted", "queued"}
    ):
        return "already_submitted"
    return ""


def _submission_resource_kwargs(enqueue_payload: dict[str, Any]) -> dict[str, int]:
    resource_kwargs: dict[str, int] = {}
    max_cores = int(enqueue_payload.get("max_cores", 0) or 0)
    max_memory_gb = int(enqueue_payload.get("max_memory_gb", 0) or 0)
    if max_cores > 0:
        resource_kwargs["max_cores"] = max_cores
    if max_memory_gb > 0:
        resource_kwargs["max_memory_gb"] = max_memory_gb
    return resource_kwargs


def _submission_force(enqueue_payload: dict[str, Any]) -> bool:
    value = enqueue_payload.get("force", False)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _record_missing_reaction_dir(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    submission_record = {
        "status": "failed",
        "reason": "missing_reaction_dir",
        "submitted_at": now_utc_iso(),
    }
    stage_id = stage.get("stage_id", "")
    task["status"] = "submission_failed"
    task["submission_result"] = submission_record
    stage["status"] = "submission_failed"
    stage_metadata["submission_status"] = "submission_failed"
    stage_metadata["submitted_at"] = submission_record["submitted_at"]
    return (
        {"stage_id": stage_id, "reason": "missing_reaction_dir"},
        {"stage_id": stage_id, "status": "submission_failed", "reason": "missing_reaction_dir"},
    )


def _record_submission_outcome(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    reaction_dir: str,
    submission_record: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    stage_id = stage.get("stage_id", "")
    stdout_payload = _mapping_payload(submission_record.get("parsed_stdout"))
    returncode = int(submission_record.get("returncode", 1))
    submission_record["submitted_at"] = now_utc_iso()
    task["submission_result"] = submission_record

    if submission_record["status"] == "submitted":
        task["status"] = "submitted"
        stage["status"] = "queued"
        stage_metadata["queue_id"] = stdout_payload.get("queue_id", "")
        stage_metadata["submission_status"] = "submitted"
        stage_metadata["submitted_at"] = submission_record["submitted_at"]
        stage_metadata.pop("submission_deferred_reason", None)
        stage_metadata.pop("last_submission_attempt_at", None)
        return (
            "submitted",
            {
                "stage_id": stage_id,
                "queue_id": stdout_payload.get("queue_id", ""),
                "reaction_dir": stdout_payload.get("job_dir")
                or stdout_payload.get("reaction_dir", reaction_dir),
            },
            {
                "stage_id": stage_id,
                "status": "submitted",
                "queue_id": stdout_payload.get("queue_id", ""),
                "returncode": returncode,
            },
        )

    if _submission_is_deferred(submission_record):
        reason = _submission_deferred_reason(submission_record)
        task["status"] = "planned"
        stage["status"] = "planned"
        stage_metadata["submission_status"] = "waiting_for_slot"
        stage_metadata["submission_deferred_reason"] = reason
        stage_metadata["last_submission_attempt_at"] = submission_record["submitted_at"]
        stage_metadata.pop("submitted_at", None)
        stage_metadata.pop("queue_id", None)
        return (
            "deferred",
            {
                "stage_id": stage_id,
                "reason": reason,
            },
            {
                "stage_id": stage_id,
                "status": "waiting_for_slot",
                "reason": reason,
                "returncode": returncode,
            },
        )

    task["status"] = "submission_failed"
    stage["status"] = "submission_failed"
    stage_metadata["submission_status"] = "submission_failed"
    stage_metadata["submitted_at"] = submission_record["submitted_at"]
    stage_metadata.pop("submission_deferred_reason", None)
    stage_metadata.pop("last_submission_attempt_at", None)
    return (
        "failed",
        {
            "stage_id": stage_id,
            "returncode": returncode,
            "stderr": str(submission_record.get("stderr", "")).strip(),
            "stdout": str(submission_record.get("stdout", "")).strip(),
        },
        {
            "stage_id": stage_id,
            "status": "submission_failed",
            "queue_id": stdout_payload.get("queue_id", ""),
            "returncode": returncode,
        },
    )


def _submission_summary_state(
    *,
    submitted: list[dict[str, Any]],
    skipped: list[dict[str, Any]],
    failed: list[dict[str, Any]],
) -> tuple[str | None, str]:
    if failed and submitted:
        return "queued", "partially_submitted"
    if failed:
        return "submission_failed", "submission_failed"
    if submitted:
        return "queued", "submitted"
    if skipped:
        return None, "skipped"
    return None, ""


def _orca_submitter_matches(enqueue_payload: dict[str, Any]) -> bool:
    return _normalize_text(enqueue_payload.get("submitter")) in {"", *ORCA_SUBMITTERS}


def _workflow_metadata(payload: dict[str, Any]) -> dict[str, Any] | None:
    payload.setdefault("metadata", {})
    return payload["metadata"] if isinstance(payload["metadata"], dict) else None


def _submission_config(
    *,
    orca_auto_config: str,
    orca_auto_executable: str,
    orca_auto_repo_root: str | None,
) -> _SiblingSubmitterConfig:
    return _SiblingSubmitterConfig(
        config_path=_normalize_text(orca_auto_config),
        executable=_normalize_text(orca_auto_executable) or CHEMSTACK_EXECUTABLE,
        repo_root=_normalize_text(orca_auto_repo_root) or None,
    )


def _submission_kwargs(enqueue_payload: dict[str, Any]) -> dict[str, Any]:
    submission_kwargs: dict[str, Any] = _submission_resource_kwargs(enqueue_payload)
    if _submission_force(enqueue_payload):
        submission_kwargs["force"] = True
    return submission_kwargs


def _submission_stage_outcome(
    *,
    stage: dict[str, Any],
    submitter_config: _SiblingSubmitterConfig,
    skip_submitted: bool,
) -> _WorkflowStageOutcome | None:
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    enqueue_payload = task.get("enqueue_payload")
    if not isinstance(enqueue_payload, dict):
        return None
    stage_metadata = _ensure_submission_metadata(stage, task)

    skip_reason = _skip_submission_reason(stage=stage, task=task, skip_submitted=skip_submitted)
    stage_id = stage.get("stage_id", "")
    if skip_reason:
        return _WorkflowStageOutcome(
            bucket="skipped",
            detail={"stage_id": stage_id, "reason": skip_reason},
            stage_result={"stage_id": stage_id, "status": "skipped", "reason": skip_reason},
        )

    reaction_dir = _normalize_text(enqueue_payload.get("reaction_dir"))
    if not reaction_dir:
        fail_record, stage_result = _record_missing_reaction_dir(
            stage=stage, task=task, stage_metadata=stage_metadata
        )
        return _WorkflowStageOutcome(bucket="failed", detail=fail_record, stage_result=stage_result)
    if not _orca_submitter_matches(enqueue_payload):
        return None

    submission_record = submit_reaction_dir(
        reaction_dir=reaction_dir,
        priority=int(enqueue_payload.get("priority", 10) or 10),
        config_path=submitter_config.config_path,
        executable=submitter_config.executable,
        repo_root=submitter_config.repo_root,
        **_submission_kwargs(enqueue_payload),
    )
    outcome, detail_record, stage_result = _record_submission_outcome(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        reaction_dir=reaction_dir,
        submission_record=submission_record,
    )
    bucket = "skipped" if outcome == "deferred" else outcome
    return _WorkflowStageOutcome(bucket=bucket, detail=detail_record, stage_result=stage_result)


def _record_submission_summary(payload: dict[str, Any], buckets: _WorkflowBuckets) -> None:
    payload_status, summary_status = _submission_summary_state(
        submitted=buckets.submitted,
        skipped=buckets.skipped,
        failed=buckets.failed,
    )
    if payload_status:
        payload["status"] = payload_status
    metadata = _workflow_metadata(payload)
    if metadata is not None:
        metadata["submission_summary"] = {
            "status": summary_status,
            "submitted_count": len(buckets.submitted),
            "skipped_count": len(buckets.skipped),
            "failed_count": len(buckets.failed),
            "stage_results": buckets.stage_results,
            "updated_at": now_utc_iso(),
        }


def submit_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_auto_config: str,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
    skip_submitted: bool = True,
) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=workflow_target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    buckets = _WorkflowBuckets()
    submitter_config = _submission_config(
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        outcome = _submission_stage_outcome(
            stage=stage,
            submitter_config=submitter_config,
            skip_submitted=skip_submitted,
        )
        if outcome is not None:
            buckets.record(outcome)

    _record_submission_summary(payload, buckets)
    write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "submitted": buckets.submitted,
        "skipped": buckets.skipped,
        "failed": buckets.failed,
    }


def _cancel_config(
    *,
    orca_auto_config: str | None,
    orca_auto_executable: str,
    orca_auto_repo_root: str | None,
) -> _SiblingSubmitterConfig:
    return _SiblingSubmitterConfig(
        config_path=_normalize_text(orca_auto_config),
        executable=_normalize_text(orca_auto_executable) or CHEMSTACK_EXECUTABLE,
        repo_root=_normalize_text(orca_auto_repo_root) or None,
    )


def _cancel_stage_context(stage: dict[str, Any]) -> _CancelStageContext | None:
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    metadata = task.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        task["metadata"] = metadata
    stage_metadata = stage.get("metadata")
    if not isinstance(stage_metadata, dict):
        stage_metadata = {}
        stage["metadata"] = stage_metadata
    enqueue_payload = task.get("enqueue_payload")
    if not isinstance(enqueue_payload, dict):
        enqueue_payload = {}

    payload = task.get("payload")
    payload_reaction_dir = payload.get("reaction_dir") if isinstance(payload, dict) else ""
    return _CancelStageContext(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        enqueue_payload=enqueue_payload,
        stage_id=_normalize_text(stage.get("stage_id")),
        task_status=_normalize_text(task.get("status")).lower(),
        stage_status=_normalize_text(stage.get("status")).lower(),
        queue_id=_normalize_text(stage_metadata.get("queue_id")),
        reaction_dir=_normalize_text(payload_reaction_dir)
        or _normalize_text(enqueue_payload.get("reaction_dir")),
    )


def _cancel_skip_reason(context: _CancelStageContext) -> str:
    if context.task_status in {"cancelled", "cancel_requested"} or context.stage_status in {
        "cancelled",
        "cancel_requested",
    }:
        return "already_cancelled"
    if context.task_status in {"completed", "failed"} or context.stage_status in {
        "completed",
        "failed",
    }:
        return "already_terminal"
    return ""


def _record_cancel_skip(context: _CancelStageContext, reason: str) -> _WorkflowStageOutcome:
    return _WorkflowStageOutcome(
        bucket="skipped",
        detail={"stage_id": context.stage_id, "reason": reason},
        stage_result={"stage_id": context.stage_id, "status": "skipped", "reason": reason},
    )


def _needs_orca_cancel(context: _CancelStageContext) -> bool:
    return bool(
        context.queue_id
        or context.task_status in {"submitted"}
        or context.stage_status in {"queued", "running"}
    )


def _record_local_cancel(context: _CancelStageContext) -> _WorkflowStageOutcome:
    cancel_record = {
        "status": "cancelled",
        "cancelled_at": now_utc_iso(),
        "mode": "local",
    }
    context.task["status"] = "cancelled"
    context.task["cancel_result"] = cancel_record
    context.stage["status"] = "cancelled"
    context.stage_metadata["cancel_status"] = "cancelled"
    context.stage_metadata["cancelled_at"] = cancel_record["cancelled_at"]
    return _WorkflowStageOutcome(
        bucket="cancelled",
        detail={"stage_id": context.stage_id, "mode": "local"},
        stage_result={"stage_id": context.stage_id, "status": "cancelled", "mode": "local"},
    )


def _record_cancel_failure(
    context: _CancelStageContext,
    *,
    reason: str,
    stage_result: dict[str, Any] | None = None,
) -> _WorkflowStageOutcome:
    context.task["cancel_result"] = {
        "status": "failed",
        "reason": reason,
        "cancelled_at": now_utc_iso(),
    }
    return _WorkflowStageOutcome(
        bucket="failed",
        detail={"stage_id": context.stage_id, "reason": reason},
        stage_result=stage_result
        or {"stage_id": context.stage_id, "status": "cancel_failed", "reason": reason},
    )


def _record_remote_cancel_success(
    context: _CancelStageContext,
    *,
    cancel_record: dict[str, Any],
    cancel_status: str,
) -> _WorkflowStageOutcome:
    context.task["status"] = cancel_status
    context.stage["status"] = cancel_status
    context.stage_metadata["cancel_status"] = cancel_status
    context.stage_metadata["cancelled_at"] = cancel_record["cancelled_at"]
    bucket = "requested" if cancel_status == "cancel_requested" else "cancelled"
    return _WorkflowStageOutcome(
        bucket=bucket,
        detail={
            "stage_id": context.stage_id,
            "queue_id": context.queue_id,
            "reaction_dir": context.reaction_dir,
        },
        stage_result={"stage_id": context.stage_id, "status": cancel_status},
    )


def _record_remote_cancel_failed(
    context: _CancelStageContext, cancel_record: dict[str, Any]
) -> _WorkflowStageOutcome:
    returncode = int(cancel_record.get("returncode", 1))
    return _WorkflowStageOutcome(
        bucket="failed",
        detail={
            "stage_id": context.stage_id,
            "queue_id": context.queue_id,
            "reaction_dir": context.reaction_dir,
            "returncode": returncode,
        },
        stage_result={
            "stage_id": context.stage_id,
            "status": "cancel_failed",
            "returncode": returncode,
        },
    )


def _record_remote_cancel(
    context: _CancelStageContext,
    *,
    cancel_identifier: str,
    submitter_config: _SiblingSubmitterConfig,
) -> _WorkflowStageOutcome:
    cancel_record = cancel_target(
        target=cancel_identifier,
        config_path=submitter_config.config_path,
        executable=submitter_config.executable,
        repo_root=submitter_config.repo_root,
    )
    cancel_status = str(cancel_record.get("status", "failed"))
    cancel_record["cancelled_at"] = now_utc_iso()
    cancel_record["target"] = cancel_identifier
    context.task["cancel_result"] = cancel_record
    if cancel_status in {"cancel_requested", "cancelled"}:
        return _record_remote_cancel_success(
            context, cancel_record=cancel_record, cancel_status=cancel_status
        )
    return _record_remote_cancel_failed(context, cancel_record)


def _cancel_stage_outcome(
    *,
    stage: dict[str, Any],
    submitter_config: _SiblingSubmitterConfig,
) -> _WorkflowStageOutcome | None:
    context = _cancel_stage_context(stage)
    if context is None:
        return None
    skip_reason = _cancel_skip_reason(context)
    if skip_reason:
        return _record_cancel_skip(context, skip_reason)
    if not _needs_orca_cancel(context):
        return _record_local_cancel(context)

    cancel_identifier = context.queue_id or context.reaction_dir
    if not cancel_identifier:
        return _record_cancel_failure(context, reason="missing_cancel_target")
    if not submitter_config.config_path:
        return _record_cancel_failure(context, reason="orca_auto_config_required")
    if not _orca_submitter_matches(context.enqueue_payload):
        return None
    return _record_remote_cancel(
        context,
        cancel_identifier=cancel_identifier,
        submitter_config=submitter_config,
    )


def _write_cancellation_summary(payload: dict[str, Any], buckets: _WorkflowBuckets) -> None:
    if buckets.requested:
        payload["status"] = "cancel_requested"
    elif buckets.cancelled:
        payload["status"] = "cancelled"
    elif buckets.failed:
        payload["status"] = "cancel_failed"
    metadata = _workflow_metadata(payload)
    if metadata is not None:
        metadata["cancellation_summary"] = {
            "cancelled_count": len(buckets.cancelled),
            "requested_count": len(buckets.requested),
            "skipped_count": len(buckets.skipped),
            "failed_count": len(buckets.failed),
            "stage_results": buckets.stage_results,
            "updated_at": now_utc_iso(),
        }


def cancel_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=workflow_target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    buckets = _WorkflowBuckets()
    submitter_config = _cancel_config(
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        outcome = _cancel_stage_outcome(stage=stage, submitter_config=submitter_config)
        if outcome is not None:
            buckets.record(outcome)

    _write_cancellation_summary(payload, buckets)
    write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "cancelled": buckets.cancelled,
        "requested": buckets.requested,
        "skipped": buckets.skipped,
        "failed": buckets.failed,
    }


__all__ = [
    "cancel_target",
    "cancel_reaction_ts_search_workflow",
    "submit_reaction_dir",
    "submit_reaction_ts_search_workflow",
]
