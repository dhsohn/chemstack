from __future__ import annotations

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
from . import sibling_engine as _sibling_engine

_SUBMIT_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_ORCA_INTERNAL_MODULE
_CANCEL_TIMEOUT_SECONDS = 5.0


@dataclass(frozen=True)
class _SubmitterDeps:
    _normalize_text: Any
    run_sibling_app: Any


def _submitter_deps() -> _SubmitterDeps:
    return _SubmitterDeps(
        _normalize_text=_normalize_text,
        run_sibling_app=run_sibling_app,
    )


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


@dataclass(frozen=True)
class _TaskStageMutation:
    task_status: str | None = None
    stage_status: str | None = None
    task_record_key: str | None = None
    metadata_updates: dict[str, Any] = field(default_factory=dict)
    metadata_removals: tuple[str, ...] = ()


@dataclass(frozen=True)
class _RecordedStageTransition:
    bucket: str
    detail: dict[str, Any]
    stage_result: dict[str, Any]
    mutation: _TaskStageMutation


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


def _apply_task_stage_mutation(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    mutation: _TaskStageMutation,
    task_record: Any = None,
) -> None:
    if mutation.task_status is not None:
        task["status"] = mutation.task_status
    if mutation.stage_status is not None:
        stage["status"] = mutation.stage_status
    if mutation.task_record_key is not None:
        task[mutation.task_record_key] = task_record
    stage_metadata.update(mutation.metadata_updates)
    for key in mutation.metadata_removals:
        stage_metadata.pop(key, None)


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
    return _sibling_engine.cli_cancel_status(returncode=returncode, stdout=stdout)


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
    return _sibling_engine.orca_cancel_target(
        deps=_submitter_deps(),
        executable=_normalize_text(executable) or CHEMSTACK_EXECUTABLE,
        config_path=config_path,
        repo_root=repo_root,
        module_name=_CANCEL_MODULE_NAME,
        target=target,
        timeout_seconds=_CANCEL_TIMEOUT_SECONDS,
    )


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


def _submission_result_mutation(
    *,
    task_status: str,
    stage_status: str,
    metadata_updates: dict[str, Any],
    metadata_removals: tuple[str, ...] = (),
) -> _TaskStageMutation:
    return _TaskStageMutation(
        task_status=task_status,
        stage_status=stage_status,
        task_record_key="submission_result",
        metadata_updates=metadata_updates,
        metadata_removals=metadata_removals,
    )


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
    _apply_task_stage_mutation(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        mutation=_submission_result_mutation(
            task_status="submission_failed",
            stage_status="submission_failed",
            metadata_updates={
                "submission_status": "submission_failed",
                "submitted_at": submission_record["submitted_at"],
            },
        ),
        task_record=submission_record,
    )
    return (
        {"stage_id": stage_id, "reason": "missing_reaction_dir"},
        {"stage_id": stage_id, "status": "submission_failed", "reason": "missing_reaction_dir"},
    )


def _submitted_stage_transition(
    *,
    stage_id: str,
    stdout_payload: dict[str, Any],
    reaction_dir: str,
    submitted_at: str,
    returncode: int,
) -> _RecordedStageTransition:
    queue_id = stdout_payload.get("queue_id", "")
    return _RecordedStageTransition(
        bucket="submitted",
        detail={
            "stage_id": stage_id,
            "queue_id": queue_id,
            "reaction_dir": stdout_payload.get("job_dir")
            or stdout_payload.get("reaction_dir", reaction_dir),
        },
        stage_result={
            "stage_id": stage_id,
            "status": "submitted",
            "queue_id": queue_id,
            "returncode": returncode,
        },
        mutation=_submission_result_mutation(
            task_status="submitted",
            stage_status="queued",
            metadata_updates={
                "queue_id": queue_id,
                "submission_status": "submitted",
                "submitted_at": submitted_at,
            },
            metadata_removals=("submission_deferred_reason", "last_submission_attempt_at"),
        ),
    )


def _deferred_submission_transition(
    *,
    stage_id: str,
    submission_record: dict[str, Any],
    submitted_at: str,
    returncode: int,
) -> _RecordedStageTransition:
    reason = _submission_deferred_reason(submission_record)
    return _RecordedStageTransition(
        bucket="deferred",
        detail={
            "stage_id": stage_id,
            "reason": reason,
        },
        stage_result={
            "stage_id": stage_id,
            "status": "waiting_for_slot",
            "reason": reason,
            "returncode": returncode,
        },
        mutation=_submission_result_mutation(
            task_status="planned",
            stage_status="planned",
            metadata_updates={
                "submission_status": "waiting_for_slot",
                "submission_deferred_reason": reason,
                "last_submission_attempt_at": submitted_at,
            },
            metadata_removals=("submitted_at", "queue_id"),
        ),
    )


def _failed_submission_transition(
    *,
    stage_id: str,
    stdout_payload: dict[str, Any],
    submission_record: dict[str, Any],
    submitted_at: str,
    returncode: int,
) -> _RecordedStageTransition:
    return _RecordedStageTransition(
        bucket="failed",
        detail={
            "stage_id": stage_id,
            "returncode": returncode,
            "stderr": str(submission_record.get("stderr", "")).strip(),
            "stdout": str(submission_record.get("stdout", "")).strip(),
        },
        stage_result={
            "stage_id": stage_id,
            "status": "submission_failed",
            "queue_id": stdout_payload.get("queue_id", ""),
            "returncode": returncode,
        },
        mutation=_submission_result_mutation(
            task_status="submission_failed",
            stage_status="submission_failed",
            metadata_updates={
                "submission_status": "submission_failed",
                "submitted_at": submitted_at,
            },
            metadata_removals=("submission_deferred_reason", "last_submission_attempt_at"),
        ),
    )


def _submission_transition(
    *,
    stage_id: str,
    reaction_dir: str,
    submission_record: dict[str, Any],
    stdout_payload: dict[str, Any],
    returncode: int,
) -> _RecordedStageTransition:
    submitted_at = submission_record["submitted_at"]
    if submission_record["status"] == "submitted":
        return _submitted_stage_transition(
            stage_id=stage_id,
            stdout_payload=stdout_payload,
            reaction_dir=reaction_dir,
            submitted_at=submitted_at,
            returncode=returncode,
        )
    if _submission_is_deferred(submission_record):
        return _deferred_submission_transition(
            stage_id=stage_id,
            submission_record=submission_record,
            submitted_at=submitted_at,
            returncode=returncode,
        )
    return _failed_submission_transition(
        stage_id=stage_id,
        stdout_payload=stdout_payload,
        submission_record=submission_record,
        submitted_at=submitted_at,
        returncode=returncode,
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
    transition = _submission_transition(
        stage_id=stage_id,
        reaction_dir=reaction_dir,
        submission_record=submission_record,
        stdout_payload=stdout_payload,
        returncode=returncode,
    )
    _apply_task_stage_mutation(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        mutation=transition.mutation,
        task_record=submission_record,
    )
    return (
        transition.bucket,
        transition.detail,
        transition.stage_result,
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


def _cancel_result_mutation(
    *,
    task_status: str | None = None,
    stage_status: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> _TaskStageMutation:
    return _TaskStageMutation(
        task_status=task_status,
        stage_status=stage_status,
        task_record_key="cancel_result",
        metadata_updates=metadata_updates or {},
    )


def _apply_cancel_mutation(
    context: _CancelStageContext,
    *,
    mutation: _TaskStageMutation,
    cancel_record: dict[str, Any],
) -> None:
    _apply_task_stage_mutation(
        stage=context.stage,
        task=context.task,
        stage_metadata=context.stage_metadata,
        mutation=mutation,
        task_record=cancel_record,
    )


def _record_local_cancel(context: _CancelStageContext) -> _WorkflowStageOutcome:
    cancel_record = {
        "status": "cancelled",
        "cancelled_at": now_utc_iso(),
        "mode": "local",
    }
    _apply_cancel_mutation(
        context,
        mutation=_cancel_result_mutation(
            task_status="cancelled",
            stage_status="cancelled",
            metadata_updates={
                "cancel_status": "cancelled",
                "cancelled_at": cancel_record["cancelled_at"],
            },
        ),
        cancel_record=cancel_record,
    )
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
    cancel_record = {
        "status": "failed",
        "reason": reason,
        "cancelled_at": now_utc_iso(),
    }
    _apply_cancel_mutation(
        context,
        mutation=_cancel_result_mutation(),
        cancel_record=cancel_record,
    )
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
    _apply_cancel_mutation(
        context,
        mutation=_cancel_result_mutation(
            task_status=cancel_status,
            stage_status=cancel_status,
            metadata_updates={
                "cancel_status": cancel_status,
                "cancelled_at": cancel_record["cancelled_at"],
            },
        ),
        cancel_record=cancel_record,
    )
    bucket = {"cancel_requested": "requested"}.get(cancel_status, "cancelled")
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
    _apply_cancel_mutation(
        context,
        mutation=_cancel_result_mutation(),
        cancel_record=cancel_record,
    )
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
