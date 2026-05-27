from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import ORCA_SUBMITTERS

from .orca_models import (
    RecordedStageTransition,
    SiblingSubmitterConfig,
    TaskStageMutation,
    WorkflowBuckets,
    WorkflowStageOutcome,
    apply_task_stage_mutation,
    ensure_submission_metadata,
    mapping_payload,
    workflow_metadata,
)


@dataclass(frozen=True)
class SubmissionDeps:
    normalize_text: Callable[[Any], str]
    now_utc_iso: Callable[[], str]
    resolve_workflow_workspace: Callable[..., Path]
    load_workflow_payload: Callable[[Path], dict[str, Any]]
    write_workflow_payload: Callable[[Path, dict[str, Any]], Any]
    sync_workflow_registry: Callable[[str | Path, Path, dict[str, Any]], Any]
    submit_reaction_dir: Callable[..., dict[str, Any]]


def submission_is_deferred(value: dict[str, Any], *, normalize_text: Callable[[Any], str]) -> bool:
    return normalize_text(value.get("status")).lower() in {
        "blocked",
        "waiting_for_slot",
        "admission_blocked",
        "admission_limit_reached",
        "deferred",
    }


def submission_deferred_reason(
    value: dict[str, Any],
    *,
    normalize_text: Callable[[Any], str],
) -> str:
    return (
        normalize_text(value.get("reason"))
        or normalize_text(value.get("status"))
        or "waiting_for_slot"
    )


def skip_submission_reason(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    skip_submitted: bool,
    normalize_text: Callable[[Any], str],
) -> str:
    if not skip_submitted:
        return ""
    existing_submission = task.get("submission_result")
    task_status = normalize_text(task.get("status")).lower()
    stage_status = normalize_text(stage.get("status")).lower()
    if (
        (isinstance(existing_submission, dict) and existing_submission.get("status") == "submitted")
        or task_status == "submitted"
        or stage_status in {"submitted", "queued"}
    ):
        return "already_submitted"
    return ""


def submission_resource_kwargs(enqueue_payload: dict[str, Any]) -> dict[str, int]:
    resource_kwargs: dict[str, int] = {}
    max_cores = int(enqueue_payload.get("max_cores", 0) or 0)
    max_memory_gb = int(enqueue_payload.get("max_memory_gb", 0) or 0)
    if max_cores > 0:
        resource_kwargs["max_cores"] = max_cores
    if max_memory_gb > 0:
        resource_kwargs["max_memory_gb"] = max_memory_gb
    return resource_kwargs


def submission_force(enqueue_payload: dict[str, Any]) -> bool:
    value = enqueue_payload.get("force", False)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def submission_result_mutation(
    *,
    task_status: str,
    stage_status: str,
    metadata_updates: dict[str, Any],
    metadata_removals: tuple[str, ...] = (),
) -> TaskStageMutation:
    return TaskStageMutation(
        task_status=task_status,
        stage_status=stage_status,
        task_record_key="submission_result",
        metadata_updates=metadata_updates,
        metadata_removals=metadata_removals,
    )


def record_missing_reaction_dir(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    now_utc_iso: Callable[[], str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    submission_record = {
        "status": "failed",
        "reason": "missing_reaction_dir",
        "submitted_at": now_utc_iso(),
    }
    stage_id = stage.get("stage_id", "")
    apply_task_stage_mutation(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        mutation=submission_result_mutation(
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


def submitted_stage_transition(
    *,
    stage_id: str,
    stdout_payload: dict[str, Any],
    reaction_dir: str,
    submitted_at: str,
    returncode: int,
) -> RecordedStageTransition:
    queue_id = stdout_payload.get("queue_id", "")
    return RecordedStageTransition(
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
        mutation=submission_result_mutation(
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


def deferred_submission_transition(
    *,
    stage_id: str,
    submission_record: dict[str, Any],
    submitted_at: str,
    returncode: int,
    normalize_text: Callable[[Any], str],
) -> RecordedStageTransition:
    reason = submission_deferred_reason(submission_record, normalize_text=normalize_text)
    return RecordedStageTransition(
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
        mutation=submission_result_mutation(
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


def failed_submission_transition(
    *,
    stage_id: str,
    stdout_payload: dict[str, Any],
    submission_record: dict[str, Any],
    submitted_at: str,
    returncode: int,
) -> RecordedStageTransition:
    return RecordedStageTransition(
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
        mutation=submission_result_mutation(
            task_status="submission_failed",
            stage_status="submission_failed",
            metadata_updates={
                "submission_status": "submission_failed",
                "submitted_at": submitted_at,
            },
            metadata_removals=("submission_deferred_reason", "last_submission_attempt_at"),
        ),
    )


def submission_transition(
    *,
    stage_id: str,
    reaction_dir: str,
    submission_record: dict[str, Any],
    stdout_payload: dict[str, Any],
    returncode: int,
    normalize_text: Callable[[Any], str],
) -> RecordedStageTransition:
    submitted_at = submission_record["submitted_at"]
    if submission_record["status"] == "submitted":
        return submitted_stage_transition(
            stage_id=stage_id,
            stdout_payload=stdout_payload,
            reaction_dir=reaction_dir,
            submitted_at=submitted_at,
            returncode=returncode,
        )
    if submission_is_deferred(submission_record, normalize_text=normalize_text):
        return deferred_submission_transition(
            stage_id=stage_id,
            submission_record=submission_record,
            submitted_at=submitted_at,
            returncode=returncode,
            normalize_text=normalize_text,
        )
    return failed_submission_transition(
        stage_id=stage_id,
        stdout_payload=stdout_payload,
        submission_record=submission_record,
        submitted_at=submitted_at,
        returncode=returncode,
    )


def record_submission_outcome(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    reaction_dir: str,
    submission_record: dict[str, Any],
    now_utc_iso: Callable[[], str],
    normalize_text: Callable[[Any], str],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    stage_id = stage.get("stage_id", "")
    stdout_payload = mapping_payload(submission_record.get("parsed_stdout"))
    returncode = int(submission_record.get("returncode", 1))
    submission_record["submitted_at"] = now_utc_iso()
    transition = submission_transition(
        stage_id=stage_id,
        reaction_dir=reaction_dir,
        submission_record=submission_record,
        stdout_payload=stdout_payload,
        returncode=returncode,
        normalize_text=normalize_text,
    )
    apply_task_stage_mutation(
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


def submission_summary_state(
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


def orca_submitter_matches(
    enqueue_payload: dict[str, Any],
    *,
    normalize_text: Callable[[Any], str],
) -> bool:
    return normalize_text(enqueue_payload.get("submitter")) in {"", *ORCA_SUBMITTERS}


def submission_config(
    *,
    orca_config: str,
    orca_repo_root: str | None,
    normalize_text: Callable[[Any], str],
) -> SiblingSubmitterConfig:
    return SiblingSubmitterConfig(
        config_path=normalize_text(orca_config),
        repo_root=normalize_text(orca_repo_root) or None,
    )


def submission_kwargs(enqueue_payload: dict[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = submission_resource_kwargs(enqueue_payload)
    if submission_force(enqueue_payload):
        kwargs["force"] = True
    return kwargs


def submission_stage_outcome(
    *,
    stage: dict[str, Any],
    submitter_config: SiblingSubmitterConfig,
    skip_submitted: bool,
    deps: SubmissionDeps,
) -> WorkflowStageOutcome | None:
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    enqueue_payload = task.get("enqueue_payload")
    if not isinstance(enqueue_payload, dict):
        return None
    stage_metadata = ensure_submission_metadata(stage, task)

    skip_reason = skip_submission_reason(
        stage=stage,
        task=task,
        skip_submitted=skip_submitted,
        normalize_text=deps.normalize_text,
    )
    stage_id = stage.get("stage_id", "")
    if skip_reason:
        return WorkflowStageOutcome(
            bucket="skipped",
            detail={"stage_id": stage_id, "reason": skip_reason},
            stage_result={"stage_id": stage_id, "status": "skipped", "reason": skip_reason},
        )

    reaction_dir = deps.normalize_text(enqueue_payload.get("reaction_dir"))
    if not reaction_dir:
        fail_record, stage_result = record_missing_reaction_dir(
            stage=stage,
            task=task,
            stage_metadata=stage_metadata,
            now_utc_iso=deps.now_utc_iso,
        )
        return WorkflowStageOutcome(bucket="failed", detail=fail_record, stage_result=stage_result)
    if not orca_submitter_matches(enqueue_payload, normalize_text=deps.normalize_text):
        return None

    submission_record = deps.submit_reaction_dir(
        reaction_dir=reaction_dir,
        priority=int(enqueue_payload.get("priority", 10) or 10),
        config_path=submitter_config.config_path,
        repo_root=submitter_config.repo_root,
        **submission_kwargs(enqueue_payload),
    )
    outcome, detail_record, stage_result = record_submission_outcome(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        reaction_dir=reaction_dir,
        submission_record=submission_record,
        now_utc_iso=deps.now_utc_iso,
        normalize_text=deps.normalize_text,
    )
    bucket = "skipped" if outcome == "deferred" else outcome
    return WorkflowStageOutcome(bucket=bucket, detail=detail_record, stage_result=stage_result)


def record_submission_summary(payload: dict[str, Any], buckets: WorkflowBuckets, deps: SubmissionDeps) -> None:
    payload_status, summary_status = submission_summary_state(
        submitted=buckets.submitted,
        skipped=buckets.skipped,
        failed=buckets.failed,
    )
    if payload_status:
        payload["status"] = payload_status
    metadata = workflow_metadata(payload)
    if metadata is not None:
        metadata["submission_summary"] = {
            "status": summary_status,
            "submitted_count": len(buckets.submitted),
            "skipped_count": len(buckets.skipped),
            "failed_count": len(buckets.failed),
            "stage_results": buckets.stage_results,
            "updated_at": deps.now_utc_iso(),
        }


def submit_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_config: str,
    orca_repo_root: str | None = None,
    skip_submitted: bool = True,
    deps: SubmissionDeps,
) -> dict[str, Any]:
    workspace_dir = deps.resolve_workflow_workspace(
        target=workflow_target,
        workflow_root=workflow_root,
    )
    payload = deps.load_workflow_payload(workspace_dir)
    buckets = WorkflowBuckets()
    submitter_config = submission_config(
        orca_config=orca_config,
        orca_repo_root=orca_repo_root,
        normalize_text=deps.normalize_text,
    )

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        outcome = submission_stage_outcome(
            stage=stage,
            submitter_config=submitter_config,
            skip_submitted=skip_submitted,
            deps=deps,
        )
        if outcome is not None:
            buckets.record(outcome)

    record_submission_summary(payload, buckets, deps)
    deps.write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        deps.sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "submitted": buckets.submitted,
        "skipped": buckets.skipped,
        "failed": buckets.failed,
    }
