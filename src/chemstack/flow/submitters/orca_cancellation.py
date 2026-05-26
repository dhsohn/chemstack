from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE

from .orca_models import (
    CancelStageContext,
    SiblingSubmitterConfig,
    TaskStageMutation,
    WorkflowBuckets,
    WorkflowStageOutcome,
    apply_task_stage_mutation,
    workflow_metadata,
)
from .orca_submission import orca_submitter_matches


@dataclass(frozen=True)
class CancellationDeps:
    normalize_text: Callable[[Any], str]
    now_utc_iso: Callable[[], str]
    resolve_workflow_workspace: Callable[..., Path]
    load_workflow_payload: Callable[[Path], dict[str, Any]]
    write_workflow_payload: Callable[[Path, dict[str, Any]], Any]
    sync_workflow_registry: Callable[[str | Path, Path, dict[str, Any]], Any]
    cancel_target: Callable[..., dict[str, Any]]


def cancel_config(
    *,
    orca_config: str | None,
    orca_executable: str,
    orca_repo_root: str | None,
    normalize_text: Callable[[Any], str],
) -> SiblingSubmitterConfig:
    return SiblingSubmitterConfig(
        config_path=normalize_text(orca_config),
        executable=normalize_text(orca_executable) or CHEMSTACK_EXECUTABLE,
        repo_root=normalize_text(orca_repo_root) or None,
    )


def cancel_stage_context(stage: dict[str, Any], deps: CancellationDeps) -> CancelStageContext | None:
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
    return CancelStageContext(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        enqueue_payload=enqueue_payload,
        stage_id=deps.normalize_text(stage.get("stage_id")),
        task_status=deps.normalize_text(task.get("status")).lower(),
        stage_status=deps.normalize_text(stage.get("status")).lower(),
        queue_id=deps.normalize_text(stage_metadata.get("queue_id")),
        reaction_dir=deps.normalize_text(payload_reaction_dir)
        or deps.normalize_text(enqueue_payload.get("reaction_dir")),
    )


def cancel_skip_reason(context: CancelStageContext) -> str:
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


def record_cancel_skip(context: CancelStageContext, reason: str) -> WorkflowStageOutcome:
    return WorkflowStageOutcome(
        bucket="skipped",
        detail={"stage_id": context.stage_id, "reason": reason},
        stage_result={"stage_id": context.stage_id, "status": "skipped", "reason": reason},
    )


def needs_orca_cancel(context: CancelStageContext) -> bool:
    return bool(
        context.queue_id
        or context.task_status in {"submitted"}
        or context.stage_status in {"queued", "running"}
    )


def cancel_result_mutation(
    *,
    task_status: str | None = None,
    stage_status: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> TaskStageMutation:
    return TaskStageMutation(
        task_status=task_status,
        stage_status=stage_status,
        task_record_key="cancel_result",
        metadata_updates=metadata_updates or {},
    )


def apply_cancel_mutation(
    context: CancelStageContext,
    *,
    mutation: TaskStageMutation,
    cancel_record: dict[str, Any],
) -> None:
    apply_task_stage_mutation(
        stage=context.stage,
        task=context.task,
        stage_metadata=context.stage_metadata,
        mutation=mutation,
        task_record=cancel_record,
    )


def record_local_cancel(context: CancelStageContext, deps: CancellationDeps) -> WorkflowStageOutcome:
    cancel_record = {
        "status": "cancelled",
        "cancelled_at": deps.now_utc_iso(),
        "mode": "local",
    }
    apply_cancel_mutation(
        context,
        mutation=cancel_result_mutation(
            task_status="cancelled",
            stage_status="cancelled",
            metadata_updates={
                "cancel_status": "cancelled",
                "cancelled_at": cancel_record["cancelled_at"],
            },
        ),
        cancel_record=cancel_record,
    )
    return WorkflowStageOutcome(
        bucket="cancelled",
        detail={"stage_id": context.stage_id, "mode": "local"},
        stage_result={"stage_id": context.stage_id, "status": "cancelled", "mode": "local"},
    )


def record_cancel_failure(
    context: CancelStageContext,
    *,
    reason: str,
    deps: CancellationDeps,
    stage_result: dict[str, Any] | None = None,
) -> WorkflowStageOutcome:
    cancel_record = {
        "status": "failed",
        "reason": reason,
        "cancelled_at": deps.now_utc_iso(),
    }
    apply_cancel_mutation(
        context,
        mutation=cancel_result_mutation(),
        cancel_record=cancel_record,
    )
    return WorkflowStageOutcome(
        bucket="failed",
        detail={"stage_id": context.stage_id, "reason": reason},
        stage_result=stage_result
        or {"stage_id": context.stage_id, "status": "cancel_failed", "reason": reason},
    )


def record_remote_cancel_success(
    context: CancelStageContext,
    *,
    cancel_record: dict[str, Any],
    cancel_status: str,
) -> WorkflowStageOutcome:
    apply_cancel_mutation(
        context,
        mutation=cancel_result_mutation(
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
    return WorkflowStageOutcome(
        bucket=bucket,
        detail={
            "stage_id": context.stage_id,
            "queue_id": context.queue_id,
            "reaction_dir": context.reaction_dir,
        },
        stage_result={"stage_id": context.stage_id, "status": cancel_status},
    )


def record_remote_cancel_failed(
    context: CancelStageContext,
    cancel_record: dict[str, Any],
) -> WorkflowStageOutcome:
    returncode = int(cancel_record.get("returncode", 1))
    apply_cancel_mutation(
        context,
        mutation=cancel_result_mutation(),
        cancel_record=cancel_record,
    )
    return WorkflowStageOutcome(
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


def record_remote_cancel(
    context: CancelStageContext,
    *,
    cancel_identifier: str,
    submitter_config: SiblingSubmitterConfig,
    deps: CancellationDeps,
) -> WorkflowStageOutcome:
    cancel_record = deps.cancel_target(
        target=cancel_identifier,
        config_path=submitter_config.config_path,
        executable=submitter_config.executable,
        repo_root=submitter_config.repo_root,
    )
    cancel_status = str(cancel_record.get("status", "failed"))
    cancel_record["cancelled_at"] = deps.now_utc_iso()
    cancel_record["target"] = cancel_identifier
    if cancel_status in {"cancel_requested", "cancelled"}:
        return record_remote_cancel_success(
            context,
            cancel_record=cancel_record,
            cancel_status=cancel_status,
        )
    return record_remote_cancel_failed(context, cancel_record)


def cancel_stage_outcome(
    *,
    stage: dict[str, Any],
    submitter_config: SiblingSubmitterConfig,
    deps: CancellationDeps,
) -> WorkflowStageOutcome | None:
    context = cancel_stage_context(stage, deps)
    if context is None:
        return None
    skip_reason = cancel_skip_reason(context)
    if skip_reason:
        return record_cancel_skip(context, skip_reason)
    if not needs_orca_cancel(context):
        return record_local_cancel(context, deps)

    cancel_identifier = context.queue_id or context.reaction_dir
    if not cancel_identifier:
        return record_cancel_failure(context, reason="missing_cancel_target", deps=deps)
    if not submitter_config.config_path:
        return record_cancel_failure(context, reason="orca_config_required", deps=deps)
    if not orca_submitter_matches(context.enqueue_payload, normalize_text=deps.normalize_text):
        return None
    return record_remote_cancel(
        context,
        cancel_identifier=cancel_identifier,
        submitter_config=submitter_config,
        deps=deps,
    )


def write_cancellation_summary(payload: dict[str, Any], buckets: WorkflowBuckets, deps: CancellationDeps) -> None:
    if buckets.requested:
        payload["status"] = "cancel_requested"
    elif buckets.cancelled:
        payload["status"] = "cancelled"
    elif buckets.failed:
        payload["status"] = "cancel_failed"
    metadata = workflow_metadata(payload)
    if metadata is not None:
        metadata["cancellation_summary"] = {
            "cancelled_count": len(buckets.cancelled),
            "requested_count": len(buckets.requested),
            "skipped_count": len(buckets.skipped),
            "failed_count": len(buckets.failed),
            "stage_results": buckets.stage_results,
            "updated_at": deps.now_utc_iso(),
        }


def cancel_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_config: str | None = None,
    orca_executable: str = CHEMSTACK_EXECUTABLE,
    orca_repo_root: str | None = None,
    deps: CancellationDeps,
) -> dict[str, Any]:
    workspace_dir = deps.resolve_workflow_workspace(
        target=workflow_target,
        workflow_root=workflow_root,
    )
    payload = deps.load_workflow_payload(workspace_dir)
    buckets = WorkflowBuckets()
    submitter_config = cancel_config(
        orca_config=orca_config,
        orca_executable=orca_executable,
        orca_repo_root=orca_repo_root,
        normalize_text=deps.normalize_text,
    )

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        outcome = cancel_stage_outcome(stage=stage, submitter_config=submitter_config, deps=deps)
        if outcome is not None:
            buckets.record(outcome)

    write_cancellation_summary(payload, buckets, deps)
    deps.write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        deps.sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "cancelled": buckets.cancelled,
        "requested": buckets.requested,
        "skipped": buckets.skipped,
        "failed": buckets.failed,
    }
