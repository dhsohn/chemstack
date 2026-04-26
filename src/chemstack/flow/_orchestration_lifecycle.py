from __future__ import annotations

from typing import Any, Callable

from .workflow_status import WORKFLOW_FAILED_STATUSES, WORKFLOW_TERMINAL_STATUSES


def workflow_sync_only_impl(payload: dict[str, Any], *, normalize_text_fn: Callable[[Any], str]) -> bool:
    return normalize_text_fn(payload.get("status")).lower() in {
        "completed",
        "cancel_requested",
        "cancelled",
        "cancel_failed",
    }


def workflow_has_active_children_impl(
    payload: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    workflow_has_active_downstream_fn: Callable[[dict[str, Any]], bool],
) -> bool:
    active_statuses = {"queued", "running", "submitted", "cancel_requested"}
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        stage_status = normalize_text_fn(raw_stage.get("status")).lower()
        if stage_status in active_statuses:
            return True
        task = raw_stage.get("task")
        if not isinstance(task, dict):
            continue
        task_status = normalize_text_fn(task.get("status")).lower()
        if task_status in active_statuses:
            return True
    return workflow_has_active_downstream_fn(payload)


def latest_child_stage_summary_impl(
    stage_summaries: list[dict[str, Any]],
    *,
    normalize_text_fn: Callable[[Any], str],
) -> dict[str, Any]:
    if not stage_summaries:
        return {}
    priority = {
        "running": 5,
        "submitted": 4,
        "queued": 3,
        "planned": 2,
        "cancel_requested": 1,
    }
    chosen = stage_summaries[-1]
    best_priority = -1
    for item in stage_summaries:
        status = normalize_text_fn(item.get("status")).lower()
        task_status = normalize_text_fn(item.get("task_status")).lower()
        score = max(priority.get(status, 0), priority.get(task_status, 0))
        if score >= best_priority:
            best_priority = score
            chosen = item
    return {
        "stage_id": normalize_text_fn(chosen.get("stage_id")),
        "stage_kind": normalize_text_fn(chosen.get("stage_kind")),
        "engine": normalize_text_fn(chosen.get("engine")),
        "task_kind": normalize_text_fn(chosen.get("task_kind")),
        "status": normalize_text_fn(chosen.get("status")),
        "task_status": normalize_text_fn(chosen.get("task_status")),
        "analyzer_status": normalize_text_fn(chosen.get("analyzer_status")),
        "reason": normalize_text_fn(chosen.get("reason")),
        "queue_id": normalize_text_fn(chosen.get("queue_id")),
        "run_id": normalize_text_fn(chosen.get("run_id")),
        "latest_known_path": normalize_text_fn(chosen.get("latest_known_path")),
        "organized_output_dir": normalize_text_fn(chosen.get("organized_output_dir")),
        "completed_at": normalize_text_fn(chosen.get("completed_at")),
    }


def downstream_terminal_result_impl(
    child_payload: dict[str, Any],
    child_summary: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
) -> dict[str, Any]:
    status = normalize_text_fn(child_summary.get("status")).lower()
    if status not in WORKFLOW_TERMINAL_STATUSES:
        return {}
    metadata = child_payload.get("metadata")
    workflow_error: dict[str, Any] = {}
    if isinstance(metadata, dict) and isinstance(metadata.get("workflow_error"), dict):
        workflow_error = metadata.get("workflow_error") or {}
    last_completed_at = ""
    for stage in child_summary.get("stage_summaries", []):
        if not isinstance(stage, dict):
            continue
        completed_at = normalize_text_fn(stage.get("completed_at"))
        if completed_at:
            last_completed_at = completed_at
    return {
        "status": normalize_text_fn(child_summary.get("status")),
        "completed_at": last_completed_at,
        "failure_reason": normalize_text_fn(workflow_error.get("reason")),
        "failure_scope": normalize_text_fn(workflow_error.get("scope")),
    }


def stage_failure_is_recoverable_impl(
    stage: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    stage_metadata_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> bool:
    status = normalize_text_fn(stage.get("status")).lower()
    if status not in WORKFLOW_FAILED_STATUSES:
        return False
    task = stage.get("task")
    if not isinstance(task, dict):
        return False
    engine = normalize_text_fn(task.get("engine"))
    metadata = stage_metadata_fn(stage)
    if engine == "xtb":
        return normalize_text_fn(metadata.get("reaction_handoff_status")) == "ready"
    if engine == "orca":
        return normalize_text_fn(metadata.get("reaction_candidate_status")) == "superseded"
    return False


def effective_stage_status_impl(
    stage: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    stage_failure_is_recoverable_fn: Callable[[dict[str, Any]], bool],
) -> str:
    if stage_failure_is_recoverable_fn(stage):
        return "completed"
    return normalize_text_fn(stage.get("status")).lower()


def recompute_workflow_status_impl(
    payload: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    effective_stage_status_fn: Callable[[dict[str, Any]], str],
) -> str:
    stages = [stage for stage in payload.get("stages", []) if isinstance(stage, dict)]
    failed_statuses = WORKFLOW_FAILED_STATUSES
    active_statuses = {"queued", "running", "submitted", "cancel_requested"}
    terminal_statuses = WORKFLOW_TERMINAL_STATUSES

    def _stage_engine(stage: dict[str, Any]) -> str:
        task = stage.get("task")
        if not isinstance(task, dict):
            return ""
        return normalize_text_fn(task.get("engine")).lower()

    stage_rows = [
        (stage, effective_stage_status_fn(stage), _stage_engine(stage))
        for stage in stages
    ]
    statuses = [status for _, status, _ in stage_rows]
    current_status = normalize_text_fn(payload.get("status")).lower()
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        workflow_error = metadata.get("workflow_error")
        if isinstance(workflow_error, dict) and normalize_text_fn(workflow_error.get("status")).lower() == "failed":
            return "failed"
    if any(
        status in failed_statuses and engine in {"", "crest"}
        for _, status, engine in stage_rows
    ):
        return "failed"
    if current_status == "cancelled":
        return "cancelled"
    if current_status == "cancel_requested":
        if any(status in active_statuses for status in statuses):
            return "cancel_requested"
        return "cancelled"
    if any(status in active_statuses for status in statuses):
        return "running"
    if any(status == "planned" for status in statuses):
        return "running"
    if stages and all(status in terminal_statuses for status in statuses):
        return "completed"
    if any(status == "completed" for status in statuses):
        return "running"
    return "planned"


__all__ = [
    "downstream_terminal_result_impl",
    "effective_stage_status_impl",
    "latest_child_stage_summary_impl",
    "recompute_workflow_status_impl",
    "stage_failure_is_recoverable_impl",
    "workflow_has_active_children_impl",
    "workflow_sync_only_impl",
]
