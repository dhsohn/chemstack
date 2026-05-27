from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from . import _runtime_common
from .runtime_models import (
    StageTransitionContext,
    WorkflowAdvanceResult,
    WorkflowJournalEventPayload,
)

TERMINAL_WORKFLOW_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
    }
)
ACTIVE_TERMINAL_SYNC_STATUSES = frozenset(
    {"queued", "running", "submitted", "cancel_requested"}
)


def workflow_advance_failed_result(
    record: Any, *, previous_status: str, reason: str
) -> WorkflowAdvanceResult:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": "advance_failed",
        "advanced": False,
        "reason": reason,
        "stage_count": record.stage_count,
    }


def workflow_skipped_terminal_result(
    record: Any, *, previous_status: str
) -> WorkflowAdvanceResult:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": previous_status,
        "advanced": False,
        "reason": "terminal_status",
        "stage_count": record.stage_count,
    }


def workflow_advanced_result(
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    status: str,
    reason: str = "",
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> WorkflowAdvanceResult:
    result: WorkflowAdvanceResult = {
        "workflow_id": normalize_text_fn(payload.get("workflow_id")) or record.workflow_id,
        "template_name": normalize_text_fn(payload.get("template_name")) or record.template_name,
        "previous_status": previous_status,
        "status": status,
        "advanced": True,
        "changed": status != previous_status,
        "stage_count": len(payload.get("stages", []))
        if isinstance(payload.get("stages"), list)
        else record.stage_count,
    }
    if reason:
        result["reason"] = reason
    return result


def workflow_needs_terminal_sync(
    workspace_dir: str | Path,
    *,
    load_workflow_payload_fn: Callable[[str | Path], dict[str, Any]],
    workflow_has_active_downstream_fn: Callable[[dict[str, Any]], bool],
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> bool:
    try:
        payload = load_workflow_payload_fn(workspace_dir)
    except (FileNotFoundError, ValueError):
        return False
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and bool(metadata.get("final_child_sync_pending")):
        return True
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        if normalize_text_fn(raw_stage.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES:
            return True
        task = raw_stage.get("task")
        if (
            isinstance(task, dict)
            and normalize_text_fn(task.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES
        ):
            return True
    return workflow_has_active_downstream_fn(payload)


def stage_key(stage: dict[str, Any], index: int) -> str:
    stage_id = _runtime_common.normalize_text(stage.get("stage_id"))
    if stage_id:
        return stage_id
    return f"index:{index}"


def stage_event_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    text_fields = (
        "stage_id",
        "stage_kind",
        "engine",
        "task_kind",
        "task_status",
        "queue_id",
        "reaction_dir",
        "selected_input_xyz",
        "selected_inp",
        "submission_status",
        "run_id",
        "latest_known_path",
        "organized_output_dir",
        "optimized_xyz_path",
        "analyzer_status",
        "reason",
        "reaction_handoff_status",
        "reaction_handoff_reason",
        "completed_at",
        "last_out_path",
    )
    int_fields = (
        "xtb_handoff_retries_used",
        "xtb_handoff_retry_limit",
        "orca_attempt_count",
        "orca_max_retries",
        "output_artifact_count",
    )
    for field in text_fields:
        text = _runtime_common.normalize_text(stage.get(field))
        if text:
            metadata[field] = text
    for field in int_fields:
        value = _runtime_common.safe_int(stage.get(field))
        if value is not None:
            metadata[field] = value
    return metadata


def stage_status_event_type(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
    *,
    suppress_terminal_event: bool,
) -> str:
    previous_status = _runtime_common.normalize_text(previous_stage.get("status")).lower()
    current_status = _runtime_common.normalize_text(current_stage.get("status")).lower()
    if not current_status or current_status == previous_status:
        return ""
    if current_status == "queued":
        return "workflow_stage_submitted"
    if current_status in {"submitted", "running"}:
        return "workflow_stage_status_changed"
    if suppress_terminal_event:
        return ""
    if current_status == "completed":
        return "workflow_stage_completed"
    if current_status in {"failed", "submission_failed", "cancel_failed"}:
        return "workflow_stage_failed"
    if current_status == "cancelled":
        return "workflow_stage_cancelled"
    return ""


def stage_handoff_event_type(previous_stage: dict[str, Any], current_stage: dict[str, Any]) -> str:
    engine = _runtime_common.normalize_text(
        current_stage.get("engine") or previous_stage.get("engine")
    ).lower()
    task_kind = _runtime_common.normalize_text(
        current_stage.get("task_kind") or previous_stage.get("task_kind")
    ).lower()
    if engine != "xtb" or task_kind != "path_search":
        return ""
    previous_handoff = _runtime_common.normalize_text(
        previous_stage.get("reaction_handoff_status")
    ).lower()
    current_handoff = _runtime_common.normalize_text(
        current_stage.get("reaction_handoff_status")
    ).lower()
    if not current_handoff or current_handoff == previous_handoff:
        return ""
    if current_handoff == "ready":
        return "workflow_stage_handoff_ready"
    if current_handoff == "retrying":
        return "workflow_stage_handoff_retrying"
    if current_handoff == "failed":
        return "workflow_stage_handoff_failed"
    return ""


def stage_transition_context(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
) -> StageTransitionContext:
    return {
        "previous_stage_status": _runtime_common.normalize_text(
            previous_stage.get("status")
        ).lower(),
        "current_stage_status": _runtime_common.normalize_text(
            current_stage.get("status")
        ).lower(),
        "previous_handoff_status": _runtime_common.normalize_text(
            previous_stage.get("reaction_handoff_status")
        ).lower(),
        "current_handoff_status": _runtime_common.normalize_text(
            current_stage.get("reaction_handoff_status")
        ).lower(),
        "stage_id": _runtime_common.normalize_text(
            current_stage.get("stage_id") or previous_stage.get("stage_id")
        ),
        "engine": _runtime_common.normalize_text(
            current_stage.get("engine") or previous_stage.get("engine")
        ),
        "task_kind": _runtime_common.normalize_text(
            current_stage.get("task_kind") or previous_stage.get("task_kind")
        ),
    }


def stage_transition_metadata(
    metadata: dict[str, Any],
    context: StageTransitionContext,
    *,
    include_handoff: bool,
) -> dict[str, Any]:
    event_metadata = dict(metadata)
    if context["previous_stage_status"]:
        event_metadata["previous_stage_status"] = context["previous_stage_status"]
    if context["current_stage_status"]:
        event_metadata["stage_status"] = context["current_stage_status"]
    if include_handoff and context["previous_handoff_status"]:
        event_metadata["previous_reaction_handoff_status"] = context["previous_handoff_status"]
    if include_handoff and context["current_handoff_status"]:
        event_metadata["reaction_handoff_status"] = context["current_handoff_status"]
    return event_metadata


def status_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: StageTransitionContext,
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> WorkflowJournalEventPayload:
    reason = ""
    if event_type in {"workflow_stage_failed", "workflow_stage_cancelled"}:
        reason = _runtime_common.normalize_text(current_stage.get("reason"))
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "status": context["current_stage_status"],
        "previous_status": context["previous_stage_status"],
        "reason": reason,
        "worker_session_id": worker_session_id,
        "stage_id": context["stage_id"],
        "engine": context["engine"],
        "task_kind": context["task_kind"],
        "stage_status": context["current_stage_status"],
        "previous_stage_status": context["previous_stage_status"],
        "metadata": stage_transition_metadata(metadata, context, include_handoff=False),
    }


def handoff_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: StageTransitionContext,
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> WorkflowJournalEventPayload:
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "status": context["current_handoff_status"],
        "previous_status": context["previous_handoff_status"],
        "reason": _runtime_common.normalize_text(
            current_stage.get("reaction_handoff_reason") or current_stage.get("reason")
        ),
        "worker_session_id": worker_session_id,
        "stage_id": context["stage_id"],
        "engine": context["engine"],
        "task_kind": context["task_kind"],
        "stage_status": context["current_stage_status"],
        "previous_stage_status": context["previous_stage_status"],
        "reaction_handoff_status": context["current_handoff_status"],
        "previous_reaction_handoff_status": context["previous_handoff_status"],
        "metadata": stage_transition_metadata(metadata, context, include_handoff=True),
    }


def stage_transition_event_payloads(
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> list[WorkflowJournalEventPayload]:
    previous_stages = list(previous_summary.get("stage_summaries", []))
    current_stages = list(current_summary.get("stage_summaries", []))
    previous_by_key = {
        stage_key(stage, index): dict(stage) for index, stage in enumerate(previous_stages)
    }
    event_payloads: list[WorkflowJournalEventPayload] = []

    for index, raw_stage in enumerate(current_stages):
        current_stage = dict(raw_stage)
        previous_stage = previous_by_key.get(stage_key(current_stage, index), {})
        handoff_event_type = stage_handoff_event_type(previous_stage, current_stage)
        status_event_type = stage_status_event_type(
            previous_stage,
            current_stage,
            suppress_terminal_event=handoff_event_type
            in {"workflow_stage_handoff_ready", "workflow_stage_handoff_failed"},
        )
        metadata = stage_event_metadata(current_stage)
        context = stage_transition_context(previous_stage, current_stage)

        if status_event_type:
            event_payloads.append(
                status_transition_event_payload(
                    event_type=status_event_type,
                    current_stage=current_stage,
                    context=context,
                    metadata=metadata,
                    workflow_id=workflow_id,
                    template_name=template_name,
                    worker_session_id=worker_session_id,
                )
            )

        if handoff_event_type:
            event_payloads.append(
                handoff_transition_event_payload(
                    event_type=handoff_event_type,
                    current_stage=current_stage,
                    context=context,
                    metadata=metadata,
                    workflow_id=workflow_id,
                    template_name=template_name,
                    worker_session_id=worker_session_id,
                )
            )
    return event_payloads


def append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    stage_transition_event_payloads_fn: Callable[..., list[WorkflowJournalEventPayload]],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    for payload in stage_transition_event_payloads_fn(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event_fn(workflow_root, **payload)


def append_phase_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    phase_transition_event_payloads_fn: Callable[..., list[dict[str, Any]]],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    for payload in phase_transition_event_payloads_fn(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event_fn(workflow_root, **payload)


def append_workflow_advance_failed_event(
    workflow_root: str | Path,
    record: Any,
    *,
    previous_status: str,
    reason: str,
    worker_session_id: str,
    append_workflow_journal_event_fn: Callable[..., Any],
) -> None:
    append_workflow_journal_event_fn(
        workflow_root,
        event_type="workflow_advance_failed",
        workflow_id=record.workflow_id,
        template_name=record.template_name,
        previous_status=previous_status,
        status="advance_failed",
        reason=reason,
        worker_session_id=worker_session_id,
    )


def append_workflow_advanced_events(
    workflow_root: str | Path,
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    current_summary: dict[str, Any],
    previous_summary: dict[str, Any],
    worker_session_id: str,
    reason: str = "",
    append_workflow_journal_event_fn: Callable[..., Any],
    append_phase_transition_events_fn: Callable[..., None],
    append_stage_transition_events_fn: Callable[..., None],
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
) -> None:
    status = normalize_text_fn(payload.get("status")).lower()
    workflow_id = normalize_text_fn(payload.get("workflow_id")) or record.workflow_id
    template_name = normalize_text_fn(payload.get("template_name")) or record.template_name
    if status != previous_status:
        event_kwargs: WorkflowJournalEventPayload = {
            "event_type": "workflow_status_changed",
            "workflow_id": workflow_id,
            "template_name": template_name,
            "previous_status": previous_status,
            "status": status,
            "worker_session_id": worker_session_id,
        }
        if reason:
            event_kwargs["reason"] = reason
        append_workflow_journal_event_fn(workflow_root, **event_kwargs)
    append_phase_transition_events_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )
    append_stage_transition_events_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )
