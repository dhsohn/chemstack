from __future__ import annotations

from typing import Any

from chemstack.core.utils.coercion import normalize_text, safe_int as _shared_safe_int


def safe_int(value: Any) -> int | None:
    return _shared_safe_int(value, default=None)


def stage_key(stage: dict[str, Any], index: int) -> str:
    stage_id = normalize_text(stage.get("stage_id"))
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
        text = normalize_text(stage.get(field))
        if text:
            metadata[field] = text
    for field in int_fields:
        value = safe_int(stage.get(field))
        if value is not None:
            metadata[field] = value
    return metadata


def stage_status_event_type(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
    *,
    suppress_terminal_event: bool,
) -> str:
    previous_status = normalize_text(previous_stage.get("status")).lower()
    current_status = normalize_text(current_stage.get("status")).lower()
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
    engine = normalize_text(current_stage.get("engine") or previous_stage.get("engine")).lower()
    task_kind = normalize_text(
        current_stage.get("task_kind") or previous_stage.get("task_kind")
    ).lower()
    if engine != "xtb" or task_kind != "path_search":
        return ""
    previous_handoff = normalize_text(previous_stage.get("reaction_handoff_status")).lower()
    current_handoff = normalize_text(current_stage.get("reaction_handoff_status")).lower()
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
) -> dict[str, str]:
    return {
        "previous_stage_status": normalize_text(previous_stage.get("status")).lower(),
        "current_stage_status": normalize_text(current_stage.get("status")).lower(),
        "previous_handoff_status": normalize_text(
            previous_stage.get("reaction_handoff_status")
        ).lower(),
        "current_handoff_status": normalize_text(
            current_stage.get("reaction_handoff_status")
        ).lower(),
        "stage_id": normalize_text(current_stage.get("stage_id") or previous_stage.get("stage_id")),
        "engine": normalize_text(current_stage.get("engine") or previous_stage.get("engine")),
        "task_kind": normalize_text(
            current_stage.get("task_kind") or previous_stage.get("task_kind")
        ),
    }


def stage_transition_metadata(
    metadata: dict[str, Any],
    context: dict[str, str],
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
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    reason = ""
    if event_type in {"workflow_stage_failed", "workflow_stage_cancelled"}:
        reason = normalize_text(current_stage.get("reason"))
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
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "status": context["current_handoff_status"],
        "previous_status": context["previous_handoff_status"],
        "reason": normalize_text(
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
) -> list[dict[str, Any]]:
    previous_stages = list(previous_summary.get("stage_summaries", []))
    current_stages = list(current_summary.get("stage_summaries", []))
    previous_by_key = {
        stage_key(stage, index): dict(stage) for index, stage in enumerate(previous_stages)
    }
    event_payloads: list[dict[str, Any]] = []

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
