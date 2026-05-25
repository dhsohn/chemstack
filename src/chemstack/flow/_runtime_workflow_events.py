from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ._runtime_common import normalize_text


def append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    stage_transition_event_payloads_fn: Callable[..., list[dict[str, Any]]],
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
    normalize_text_fn: Callable[[Any], str] = normalize_text,
) -> None:
    status = normalize_text_fn(payload.get("status")).lower()
    workflow_id = normalize_text_fn(payload.get("workflow_id")) or record.workflow_id
    template_name = normalize_text_fn(payload.get("template_name")) or record.template_name
    if status != previous_status:
        event_kwargs: dict[str, Any] = {
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
