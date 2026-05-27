from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import active_slot_count
from chemstack.core.utils import now_utc_iso, timestamped_token

from . import _runtime_common, runtime_admission, runtime_events, runtime_models
from ._workflow_phases import phase_transition_event_payloads
from .engine_options import WorkflowEngineOptions
from .orchestration import advance_workflow
from .registry import (
    append_workflow_journal_event,
    list_workflow_registry,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .state import load_workflow_payload, workflow_has_active_downstream, workflow_summary

WORKFLOW_WORKER_LOCK_NAME = "workflow_worker.lock"
TERMINAL_WORKFLOW_STATUSES = runtime_events.TERMINAL_WORKFLOW_STATUSES
ACTIVE_TERMINAL_SYNC_STATUSES = runtime_events.ACTIVE_TERMINAL_SYNC_STATUSES

StageTransitionContext = runtime_models.StageTransitionContext
WorkflowAdvanceResult = runtime_models.WorkflowAdvanceResult
WorkflowJournalEventPayload = runtime_models.WorkflowJournalEventPayload
WorkflowRegistryCyclePayload = runtime_models.WorkflowRegistryCyclePayload
WorkflowRegistryAdvanceRequest = runtime_models.WorkflowRegistryAdvanceRequest
WorkflowRuntimeContext = runtime_models.WorkflowRuntimeContext
_WorkflowCycle = runtime_models._WorkflowCycle
_WorkflowCycleProgress = runtime_models._WorkflowCycleProgress

submission_admission_limit_from_config = runtime_admission.submission_admission_limit_from_config
_mapping_section = runtime_admission._mapping_section
_resolve_configured_path = runtime_admission._resolve_configured_path
_submission_admission_root_from_config = runtime_admission._submission_admission_root_from_config
submission_admission_has_capacity = runtime_admission.submission_admission_has_capacity
workflow_submission_has_capacity = runtime_admission.workflow_submission_has_capacity

workflow_advance_failed_result = runtime_events.workflow_advance_failed_result
workflow_skipped_terminal_result = runtime_events.workflow_skipped_terminal_result
workflow_advanced_result = runtime_events.workflow_advanced_result
workflow_needs_terminal_sync = runtime_events.workflow_needs_terminal_sync
stage_key = runtime_events.stage_key
stage_event_metadata = runtime_events.stage_event_metadata
stage_status_event_type = runtime_events.stage_status_event_type
stage_handoff_event_type = runtime_events.stage_handoff_event_type
stage_transition_context = runtime_events.stage_transition_context
stage_transition_metadata = runtime_events.stage_transition_metadata
status_transition_event_payload = runtime_events.status_transition_event_payload
handoff_transition_event_payload = runtime_events.handoff_transition_event_payload
stage_transition_event_payloads = runtime_events.stage_transition_event_payloads
append_stage_transition_events = runtime_events.append_stage_transition_events
append_phase_transition_events = runtime_events.append_phase_transition_events
append_workflow_advance_failed_event = runtime_events.append_workflow_advance_failed_event
append_workflow_advanced_events = runtime_events.append_workflow_advanced_events


def workflow_lease_expires_at(lease_seconds: float) -> str:
    if lease_seconds <= 0:
        return ""
    try:
        from datetime import datetime, timedelta, timezone

        return (datetime.now(timezone.utc) + timedelta(seconds=float(lease_seconds))).isoformat()
    except Exception:
        return ""


def start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
    now_utc_iso_fn: Callable[[], str],
    timestamped_token_fn: Callable[[str], str],
    workflow_submission_has_capacity_fn: Callable[..., bool],
    write_workflow_worker_state_fn: Callable[..., Any],
    append_workflow_journal_event_fn: Callable[..., Any],
    workflow_lease_expires_at_fn: Callable[[float], str] = workflow_lease_expires_at,
) -> _WorkflowCycle:
    cycle_started_at = now_utc_iso_fn()
    session_id = _runtime_common.normalize_text(context.worker_session_id) or timestamped_token_fn(
        "wf_worker"
    )
    requested_submit_ready = bool(context.submit_ready)
    cycle_submit_ready = requested_submit_ready and workflow_submission_has_capacity_fn(
        context.options.crest_config,
        context.options.xtb_config,
        context.options.orca_config,
    )
    admission_blocked = requested_submit_ready and not cycle_submit_ready
    lease_expires_at = workflow_lease_expires_at_fn(context.lease_seconds)

    write_workflow_worker_state_fn(
        context.root,
        worker_session_id=session_id,
        status="running",
        workflow_root_path=context.root,
        last_cycle_started_at=cycle_started_at,
        last_heartbeat_at=cycle_started_at,
        lease_expires_at=lease_expires_at,
        interval_seconds=context.interval_seconds,
        submit_ready=cycle_submit_ready,
        metadata={"admission_blocked": True} if admission_blocked else None,
    )
    append_workflow_journal_event_fn(
        context.root,
        event_type="worker_cycle_started",
        worker_session_id=session_id,
        metadata={
            "cycle_started_at": cycle_started_at,
            "refresh_registry": bool(context.refresh_registry),
            "submit_ready": cycle_submit_ready,
            "requested_submit_ready": requested_submit_ready,
            "admission_blocked": admission_blocked,
        },
    )
    return _WorkflowCycle(
        root=context.root,
        cycle_started_at=cycle_started_at,
        session_id=session_id,
        requested_submit_ready=requested_submit_ready,
        cycle_submit_ready=cycle_submit_ready,
        admission_blocked=admission_blocked,
        lease_expires_at=lease_expires_at,
    )


def finish_workflow_cycle(
    *,
    cycle: _WorkflowCycle,
    discovered_count: int,
    progress: _WorkflowCycleProgress,
    interval_seconds: float | None,
    now_utc_iso_fn: Callable[[], str],
    write_workflow_worker_state_fn: Callable[..., Any],
    append_workflow_journal_event_fn: Callable[..., Any],
) -> str:
    cycle_finished_at = now_utc_iso_fn()
    finished_metadata = {
        "discovered_count": discovered_count,
        "advanced_count": progress.advanced_count,
        "skipped_count": progress.skipped_count,
        "failed_count": progress.failed_count,
    }
    if cycle.admission_blocked:
        finished_metadata["admission_blocked"] = True
    write_workflow_worker_state_fn(
        cycle.root,
        worker_session_id=cycle.session_id,
        status="idle",
        workflow_root_path=cycle.root,
        last_cycle_started_at=cycle.cycle_started_at,
        last_cycle_finished_at=cycle_finished_at,
        last_heartbeat_at=cycle_finished_at,
        lease_expires_at=cycle.lease_expires_at,
        interval_seconds=interval_seconds,
        submit_ready=cycle.cycle_submit_ready,
        metadata=finished_metadata,
    )
    append_workflow_journal_event_fn(
        cycle.root,
        event_type="worker_cycle_finished",
        worker_session_id=cycle.session_id,
        metadata={
            "cycle_started_at": cycle.cycle_started_at,
            "cycle_finished_at": cycle_finished_at,
            "discovered_count": discovered_count,
            "advanced_count": progress.advanced_count,
            "skipped_count": progress.skipped_count,
            "failed_count": progress.failed_count,
            "admission_blocked": cycle.admission_blocked,
        },
    )
    return cycle_finished_at


def _safe_workflow_summary(
    workspace_dir: str | Path,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return workflow_summary(workspace_dir, payload=payload)
    except (FileNotFoundError, ValueError, TypeError):
        return {}


def _append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    _append_summary_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        append_fn=append_stage_transition_events,
        payloads_kwarg="stage_transition_event_payloads_fn",
        payloads_fn=stage_transition_event_payloads,
    )


def _append_summary_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
    append_fn: Any,
    payloads_kwarg: str,
    payloads_fn: Any,
) -> None:
    append_fn(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        **{
            payloads_kwarg: payloads_fn,
            "append_workflow_journal_event_fn": append_workflow_journal_event,
        },
    )


def _append_phase_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    _append_summary_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        append_fn=append_phase_transition_events,
        payloads_kwarg="phase_transition_event_payloads_fn",
        payloads_fn=phase_transition_event_payloads,
    )


def workflow_worker_lock_path(workflow_root: str | Path) -> Path:
    return Path(workflow_root).expanduser().resolve() / WORKFLOW_WORKER_LOCK_NAME


def _workflow_is_terminal_status(status: Any) -> bool:
    return _runtime_common.normalize_text(status).lower() in TERMINAL_WORKFLOW_STATUSES


def _workflow_needs_terminal_sync(workspace_dir: str | Path) -> bool:
    return workflow_needs_terminal_sync(
        workspace_dir,
        load_workflow_payload_fn=load_workflow_payload,
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
        normalize_text_fn=_runtime_common.normalize_text,
    )


def _workflow_needs_terminal_child_sync(record: Any, *, previous_status: str) -> bool:
    return _workflow_is_terminal_status(previous_status) and _workflow_needs_terminal_sync(
        record.workspace_dir
    )


def _start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
) -> _WorkflowCycle:
    def cycle_submission_has_capacity(*config_paths: str | Path | None) -> bool:
        def submission_limit(config_path: str | Path) -> int | None:
            return submission_admission_limit_from_config(
                config_path,
                positive_int_fn=_runtime_common.positive_int,
            )

        def submission_has_capacity(config_path: str | Path) -> bool | None:
            return submission_admission_has_capacity(
                config_path,
                submission_admission_limit_from_config_fn=submission_limit,
                active_slot_count_fn=active_slot_count,
            )

        return workflow_submission_has_capacity(
            *config_paths,
            submission_admission_has_capacity_fn=submission_has_capacity,
            normalize_text_fn=_runtime_common.normalize_text,
        )

    return start_workflow_cycle(
        context=context,
        now_utc_iso_fn=now_utc_iso,
        timestamped_token_fn=timestamped_token,
        workflow_submission_has_capacity_fn=cycle_submission_has_capacity,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        workflow_lease_expires_at_fn=workflow_lease_expires_at,
    )


def _advance_workflow_record(
    *,
    cycle: _WorkflowCycle,
    record: Any,
    options: WorkflowEngineOptions,
) -> tuple[str, WorkflowAdvanceResult]:
    previous_status = _runtime_common.normalize_text(record.status).lower()
    terminal_sync = _workflow_needs_terminal_child_sync(
        record,
        previous_status=previous_status,
    )
    if _workflow_is_terminal_status(previous_status) and not terminal_sync:
        return "skipped", workflow_skipped_terminal_result(
            record,
            previous_status=previous_status,
        )

    previous_summary = _safe_workflow_summary(record.workspace_dir)
    try:
        payload = advance_workflow(
            target=record.workflow_id,
            workflow_root=cycle.root,
            engine_options=options,
            submit_ready=False if terminal_sync else cycle.cycle_submit_ready,
        )
    except Exception as exc:
        reason = f"terminal_child_sync_failed: {exc}" if terminal_sync else str(exc)
        append_workflow_advance_failed_event(
            cycle.root,
            previous_status=previous_status,
            reason=reason,
            worker_session_id=cycle.session_id,
            record=record,
            append_workflow_journal_event_fn=append_workflow_journal_event,
        )
        return "failed", workflow_advance_failed_result(
            record,
            previous_status=previous_status,
            reason=reason,
        )

    status = _runtime_common.normalize_text(payload.get("status")).lower()
    current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
    reason = "terminal_child_sync" if terminal_sync else ""
    append_workflow_advanced_events(
        cycle.root,
        record,
        payload,
        previous_status=previous_status,
        previous_summary=previous_summary,
        current_summary=current_summary,
        worker_session_id=cycle.session_id,
        reason=reason,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        append_phase_transition_events_fn=_append_phase_transition_events,
        append_stage_transition_events_fn=_append_stage_transition_events,
        normalize_text_fn=_runtime_common.normalize_text,
    )
    return "advanced", workflow_advanced_result(
        record,
        payload,
        previous_status=previous_status,
        status=status,
        reason=reason,
        normalize_text_fn=_runtime_common.normalize_text,
    )


def _advance_workflow_records(
    *,
    cycle: _WorkflowCycle,
    records: list[Any],
    options: WorkflowEngineOptions,
) -> _WorkflowCycleProgress:
    workflow_results: list[WorkflowAdvanceResult] = []
    advanced_count = 0
    skipped_count = 0
    failed_count = 0
    for record in records:
        outcome, result = _advance_workflow_record(cycle=cycle, record=record, options=options)
        workflow_results.append(result)
        if outcome == "advanced":
            advanced_count += 1
        elif outcome == "skipped":
            skipped_count += 1
        elif outcome == "failed":
            failed_count += 1
    return _WorkflowCycleProgress(
        workflow_results=workflow_results,
        advanced_count=advanced_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
    )


def _finish_workflow_cycle(
    *,
    cycle: _WorkflowCycle,
    discovered_count: int,
    progress: _WorkflowCycleProgress,
    interval_seconds: float | None,
) -> str:
    return finish_workflow_cycle(
        cycle=cycle,
        discovered_count=discovered_count,
        progress=progress,
        interval_seconds=interval_seconds,
        now_utc_iso_fn=now_utc_iso,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
    )


def _workflow_registry_records(context: WorkflowRuntimeContext) -> list[Any]:
    if context.refresh_registry:
        return reindex_workflow_registry(context.root)
    return list_workflow_registry(context.root)


def _workflow_registry_cycle_payload(
    *,
    context: WorkflowRuntimeContext,
    request: WorkflowRegistryAdvanceRequest,
    cycle: _WorkflowCycle,
    records: list[Any],
    progress: _WorkflowCycleProgress,
    cycle_finished_at: str,
) -> WorkflowRegistryCyclePayload:
    return {
        "workflow_root": str(context.root),
        "worker_session_id": cycle.session_id,
        "cycle_started_at": cycle.cycle_started_at,
        "cycle_finished_at": cycle_finished_at,
        "refresh_registry": bool(request.refresh_registry),
        "submit_ready": cycle.cycle_submit_ready,
        "requested_submit_ready": cycle.requested_submit_ready,
        "admission_blocked": cycle.admission_blocked,
        "discovered_count": len(records),
        "advanced_count": progress.advanced_count,
        "skipped_count": progress.skipped_count,
        "failed_count": progress.failed_count,
        "workflow_results": progress.workflow_results,
    }


def advance_workflow_registry_once(
    *,
    workflow_root: str | Path,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    submit_ready: bool = True,
    refresh_registry: bool = False,
    worker_session_id: str = "",
    interval_seconds: float | None = None,
    lease_seconds: float = 60.0,
) -> WorkflowRegistryCyclePayload:
    request = WorkflowRegistryAdvanceRequest.from_values(
        workflow_root=workflow_root,
        crest_config=crest_config,
        xtb_config=xtb_config,
        orca_config=orca_config,
        orca_repo_root=orca_repo_root,
        submit_ready=submit_ready,
        refresh_registry=refresh_registry,
        worker_session_id=worker_session_id,
        interval_seconds=interval_seconds,
        lease_seconds=lease_seconds,
    )
    return _advance_workflow_registry_request(request)


def _advance_workflow_registry_request(
    request: WorkflowRegistryAdvanceRequest,
) -> WorkflowRegistryCyclePayload:
    runtime_context = request.runtime_context()
    cycle = _start_workflow_cycle(context=runtime_context)
    records = _workflow_registry_records(runtime_context)
    progress = _advance_workflow_records(
        cycle=cycle,
        records=records,
        options=request.options,
    )
    cycle_finished_at = _finish_workflow_cycle(
        cycle=cycle,
        discovered_count=len(records),
        progress=progress,
        interval_seconds=request.interval_seconds,
    )
    return _workflow_registry_cycle_payload(
        context=runtime_context,
        request=request,
        cycle=cycle,
        records=records,
        progress=progress,
        cycle_finished_at=cycle_finished_at,
    )


__all__ = [
    "TERMINAL_WORKFLOW_STATUSES",
    "WorkflowRegistryAdvanceRequest",
    "WORKFLOW_WORKER_LOCK_NAME",
    "WorkflowRuntimeContext",
    "advance_workflow_registry_once",
    "workflow_worker_lock_path",
]
