from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.admission import active_slot_count
from chemstack.core.utils import now_utc_iso, timestamped_token
from chemstack.flow.submitters.common import sibling_runtime_paths

from .orchestration import advance_workflow
from . import (
    _runtime_capacity,
    _runtime_common,
    _runtime_cycle,
    _runtime_results,
    _runtime_stage_events,
    _runtime_terminal_sync,
    _runtime_workflow_events,
)
from ._workflow_phases import phase_transition_event_payloads
from .engine_options import WorkflowEngineOptions
from .registry import (
    append_workflow_journal_event,
    list_workflow_registry,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .state import load_workflow_payload, workflow_has_active_downstream, workflow_summary

WORKFLOW_WORKER_LOCK_NAME = "workflow_worker.lock"
TERMINAL_WORKFLOW_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
    }
)


@dataclass(frozen=True)
class WorkflowRuntimeContext:
    root: Path
    options: WorkflowEngineOptions
    submit_ready: bool = True
    refresh_registry: bool = False
    worker_session_id: str = ""
    interval_seconds: float | None = None
    lease_seconds: float = 60.0


@dataclass(frozen=True)
class WorkflowRegistryAdvanceRequest:
    workflow_root: str | Path
    options: WorkflowEngineOptions
    submit_ready: bool = True
    refresh_registry: bool = False
    worker_session_id: str = ""
    interval_seconds: float | None = None
    lease_seconds: float = 60.0

    @classmethod
    def from_values(
        cls,
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
    ) -> WorkflowRegistryAdvanceRequest:
        return cls(
            workflow_root=workflow_root,
            options=WorkflowEngineOptions.from_values(
                crest_config=crest_config,
                xtb_config=xtb_config,
                orca_config=orca_config,
                orca_repo_root=orca_repo_root,
            ),
            submit_ready=submit_ready,
            refresh_registry=refresh_registry,
            worker_session_id=worker_session_id,
            interval_seconds=interval_seconds,
            lease_seconds=lease_seconds,
        )

    def runtime_context(self) -> WorkflowRuntimeContext:
        return WorkflowRuntimeContext(
            root=Path(self.workflow_root).expanduser().resolve(),
            options=self.options,
            worker_session_id=self.worker_session_id,
            submit_ready=self.submit_ready,
            refresh_registry=self.refresh_registry,
            interval_seconds=self.interval_seconds,
            lease_seconds=self.lease_seconds,
        )


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
        append_fn=_runtime_workflow_events.append_stage_transition_events,
        payloads_kwarg="stage_transition_event_payloads_fn",
        payloads_fn=_runtime_stage_events.stage_transition_event_payloads,
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
        append_fn=_runtime_workflow_events.append_phase_transition_events,
        payloads_kwarg="phase_transition_event_payloads_fn",
        payloads_fn=phase_transition_event_payloads,
    )


def workflow_worker_lock_path(workflow_root: str | Path) -> Path:
    return Path(workflow_root).expanduser().resolve() / WORKFLOW_WORKER_LOCK_NAME


def _workflow_is_terminal_status(status: Any) -> bool:
    return _runtime_common.normalize_text(status).lower() in TERMINAL_WORKFLOW_STATUSES


def _workflow_needs_terminal_sync(workspace_dir: str | Path) -> bool:
    return _runtime_terminal_sync.workflow_needs_terminal_sync(
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
) -> _runtime_cycle._WorkflowCycle:
    def workflow_submission_has_capacity(*config_paths: str | Path | None) -> bool:
        def submission_limit(config_path: str | Path) -> int | None:
            return _runtime_capacity.submission_admission_limit_from_config(
                config_path,
                positive_int_fn=_runtime_common.positive_int,
            )

        def submission_has_capacity(config_path: str | Path) -> bool | None:
            return _runtime_capacity.submission_admission_has_capacity(
                config_path,
                submission_admission_limit_from_config_fn=submission_limit,
                active_slot_count_fn=active_slot_count,
                sibling_runtime_paths_fn=sibling_runtime_paths,
            )

        return _runtime_capacity.workflow_submission_has_capacity(
            *config_paths,
            submission_admission_has_capacity_fn=submission_has_capacity,
            normalize_text_fn=_runtime_common.normalize_text,
        )

    return _runtime_cycle.start_workflow_cycle(
        context=context,
        now_utc_iso_fn=now_utc_iso,
        timestamped_token_fn=timestamped_token,
        workflow_submission_has_capacity_fn=workflow_submission_has_capacity,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        workflow_lease_expires_at_fn=_runtime_cycle.workflow_lease_expires_at,
    )


def _advance_workflow_record(
    *,
    cycle: _runtime_cycle._WorkflowCycle,
    record: Any,
    options: WorkflowEngineOptions,
) -> tuple[str, dict[str, Any]]:
    previous_status = _runtime_common.normalize_text(record.status).lower()
    terminal_sync = _workflow_needs_terminal_child_sync(
        record,
        previous_status=previous_status,
    )
    if _workflow_is_terminal_status(previous_status) and not terminal_sync:
        return "skipped", _runtime_results.workflow_skipped_terminal_result(
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
        _runtime_workflow_events.append_workflow_advance_failed_event(
            cycle.root,
            previous_status=previous_status,
            reason=reason,
            worker_session_id=cycle.session_id,
            record=record,
            append_workflow_journal_event_fn=append_workflow_journal_event,
        )
        return "failed", _runtime_results.workflow_advance_failed_result(
            record,
            previous_status=previous_status,
            reason=reason,
        )

    status = _runtime_common.normalize_text(payload.get("status")).lower()
    current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
    reason = "terminal_child_sync" if terminal_sync else ""
    _runtime_workflow_events.append_workflow_advanced_events(
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
    return "advanced", _runtime_results.workflow_advanced_result(
        record,
        payload,
        previous_status=previous_status,
        status=status,
        reason=reason,
        normalize_text_fn=_runtime_common.normalize_text,
    )


def _advance_workflow_records(
    *,
    cycle: _runtime_cycle._WorkflowCycle,
    records: list[Any],
    options: WorkflowEngineOptions,
) -> _runtime_cycle._WorkflowCycleProgress:
    workflow_results: list[dict[str, Any]] = []
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
    return _runtime_cycle._WorkflowCycleProgress(
        workflow_results=workflow_results,
        advanced_count=advanced_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
    )


def _finish_workflow_cycle(
    *,
    cycle: _runtime_cycle._WorkflowCycle,
    discovered_count: int,
    progress: _runtime_cycle._WorkflowCycleProgress,
    interval_seconds: float | None,
) -> str:
    return _runtime_cycle.finish_workflow_cycle(
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
    cycle: _runtime_cycle._WorkflowCycle,
    records: list[Any],
    progress: _runtime_cycle._WorkflowCycleProgress,
    cycle_finished_at: str,
) -> dict[str, Any]:
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
) -> dict[str, Any]:
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


def _advance_workflow_registry_request(request: WorkflowRegistryAdvanceRequest) -> dict[str, Any]:
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
