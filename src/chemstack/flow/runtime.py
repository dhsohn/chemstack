from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
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
        crest_auto_config: str | None = None,
        crest_auto_executable: str = "crest_auto",
        crest_auto_repo_root: str | None = None,
        xtb_auto_config: str | None = None,
        xtb_auto_executable: str = "xtb_auto",
        xtb_auto_repo_root: str | None = None,
        orca_auto_config: str | None = None,
        orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
        orca_auto_repo_root: str | None = None,
        submit_ready: bool = True,
        refresh_registry: bool = False,
        worker_session_id: str = "",
        interval_seconds: float | None = None,
        lease_seconds: float = 60.0,
    ) -> WorkflowRegistryAdvanceRequest:
        return cls(
            workflow_root=workflow_root,
            options=WorkflowEngineOptions.from_values(
                crest_auto_config=crest_auto_config,
                crest_auto_executable=crest_auto_executable,
                crest_auto_repo_root=crest_auto_repo_root,
                xtb_auto_config=xtb_auto_config,
                xtb_auto_executable=xtb_auto_executable,
                xtb_auto_repo_root=xtb_auto_repo_root,
                orca_auto_config=orca_auto_config,
                orca_auto_executable=orca_auto_executable,
                orca_auto_repo_root=orca_auto_repo_root,
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


_WorkflowCycle = _runtime_cycle._WorkflowCycle
_WorkflowCycleProgress = _runtime_cycle._WorkflowCycleProgress


def _normalize_text(value: Any) -> str:
    return _runtime_common.normalize_text(value)


def _safe_int(value: Any) -> int | None:
    return _runtime_common.safe_int(value)


def _safe_workflow_summary(
    workspace_dir: str | Path,
    *,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return workflow_summary(workspace_dir, payload=payload)
    except (FileNotFoundError, ValueError, TypeError):
        return {}


def _positive_int(value: Any) -> int | None:
    return _runtime_common.positive_int(value)


def _submission_admission_limit_from_config(config_path: str | Path) -> int | None:
    return _runtime_capacity.submission_admission_limit_from_config(
        config_path,
        positive_int_fn=_positive_int,
    )


def _submission_admission_has_capacity(config_path: str | Path) -> bool | None:
    return _runtime_capacity.submission_admission_has_capacity(
        config_path,
        submission_admission_limit_from_config_fn=_submission_admission_limit_from_config,
        active_slot_count_fn=active_slot_count,
        sibling_runtime_paths_fn=sibling_runtime_paths,
    )


def _workflow_submission_has_capacity(*config_paths: str | Path | None) -> bool:
    return _runtime_capacity.workflow_submission_has_capacity(
        *config_paths,
        submission_admission_has_capacity_fn=_submission_admission_has_capacity,
        normalize_text_fn=_normalize_text,
    )


def _stage_key(stage: dict[str, Any], index: int) -> str:
    return _runtime_stage_events.stage_key(stage, index)


def _stage_event_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    return _runtime_stage_events.stage_event_metadata(stage)


def _stage_status_event_type(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
    *,
    suppress_terminal_event: bool,
) -> str:
    return _runtime_stage_events.stage_status_event_type(
        previous_stage,
        current_stage,
        suppress_terminal_event=suppress_terminal_event,
    )


def _stage_handoff_event_type(previous_stage: dict[str, Any], current_stage: dict[str, Any]) -> str:
    return _runtime_stage_events.stage_handoff_event_type(previous_stage, current_stage)


def _stage_transition_context(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
) -> dict[str, str]:
    return _runtime_stage_events.stage_transition_context(previous_stage, current_stage)


def _stage_transition_metadata(
    metadata: dict[str, Any],
    context: dict[str, str],
    *,
    include_handoff: bool,
) -> dict[str, Any]:
    return _runtime_stage_events.stage_transition_metadata(
        metadata,
        context,
        include_handoff=include_handoff,
    )


def _status_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    return _runtime_stage_events.status_transition_event_payload(
        event_type=event_type,
        current_stage=current_stage,
        context=context,
        metadata=metadata,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )


def _handoff_transition_event_payload(
    *,
    event_type: str,
    current_stage: dict[str, Any],
    context: dict[str, str],
    metadata: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> dict[str, Any]:
    return _runtime_stage_events.handoff_transition_event_payload(
        event_type=event_type,
        current_stage=current_stage,
        context=context,
        metadata=metadata,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )


def _stage_transition_event_payloads(
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> list[dict[str, Any]]:
    return _runtime_stage_events.stage_transition_event_payloads(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )


def _append_stage_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    _runtime_workflow_events.append_stage_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        stage_transition_event_payloads_fn=_stage_transition_event_payloads,
        append_workflow_journal_event_fn=append_workflow_journal_event,
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
    _runtime_workflow_events.append_phase_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
        phase_transition_event_payloads_fn=phase_transition_event_payloads,
        append_workflow_journal_event_fn=append_workflow_journal_event,
    )


def workflow_worker_lock_path(workflow_root: str | Path) -> Path:
    return Path(workflow_root).expanduser().resolve() / WORKFLOW_WORKER_LOCK_NAME


def _workflow_needs_terminal_sync(workspace_dir: str | Path) -> bool:
    return _runtime_terminal_sync.workflow_needs_terminal_sync(
        workspace_dir,
        load_workflow_payload_fn=load_workflow_payload,
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
        normalize_text_fn=_normalize_text,
    )


def _workflow_advance_failed_result(
    record: Any, *, previous_status: str, reason: str
) -> dict[str, Any]:
    return _runtime_results.workflow_advance_failed_result(
        record,
        previous_status=previous_status,
        reason=reason,
    )


def _workflow_skipped_terminal_result(record: Any, *, previous_status: str) -> dict[str, Any]:
    return _runtime_results.workflow_skipped_terminal_result(
        record,
        previous_status=previous_status,
    )


def _workflow_advanced_result(
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    status: str,
    reason: str = "",
) -> dict[str, Any]:
    return _runtime_results.workflow_advanced_result(
        record,
        payload,
        previous_status=previous_status,
        status=status,
        reason=reason,
        normalize_text_fn=_normalize_text,
    )


def _append_workflow_advance_failed_event(
    workflow_root: str | Path,
    record: Any,
    *,
    previous_status: str,
    reason: str,
    worker_session_id: str,
) -> None:
    _runtime_workflow_events.append_workflow_advance_failed_event(
        workflow_root,
        previous_status=previous_status,
        reason=reason,
        worker_session_id=worker_session_id,
        record=record,
        append_workflow_journal_event_fn=append_workflow_journal_event,
    )


def _append_workflow_advanced_events(
    workflow_root: str | Path,
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    current_summary: dict[str, Any],
    previous_summary: dict[str, Any],
    worker_session_id: str,
    reason: str = "",
) -> None:
    _runtime_workflow_events.append_workflow_advanced_events(
        workflow_root,
        record,
        payload,
        previous_status=previous_status,
        previous_summary=previous_summary,
        current_summary=current_summary,
        worker_session_id=worker_session_id,
        reason=reason,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        append_phase_transition_events_fn=_append_phase_transition_events,
        append_stage_transition_events_fn=_append_stage_transition_events,
        normalize_text_fn=_normalize_text,
    )


def _workflow_lease_expires_at(lease_seconds: float) -> str:
    return _runtime_cycle.workflow_lease_expires_at(lease_seconds)


def _start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
) -> _WorkflowCycle:
    return _runtime_cycle.start_workflow_cycle(
        context=context,
        now_utc_iso_fn=now_utc_iso,
        timestamped_token_fn=timestamped_token,
        workflow_submission_has_capacity_fn=_workflow_submission_has_capacity,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
        workflow_lease_expires_at_fn=_workflow_lease_expires_at,
    )


def _advance_workflow_record(
    *,
    cycle: _WorkflowCycle,
    record: Any,
    options: WorkflowEngineOptions,
) -> tuple[str, dict[str, Any]]:
    previous_status = _normalize_text(record.status).lower()
    terminal_sync = previous_status in TERMINAL_WORKFLOW_STATUSES and _workflow_needs_terminal_sync(
        record.workspace_dir
    )
    if previous_status in TERMINAL_WORKFLOW_STATUSES and not terminal_sync:
        return "skipped", _workflow_skipped_terminal_result(
            record,
            previous_status=previous_status,
        )

    previous_summary = _safe_workflow_summary(record.workspace_dir)
    try:
        payload = advance_workflow(
            target=record.workflow_id,
            workflow_root=cycle.root,
            crest_auto_config=options.crest_auto_config,
            crest_auto_executable=options.crest_auto_executable,
            crest_auto_repo_root=options.crest_auto_repo_root,
            xtb_auto_config=options.xtb_auto_config,
            xtb_auto_executable=options.xtb_auto_executable,
            xtb_auto_repo_root=options.xtb_auto_repo_root,
            orca_auto_config=options.orca_auto_config,
            orca_auto_executable=options.orca_auto_executable,
            orca_auto_repo_root=options.orca_auto_repo_root,
            submit_ready=False if terminal_sync else cycle.cycle_submit_ready,
        )
    except Exception as exc:
        reason = f"terminal_child_sync_failed: {exc}" if terminal_sync else str(exc)
        _append_workflow_advance_failed_event(
            cycle.root,
            record,
            previous_status=previous_status,
            reason=reason,
            worker_session_id=cycle.session_id,
        )
        return "failed", _workflow_advance_failed_result(
            record,
            previous_status=previous_status,
            reason=reason,
        )

    status = _normalize_text(payload.get("status")).lower()
    current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
    reason = "terminal_child_sync" if terminal_sync else ""
    _append_workflow_advanced_events(
        cycle.root,
        record,
        payload,
        previous_status=previous_status,
        previous_summary=previous_summary,
        current_summary=current_summary,
        worker_session_id=cycle.session_id,
        reason=reason,
    )
    return "advanced", _workflow_advanced_result(
        record,
        payload,
        previous_status=previous_status,
        status=status,
        reason=reason,
    )


def _advance_workflow_records(
    *,
    cycle: _WorkflowCycle,
    records: list[Any],
    options: WorkflowEngineOptions,
) -> _WorkflowCycleProgress:
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
    return _runtime_cycle.finish_workflow_cycle(
        cycle=cycle,
        discovered_count=discovered_count,
        progress=progress,
        interval_seconds=interval_seconds,
        now_utc_iso_fn=now_utc_iso,
        write_workflow_worker_state_fn=write_workflow_worker_state,
        append_workflow_journal_event_fn=append_workflow_journal_event,
    )


def advance_workflow_registry_once(
    *,
    workflow_root: str | Path,
    crest_auto_config: str | None = None,
    crest_auto_executable: str = "crest_auto",
    crest_auto_repo_root: str | None = None,
    xtb_auto_config: str | None = None,
    xtb_auto_executable: str = "xtb_auto",
    xtb_auto_repo_root: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
    submit_ready: bool = True,
    refresh_registry: bool = False,
    worker_session_id: str = "",
    interval_seconds: float | None = None,
    lease_seconds: float = 60.0,
) -> dict[str, Any]:
    request = WorkflowRegistryAdvanceRequest.from_values(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        xtb_auto_config=xtb_auto_config,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
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
    records = (
        reindex_workflow_registry(runtime_context.root)
        if request.refresh_registry
        else list_workflow_registry(runtime_context.root)
    )
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
    return {
        "workflow_root": str(runtime_context.root),
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


__all__ = [
    "TERMINAL_WORKFLOW_STATUSES",
    "WorkflowRegistryAdvanceRequest",
    "WORKFLOW_WORKER_LOCK_NAME",
    "WorkflowRuntimeContext",
    "advance_workflow_registry_once",
    "workflow_worker_lock_path",
]
