from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.admission import active_slot_count
from chemstack.core.utils import now_utc_iso, timestamped_token
from chemstack.flow.submitters.common import sibling_runtime_paths

from .orchestration import advance_workflow
from . import _runtime_stage_events
from ._workflow_phases import phase_transition_event_payloads
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
class _WorkflowAdvanceOptions:
    crest_auto_config: str | None
    crest_auto_executable: str
    crest_auto_repo_root: str | None
    xtb_auto_config: str | None
    xtb_auto_executable: str
    xtb_auto_repo_root: str | None
    orca_auto_config: str | None
    orca_auto_executable: str
    orca_auto_repo_root: str | None


@dataclass(frozen=True)
class WorkflowRuntimeContext:
    root: Path
    options: _WorkflowAdvanceOptions
    submit_ready: bool = True
    refresh_registry: bool = False
    worker_session_id: str = ""
    interval_seconds: float | None = None
    lease_seconds: float = 60.0


@dataclass(frozen=True)
class _WorkflowCycle:
    root: Path
    cycle_started_at: str
    session_id: str
    requested_submit_ready: bool
    cycle_submit_ready: bool
    admission_blocked: bool
    lease_expires_at: str


@dataclass(frozen=True)
class _WorkflowCycleProgress:
    workflow_results: list[dict[str, Any]]
    advanced_count: int
    skipped_count: int
    failed_count: int


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _submission_admission_limit_from_config(config_path: str | Path) -> int | None:
    try:
        path = Path(config_path).expanduser().resolve()
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None

    scheduler = raw.get("scheduler")
    if not isinstance(scheduler, dict):
        scheduler = {}
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        runtime = {}

    for candidate in (
        scheduler.get("max_active_simulations"),
        scheduler.get("admission_limit"),
        runtime.get("admission_limit"),
        runtime.get("admission_max_concurrent"),
        runtime.get("max_concurrent"),
    ):
        parsed = _positive_int(candidate)
        if parsed is not None:
            return parsed
    return None


def _submission_admission_has_capacity(config_path: str | Path) -> bool | None:
    limit = _submission_admission_limit_from_config(config_path)
    if limit is None:
        return None
    admission_root: Path | None = None
    for engine in (None, "xtb", "crest", "orca"):
        try:
            runtime_paths = sibling_runtime_paths(str(config_path), engine=engine)
        except Exception:
            continue
        candidate = runtime_paths.get("admission_root")
        if isinstance(candidate, Path):
            admission_root = candidate
            break
    if not isinstance(admission_root, Path):
        return None
    try:
        return active_slot_count(admission_root) < limit
    except Exception:
        return None


def _workflow_submission_has_capacity(*config_paths: str | Path | None) -> bool:
    for config_path in config_paths:
        config_text = _normalize_text(config_path)
        if not config_text:
            continue
        has_capacity = _submission_admission_has_capacity(config_text)
        if has_capacity is not None:
            return has_capacity
    return True


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
    for payload in _stage_transition_event_payloads(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event(workflow_root, **payload)


def _append_phase_transition_events(
    workflow_root: str | Path,
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> None:
    for payload in phase_transition_event_payloads(
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    ):
        append_workflow_journal_event(workflow_root, **payload)


def workflow_worker_lock_path(workflow_root: str | Path) -> Path:
    return Path(workflow_root).expanduser().resolve() / WORKFLOW_WORKER_LOCK_NAME


def _workflow_needs_terminal_sync(workspace_dir: str | Path) -> bool:
    try:
        payload = load_workflow_payload(workspace_dir)
    except (FileNotFoundError, ValueError):
        return False
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and bool(metadata.get("final_child_sync_pending")):
        return True
    active_statuses = {"queued", "running", "submitted", "cancel_requested"}
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        if _normalize_text(raw_stage.get("status")).lower() in active_statuses:
            return True
        task = raw_stage.get("task")
        if (
            isinstance(task, dict)
            and _normalize_text(task.get("status")).lower() in active_statuses
        ):
            return True
    return workflow_has_active_downstream(payload)


def _workflow_advance_failed_result(
    record: Any, *, previous_status: str, reason: str
) -> dict[str, Any]:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": "advance_failed",
        "advanced": False,
        "reason": reason,
        "stage_count": record.stage_count,
    }


def _workflow_skipped_terminal_result(record: Any, *, previous_status: str) -> dict[str, Any]:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": previous_status,
        "advanced": False,
        "reason": "terminal_status",
        "stage_count": record.stage_count,
    }


def _workflow_advanced_result(
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    status: str,
    reason: str = "",
) -> dict[str, Any]:
    result = {
        "workflow_id": _normalize_text(payload.get("workflow_id")) or record.workflow_id,
        "template_name": _normalize_text(payload.get("template_name")) or record.template_name,
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


def _append_workflow_advance_failed_event(
    workflow_root: str | Path,
    record: Any,
    *,
    previous_status: str,
    reason: str,
    worker_session_id: str,
) -> None:
    append_workflow_journal_event(
        workflow_root,
        event_type="workflow_advance_failed",
        workflow_id=record.workflow_id,
        template_name=record.template_name,
        previous_status=previous_status,
        status="advance_failed",
        reason=reason,
        worker_session_id=worker_session_id,
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
    status = _normalize_text(payload.get("status")).lower()
    workflow_id = _normalize_text(payload.get("workflow_id")) or record.workflow_id
    template_name = _normalize_text(payload.get("template_name")) or record.template_name
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
        append_workflow_journal_event(workflow_root, **event_kwargs)
    _append_phase_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )
    _append_stage_transition_events(
        workflow_root,
        previous_summary=previous_summary,
        current_summary=current_summary,
        workflow_id=workflow_id,
        template_name=template_name,
        worker_session_id=worker_session_id,
    )


def _workflow_lease_expires_at(lease_seconds: float) -> str:
    if lease_seconds <= 0:
        return ""
    try:
        from datetime import datetime, timedelta, timezone

        return (datetime.now(timezone.utc) + timedelta(seconds=float(lease_seconds))).isoformat()
    except Exception:
        return ""


def _start_workflow_cycle(
    *,
    context: WorkflowRuntimeContext,
) -> _WorkflowCycle:
    cycle_started_at = now_utc_iso()
    session_id = _normalize_text(context.worker_session_id) or timestamped_token("wf_worker")
    requested_submit_ready = bool(context.submit_ready)
    cycle_submit_ready = requested_submit_ready and _workflow_submission_has_capacity(
        context.options.crest_auto_config,
        context.options.xtb_auto_config,
        context.options.orca_auto_config,
    )
    admission_blocked = requested_submit_ready and not cycle_submit_ready
    lease_expires_at = _workflow_lease_expires_at(context.lease_seconds)

    write_workflow_worker_state(
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
    append_workflow_journal_event(
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


def _advance_workflow_record(
    *,
    cycle: _WorkflowCycle,
    record: Any,
    options: _WorkflowAdvanceOptions,
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
    options: _WorkflowAdvanceOptions,
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
    cycle_finished_at = now_utc_iso()
    finished_metadata = {
        "discovered_count": discovered_count,
        "advanced_count": progress.advanced_count,
        "skipped_count": progress.skipped_count,
        "failed_count": progress.failed_count,
    }
    if cycle.admission_blocked:
        finished_metadata["admission_blocked"] = True
    write_workflow_worker_state(
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
    append_workflow_journal_event(
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
    root = Path(workflow_root).expanduser().resolve()
    options = _WorkflowAdvanceOptions(
        crest_auto_config=crest_auto_config,
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        xtb_auto_config=xtb_auto_config,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )
    runtime_context = WorkflowRuntimeContext(
        root=root,
        options=options,
        worker_session_id=worker_session_id,
        submit_ready=submit_ready,
        refresh_registry=refresh_registry,
        interval_seconds=interval_seconds,
        lease_seconds=lease_seconds,
    )
    cycle = _start_workflow_cycle(context=runtime_context)
    records = reindex_workflow_registry(root) if refresh_registry else list_workflow_registry(root)
    progress = _advance_workflow_records(
        cycle=cycle,
        records=records,
        options=options,
    )
    cycle_finished_at = _finish_workflow_cycle(
        cycle=cycle,
        discovered_count=len(records),
        progress=progress,
        interval_seconds=interval_seconds,
    )
    return {
        "workflow_root": str(root),
        "worker_session_id": cycle.session_id,
        "cycle_started_at": cycle.cycle_started_at,
        "cycle_finished_at": cycle_finished_at,
        "refresh_registry": bool(refresh_registry),
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
    "WORKFLOW_WORKER_LOCK_NAME",
    "WorkflowRuntimeContext",
    "advance_workflow_registry_once",
    "workflow_worker_lock_path",
]
