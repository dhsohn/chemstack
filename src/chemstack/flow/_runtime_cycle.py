from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from ._runtime_common import normalize_text


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
    context: Any,
    now_utc_iso_fn: Callable[[], str],
    timestamped_token_fn: Callable[[str], str],
    workflow_submission_has_capacity_fn: Callable[..., bool],
    write_workflow_worker_state_fn: Callable[..., Any],
    append_workflow_journal_event_fn: Callable[..., Any],
    workflow_lease_expires_at_fn: Callable[[float], str] = workflow_lease_expires_at,
) -> _WorkflowCycle:
    cycle_started_at = now_utc_iso_fn()
    session_id = normalize_text(context.worker_session_id) or timestamped_token_fn("wf_worker")
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
