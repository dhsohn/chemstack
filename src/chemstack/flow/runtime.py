from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.utils import now_utc_iso, timestamped_token

from .orchestration import advance_workflow
from .registry import (
    append_workflow_journal_event,
    list_workflow_registry,
    reindex_workflow_registry,
    write_workflow_worker_state,
)
from .state import load_workflow_payload, workflow_has_active_downstream

WORKFLOW_WORKER_LOCK_NAME = "workflow_worker.lock"
TERMINAL_WORKFLOW_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
    }
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
        if isinstance(task, dict) and _normalize_text(task.get("status")).lower() in active_statuses:
            return True
    return workflow_has_active_downstream(payload)


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
    cycle_started_at = now_utc_iso()
    session_id = _normalize_text(worker_session_id) or timestamped_token("wf_worker")
    lease_expires_at = ""
    if lease_seconds > 0:
        try:
            from datetime import datetime, timedelta, timezone

            lease_expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=float(lease_seconds))
            ).isoformat()
        except Exception:
            lease_expires_at = ""
    write_workflow_worker_state(
        root,
        worker_session_id=session_id,
        status="running",
        workflow_root_path=root,
        last_cycle_started_at=cycle_started_at,
        last_heartbeat_at=cycle_started_at,
        lease_expires_at=lease_expires_at,
        interval_seconds=interval_seconds,
        submit_ready=bool(submit_ready),
    )
    append_workflow_journal_event(
        root,
        event_type="worker_cycle_started",
        worker_session_id=session_id,
        metadata={
            "cycle_started_at": cycle_started_at,
            "refresh_registry": bool(refresh_registry),
            "submit_ready": bool(submit_ready),
        },
    )
    records = reindex_workflow_registry(root) if refresh_registry else list_workflow_registry(root)

    workflow_results: list[dict[str, Any]] = []
    advanced_count = 0
    skipped_count = 0
    failed_count = 0

    for record in records:
        previous_status = _normalize_text(record.status).lower()
        if previous_status in TERMINAL_WORKFLOW_STATUSES:
            if _workflow_needs_terminal_sync(record.workspace_dir):
                try:
                    payload = advance_workflow(
                        target=record.workflow_id,
                        workflow_root=root,
                        crest_auto_config=crest_auto_config,
                        crest_auto_executable=crest_auto_executable,
                        crest_auto_repo_root=crest_auto_repo_root,
                        xtb_auto_config=xtb_auto_config,
                        xtb_auto_executable=xtb_auto_executable,
                        xtb_auto_repo_root=xtb_auto_repo_root,
                        orca_auto_config=orca_auto_config,
                        orca_auto_executable=orca_auto_executable,
                        orca_auto_repo_root=orca_auto_repo_root,
                        submit_ready=False,
                    )
                except Exception as exc:
                    failed_count += 1
                    workflow_results.append(
                        {
                            "workflow_id": record.workflow_id,
                            "template_name": record.template_name,
                            "previous_status": previous_status,
                            "status": "advance_failed",
                            "advanced": False,
                            "reason": f"terminal_child_sync_failed: {exc}",
                            "stage_count": record.stage_count,
                        }
                    )
                    append_workflow_journal_event(
                        root,
                        event_type="workflow_advance_failed",
                        workflow_id=record.workflow_id,
                        template_name=record.template_name,
                        previous_status=previous_status,
                        status="advance_failed",
                        reason=f"terminal_child_sync_failed: {exc}",
                        worker_session_id=session_id,
                    )
                    continue
                status = _normalize_text(payload.get("status")).lower()
                advanced_count += 1
                workflow_results.append(
                    {
                        "workflow_id": _normalize_text(payload.get("workflow_id")) or record.workflow_id,
                        "template_name": _normalize_text(payload.get("template_name")) or record.template_name,
                        "previous_status": previous_status,
                        "status": status,
                        "advanced": True,
                        "changed": status != previous_status,
                        "reason": "terminal_child_sync",
                        "stage_count": len(payload.get("stages", [])) if isinstance(payload.get("stages"), list) else record.stage_count,
                    }
                )
                if status != previous_status:
                    append_workflow_journal_event(
                        root,
                        event_type="workflow_status_changed",
                        workflow_id=_normalize_text(payload.get("workflow_id")) or record.workflow_id,
                        template_name=_normalize_text(payload.get("template_name")) or record.template_name,
                        previous_status=previous_status,
                        status=status,
                        reason="terminal_child_sync",
                        worker_session_id=session_id,
                    )
                continue
            skipped_count += 1
            workflow_results.append(
                {
                    "workflow_id": record.workflow_id,
                    "template_name": record.template_name,
                    "previous_status": previous_status,
                    "status": previous_status,
                    "advanced": False,
                    "reason": "terminal_status",
                    "stage_count": record.stage_count,
                }
            )
            continue
        try:
            payload = advance_workflow(
                target=record.workflow_id,
                workflow_root=root,
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
            )
        except Exception as exc:
            failed_count += 1
            workflow_results.append(
                {
                    "workflow_id": record.workflow_id,
                    "template_name": record.template_name,
                    "previous_status": previous_status,
                    "status": "advance_failed",
                    "advanced": False,
                    "reason": str(exc),
                    "stage_count": record.stage_count,
                }
            )
            append_workflow_journal_event(
                root,
                event_type="workflow_advance_failed",
                workflow_id=record.workflow_id,
                template_name=record.template_name,
                previous_status=previous_status,
                status="advance_failed",
                reason=str(exc),
                worker_session_id=session_id,
            )
            continue

        status = _normalize_text(payload.get("status")).lower()
        advanced_count += 1
        workflow_results.append(
            {
                "workflow_id": _normalize_text(payload.get("workflow_id")) or record.workflow_id,
                "template_name": _normalize_text(payload.get("template_name")) or record.template_name,
                "previous_status": previous_status,
                "status": status,
                "advanced": True,
                "changed": status != previous_status,
                "stage_count": len(payload.get("stages", [])) if isinstance(payload.get("stages"), list) else record.stage_count,
            }
        )
        if status != previous_status:
            append_workflow_journal_event(
                root,
                event_type="workflow_status_changed",
                workflow_id=_normalize_text(payload.get("workflow_id")) or record.workflow_id,
                template_name=_normalize_text(payload.get("template_name")) or record.template_name,
                previous_status=previous_status,
                status=status,
                worker_session_id=session_id,
            )

    cycle_finished_at = now_utc_iso()
    write_workflow_worker_state(
        root,
        worker_session_id=session_id,
        status="idle",
        workflow_root_path=root,
        last_cycle_started_at=cycle_started_at,
        last_cycle_finished_at=cycle_finished_at,
        last_heartbeat_at=cycle_finished_at,
        lease_expires_at=lease_expires_at,
        interval_seconds=interval_seconds,
        submit_ready=bool(submit_ready),
        metadata={
            "discovered_count": len(records),
            "advanced_count": advanced_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        },
    )
    append_workflow_journal_event(
        root,
        event_type="worker_cycle_finished",
        worker_session_id=session_id,
        metadata={
            "cycle_started_at": cycle_started_at,
            "cycle_finished_at": cycle_finished_at,
            "discovered_count": len(records),
            "advanced_count": advanced_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
        },
    )
    return {
        "workflow_root": str(root),
        "worker_session_id": session_id,
        "cycle_started_at": cycle_started_at,
        "cycle_finished_at": cycle_finished_at,
        "refresh_registry": bool(refresh_registry),
        "submit_ready": bool(submit_ready),
        "discovered_count": len(records),
        "advanced_count": advanced_count,
        "skipped_count": skipped_count,
        "failed_count": failed_count,
        "workflow_results": workflow_results,
    }


__all__ = [
    "TERMINAL_WORKFLOW_STATUSES",
    "WORKFLOW_WORKER_LOCK_NAME",
    "advance_workflow_registry_once",
    "workflow_worker_lock_path",
]
