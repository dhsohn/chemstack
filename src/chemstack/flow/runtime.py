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


def _stage_key(stage: dict[str, Any], index: int) -> str:
    stage_id = _normalize_text(stage.get("stage_id"))
    if stage_id:
        return stage_id
    return f"index:{index}"


def _stage_event_metadata(stage: dict[str, Any]) -> dict[str, Any]:
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
        text = _normalize_text(stage.get(field))
        if text:
            metadata[field] = text
    for field in int_fields:
        value = _safe_int(stage.get(field))
        if value is not None:
            metadata[field] = value
    return metadata


def _stage_status_event_type(
    previous_stage: dict[str, Any],
    current_stage: dict[str, Any],
    *,
    suppress_terminal_event: bool,
) -> str:
    previous_status = _normalize_text(previous_stage.get("status")).lower()
    current_status = _normalize_text(current_stage.get("status")).lower()
    if not current_status or current_status == previous_status:
        return ""
    if current_status == "queued":
        return "workflow_stage_submitted"
    if suppress_terminal_event:
        return ""
    if current_status == "completed":
        return "workflow_stage_completed"
    if current_status in {"failed", "submission_failed", "cancel_failed"}:
        return "workflow_stage_failed"
    if current_status == "cancelled":
        return "workflow_stage_cancelled"
    return ""


def _stage_handoff_event_type(previous_stage: dict[str, Any], current_stage: dict[str, Any]) -> str:
    engine = _normalize_text(current_stage.get("engine") or previous_stage.get("engine")).lower()
    task_kind = _normalize_text(current_stage.get("task_kind") or previous_stage.get("task_kind")).lower()
    if engine != "xtb" or task_kind != "path_search":
        return ""
    previous_handoff = _normalize_text(previous_stage.get("reaction_handoff_status")).lower()
    current_handoff = _normalize_text(current_stage.get("reaction_handoff_status")).lower()
    if not current_handoff or current_handoff == previous_handoff:
        return ""
    if current_handoff == "ready":
        return "workflow_stage_handoff_ready"
    if current_handoff == "retrying":
        return "workflow_stage_handoff_retrying"
    if current_handoff == "failed":
        return "workflow_stage_handoff_failed"
    return ""


def _stage_transition_event_payloads(
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> list[dict[str, Any]]:
    previous_stages = list(previous_summary.get("stage_summaries", []))
    current_stages = list(current_summary.get("stage_summaries", []))
    previous_by_key = {_stage_key(stage, index): dict(stage) for index, stage in enumerate(previous_stages)}
    event_payloads: list[dict[str, Any]] = []

    for index, raw_stage in enumerate(current_stages):
        current_stage = dict(raw_stage)
        previous_stage = previous_by_key.get(_stage_key(current_stage, index), {})
        handoff_event_type = _stage_handoff_event_type(previous_stage, current_stage)
        status_event_type = _stage_status_event_type(
            previous_stage,
            current_stage,
            suppress_terminal_event=handoff_event_type in {"workflow_stage_handoff_ready", "workflow_stage_handoff_failed"},
        )
        metadata = _stage_event_metadata(current_stage)
        previous_stage_status = _normalize_text(previous_stage.get("status")).lower()
        current_stage_status = _normalize_text(current_stage.get("status")).lower()
        previous_handoff_status = _normalize_text(previous_stage.get("reaction_handoff_status")).lower()
        current_handoff_status = _normalize_text(current_stage.get("reaction_handoff_status")).lower()
        stage_id = _normalize_text(current_stage.get("stage_id") or previous_stage.get("stage_id"))
        engine = _normalize_text(current_stage.get("engine") or previous_stage.get("engine"))
        task_kind = _normalize_text(current_stage.get("task_kind") or previous_stage.get("task_kind"))

        if status_event_type:
            status_metadata = dict(metadata)
            if previous_stage_status:
                status_metadata["previous_stage_status"] = previous_stage_status
            if current_stage_status:
                status_metadata["stage_status"] = current_stage_status
            reason = ""
            if status_event_type in {"workflow_stage_failed", "workflow_stage_cancelled"}:
                reason = _normalize_text(current_stage.get("reason"))
            event_payloads.append(
                {
                    "event_type": status_event_type,
                    "workflow_id": workflow_id,
                    "template_name": template_name,
                    "status": current_stage_status,
                    "previous_status": previous_stage_status,
                    "reason": reason,
                    "worker_session_id": worker_session_id,
                    "stage_id": stage_id,
                    "engine": engine,
                    "task_kind": task_kind,
                    "stage_status": current_stage_status,
                    "previous_stage_status": previous_stage_status,
                    "metadata": status_metadata,
                }
            )

        if handoff_event_type:
            handoff_metadata = dict(metadata)
            if previous_stage_status:
                handoff_metadata["previous_stage_status"] = previous_stage_status
            if current_stage_status:
                handoff_metadata["stage_status"] = current_stage_status
            if previous_handoff_status:
                handoff_metadata["previous_reaction_handoff_status"] = previous_handoff_status
            if current_handoff_status:
                handoff_metadata["reaction_handoff_status"] = current_handoff_status
            event_payloads.append(
                {
                    "event_type": handoff_event_type,
                    "workflow_id": workflow_id,
                    "template_name": template_name,
                    "status": current_handoff_status,
                    "previous_status": previous_handoff_status,
                    "reason": _normalize_text(current_stage.get("reaction_handoff_reason") or current_stage.get("reason")),
                    "worker_session_id": worker_session_id,
                    "stage_id": stage_id,
                    "engine": engine,
                    "task_kind": task_kind,
                    "stage_status": current_stage_status,
                    "previous_stage_status": previous_stage_status,
                    "reaction_handoff_status": current_handoff_status,
                    "previous_reaction_handoff_status": previous_handoff_status,
                    "metadata": handoff_metadata,
                }
            )
    return event_payloads


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
        previous_summary = _safe_workflow_summary(record.workspace_dir)
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
                current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
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
                _append_stage_transition_events(
                    root,
                    previous_summary=previous_summary,
                    current_summary=current_summary,
                    workflow_id=_normalize_text(payload.get("workflow_id")) or record.workflow_id,
                    template_name=_normalize_text(payload.get("template_name")) or record.template_name,
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
        current_summary = _safe_workflow_summary(record.workspace_dir, payload=payload)
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
        _append_stage_transition_events(
            root,
            previous_summary=previous_summary,
            current_summary=current_summary,
            workflow_id=_normalize_text(payload.get("workflow_id")) or record.workflow_id,
            template_name=_normalize_text(payload.get("template_name")) or record.template_name,
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
