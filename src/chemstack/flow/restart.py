from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import is_orca_submitter
from chemstack.core.utils import now_utc_iso

from .registry import append_workflow_journal_event, sync_workflow_registry
from .state import acquire_workflow_lock, load_workflow_payload, workflow_summary, write_workflow_payload
from .workflow_status import WORKFLOW_FAILED_STATUSES

_RESTARTABLE_WORKFLOW_STATUSES = frozenset({*WORKFLOW_FAILED_STATUSES, "cancelled"})
_RESTARTABLE_STAGE_STATUSES = frozenset(
    {
        "failed",
        "cancelled",
        "cancel_failed",
        "submission_failed",
    }
)
_ACTIVE_STAGE_STATUSES = frozenset(
    {
        "queued",
        "running",
        "submitted",
        "cancel_requested",
    }
)
_STALE_STAGE_METADATA_KEYS = frozenset(
    {
        "analyzer_status",
        "cancel_requested",
        "child_job_id",
        "completed_at",
        "latest_known_path",
        "orca_attempts",
        "orca_current_attempt_number",
        "orca_final_result",
        "orca_latest_attempt_number",
        "orca_latest_attempt_status",
        "optimized_xyz_path",
        "organized_output_dir",
        "queue_id",
        "queue_status",
        "reason",
        "run_id",
        "state_status",
        "submission_status",
        "submitted_at",
    }
)
_STALE_TASK_PAYLOAD_KEYS = frozenset(
    {
        "last_out_path",
        "optimized_xyz_path",
        "orca_latest_attempt_inp",
        "orca_latest_attempt_out",
    }
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _stage_task(stage: dict[str, Any]) -> dict[str, Any]:
    task = stage.get("task")
    if isinstance(task, dict):
        return task
    task = {}
    stage["task"] = task
    return task


def _stage_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    metadata = stage.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    metadata = {}
    stage["metadata"] = metadata
    return metadata


def _task_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        return payload
    payload = {}
    task["payload"] = payload
    return payload


def _enqueue_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("enqueue_payload")
    if isinstance(payload, dict):
        return payload
    payload = {}
    task["enqueue_payload"] = payload
    return payload


def _task_is_orca(task: dict[str, Any]) -> bool:
    engine = _normalize_text(task.get("engine")).lower()
    if engine == "orca":
        return True
    enqueue_payload = _coerce_mapping(task.get("enqueue_payload"))
    return is_orca_submitter(enqueue_payload.get("submitter"))


def _stage_needs_restart(stage: dict[str, Any]) -> bool:
    task = _coerce_mapping(stage.get("task"))
    stage_status = _normalize_text(stage.get("status")).lower()
    task_status = _normalize_text(task.get("status")).lower()
    if stage_status == "completed" and task_status == "completed":
        return False
    return stage_status in _RESTARTABLE_STAGE_STATUSES or task_status in _RESTARTABLE_STAGE_STATUSES


def _active_stage_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        task = _coerce_mapping(raw_stage.get("task"))
        stage_status = _normalize_text(raw_stage.get("status")).lower()
        task_status = _normalize_text(task.get("status")).lower()
        if stage_status not in _ACTIVE_STAGE_STATUSES and task_status not in _ACTIVE_STAGE_STATUSES:
            continue
        rows.append(
            {
                "stage_id": _normalize_text(raw_stage.get("stage_id")),
                "status": stage_status,
                "task_status": task_status,
                "engine": _normalize_text(task.get("engine")),
            }
        )
    return rows


def _active_restart_error(workflow_id: str, rows: list[dict[str, str]]) -> ValueError:
    shown = []
    for row in rows[:5]:
        stage_id = row.get("stage_id") or "stage"
        status = row.get("status") or "-"
        task_status = row.get("task_status") or "-"
        shown.append(f"{stage_id}(status={status}, task_status={task_status})")
    suffix = f"; active_stages={', '.join(shown)}" if shown else ""
    if len(rows) > len(shown):
        suffix += f"; remaining_active_count={len(rows) - len(shown)}"
    return ValueError(
        f"workflow still has active stages; wait for cancellation/sync to finish before restart: "
        f"{workflow_id}{suffix}"
    )


def _clear_phase_notification_state(metadata: dict[str, Any], restarted_stages: list[dict[str, str]]) -> None:
    phase_notifications = metadata.get("phase_notifications")
    if not isinstance(phase_notifications, dict):
        return

    engines = {
        _normalize_text(stage.get("engine")).lower()
        for stage in restarted_stages
        if _normalize_text(stage.get("engine"))
    }
    for engine in engines:
        phase_notifications.pop(f"{engine}_summary", None)
    if not phase_notifications:
        metadata.pop("phase_notifications", None)


def _reset_stage_for_restart(stage: dict[str, Any]) -> dict[str, str]:
    task = _stage_task(stage)
    metadata = _stage_metadata(stage)
    task_payload = _task_payload(task)
    enqueue_payload = _enqueue_payload(task)

    previous = {
        "stage_id": _normalize_text(stage.get("stage_id")),
        "previous_status": _normalize_text(stage.get("status")),
        "previous_task_status": _normalize_text(task.get("status")),
        "engine": _normalize_text(task.get("engine")),
    }

    stage["status"] = "planned"
    task["status"] = "planned"
    stage["output_artifacts"] = []
    task.pop("submission_result", None)
    task.pop("cancel_result", None)

    for key in _STALE_STAGE_METADATA_KEYS:
        metadata.pop(key, None)
    for key in _STALE_TASK_PAYLOAD_KEYS:
        task_payload.pop(key, None)

    if _task_is_orca(task):
        enqueue_payload["force"] = True

    return previous


def restart_failed_workflow(
    *,
    workspace_dir: str | Path,
    workflow_root: str | Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    workspace = Path(workspace_dir).expanduser().resolve()
    root = Path(workflow_root).expanduser().resolve() if workflow_root is not None else workspace.parent

    with acquire_workflow_lock(workspace):
        payload = load_workflow_payload(workspace)
        previous_status = _normalize_text(payload.get("status")).lower()
        force_restart = bool(force)
        if previous_status not in _RESTARTABLE_WORKFLOW_STATUSES and not force_restart:
            raise ValueError(
                f"workflow is not failed or cancelled: {payload.get('workflow_id', workspace.name)} "
                f"(status={previous_status or 'unknown'})"
            )
        workflow_id = _normalize_text(payload.get("workflow_id")) or workspace.name

        active_stages = _active_stage_rows(payload)
        if active_stages:
            raise _active_restart_error(workflow_id, active_stages)

        restarted_stages: list[dict[str, str]] = []
        for raw_stage in payload.get("stages", []):
            if not isinstance(raw_stage, dict) or not _stage_needs_restart(raw_stage):
                continue
            restarted_stages.append(_reset_stage_for_restart(raw_stage))

        if not restarted_stages:
            raise ValueError(
                f"workflow has no failed or cancelled stages to restart: "
                f"{workflow_id}"
            )

        restarted_at = now_utc_iso()
        payload["status"] = "planned"
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata
        metadata.pop("workflow_error", None)
        _clear_phase_notification_state(metadata, restarted_stages)
        metadata["final_child_sync_pending"] = False
        metadata["final_child_sync_completed_at"] = ""
        metadata["last_restarted_at"] = restarted_at
        metadata["restart_summary"] = {
            "status": "restarted",
            "previous_status": previous_status,
            "restarted_at": restarted_at,
            "restarted_count": len(restarted_stages),
            "stages": restarted_stages,
        }

        write_workflow_payload(workspace, payload)
        sync_workflow_registry(root, workspace, payload)
        summary = workflow_summary(workspace, payload)

    append_workflow_journal_event(
        root,
        event_type="workflow_restarted",
        workflow_id=_normalize_text(payload.get("workflow_id")),
        template_name=_normalize_text(payload.get("template_name")),
        previous_status=previous_status,
        status="planned",
        reason="run_dir_restart",
        metadata={
            "workspace_dir": str(workspace),
            "restarted_count": len(restarted_stages),
            "stages": restarted_stages,
        },
    )
    return {
        "workflow_id": _normalize_text(payload.get("workflow_id")),
        "template_name": _normalize_text(payload.get("template_name")),
        "workspace_dir": str(workspace),
        "workflow_root": str(root),
        "status": "restarted",
        "workflow_status": "planned",
        "previous_status": previous_status,
        "restarted_count": len(restarted_stages),
        "restarted_stages": restarted_stages,
        "summary": summary,
    }


__all__ = ["restart_failed_workflow"]
