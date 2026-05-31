from __future__ import annotations

from typing import Any

from ._orchestration_stage_views import WorkflowStageView, WorkflowTaskView


def stage_needs_restart(stage: dict[str, Any], *, deps: Any) -> bool:
    task = deps._coerce_mapping(stage.get("task"))
    stage_status = deps._normalize_text(stage.get("status")).lower()
    task_status = deps._normalize_text(task.get("status")).lower()
    if stage_status == "completed" and task_status == "completed":
        return False
    return (
        stage_status in deps._RESTARTABLE_STAGE_STATUSES
        or task_status in deps._RESTARTABLE_STAGE_STATUSES
    )


def active_stage_rows(payload: dict[str, Any], *, deps: Any) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        task = deps._coerce_mapping(raw_stage.get("task"))
        stage_status = deps._normalize_text(raw_stage.get("status")).lower()
        task_status = deps._normalize_text(task.get("status")).lower()
        if (
            stage_status not in deps._ACTIVE_STAGE_STATUSES
            and task_status not in deps._ACTIVE_STAGE_STATUSES
        ):
            continue
        rows.append(
            {
                "stage_id": deps._normalize_text(raw_stage.get("stage_id")),
                "status": stage_status,
                "task_status": task_status,
                "engine": deps._normalize_text(task.get("engine")),
            }
        )
    return rows


def active_restart_error(workflow_id: str, rows: list[dict[str, str]]) -> ValueError:
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


def clear_phase_notification_state(
    metadata: dict[str, Any], restarted_stages: list[dict[str, str]], *, deps: Any
) -> None:
    phase_notifications = metadata.get("phase_notifications")
    if not isinstance(phase_notifications, dict):
        return

    engines = {
        deps._normalize_text(stage.get("engine")).lower()
        for stage in restarted_stages
        if deps._normalize_text(stage.get("engine"))
    }
    for engine in engines:
        phase_notifications.pop(f"{engine}_summary", None)
    if not phase_notifications:
        metadata.pop("phase_notifications", None)


def reset_stage_for_restart(
    stage: dict[str, Any],
    *,
    rematerialize: bool = False,
    deps: Any,
) -> dict[str, str]:
    task = deps._stage_task(stage)
    task_payload = deps._task_payload(task)
    enqueue_payload = deps._enqueue_payload(task)
    engine = deps._task_engine(task)
    stage_view = WorkflowStageView(stage)
    task_view = WorkflowTaskView(task)

    previous = {
        "stage_id": deps._normalize_text(stage.get("stage_id")),
        "previous_status": deps._normalize_text(stage.get("status")),
        "previous_task_status": deps._normalize_text(task.get("status")),
        "engine": deps._normalize_text(task.get("engine")),
    }

    stage_view.set_status_pair(stage_status="planned", task_status="planned")
    stage_view.set_output_artifacts([])
    task_view.clear_keys("submission_result", "cancel_result")

    stage_view.clear_metadata_keys(*deps._STALE_STAGE_METADATA_KEYS)
    task_view.clear_payload_keys(*deps._STALE_TASK_PAYLOAD_KEYS)

    if rematerialize and engine in deps._REMATERIALIZED_ENGINES:
        for key in deps._REMATERIALIZED_TASK_PAYLOAD_KEYS:
            if key in task_payload:
                task_view.set_payload_field(key, "")
        if "job_dir" in enqueue_payload:
            task_view.update_enqueue_payload({"job_dir": ""})

    if deps._task_is_orca(task):
        task_view.update_enqueue_payload({"force": True})

    return previous
