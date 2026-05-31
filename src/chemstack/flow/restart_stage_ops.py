from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from chemstack.core.app_ids import is_orca_submitter
from chemstack.core.utils import (
    mapping_or_empty as _coerce_mapping,
    normalize_text as _normalize_text,
)

from ._orchestration_stage_views import WorkflowStageView, WorkflowTaskView
from . import restart_stages as _restart_stages

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
_REMATERIALIZED_ENGINES = frozenset({"crest", "xtb"})
_REMATERIALIZED_TASK_PAYLOAD_KEYS = frozenset(
    {"job_dir", "selected_input_xyz", "secondary_input_xyz"}
)


@dataclass(frozen=True)
class _RestartStageDeps:
    _ACTIVE_STAGE_STATUSES: Any
    _REMATERIALIZED_ENGINES: Any
    _REMATERIALIZED_TASK_PAYLOAD_KEYS: Any
    _RESTARTABLE_STAGE_STATUSES: Any
    _STALE_STAGE_METADATA_KEYS: Any
    _STALE_TASK_PAYLOAD_KEYS: Any
    _coerce_mapping: Any
    _enqueue_payload: Any
    _normalize_text: Any
    _stage_metadata: Any
    _stage_task: Any
    _task_engine: Any
    _task_is_orca: Any
    _task_payload: Any


def _restart_stage_deps() -> _RestartStageDeps:
    return _RestartStageDeps(
        _ACTIVE_STAGE_STATUSES=_ACTIVE_STAGE_STATUSES,
        _REMATERIALIZED_ENGINES=_REMATERIALIZED_ENGINES,
        _REMATERIALIZED_TASK_PAYLOAD_KEYS=_REMATERIALIZED_TASK_PAYLOAD_KEYS,
        _RESTARTABLE_STAGE_STATUSES=_RESTARTABLE_STAGE_STATUSES,
        _STALE_STAGE_METADATA_KEYS=_STALE_STAGE_METADATA_KEYS,
        _STALE_TASK_PAYLOAD_KEYS=_STALE_TASK_PAYLOAD_KEYS,
        _coerce_mapping=_coerce_mapping,
        _enqueue_payload=_enqueue_payload,
        _normalize_text=_normalize_text,
        _stage_metadata=_stage_metadata,
        _stage_task=_stage_task,
        _task_engine=_task_engine,
        _task_is_orca=_task_is_orca,
        _task_payload=_task_payload,
    )


def _stage_task(stage: dict[str, Any]) -> dict[str, Any]:
    return WorkflowStageView(stage).ensure_task().raw


def _stage_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    return WorkflowStageView(stage).metadata(None)


def _task_metadata(task: dict[str, Any]) -> dict[str, Any]:
    return WorkflowTaskView(task).metadata(None)


def _task_payload(task: dict[str, Any]) -> dict[str, Any]:
    return WorkflowTaskView(task).payload(None)


def _enqueue_payload(task: dict[str, Any]) -> dict[str, Any]:
    return WorkflowTaskView(task).enqueue_payload()


def _task_is_orca(task: dict[str, Any]) -> bool:
    engine = _normalize_text(task.get("engine")).lower()
    if engine == "orca":
        return True
    enqueue_payload = _coerce_mapping(task.get("enqueue_payload"))
    return is_orca_submitter(enqueue_payload.get("submitter"))


def _task_engine(task: dict[str, Any]) -> str:
    return _normalize_text(task.get("engine")).lower()


def _stage_needs_restart(stage: dict[str, Any]) -> bool:
    return _restart_stages.stage_needs_restart(stage, deps=_restart_stage_deps())


def _active_stage_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    return _restart_stages.active_stage_rows(payload, deps=_restart_stage_deps())


def _active_restart_error(workflow_id: str, rows: list[dict[str, str]]) -> ValueError:
    return _restart_stages.active_restart_error(workflow_id, rows)


def _clear_phase_notification_state(
    metadata: dict[str, Any], restarted_stages: list[dict[str, str]]
) -> None:
    _restart_stages.clear_phase_notification_state(
        metadata,
        restarted_stages,
        deps=_restart_stage_deps(),
    )


def _reset_stage_for_restart(
    stage: dict[str, Any],
    *,
    rematerialize: bool = False,
) -> dict[str, str]:
    return _restart_stages.reset_stage_for_restart(
        stage,
        rematerialize=rematerialize,
        deps=_restart_stage_deps(),
    )
