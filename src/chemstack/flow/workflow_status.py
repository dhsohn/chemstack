from __future__ import annotations

from typing import Any, Iterable

from chemstack.core.statuses import (
    WORKFLOW_ACTIVE_STATUSES,
    WORKFLOW_ATTENTION_STATUSES,
    WORKFLOW_FAILED_STATUSES,
    WORKFLOW_STATUS_ORDER,
    WORKFLOW_TERMINAL_STATUSES,
)
from chemstack.core.utils.coercion import normalize_text as _normalize_text


def normalize_workflow_status(value: Any) -> str:
    return _normalize_text(value).lower()


def workflow_status_is_active(value: Any) -> bool:
    return normalize_workflow_status(value) in WORKFLOW_ACTIVE_STATUSES


def workflow_status_needs_attention(value: Any) -> bool:
    return normalize_workflow_status(value) in WORKFLOW_ATTENTION_STATUSES


def workflow_status_is_terminal(value: Any) -> bool:
    return normalize_workflow_status(value) in WORKFLOW_TERMINAL_STATUSES


def workflow_stage_is_terminal(stage_summary: dict[str, Any]) -> bool:
    return all(
        workflow_status_is_terminal(stage_summary.get(key)) for key in ("status", "task_status")
    )


def select_current_stage(stage_summaries: Iterable[Any]) -> dict[str, Any]:
    stages = [stage for stage in stage_summaries if isinstance(stage, dict)]
    if not stages:
        return {}

    for stage in stages:
        if not workflow_stage_is_terminal(stage):
            return dict(stage)
    return dict(stages[-1])


__all__ = [
    "WORKFLOW_ACTIVE_STATUSES",
    "WORKFLOW_ATTENTION_STATUSES",
    "WORKFLOW_FAILED_STATUSES",
    "WORKFLOW_STATUS_ORDER",
    "WORKFLOW_TERMINAL_STATUSES",
    "normalize_workflow_status",
    "select_current_stage",
    "workflow_status_is_active",
    "workflow_status_is_terminal",
    "workflow_status_needs_attention",
    "workflow_stage_is_terminal",
]
