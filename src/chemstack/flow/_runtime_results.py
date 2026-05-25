from __future__ import annotations

from typing import Any, Callable

from ._runtime_common import normalize_text


def workflow_advance_failed_result(
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


def workflow_skipped_terminal_result(record: Any, *, previous_status: str) -> dict[str, Any]:
    return {
        "workflow_id": record.workflow_id,
        "template_name": record.template_name,
        "previous_status": previous_status,
        "status": previous_status,
        "advanced": False,
        "reason": "terminal_status",
        "stage_count": record.stage_count,
    }


def workflow_advanced_result(
    record: Any,
    payload: dict[str, Any],
    *,
    previous_status: str,
    status: str,
    reason: str = "",
    normalize_text_fn: Callable[[Any], str] = normalize_text,
) -> dict[str, Any]:
    result = {
        "workflow_id": normalize_text_fn(payload.get("workflow_id")) or record.workflow_id,
        "template_name": normalize_text_fn(payload.get("template_name")) or record.template_name,
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
