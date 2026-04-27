from __future__ import annotations

from typing import Any, Iterable

WORKFLOW_PHASE_FINISHED_EVENT = "workflow_phase_finished"
SUPPRESSED_STAGE_NOTIFICATION_ENGINES = frozenset({"crest", "xtb"})
TERMINAL_STAGE_STATUSES = frozenset(
    {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
        "submission_failed",
    }
)
FAILED_STAGE_STATUSES = frozenset({"failed", "cancel_failed", "submission_failed"})


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _stage_row(stage: Any) -> dict[str, str]:
    if not isinstance(stage, dict):
        return {}

    task = stage.get("task")
    metadata = _coerce_mapping(stage.get("metadata"))
    if isinstance(task, dict):
        payload = _coerce_mapping(task.get("payload"))
        stage_id = _normalize_text(stage.get("stage_id"))
        return {
            "stage_id": stage_id,
            "label": _normalize_text(metadata.get("input_role") or payload.get("input_role") or payload.get("reaction_key"))
            or stage_id,
            "engine": _normalize_text(task.get("engine")).lower(),
            "task_kind": _normalize_text(task.get("task_kind")).lower(),
            "status": _normalize_text(stage.get("status")).lower(),
            "task_status": _normalize_text(task.get("status")).lower(),
            "reason": _normalize_text(metadata.get("reason")).lower(),
            "reaction_handoff_status": _normalize_text(metadata.get("reaction_handoff_status")).lower(),
            "reaction_handoff_reason": _normalize_text(metadata.get("reaction_handoff_reason")).lower(),
        }

    stage_id = _normalize_text(stage.get("stage_id"))
    return {
        "stage_id": stage_id,
        "label": _normalize_text(stage.get("input_role") or stage.get("reaction_key")) or stage_id,
        "engine": _normalize_text(stage.get("engine")).lower(),
        "task_kind": _normalize_text(stage.get("task_kind")).lower(),
        "status": _normalize_text(stage.get("status")).lower(),
        "task_status": _normalize_text(stage.get("task_status")).lower(),
        "reason": _normalize_text(stage.get("reason")).lower(),
        "reaction_handoff_status": _normalize_text(stage.get("reaction_handoff_status")).lower(),
        "reaction_handoff_reason": _normalize_text(stage.get("reaction_handoff_reason")).lower(),
    }


def _count_values(rows: Iterable[dict[str, str]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = _normalize_text(row.get(key)).lower()
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _stage_status_details(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    details: list[dict[str, str]] = []
    for row in rows:
        status = _normalize_text(row.get("status")).lower()
        task_status = _normalize_text(row.get("task_status")).lower()
        detail = {
            "stage_id": _normalize_text(row.get("stage_id")),
            "label": _normalize_text(row.get("label") or row.get("stage_id")),
            "status": status or task_status or "unknown",
            "task_status": task_status,
        }
        reason = _normalize_text(row.get("reaction_handoff_reason") or row.get("reason"))
        if reason:
            detail["reason"] = reason
        handoff_status = _normalize_text(row.get("reaction_handoff_status")).lower()
        if handoff_status:
            detail["reaction_handoff_status"] = handoff_status
        details.append(detail)
    return details


def _row_is_terminal(row: dict[str, str]) -> bool:
    statuses = [
        value
        for value in (
            _normalize_text(row.get("status")).lower(),
            _normalize_text(row.get("task_status")).lower(),
        )
        if value
    ]
    if not statuses:
        return False
    return all(value in TERMINAL_STAGE_STATUSES for value in statuses)


def _phase_outcome(rows: list[dict[str, str]]) -> str:
    def _row_success(row: dict[str, str]) -> bool:
        handoff_status = _normalize_text(row.get("reaction_handoff_status")).lower()
        if handoff_status:
            return handoff_status == "ready"
        return (
            _normalize_text(row.get("status")).lower() == "completed"
            or _normalize_text(row.get("task_status")).lower() == "completed"
        )

    has_failure = any(
        _normalize_text(row.get("status")).lower() in FAILED_STAGE_STATUSES
        or _normalize_text(row.get("task_status")).lower() in FAILED_STAGE_STATUSES
        or _normalize_text(row.get("reaction_handoff_status")).lower() == "failed"
        for row in rows
    )
    has_cancel = any(
        _normalize_text(row.get("status")).lower() == "cancelled"
        or _normalize_text(row.get("task_status")).lower() == "cancelled"
        for row in rows
    )
    has_success = any(_row_success(row) for row in rows)
    if has_failure and has_success:
        return "mixed"
    if has_failure:
        return "failed"
    if has_cancel and has_success:
        return "mixed"
    if has_cancel:
        return "cancelled"
    return "completed"


def phase_snapshot(stages: Iterable[Any], *, engine: str) -> dict[str, Any]:
    engine_text = _normalize_text(engine).lower()
    rows = [
        row
        for row in (_stage_row(stage) for stage in stages)
        if row and _normalize_text(row.get("engine")).lower() == engine_text
    ]
    terminal_rows = [row for row in rows if _row_is_terminal(row)]
    handoff_counts = _count_values(rows, "reaction_handoff_status")
    failure_reasons: list[str] = []
    for row in rows:
        reason = _normalize_text(row.get("reaction_handoff_reason") or row.get("reason")).lower()
        if reason and reason not in failure_reasons:
            failure_reasons.append(reason)
    return {
        "engine": engine_text,
        "stage_count": len(rows),
        "stage_ids": [_normalize_text(row.get("stage_id")) for row in rows if _normalize_text(row.get("stage_id"))],
        "stage_statuses": _stage_status_details(rows),
        "terminal_stage_ids": [
            _normalize_text(row.get("stage_id"))
            for row in terminal_rows
            if _normalize_text(row.get("stage_id"))
        ],
        "nonterminal_stage_ids": [
            _normalize_text(row.get("stage_id"))
            for row in rows
            if _normalize_text(row.get("stage_id")) and not _row_is_terminal(row)
        ],
        "status_counts": _count_values(rows, "status"),
        "task_status_counts": _count_values(rows, "task_status"),
        "reaction_handoff_status_counts": handoff_counts,
        "failure_reasons": failure_reasons,
        "finished": bool(rows) and len(terminal_rows) == len(rows),
        "outcome": _phase_outcome(rows) if rows else "",
    }


def phase_finished(stages: Iterable[Any], *, engine: str) -> bool:
    return bool(phase_snapshot(stages, engine=engine).get("finished"))


def _phase_definitions(template_name: str) -> tuple[dict[str, str], ...]:
    normalized = _normalize_text(template_name).lower()
    definitions = [{"phase": "crest", "phase_label": "CREST", "engine": "crest"}]
    if normalized == "reaction_ts_search":
        definitions.append({"phase": "xtb", "phase_label": "xTB", "engine": "xtb"})
    return tuple(definitions)


def phase_transition_event_payloads(
    *,
    previous_summary: dict[str, Any],
    current_summary: dict[str, Any],
    workflow_id: str,
    template_name: str,
    worker_session_id: str,
) -> list[dict[str, Any]]:
    previous_stages = list(_coerce_mapping(previous_summary).get("stage_summaries") or [])
    current_stages = list(_coerce_mapping(current_summary).get("stage_summaries") or [])
    event_payloads: list[dict[str, Any]] = []

    for definition in _phase_definitions(template_name):
        previous_phase = phase_snapshot(previous_stages, engine=definition["engine"])
        current_phase = phase_snapshot(current_stages, engine=definition["engine"])
        if not current_phase["stage_count"] or not current_phase["finished"] or previous_phase["finished"]:
            continue
        event_payloads.append(
            {
                "event_type": WORKFLOW_PHASE_FINISHED_EVENT,
                "workflow_id": _normalize_text(workflow_id),
                "template_name": _normalize_text(template_name),
                "status": _normalize_text(current_phase.get("outcome")),
                "previous_status": "running" if previous_phase["stage_count"] and not previous_phase["finished"] else "",
                "worker_session_id": _normalize_text(worker_session_id),
                "metadata": {
                    "phase": definition["phase"],
                    "phase_label": definition["phase_label"],
                    "engine": definition["engine"],
                    "phase_outcome": _normalize_text(current_phase.get("outcome")),
                    "stage_count": int(current_phase.get("stage_count", 0) or 0),
                    "stage_ids": list(current_phase.get("stage_ids") or []),
                    "stage_statuses": list(current_phase.get("stage_statuses") or []),
                    "terminal_stage_ids": list(current_phase.get("terminal_stage_ids") or []),
                    "stage_status_counts": dict(current_phase.get("status_counts") or {}),
                    "task_status_counts": dict(current_phase.get("task_status_counts") or {}),
                    "reaction_handoff_status_counts": dict(current_phase.get("reaction_handoff_status_counts") or {}),
                    "failure_reasons": list(current_phase.get("failure_reasons") or []),
                },
            }
        )
    return event_payloads


__all__ = [
    "SUPPRESSED_STAGE_NOTIFICATION_ENGINES",
    "TERMINAL_STAGE_STATUSES",
    "WORKFLOW_PHASE_FINISHED_EVENT",
    "phase_finished",
    "phase_snapshot",
    "phase_transition_event_payloads",
]
