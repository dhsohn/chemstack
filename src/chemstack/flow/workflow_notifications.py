from __future__ import annotations

from typing import Any, Callable

from chemstack.core.config import TelegramConfig
from chemstack.core.notifications import (
    build_telegram_transport,
    escape_html as _escape_html,
    html_code as _metric_code,
    load_telegram_config_from_file,
    split_telegram_message,
)
from chemstack.core.utils import now_utc_iso
from chemstack.flow.workflow_status import workflow_status_is_terminal

_ACTIVE_STATUSES = frozenset({"planned", "queued", "running", "submitted", "cancel_requested", "retrying"})


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_sequence(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_telegram_config(config_path: str | None) -> TelegramConfig:
    return load_telegram_config_from_file(config_path)


def _phase_notification_state(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        payload["metadata"] = {}
        metadata = payload["metadata"]
    state = metadata.get("phase_notifications")
    if isinstance(state, dict):
        return state
    metadata["phase_notifications"] = {}
    return metadata["phase_notifications"]


def _stage_engine(stage: dict[str, Any]) -> str:
    return _normalize_text(_coerce_mapping(stage.get("task")).get("engine")).lower()


def _stage_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    return _coerce_mapping(stage.get("metadata"))


def _stage_task_payload(stage: dict[str, Any]) -> dict[str, Any]:
    task = _coerce_mapping(stage.get("task"))
    return _coerce_mapping(task.get("payload"))


def _stage_is_terminal(
    stage: dict[str, Any],
    *,
    stage_failure_is_recoverable_fn: Callable[[dict[str, Any]], bool] | None,
) -> bool:
    status = _normalize_text(stage.get("status")).lower()
    task_status = _normalize_text(_coerce_mapping(stage.get("task")).get("status")).lower()
    if status in _ACTIVE_STATUSES or task_status in _ACTIVE_STATUSES:
        return False
    if stage_failure_is_recoverable_fn is not None and stage_failure_is_recoverable_fn(stage):
        return True
    return workflow_status_is_terminal(status) or workflow_status_is_terminal(task_status)


def _terminal_phase_stages(
    payload: dict[str, Any],
    *,
    phase_engine: str,
    stage_failure_is_recoverable_fn: Callable[[dict[str, Any]], bool] | None,
) -> list[dict[str, Any]] | None:
    stages = [
        stage
        for stage in payload.get("stages", [])
        if isinstance(stage, dict) and _stage_engine(stage) == phase_engine
    ]
    if not stages:
        return None
    if not all(
        _stage_is_terminal(stage, stage_failure_is_recoverable_fn=stage_failure_is_recoverable_fn)
        for stage in stages
    ):
        return None
    return stages


def _stage_result_bucket(
    stage: dict[str, Any],
    *,
    phase_engine: str,
    stage_failure_is_recoverable_fn: Callable[[dict[str, Any]], bool] | None,
) -> str:
    status = _normalize_text(stage.get("status")).lower()
    metadata = _stage_metadata(stage)
    if stage_failure_is_recoverable_fn is not None and stage_failure_is_recoverable_fn(stage):
        return "completed"
    if phase_engine == "xtb" and _normalize_text(metadata.get("reaction_handoff_status")).lower() == "ready":
        return "completed"
    if status == "completed":
        return "completed"
    if status == "cancelled":
        return "cancelled"
    return "failed"


def _count_output_artifacts(stage: dict[str, Any]) -> int:
    return len([item for item in _coerce_sequence(stage.get("output_artifacts")) if isinstance(item, dict)])


def _xtb_candidate_count(stage: dict[str, Any]) -> int:
    metadata = _stage_metadata(stage)
    attempts = [item for item in _coerce_sequence(metadata.get("xtb_attempts")) if isinstance(item, dict)]
    if attempts:
        latest = attempts[-1]
        return _safe_int(latest.get("candidate_count"), default=0)
    return _count_output_artifacts(stage)


def _phase_label(phase_engine: str) -> str:
    return {"crest": "CREST", "xtb": "xTB"}.get(phase_engine, phase_engine.upper())


def _phase_outcome(counts: dict[str, int]) -> str:
    failed = counts.get("failed", 0)
    cancelled = counts.get("cancelled", 0)
    completed = counts.get("completed", 0)
    if failed and completed:
        return "mixed"
    if failed:
        return "failed"
    if cancelled and completed:
        return "mixed"
    if cancelled:
        return "cancelled"
    if completed:
        return "completed"
    return "unknown"


def _phase_stage_block(
    stage: dict[str, Any],
    *,
    phase_engine: str,
    bucket: str,
) -> str:
    stage_id = _normalize_text(stage.get("stage_id")) or "stage"
    status = _normalize_text(stage.get("status")).lower() or "unknown"
    task_payload = _stage_task_payload(stage)
    metadata = _stage_metadata(stage)
    if phase_engine == "crest":
        role = _normalize_text(task_payload.get("input_role")) or stage_id
        return "\n".join(
            [
                f"<b>Stage</b>: {_escape_html(role)}  <b>Result</b>: {_metric_code(bucket)}",
                (
                    f"<b>Status</b>: {_metric_code(status)}  "
                    f"<b>Retained conformers</b>: {_metric_code(_count_output_artifacts(stage))}"
                ),
            ]
        )
    if phase_engine == "xtb":
        reaction_key = _normalize_text(task_payload.get("reaction_key")) or stage_id
        handoff_status = _normalize_text(metadata.get("reaction_handoff_status")).lower() or "none"
        return "\n".join(
            [
                f"<b>Stage</b>: {_escape_html(reaction_key)}  <b>Result</b>: {_metric_code(bucket)}",
                (
                    f"<b>Status</b>: {_metric_code(status)}  "
                    f"<b>Handoff</b>: {_metric_code(handoff_status)}  "
                    f"<b>Candidates</b>: {_metric_code(_xtb_candidate_count(stage))}"
                ),
            ]
        )
    return "\n".join(
        [
            f"<b>Stage</b>: {_escape_html(stage_id)}  <b>Result</b>: {_metric_code(bucket)}",
            f"<b>Status</b>: {_metric_code(status)}",
        ]
    )


def _extra_lines_section(extra_lines: list[str] | None) -> str | None:
    rows: list[str] = []
    for raw_line in extra_lines or []:
        line = _normalize_text(raw_line)
        if not line:
            continue
        if ":" in line:
            key, value = line.split(":", 1)
            normalized_key = _normalize_text(key)
            normalized_value = _normalize_text(value) or "-"
            if normalized_key:
                rows.append(f"<b>{_escape_html(normalized_key)}</b>: {_metric_code(normalized_value)}")
                continue
        rows.append(_escape_html(line))
    if not rows:
        return None
    return "<b>Notes</b>\n" + "\n".join(rows)


def _format_phase_summary_message(
    *,
    payload: dict[str, Any],
    phase_engine: str,
    stages: list[dict[str, Any]],
    counts: dict[str, int],
    stage_buckets: dict[int, str],
    extra_lines: list[str] | None,
) -> str:
    phase = _phase_label(phase_engine)
    workflow_id = _normalize_text(payload.get("workflow_id")) or "-"
    template_name = _normalize_text(payload.get("template_name")) or "-"
    outcome = _phase_outcome(counts)

    overview = [
        f"<b>ChemStack Flow {phase} Phase Summary</b>",
        f"<b>Workflow</b>: {_metric_code(workflow_id)}",
        f"<b>Template</b>: {_metric_code(template_name)}",
        f"<b>Outcome</b>: {_metric_code(outcome)}",
        (
            f"<b>Stages</b>: {_metric_code(len(stages))}  "
            f"completed={_metric_code(counts['completed'])}  "
            f"failed={_metric_code(counts['failed'])}  "
            f"cancelled={_metric_code(counts['cancelled'])}"
        ),
    ]
    if phase_engine == "xtb":
        ready_count = sum(
            1
            for stage in stages
            if _normalize_text(_stage_metadata(stage).get("reaction_handoff_status")).lower() == "ready"
        )
        overview.append(f"<b>Ready for ORCA</b>: {_metric_code(ready_count)}")

    stage_blocks = [
        _phase_stage_block(
            stage,
            phase_engine=phase_engine,
            bucket=stage_buckets.get(id(stage), "failed"),
        )
        for stage in stages
    ]

    sections: list[str] = ["\n".join(overview)]
    notes = _extra_lines_section(extra_lines)
    if notes is not None:
        sections.append(notes)
    if stage_blocks:
        sections.append("<b>Stage details</b>\n" + "\n\n".join(stage_blocks))
    return "\n\n".join(sections)


def maybe_notify_workflow_phase_summary(
    *,
    payload: dict[str, Any],
    config_path: str | None,
    phase_engine: str,
    stage_failure_is_recoverable_fn: Callable[[dict[str, Any]], bool] | None = None,
    extra_lines: list[str] | None = None,
) -> bool:
    normalized_engine = _normalize_text(phase_engine).lower()
    if normalized_engine not in {"crest", "xtb"}:
        return False

    stages = _terminal_phase_stages(
        payload,
        phase_engine=normalized_engine,
        stage_failure_is_recoverable_fn=stage_failure_is_recoverable_fn,
    )
    if not stages:
        return False

    notification_state = _phase_notification_state(payload)
    state_key = f"{normalized_engine}_summary"
    previous_state = _coerce_mapping(notification_state.get(state_key))
    if previous_state.get("sent_at"):
        return False

    telegram = _load_telegram_config(config_path)
    if not telegram.enabled:
        return False

    counts = {"completed": 0, "failed": 0, "cancelled": 0}
    stage_buckets: dict[int, str] = {}
    for stage in stages:
        bucket = _stage_result_bucket(
            stage,
            phase_engine=normalized_engine,
            stage_failure_is_recoverable_fn=stage_failure_is_recoverable_fn,
        )
        counts[bucket] += 1
        stage_buckets[id(stage)] = bucket

    message = _format_phase_summary_message(
        payload=payload,
        phase_engine=normalized_engine,
        stages=stages,
        counts=counts,
        stage_buckets=stage_buckets,
        extra_lines=extra_lines,
    )
    chunks = split_telegram_message(message)
    if not chunks:
        return False

    transport = build_telegram_transport(telegram)
    for chunk in chunks:
        result = transport.send_text(chunk, parse_mode="HTML")
        if result.sent or result.skipped:
            continue
        fallback_result = transport.send_text(chunk, parse_mode=None)
        if not (fallback_result.sent or fallback_result.skipped):
            return False

    notification_state[state_key] = {
        "sent_at": now_utc_iso(),
        "stage_count": len(stages),
    }
    return True


__all__ = ["maybe_notify_workflow_phase_summary"]
