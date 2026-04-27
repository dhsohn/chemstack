from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import yaml

from chemstack.core.config import TelegramConfig
from chemstack.core.notifications import build_telegram_transport
from chemstack.core.utils import now_utc_iso
from chemstack.flow.workflow_status import workflow_status_is_terminal

_ACTIVE_STATUSES = frozenset({"planned", "queued", "running", "submitted", "cancel_requested", "retrying"})
_MAX_TELEGRAM_MESSAGE_LENGTH = 4096
_DIVIDER = "\u2500" * 28


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _escape_html(value: Any) -> str:
    text = _normalize_text(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_sequence(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_telegram_config(config_path: str | None) -> TelegramConfig:
    config_text = _normalize_text(config_path)
    if not config_text:
        return TelegramConfig()
    try:
        path = Path(config_text).expanduser().resolve()
    except OSError:
        return TelegramConfig()
    if not path.exists():
        return TelegramConfig()

    try:
        parsed = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return TelegramConfig()
    if not isinstance(parsed, dict):
        return TelegramConfig()

    telegram_raw = parsed.get("telegram")
    if not isinstance(telegram_raw, dict):
        return TelegramConfig()

    return TelegramConfig(
        bot_token=_normalize_text(telegram_raw.get("bot_token")),
        chat_id=_normalize_text(telegram_raw.get("chat_id")),
        timeout_seconds=max(
            0.1,
            _safe_float(
                telegram_raw.get("timeout_seconds", TelegramConfig.timeout_seconds),
                default=TelegramConfig.timeout_seconds,
            ),
        ),
        max_attempts=max(1, _safe_int(telegram_raw.get("max_attempts"), default=TelegramConfig.max_attempts)),
        retry_backoff_seconds=max(
            0.0,
            _safe_float(
                telegram_raw.get(
                    "retry_backoff_seconds",
                    TelegramConfig.retry_backoff_seconds,
                ),
                default=TelegramConfig.retry_backoff_seconds,
            ),
        ),
    )


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


def _status_icon(status: str) -> str:
    icons = {
        "completed": "\u2705",
        "failed": "\u274c",
        "cancelled": "\u26d4",
        "ready": "\u2705",
        "running": "\u25b6",
        "queued": "\u23f3",
        "planned": "\u23f3",
        "submitted": "\U0001f4e4",
    }
    return icons.get(_normalize_text(status).lower(), "\u2022")


def _phase_label(phase_engine: str) -> str:
    return {"crest": "CREST", "xtb": "xTB"}.get(phase_engine, phase_engine.upper())


def _metric_code(value: Any) -> str:
    return f"<code>{_escape_html(value)}</code>"


def _phase_stage_block(
    stage: dict[str, Any],
    *,
    phase_engine: str,
    bucket: str,
) -> str:
    detail_separator = " \u00b7 "
    stage_id = _normalize_text(stage.get("stage_id")) or "stage"
    status = _normalize_text(stage.get("status")).lower() or "unknown"
    task_payload = _stage_task_payload(stage)
    metadata = _stage_metadata(stage)
    if phase_engine == "crest":
        role = _normalize_text(task_payload.get("input_role")) or stage_id
        details = [
            f"\U0001f4cd {_metric_code(status)}",
            f"retained_conformers={_metric_code(_count_output_artifacts(stage))}",
        ]
        return f"{_status_icon(bucket)} <b>{_escape_html(role)}</b>\n   {detail_separator.join(details)}"
    if phase_engine == "xtb":
        reaction_key = _normalize_text(task_payload.get("reaction_key")) or stage_id
        handoff_status = _normalize_text(metadata.get("reaction_handoff_status")).lower() or "none"
        details = [
            f"\U0001f4cd {_metric_code(status)}",
            f"handoff={_metric_code(handoff_status)}",
            f"candidates={_metric_code(_xtb_candidate_count(stage))}",
        ]
        return f"{_status_icon(bucket)} <b>{_escape_html(reaction_key)}</b>\n   {detail_separator.join(details)}"
    return f"{_status_icon(bucket)} <b>{_escape_html(stage_id)}</b>\n   \U0001f4cd {_metric_code(status)}"


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
                rows.append(f"   {_escape_html(normalized_key)}: {_metric_code(normalized_value)}")
                continue
        rows.append(f"   {_escape_html(line)}")
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
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    workflow_id = _normalize_text(payload.get("workflow_id")) or "-"
    template_name = _normalize_text(payload.get("template_name")) or "-"

    overview = [
        f"\U0001f9ed <b>chem_flow {phase} phase summary</b>  {_metric_code(now)}",
        _DIVIDER,
        f"<b>Workflow</b>: {_metric_code(workflow_id)}",
        f"<b>Template</b>: {_metric_code(template_name)}",
        (
            f"<b>Stages</b>: {_metric_code(len(stages))}  "
            f"{_status_icon('completed')} {counts['completed']} \u00b7 "
            f"{_status_icon('failed')} {counts['failed']} \u00b7 "
            f"{_status_icon('cancelled')} {counts['cancelled']}"
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
        sections.append("\n\n".join(stage_blocks))
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
    result = build_telegram_transport(telegram).send_text(
        message[:_MAX_TELEGRAM_MESSAGE_LENGTH],
        parse_mode="HTML",
    )
    if not (result.sent or result.skipped):
        return False

    notification_state[state_key] = {
        "sent_at": now_utc_iso(),
        "stage_count": len(stages),
    }
    return True


__all__ = ["maybe_notify_workflow_phase_summary"]
