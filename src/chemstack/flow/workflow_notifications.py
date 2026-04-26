from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import yaml

from chemstack.core.config import TelegramConfig
from chemstack.core.notifications import build_telegram_transport
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


def _phase_stage_line(
    stage: dict[str, Any],
    *,
    phase_engine: str,
) -> str:
    stage_id = _normalize_text(stage.get("stage_id")) or "stage"
    status = _normalize_text(stage.get("status")).lower() or "unknown"
    task_payload = _stage_task_payload(stage)
    metadata = _stage_metadata(stage)
    if phase_engine == "crest":
        role = _normalize_text(task_payload.get("input_role")) or stage_id
        return (
            f"- {role}: status={status}"
            f" retained_conformers={_count_output_artifacts(stage)}"
        )
    if phase_engine == "xtb":
        reaction_key = _normalize_text(task_payload.get("reaction_key")) or stage_id
        handoff_status = _normalize_text(metadata.get("reaction_handoff_status")).lower() or "none"
        return (
            f"- {reaction_key}: status={status}"
            f" handoff={handoff_status}"
            f" candidates={_xtb_candidate_count(stage)}"
        )
    return f"- {stage_id}: status={status}"


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
    for stage in stages:
        bucket = _stage_result_bucket(
            stage,
            phase_engine=normalized_engine,
            stage_failure_is_recoverable_fn=stage_failure_is_recoverable_fn,
        )
        counts[bucket] += 1

    lines = [
        f"[chem_flow] {normalized_engine.upper()} phase summary",
        f"workflow_id: {_normalize_text(payload.get('workflow_id'))}",
        f"template_name: {_normalize_text(payload.get('template_name'))}",
        f"stage_count: {len(stages)}",
        f"completed: {counts['completed']}",
        f"failed: {counts['failed']}",
        f"cancelled: {counts['cancelled']}",
    ]
    if normalized_engine == "xtb":
        ready_count = sum(
            1
            for stage in stages
            if _normalize_text(_stage_metadata(stage).get("reaction_handoff_status")).lower() == "ready"
        )
        lines.append(f"ready_for_orca: {ready_count}")
    if extra_lines:
        lines.extend(line for line in extra_lines if _normalize_text(line))
    lines.extend(_phase_stage_line(stage, phase_engine=normalized_engine) for stage in stages)

    result = build_telegram_transport(telegram).send_text("\n".join(lines))
    if not (result.sent or result.skipped):
        return False

    notification_state[state_key] = {
        "sent_at": now_utc_iso(),
        "stage_count": len(stages),
    }
    return True


__all__ = ["maybe_notify_workflow_phase_summary"]
