from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.utils import (
    coerce_mapping as _coerce_mapping,
    file_lock,
    normalize_text as _normalize_text,
    now_utc_iso,
    timestamped_token,
)

from . import _registry_notifications as _notifications
from .registry_store import _registry_lock_path

WORKFLOW_JOURNAL_FILE_NAME = "workflow_registry.journal.jsonl"


def workflow_journal_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_JOURNAL_FILE_NAME


def _maybe_notify_journal_event(event: dict[str, Any], workflow_root: str | Path) -> None:
    event_type = _normalize_text(event.get("event_type"))
    if not _notifications.journal_notification_enabled(event_type):
        return
    if _notifications.should_suppress_stage_notification(event):
        return
    transport = _notifications.telegram_transport_from_env()
    if transport is None:
        return
    try:
        transport.send_text(_notifications.journal_event_message(event, workflow_root), parse_mode="HTML")
    except Exception:
        return


def append_workflow_journal_event(
    workflow_root: str | Path,
    *,
    event_type: str,
    workflow_id: str = "",
    template_name: str = "",
    status: str = "",
    previous_status: str = "",
    reason: str = "",
    worker_session_id: str = "",
    stage_id: str = "",
    engine: str = "",
    task_kind: str = "",
    stage_status: str = "",
    previous_stage_status: str = "",
    reaction_handoff_status: str = "",
    previous_reaction_handoff_status: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    event = {
        "event_id": timestamped_token("wf_evt"),
        "occurred_at": now_utc_iso(),
        "event_type": _normalize_text(event_type),
        "workflow_id": _normalize_text(workflow_id),
        "template_name": _normalize_text(template_name),
        "status": _normalize_text(status),
        "previous_status": _normalize_text(previous_status),
        "reason": _normalize_text(reason),
        "worker_session_id": _normalize_text(worker_session_id),
        "stage_id": _normalize_text(stage_id),
        "engine": _normalize_text(engine),
        "task_kind": _normalize_text(task_kind),
        "stage_status": _normalize_text(stage_status),
        "previous_stage_status": _normalize_text(previous_stage_status),
        "reaction_handoff_status": _normalize_text(reaction_handoff_status),
        "previous_reaction_handoff_status": _normalize_text(previous_reaction_handoff_status),
        "metadata": _coerce_mapping(metadata),
    }
    with file_lock(_registry_lock_path(resolved_root)):
        path = workflow_journal_path(resolved_root)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True))
            handle.write("\n")
    _maybe_notify_journal_event(event, resolved_root)
    return event


def list_workflow_journal(workflow_root: str | Path, *, limit: int = 50) -> list[dict[str, Any]]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    path = workflow_journal_path(resolved_root)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with file_lock(_registry_lock_path(resolved_root)):
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                raw = json.loads(text)
            except Exception:
                continue
            if isinstance(raw, dict):
                rows.append({str(key): value for key, value in raw.items()})
    rows.sort(key=lambda item: _normalize_text(item.get("occurred_at")), reverse=True)
    if limit > 0:
        return rows[:limit]
    return rows


__all__ = [
    "WORKFLOW_JOURNAL_FILE_NAME",
    "append_workflow_journal_event",
    "list_workflow_journal",
    "workflow_journal_path",
]
