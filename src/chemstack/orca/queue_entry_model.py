from __future__ import annotations

from typing import Any

from chemstack.core.queue import store as _core_queue_store
from chemstack.core.queue.types import QueueStatus
from chemstack.core.utils import normalize_bool as _normalize_bool
from chemstack.core.utils import normalize_text as _normalize_text

from ..core.app_ids import CHEMSTACK_ORCA_APP_NAME
from .types import QueueEntry

QUEUE_APP_NAME = CHEMSTACK_ORCA_APP_NAME
QUEUE_ENGINE = "orca"
QUEUE_TASK_KIND = "orca_run_inp"

def normalize_text(value: object | None) -> str:
    if isinstance(value, QueueStatus):
        return value.value
    return _normalize_text(value)


def normalize_bool(value: object) -> bool:
    if not isinstance(value, (bool, str)) and value is not None:
        return bool(value)
    return _normalize_bool(value)


def normalize_priority(value: object, *, default: int = 10) -> int:
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return int(value)
    except (TypeError, ValueError):
        pass
    return default


def normalize_optional_text(value: object | None) -> str | None:
    text = normalize_text(value)
    if not text or text.lower() == "none":
        return None
    return text


def normalize_metadata(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items()}


def _normalized_raw(raw: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(raw)
    metadata = normalize_metadata(normalized.get("metadata"))
    reaction_dir = normalize_text(metadata.get("reaction_dir"))
    force = normalize_bool(metadata.get("force", False))
    run_id = normalize_optional_text(metadata.get("run_id"))

    if reaction_dir:
        metadata["reaction_dir"] = reaction_dir
    metadata["force"] = force
    if run_id is not None:
        metadata["run_id"] = run_id
    else:
        metadata.pop("run_id", None)

    normalized["app_name"] = normalize_text(normalized.get("app_name")) or QUEUE_APP_NAME
    queue_id = normalize_text(normalized.get("queue_id"))
    task_id = normalize_text(normalized.get("task_id")) or queue_id
    if task_id:
        normalized["task_id"] = task_id
    normalized["task_kind"] = normalize_text(normalized.get("task_kind")) or QUEUE_TASK_KIND
    normalized["engine"] = normalize_text(normalized.get("engine")) or QUEUE_ENGINE
    normalized["priority"] = normalize_priority(normalized.get("priority"), default=10)
    normalized["status"] = (
        normalize_text(normalized.get("status")).lower() or QueueStatus.PENDING.value
    )
    normalized["started_at"] = normalize_optional_text(normalized.get("started_at")) or ""
    normalized["finished_at"] = normalize_optional_text(normalized.get("finished_at")) or ""
    normalized["error"] = normalize_optional_text(normalized.get("error")) or ""
    normalized["cancel_requested"] = normalize_bool(normalized.get("cancel_requested", False))
    normalized["metadata"] = metadata
    return normalized


def entry_from_json_payload(raw: dict[str, Any]) -> QueueEntry:
    return _core_queue_store.entry_from_dict(_normalized_raw(raw))


def entry_metadata(
    *,
    reaction_dir: str,
    force: bool,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = normalize_metadata(extra)
    metadata.setdefault("reaction_dir", reaction_dir)
    metadata.setdefault("force", force)
    return metadata


def queue_entry_metadata(entry: QueueEntry) -> dict[str, Any]:
    return dict(entry.metadata)


def queue_entry_run_id(entry: QueueEntry) -> str | None:
    return normalize_optional_text(entry.metadata.get("run_id"))


def queue_entry_id(entry: QueueEntry) -> str:
    return normalize_text(entry.queue_id)


def queue_entry_task_id(entry: QueueEntry) -> str | None:
    task_id = normalize_text(entry.task_id)
    return task_id or None


def queue_entry_status(entry: QueueEntry) -> str:
    return entry.status.value


def queue_entry_reaction_dir(entry: QueueEntry) -> str:
    return normalize_text(entry.metadata.get("reaction_dir"))


def queue_entry_force(entry: QueueEntry) -> bool:
    return normalize_bool(entry.metadata.get("force", False))


def queue_entry_priority(entry: QueueEntry) -> int:
    return int(entry.priority)


def queue_entry_app_name(entry: QueueEntry) -> str:
    return normalize_text(entry.app_name) or QUEUE_APP_NAME
