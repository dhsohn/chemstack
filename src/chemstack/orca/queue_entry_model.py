from __future__ import annotations

from typing import Any, cast

from ..core.app_ids import CHEMSTACK_ORCA_APP_NAME
from .types import QueueEntry

QUEUE_APP_NAME = CHEMSTACK_ORCA_APP_NAME
QUEUE_ENGINE = "orca"
QUEUE_TASK_KIND = "orca_run_inp"


def normalize_text(value: object | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


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


def normalize_entry(entry: QueueEntry) -> QueueEntry:
    normalized = cast(QueueEntry, dict(entry))
    metadata = normalize_metadata(normalized.get("metadata"))
    reaction_dir = normalize_text(metadata.get("reaction_dir")) or normalize_text(
        normalized.get("reaction_dir")
    )
    force = normalize_bool(metadata.get("force", normalized.get("force", False)))
    run_id = normalize_optional_text(metadata.get("run_id")) or normalize_optional_text(
        normalized.get("run_id")
    )

    if reaction_dir:
        normalized["reaction_dir"] = reaction_dir
        metadata["reaction_dir"] = reaction_dir
    normalized["force"] = force
    metadata["force"] = force
    if run_id is not None:
        normalized["run_id"] = run_id
        metadata["run_id"] = run_id
    elif "run_id" in normalized:
        normalized["run_id"] = None

    normalized["app_name"] = normalize_text(normalized.get("app_name")) or QUEUE_APP_NAME
    task_id = normalize_text(normalized.get("task_id")) or normalize_text(
        normalized.get("queue_id")
    )
    if task_id:
        normalized["task_id"] = task_id
    normalized["task_kind"] = normalize_text(normalized.get("task_kind")) or QUEUE_TASK_KIND
    normalized["engine"] = normalize_text(normalized.get("engine")) or QUEUE_ENGINE
    normalized["priority"] = normalize_priority(normalized.get("priority"), default=10)
    normalized["status"] = normalize_text(normalized.get("status")).lower()
    normalized["started_at"] = normalize_optional_text(normalized.get("started_at"))
    normalized["finished_at"] = normalize_optional_text(normalized.get("finished_at"))
    normalized["error"] = normalize_optional_text(normalized.get("error"))
    normalized["metadata"] = metadata
    return normalized


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
    return dict(normalize_metadata(normalize_entry(entry).get("metadata")))


def queue_entry_run_id(entry: QueueEntry) -> str | None:
    return normalize_optional_text(normalize_entry(entry).get("run_id"))


def queue_entry_id(entry: QueueEntry) -> str:
    return normalize_text(normalize_entry(entry).get("queue_id"))


def queue_entry_task_id(entry: QueueEntry) -> str | None:
    task_id = normalize_text(normalize_entry(entry).get("task_id"))
    return task_id or None


def queue_entry_status(entry: QueueEntry) -> str:
    return normalize_text(normalize_entry(entry).get("status")).lower()


def queue_entry_reaction_dir(entry: QueueEntry) -> str:
    normalized = normalize_entry(entry)
    metadata = normalize_metadata(normalized.get("metadata"))
    return normalize_text(metadata.get("reaction_dir")) or normalize_text(
        normalized.get("reaction_dir")
    )


def queue_entry_force(entry: QueueEntry) -> bool:
    normalized = normalize_entry(entry)
    metadata = normalize_metadata(normalized.get("metadata"))
    return normalize_bool(metadata.get("force", normalized.get("force", False)))


def queue_entry_priority(entry: QueueEntry) -> int:
    return normalize_priority(normalize_entry(entry).get("priority"), default=10)


def queue_entry_app_name(entry: QueueEntry) -> str:
    return normalize_text(normalize_entry(entry).get("app_name")) or QUEUE_APP_NAME
