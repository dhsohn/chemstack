from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any

from orca_auto.core.artifacts import QUEUE_FILE

from ..utils.persistence import (
    atomic_write_json,
    coerce_bool,
    coerce_int,
    load_json_list_file,
    resolve_root_path,
)
from .types import QueueEntry, QueueStatus

QUEUE_FILE_NAME = QUEUE_FILE
QUEUE_LOCK_NAME = "queue.lock"


class QueueStoreCorruptError(RuntimeError):
    """Raised when the queue file exists but cannot be safely loaded."""


def queue_path(root: Path) -> Path:
    return root / QUEUE_FILE_NAME


def queue_lock_path(root: Path) -> Path:
    return root / QUEUE_LOCK_NAME


def entry_to_dict(entry: QueueEntry) -> dict[str, Any]:
    data = asdict(entry)
    data["status"] = entry.status.value
    return data


def _entry_from_dict(raw: dict[str, Any]) -> QueueEntry:
    status_raw = str(raw.get("status", QueueStatus.PENDING.value)).strip().lower()
    try:
        status = QueueStatus(status_raw)
    except ValueError:
        status = QueueStatus.PENDING

    metadata = raw.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    return QueueEntry(
        queue_id=str(raw.get("queue_id", "")).strip(),
        app_name=str(raw.get("app_name", "")).strip(),
        task_id=str(raw.get("task_id", "")).strip(),
        task_kind=str(raw.get("task_kind", "")).strip(),
        engine=str(raw.get("engine", "")).strip(),
        status=status,
        priority=coerce_int(raw.get("priority", 10), default=10),
        enqueued_at=str(raw.get("enqueued_at", "")).strip(),
        started_at=str(raw.get("started_at", "")).strip(),
        finished_at=str(raw.get("finished_at", "")).strip(),
        cancel_requested=coerce_bool(raw.get("cancel_requested", False)),
        error=str(raw.get("error", "")).strip(),
        metadata=metadata,
    )


def entry_from_dict(raw: dict[str, Any]) -> QueueEntry:
    return _entry_from_dict(raw)


def load_entries(
    root: str | Path,
    *,
    entry_from_dict_fn: Callable[[dict[str, Any]], QueueEntry] = entry_from_dict,
    corrupt_error: type[Exception] = QueueStoreCorruptError,
) -> list[QueueEntry]:
    resolved_root = resolve_root_path(root)
    raw = load_json_list_file(
        queue_path(resolved_root),
        corrupt_error=corrupt_error,
        description="Queue file",
    )
    return [entry_from_dict_fn(item) for item in raw if isinstance(item, dict)]


def save_entries(
    root: str | Path,
    entries: Sequence[QueueEntry],
    *,
    entry_to_dict_fn: Callable[[QueueEntry], dict[str, Any]] = entry_to_dict,
) -> None:
    resolved_root = resolve_root_path(root)
    atomic_write_json(
        queue_path(resolved_root),
        [entry_to_dict_fn(item) for item in entries],
        ensure_ascii=True,
        indent=2,
    )
