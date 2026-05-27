from __future__ import annotations

from collections.abc import Callable, Sequence
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Iterator

from chemstack.core.artifacts import QUEUE_FILE

from ..utils.lock import file_lock
from ..utils.persistence import (
    atomic_write_json,
    coerce_bool,
    coerce_int,
    load_json_list_file,
    now_utc_iso,
    resolve_root_path,
    timestamped_token,
)
from .types import QueueEntry, QueueStatus

QUEUE_FILE_NAME = QUEUE_FILE
QUEUE_LOCK_NAME = "queue.lock"
_ACTIVE_STATUSES = frozenset({QueueStatus.PENDING, QueueStatus.RUNNING})
_TERMINAL_STATUSES = frozenset({QueueStatus.COMPLETED, QueueStatus.FAILED, QueueStatus.CANCELLED})


class DuplicateQueueEntryError(RuntimeError):
    """Raised when an equivalent active task is already queued or running."""


class QueueStoreCorruptError(RuntimeError):
    """Raised when the queue file exists but cannot be safely loaded."""


def _queue_path(root: Path) -> Path:
    return root / QUEUE_FILE_NAME


def _lock_path(root: Path) -> Path:
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


@contextmanager
def queue_lock(root: str | Path, *, timeout_seconds: float = 10.0) -> Iterator[None]:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root), timeout_seconds=timeout_seconds):
        yield


def load_entries(
    root: str | Path,
    *,
    entry_from_dict_fn: Callable[[dict[str, Any]], QueueEntry] = entry_from_dict,
    corrupt_error: type[Exception] = QueueStoreCorruptError,
) -> list[QueueEntry]:
    resolved_root = resolve_root_path(root)
    raw = load_json_list_file(
        _queue_path(resolved_root),
        corrupt_error=corrupt_error,
        description="Queue file",
    )
    return [entry_from_dict_fn(item) for item in raw if isinstance(item, dict)]


def save_entries(root: str | Path, entries: Sequence[QueueEntry]) -> None:
    resolved_root = resolve_root_path(root)
    atomic_write_json(
        resolved_root / QUEUE_FILE_NAME,
        [entry_to_dict(item) for item in entries],
        ensure_ascii=True,
        indent=2,
    )


def list_queue(root: str | Path) -> list[QueueEntry]:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        return load_entries(resolved_root)


def mutate_entries(
    root: str | Path,
    mutator: Callable[[list[Any]], tuple[Any, bool]],
    *,
    load_entries_fn: Callable[[Path], list[Any]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[Any]], Any] | None = None,
) -> Any:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
        result, changed = mutator(entries)
        if changed:
            saver(resolved_root, entries)
        return result


def _entry_timestamp(entry: QueueEntry) -> str:
    return entry.finished_at or entry.started_at or entry.enqueued_at


def clear_terminal(root: str | Path, *, keep_last: int = 0) -> int:
    resolved_root = resolve_root_path(root)
    if not _queue_path(resolved_root).exists():
        return 0

    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        terminal_entries = [entry for entry in entries if entry.status in _TERMINAL_STATUSES]
        if not terminal_entries:
            return 0

        kept_terminal_ids: set[str] = set()
        if keep_last > 0:
            terminal_entries = sorted(
                terminal_entries,
                key=lambda entry: (_entry_timestamp(entry), entry.queue_id),
                reverse=True,
            )
            kept_terminal_ids = {entry.queue_id for entry in terminal_entries[:keep_last]}

        kept_entries = [
            entry
            for entry in entries
            if entry.status not in _TERMINAL_STATUSES or entry.queue_id in kept_terminal_ids
        ]
        removed_count = len(entries) - len(kept_entries)
        if removed_count > 0:
            save_entries(resolved_root, kept_entries)
        return removed_count


def enqueue(
    root: str | Path,
    *,
    app_name: str,
    task_id: str,
    task_kind: str,
    engine: str,
    priority: int = 10,
    metadata: dict[str, Any] | None = None,
) -> QueueEntry:
    resolved_root = resolve_root_path(root)
    entry = QueueEntry(
        queue_id=timestamped_token("q"),
        app_name=app_name.strip(),
        task_id=task_id.strip(),
        task_kind=task_kind.strip(),
        engine=engine.strip(),
        priority=int(priority),
        enqueued_at=now_utc_iso(),
        metadata=dict(metadata or {}),
    )

    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        for existing in entries:
            if existing.app_name != entry.app_name or existing.task_id != entry.task_id:
                continue
            if existing.status in _ACTIVE_STATUSES:
                raise DuplicateQueueEntryError(
                    f"Active queue entry already exists for app={entry.app_name} task_id={entry.task_id}"
                )
        entries.append(entry)
        save_entries(resolved_root, entries)
    return entry


def dequeue_next(root: str | Path) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        pending = [
            (entry.priority, entry.enqueued_at, index, entry)
            for index, entry in enumerate(entries)
            if entry.status == QueueStatus.PENDING and not entry.cancel_requested
        ]
        if not pending:
            return None
        _, _, index, current = min(pending, key=lambda item: (item[0], item[1], item[2]))
        updated = replace(current, status=QueueStatus.RUNNING, started_at=now_utc_iso())
        entries[index] = updated
        save_entries(resolved_root, entries)
        return updated


def request_cancel(root: str | Path, queue_id: str) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        for index, entry in enumerate(entries):
            if entry.queue_id != queue_id:
                continue
            if entry.status == QueueStatus.PENDING:
                updated = replace(
                    entry,
                    status=QueueStatus.CANCELLED,
                    cancel_requested=True,
                    finished_at=now_utc_iso(),
                )
            elif entry.status == QueueStatus.RUNNING:
                updated = replace(entry, cancel_requested=True)
            else:
                return None
            entries[index] = updated
            save_entries(resolved_root, entries)
            return updated
    return None


def get_cancel_requested(root: str | Path, queue_id: str) -> bool:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        for entry in entries:
            if entry.queue_id == queue_id:
                return bool(entry.cancel_requested)
    return False


def requeue_running_entry(root: str | Path, queue_id: str) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        for index, entry in enumerate(entries):
            if entry.queue_id != queue_id or entry.status != QueueStatus.RUNNING:
                continue
            updated = replace(
                entry,
                status=QueueStatus.PENDING,
                started_at="",
                cancel_requested=False,
                error="",
            )
            entries[index] = updated
            save_entries(resolved_root, entries)
            return updated
    return None


def _mark_status(
    root: str | Path,
    queue_id: str,
    *,
    status: QueueStatus,
    error: str = "",
    metadata_update: dict[str, Any] | None = None,
) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    with queue_lock(resolved_root):
        entries = load_entries(resolved_root)
        for index, entry in enumerate(entries):
            if entry.queue_id != queue_id:
                continue
            merged = dict(entry.metadata)
            if metadata_update:
                merged.update(metadata_update)
            updated = replace(
                entry,
                status=status,
                finished_at=now_utc_iso(),
                error=error.strip(),
                metadata=merged,
            )
            entries[index] = updated
            save_entries(resolved_root, entries)
            return updated
    return None


def mark_completed(
    root: str | Path, queue_id: str, *, metadata_update: dict[str, Any] | None = None
) -> QueueEntry | None:
    return _mark_status(
        root, queue_id, status=QueueStatus.COMPLETED, metadata_update=metadata_update
    )


def mark_failed(
    root: str | Path,
    queue_id: str,
    *,
    error: str,
    metadata_update: dict[str, Any] | None = None,
) -> QueueEntry | None:
    return _mark_status(
        root, queue_id, status=QueueStatus.FAILED, error=error, metadata_update=metadata_update
    )


def mark_cancelled(
    root: str | Path,
    queue_id: str,
    *,
    error: str = "",
    metadata_update: dict[str, Any] | None = None,
) -> QueueEntry | None:
    return _mark_status(
        root, queue_id, status=QueueStatus.CANCELLED, error=error, metadata_update=metadata_update
    )
