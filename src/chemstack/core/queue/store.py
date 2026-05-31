from __future__ import annotations

from collections.abc import Callable, Collection, Sequence
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Iterator, TypeVar

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
_QueueEntryT = TypeVar("_QueueEntryT", bound=QueueEntry)
_MutationResultT = TypeVar("_MutationResultT")

QueueDuplicatePolicy = Callable[[Sequence[QueueEntry], QueueEntry], None]
DuplicateErrorFactory = Callable[[str, QueueEntry], Exception]


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


def _status_value(status: QueueStatus | str) -> str:
    if isinstance(status, QueueStatus):
        return status.value
    return str(status).strip().lower()


def _status_values(statuses: Collection[QueueStatus | str]) -> set[str]:
    return {_status_value(status) for status in statuses}


def find_entry_by_key(
    entries: Sequence[_QueueEntryT],
    key: str,
    *,
    key_fn: Callable[[_QueueEntryT], str],
    statuses: Collection[QueueStatus | str],
    reverse: bool = False,
) -> _QueueEntryT | None:
    """Find the first entry matching a duplicate key and status set."""
    status_values = _status_values(statuses)
    candidates = reversed(entries) if reverse else entries
    for entry in candidates:
        if key_fn(entry) == key and _status_value(entry.status) in status_values:
            return entry
    return None


def _default_duplicate_key_error(key: str, existing: QueueEntry) -> DuplicateQueueEntryError:
    status = _status_value(existing.status) or "?"
    qid = existing.queue_id or "?"
    return DuplicateQueueEntryError(
        f"Queue entry already exists for key={key} (queue_id={qid}, status={status})"
    )


def reject_duplicate_entry_key(
    entries: Sequence[QueueEntry],
    *,
    key: str,
    key_fn: Callable[[QueueEntry], str],
    force: bool = False,
    active_statuses: Collection[QueueStatus | str] = _ACTIVE_STATUSES,
    terminal_statuses: Collection[QueueStatus | str] = _TERMINAL_STATUSES,
    error_factory: DuplicateErrorFactory | None = None,
) -> None:
    """Reject duplicate active entries and, unless forced, terminal entries.

    This helper lets adapters define duplicate identity by any stable key while
    sharing the active/terminal/force policy used by queue-backed workloads.
    """
    make_error = error_factory or _default_duplicate_key_error
    active = find_entry_by_key(
        entries,
        key,
        key_fn=key_fn,
        statuses=active_statuses,
    )
    if active is not None:
        raise make_error(key, active)

    if force:
        return

    terminal = find_entry_by_key(
        entries,
        key,
        key_fn=key_fn,
        statuses=terminal_statuses,
        reverse=True,
    )
    if terminal is not None:
        raise make_error(key, terminal)


def reject_active_task_duplicate(
    entries: Sequence[QueueEntry],
    entry: QueueEntry,
) -> None:
    for existing in entries:
        if existing.app_name != entry.app_name or existing.task_id != entry.task_id:
            continue
        if existing.status in _ACTIVE_STATUSES:
            raise DuplicateQueueEntryError(
                f"Active queue entry already exists for app={entry.app_name} task_id={entry.task_id}"
            )


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


def list_queue(
    root: str | Path,
    *,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
) -> list[QueueEntry]:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    with queue_lock(resolved_root):
        return loader(resolved_root)


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


def mutate_entry_by_id(
    root: str | Path,
    queue_id: str,
    updater: Callable[[QueueEntry], tuple[_MutationResultT, QueueEntry | None]],
    *,
    missing_result: _MutationResultT,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> _MutationResultT:
    def mutate(entries: list[QueueEntry]) -> tuple[_MutationResultT, bool]:
        for index, entry in enumerate(entries):
            if entry.queue_id != queue_id:
                continue
            result, updated_entry = updater(entry)
            if updated_entry is None:
                return result, False
            entries[index] = updated_entry
            return result, True
        return missing_result, False

    return mutate_entries(
        root,
        mutate,
        load_entries_fn=load_entries_fn,
        save_entries_fn=save_entries_fn,
    )


def _entry_timestamp(entry: QueueEntry) -> str:
    return entry.finished_at or entry.started_at or entry.enqueued_at


def clear_terminal(
    root: str | Path,
    *,
    keep_last: int = 0,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> int:
    resolved_root = resolve_root_path(root)
    if not _queue_path(resolved_root).exists():
        return 0
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries

    with queue_lock(resolved_root):
        entries = loader(resolved_root)
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
            saver(resolved_root, kept_entries)
        return removed_count


def enqueue_entry(
    root: str | Path,
    entry: QueueEntry,
    *,
    duplicate_policy: QueueDuplicatePolicy | None = None,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    reject_duplicate = duplicate_policy or reject_active_task_duplicate

    with queue_lock(resolved_root):
        entries = loader(resolved_root)
        reject_duplicate(entries, entry)
        entries.append(entry)
        saver(resolved_root, entries)
    return entry


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
    return enqueue_entry(resolved_root, entry)


def dequeue_next(
    root: str | Path,
    *,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
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
        saver(resolved_root, entries)
        return updated


def request_cancel(
    root: str | Path,
    queue_id: str,
    *,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
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
            saver(resolved_root, entries)
            return updated
    return None


def get_cancel_requested(
    root: str | Path,
    queue_id: str,
    *,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
) -> bool:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
        for entry in entries:
            if entry.queue_id == queue_id:
                return bool(entry.cancel_requested)
    return False


def requeue_running_entry(
    root: str | Path,
    queue_id: str,
    *,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
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
            saver(resolved_root, entries)
            return updated
    return None


def _mark_status(
    root: str | Path,
    queue_id: str,
    *,
    status: QueueStatus,
    error: str = "",
    metadata_update: dict[str, Any] | None = None,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    resolved_root = resolve_root_path(root)
    loader = load_entries_fn or load_entries
    saver = save_entries_fn or save_entries
    with queue_lock(resolved_root):
        entries = loader(resolved_root)
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
            saver(resolved_root, entries)
            return updated
    return None


def mark_completed(
    root: str | Path,
    queue_id: str,
    *,
    metadata_update: dict[str, Any] | None = None,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    return _mark_status(
        root,
        queue_id,
        status=QueueStatus.COMPLETED,
        metadata_update=metadata_update,
        load_entries_fn=load_entries_fn,
        save_entries_fn=save_entries_fn,
    )


def mark_failed(
    root: str | Path,
    queue_id: str,
    *,
    error: str,
    metadata_update: dict[str, Any] | None = None,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    return _mark_status(
        root,
        queue_id,
        status=QueueStatus.FAILED,
        error=error,
        metadata_update=metadata_update,
        load_entries_fn=load_entries_fn,
        save_entries_fn=save_entries_fn,
    )


def mark_cancelled(
    root: str | Path,
    queue_id: str,
    *,
    error: str = "",
    metadata_update: dict[str, Any] | None = None,
    load_entries_fn: Callable[[Path], list[QueueEntry]] | None = None,
    save_entries_fn: Callable[[Path, Sequence[QueueEntry]], Any] | None = None,
) -> QueueEntry | None:
    return _mark_status(
        root,
        queue_id,
        status=QueueStatus.CANCELLED,
        error=error,
        metadata_update=metadata_update,
        load_entries_fn=load_entries_fn,
        save_entries_fn=save_entries_fn,
    )
