"""Small ORCA adapter over the shared queue primitives."""

from __future__ import annotations

import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Optional, cast

from chemstack.core.queue import store as _queue_store
from chemstack.core.queue.types import QueueEntry, QueueStatus
from chemstack.core.utils.persistence import now_utc_iso, timestamped_token

from .queue_entries import (
    ACTIVE_STATUSES,
    QUEUE_APP_NAME,
    QUEUE_ENGINE,
    QUEUE_FILE_NAME,
    QUEUE_TASK_KIND,
    TERMINAL_STATUSES,
    entry_from_json_payload,
    entry_metadata,
    find_active_entry,
    find_entry_by_queue_id,
    find_terminal_entry,
    normalize_text,
    queue_entry_app_name,
    queue_entry_force,
    queue_entry_id,
    queue_entry_metadata,
    queue_entry_priority,
    queue_entry_reaction_dir,
    queue_entry_run_id,
    queue_entry_status,
    queue_entry_task_id,
)
from .queue_orphans import reconcile_orphaned_running_entries
logger = logging.getLogger(__name__)

_UNSET = object()

__all__ = [
    "ACTIVE_STATUSES",
    "DuplicateEntryError",
    "QUEUE_APP_NAME",
    "QUEUE_ENGINE",
    "QUEUE_FILE_NAME",
    "QUEUE_TASK_KIND",
    "TERMINAL_STATUSES",
    "cancel",
    "cancel_pending_entry",
    "clear_terminal",
    "dequeue_next",
    "enqueue",
    "get_active_entry_for_reaction_dir",
    "get_cancel_requested",
    "has_pending_entries",
    "list_queue",
    "mark_cancelled",
    "mark_completed",
    "mark_failed",
    "queue_entry_app_name",
    "queue_entry_force",
    "queue_entry_id",
    "queue_entry_metadata",
    "queue_entry_priority",
    "queue_entry_reaction_dir",
    "queue_entry_run_id",
    "queue_entry_status",
    "queue_entry_task_id",
    "reconcile_orphaned_running_entries",
    "requeue_running_entry",
    "update_running_entry_state",
    "update_terminal",
]


def _now_iso() -> str:
    return now_utc_iso()


def _load_entries(allowed_root: Path) -> list[QueueEntry]:
    return _queue_store.load_entries(
        allowed_root,
        entry_from_dict_fn=entry_from_json_payload,
        corrupt_error=_queue_store.QueueStoreCorruptError,
    )


def _mutate_entries(
    allowed_root: Path,
    mutator: Any,
) -> Any:
    return _queue_store.mutate_entries(
        allowed_root,
        mutator,
        load_entries_fn=_load_entries,
        save_entries_fn=_queue_store.save_entries,
    )


def _entry_index(entries: list[QueueEntry], queue_id: str) -> int | None:
    for idx, current in enumerate(entries):
        if current.queue_id == queue_id:
            return idx
    return None


def _mutate_entry(
    allowed_root: Path,
    queue_id: str,
    updater: Any,
    *,
    missing_result: Any,
) -> Any:
    def mutate(entries: list[QueueEntry]) -> tuple[Any, bool]:
        idx = _entry_index(entries, queue_id)
        if idx is None:
            return missing_result, False

        result, updated_entry = updater(entries[idx])
        if updated_entry is None:
            return result, False

        entries[idx] = updated_entry
        return result, True

    return _mutate_entries(allowed_root, mutate)


class DuplicateEntryError(ValueError):
    """Raised when enqueueing a reaction_dir that already has an active entry."""

    def __init__(
        self,
        reaction_dir: str,
        existing: QueueEntry,
    ) -> None:
        self.existing = existing
        status = queue_entry_status(self.existing) or "?"
        qid = queue_entry_id(self.existing) or "?"
        super().__init__(
            f"Reaction directory already queued: {reaction_dir} "
            f"(queue_id={qid}, status={status}). "
            f"Use --force to re-enqueue a completed/failed job, or cancel the existing entry first."
        )


def enqueue(
    allowed_root: Path,
    reaction_dir: str,
    *,
    priority: int = 10,
    force: bool = False,
    task_id: str | None = None,
    task_kind: str = QUEUE_TASK_KIND,
    metadata: dict[str, Any] | None = None,
) -> QueueEntry:
    """Add a reaction directory to the ORCA queue."""
    resolved = str(Path(reaction_dir).expanduser().resolve())
    reconcile_orphaned_running_entries(allowed_root)

    def append_entry(entries: list[QueueEntry]) -> tuple[QueueEntry, bool]:
        active = find_active_entry(entries, resolved)
        if active is not None:
            raise DuplicateEntryError(resolved, active)

        if not force:
            terminal = find_terminal_entry(entries, resolved)
            if terminal is not None:
                raise DuplicateEntryError(resolved, terminal)

        entry = QueueEntry(
            queue_id=timestamped_token("q", token_bytes=4),
            app_name=QUEUE_APP_NAME,
            task_id=normalize_text(task_id) or timestamped_token("orca", token_bytes=4),
            task_kind=normalize_text(task_kind) or QUEUE_TASK_KIND,
            engine=QUEUE_ENGINE,
            status=QueueStatus.PENDING,
            priority=priority,
            enqueued_at=_now_iso(),
            metadata=entry_metadata(
                reaction_dir=resolved,
                force=force,
                extra=metadata,
            ),
        )
        entries.append(entry)
        return entry, True

    entry = cast(QueueEntry, _mutate_entries(allowed_root, append_entry))
    logger.info("Enqueued: %s (queue_id=%s, force=%s)", resolved, entry.queue_id, force)
    return entry


def dequeue_next(allowed_root: Path) -> Optional[QueueEntry]:
    """Return the highest-priority pending entry and mark it running."""

    def dequeue(entries: list[QueueEntry]) -> tuple[QueueEntry | None, bool]:
        pending = [
            (i, e)
            for i, e in enumerate(entries)
            if queue_entry_status(e) == QueueStatus.PENDING.value
        ]
        if not pending:
            return None, False

        pending.sort(key=lambda t: (queue_entry_priority(t[1]), t[0]))
        idx, entry = pending[0]
        entry = replace(entry, status=QueueStatus.RUNNING, started_at=_now_iso())
        entries[idx] = entry
        return entry, True

    entry = cast(QueueEntry | None, _mutate_entries(allowed_root, dequeue))
    if entry is None:
        return None
    logger.info(
        "Dequeued: %s (queue_id=%s)",
        queue_entry_reaction_dir(entry),
        queue_entry_id(entry),
    )
    return entry


def mark_completed(allowed_root: Path, queue_id: str, *, run_id: str | None = None) -> bool:
    """Mark a queue entry as completed."""
    return update_terminal(allowed_root, queue_id, QueueStatus.COMPLETED.value, run_id=run_id)


def mark_failed(
    allowed_root: Path,
    queue_id: str,
    *,
    error: str | None = None,
    run_id: str | None = None,
) -> bool:
    """Mark a queue entry as failed."""
    return update_terminal(
        allowed_root, queue_id, QueueStatus.FAILED.value, error=error, run_id=run_id
    )


def mark_cancelled(allowed_root: Path, queue_id: str) -> bool:
    """Mark a running queue entry as cancelled after the worker stops it."""
    return update_running_entry_state(
        allowed_root,
        queue_id,
        status=QueueStatus.CANCELLED.value,
        finished_at=_now_iso(),
        cancel_requested=False,
    )


def requeue_running_entry(allowed_root: Path, queue_id: str) -> bool:
    """Return a running queue entry back to pending during worker shutdown."""
    return update_running_entry_state(
        allowed_root,
        queue_id,
        status=QueueStatus.PENDING.value,
        started_at=None,
        cancel_requested=False,
    )


def cancel(allowed_root: Path, queue_id: str) -> Optional[QueueEntry]:
    """Cancel a queue entry."""

    def cancel_entry(current: QueueEntry) -> tuple[QueueEntry | None, QueueEntry | None]:
        if current.status == QueueStatus.PENDING:
            entry = cancel_pending_entry(current, finished_at=_now_iso())
            logger.info("Cancelled pending entry: %s", queue_id)
            return entry, entry
        if current.status == QueueStatus.RUNNING:
            entry = replace(current, cancel_requested=True)
            logger.info("Cancel requested for running entry: %s", queue_id)
            return entry, entry

        logger.debug(
            "Cannot cancel entry in terminal state: %s (%s)",
            queue_id,
            current.status.value,
        )
        return None, None

    return cast(
        QueueEntry | None,
        _mutate_entry(allowed_root, queue_id, cancel_entry, missing_result=None),
    )


def list_queue(
    allowed_root: Path,
    *,
    status_filter: str | None = None,
) -> list[QueueEntry]:
    """List queue entries, optionally filtered by status."""
    entries = cast(
        list[QueueEntry],
        _mutate_entries(allowed_root, lambda entries: (entries, False)),
    )
    if status_filter:
        normalized_filter = normalize_text(status_filter).lower()
        entries = [e for e in entries if queue_entry_status(e) == normalized_filter]
    return entries


def has_pending_entries(allowed_root: Path) -> bool:
    """Return True when at least one pending entry exists."""
    return bool(
        _mutate_entries(
            allowed_root,
            lambda entries: (
                any(entry.status == QueueStatus.PENDING for entry in entries),
                False,
            ),
        )
    )


def get_active_entry_for_reaction_dir(allowed_root: Path, reaction_dir: str) -> QueueEntry | None:
    """Return the active queue entry for a reaction_dir, if one exists."""
    resolved = str(Path(reaction_dir).expanduser().resolve())
    return cast(
        QueueEntry | None,
        _mutate_entries(
            allowed_root,
            lambda entries: (find_active_entry(entries, resolved), False),
        ),
    )


def get_cancel_requested(allowed_root: Path, queue_id: str) -> bool:
    """Check if a running entry has a cancel request."""
    return bool(
        _mutate_entries(
            allowed_root,
            lambda entries: (
                bool(entry.cancel_requested)
                if (entry := find_entry_by_queue_id(entries, queue_id)) is not None
                else False,
                False,
            ),
        )
    )


def clear_terminal(allowed_root: Path, *, keep_last: int = 0) -> int:
    """Remove completed/failed/cancelled entries. Returns count removed."""

    def clear_in_place(entries: list[QueueEntry]) -> tuple[int, bool]:
        active = [e for e in entries if e.status.value in ACTIVE_STATUSES]
        terminal = [e for e in entries if e.status.value in TERMINAL_STATUSES]
        if keep_last > 0:
            terminal.sort(key=lambda e: e.finished_at or "", reverse=True)
            kept = terminal[:keep_last]
            removed_count = len(terminal) - len(kept)
            next_entries = active + kept
        else:
            removed_count = len(terminal)
            next_entries = active
        if removed_count:
            entries[:] = next_entries
        return removed_count, removed_count > 0

    removed_count = int(_mutate_entries(allowed_root, clear_in_place))
    logger.info("Cleared %d terminal entries", removed_count)
    return removed_count


def update_terminal(
    allowed_root: Path,
    queue_id: str,
    status: str,
    *,
    error: str | None = None,
    run_id: str | None = None,
) -> bool:
    def update(current: QueueEntry) -> tuple[bool, QueueEntry]:
        metadata = dict(current.metadata)
        if run_id is not None:
            metadata["run_id"] = run_id
        entry = replace(
            current,
            status=QueueStatus(status),
            finished_at=_now_iso(),
            error=error if error is not None else current.error,
            metadata=metadata,
        )
        logger.info("Entry %s -> %s", queue_id, status)
        return True, entry

    return bool(_mutate_entry(allowed_root, queue_id, update, missing_result=False))


def cancel_pending_entry(entry: QueueEntry, *, finished_at: str) -> QueueEntry:
    return replace(entry, status=QueueStatus.CANCELLED, finished_at=finished_at)


def update_running_entry_state(
    allowed_root: Path,
    queue_id: str,
    *,
    status: str,
    started_at: object = _UNSET,
    finished_at: object = _UNSET,
    cancel_requested: bool | None = None,
) -> bool:
    def update(current: QueueEntry) -> tuple[bool, QueueEntry | None]:
        if current.status != QueueStatus.RUNNING:
            return False, None
        updates: dict[str, Any] = {"status": QueueStatus(status)}
        if started_at is not _UNSET:
            updates["started_at"] = cast(str | None, started_at) or ""
        if finished_at is not _UNSET:
            updates["finished_at"] = cast(str | None, finished_at) or ""
        if cancel_requested is not None:
            updates["cancel_requested"] = cancel_requested
        entry = replace(current, **updates)
        logger.info("Entry %s -> %s", queue_id, status)
        return True, entry

    return bool(_mutate_entry(allowed_root, queue_id, update, missing_result=False))
