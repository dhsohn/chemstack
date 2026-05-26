"""Persistent task queue backed by a JSON file with file-based locking.

Queue entries are stored in ``{allowed_root}/queue.json`` and protected by
``{allowed_root}/queue.lock`` using the shared core queue store.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional, Sequence, TypeVar, cast

from chemstack.core.queue import store as _core_queue_store
from chemstack.core.queue.types import QueueStatus
from chemstack.core.utils.persistence import (
    now_utc_iso,
    timestamped_token,
)
from ..core.app_ids import CHEMSTACK_ORCA_APP_NAME
from .process_tracking import active_run_lock_pid, read_pid_file
from . import queue_entry_model as _queue_entry_model
from . import queue_reconciliation as _queue_reconciliation
from .state import load_state, report_json_path
from .types import QueueEntry

logger = logging.getLogger(__name__)

QUEUE_FILE_NAME = "queue.json"
WORKER_PID_FILE_NAME = "queue_worker.pid"
QUEUE_APP_NAME = CHEMSTACK_ORCA_APP_NAME
QUEUE_ENGINE = "orca"
QUEUE_TASK_KIND = "orca_run_inp"

# Terminal statuses — entries in these states are "done" and cannot transition.
_TERMINAL_STATUSES = frozenset(
    {
        QueueStatus.COMPLETED.value,
        QueueStatus.FAILED.value,
        QueueStatus.CANCELLED.value,
    }
)

# Active statuses — entries that occupy a slot or are waiting to run.
_ACTIVE_STATUSES = frozenset(
    {
        QueueStatus.PENDING.value,
        QueueStatus.RUNNING.value,
    }
)
_UNSET = object()
_QueueEntryT = TypeVar("_QueueEntryT", bound=QueueEntry)


class QueueStoreCorruptError(RuntimeError):
    """Raised when the queue file exists but cannot be safely loaded."""


def _now_iso() -> str:
    return now_utc_iso()


def _normalize_text(value: object | None) -> str:
    return _queue_entry_model.normalize_text(value)


def _normalize_entry(entry: QueueEntry) -> QueueEntry:
    return entry


def _entry_from_json_payload(raw: dict[str, Any]) -> QueueEntry:
    return _queue_entry_model.entry_from_json_payload(raw)


def _entry_metadata(
    *,
    reaction_dir: str,
    force: bool,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _queue_entry_model.entry_metadata(
        reaction_dir=reaction_dir,
        force=force,
        extra=extra,
    )


def queue_entry_metadata(entry: QueueEntry) -> dict[str, Any]:
    return _queue_entry_model.queue_entry_metadata(entry)


def queue_entry_run_id(entry: QueueEntry) -> str | None:
    return _queue_entry_model.queue_entry_run_id(entry)


def queue_entry_id(entry: QueueEntry) -> str:
    return _queue_entry_model.queue_entry_id(entry)


def queue_entry_task_id(entry: QueueEntry) -> str | None:
    return _queue_entry_model.queue_entry_task_id(entry)


def queue_entry_status(entry: QueueEntry) -> str:
    return _queue_entry_model.queue_entry_status(entry)


def queue_entry_reaction_dir(entry: QueueEntry) -> str:
    return _queue_entry_model.queue_entry_reaction_dir(entry)


def queue_entry_force(entry: QueueEntry) -> bool:
    return _queue_entry_model.queue_entry_force(entry)


def queue_entry_priority(entry: QueueEntry) -> int:
    return _queue_entry_model.queue_entry_priority(entry)


def queue_entry_app_name(entry: QueueEntry) -> str:
    return _queue_entry_model.queue_entry_app_name(entry)


# -- Low-level persistence ------------------------------------------------


def _load_entries(allowed_root: Path) -> list[QueueEntry]:
    return _core_queue_store.load_entries(
        allowed_root,
        entry_from_dict_fn=_entry_from_json_payload,
        corrupt_error=QueueStoreCorruptError,
    )


def _save_entries(
    allowed_root: Path,
    entries: Sequence[QueueEntry],
) -> None:
    _core_queue_store.save_entries(allowed_root, entries)


def _mutate_entries(
    allowed_root: Path,
    mutator: Any,
) -> Any:
    return _core_queue_store.mutate_entries(
        allowed_root,
        mutator,
        load_entries_fn=_load_entries,
        save_entries_fn=_save_entries,
    )


def _active_lock_pid(reaction_dir: Path) -> int | None:
    return active_run_lock_pid(
        reaction_dir,
        on_pid_reuse=lambda pid, expected_ticks, observed_ticks: logger.info(
            "Ignoring stale run.lock due to PID reuse: reaction_dir=%s pid=%d expected=%d observed=%s",
            reaction_dir,
            pid,
            expected_ticks,
            observed_ticks,
        ),
    )


def _read_worker_pid(allowed_root: Path) -> int | None:
    return read_pid_file(allowed_root / WORKER_PID_FILE_NAME)


def _load_report_payload(reaction_dir: Path) -> dict | None:
    return _queue_reconciliation.load_report_payload(
        reaction_dir,
        report_json_path_fn=report_json_path,
        logger=logger,
    )


def _terminal_report_data(
    reaction_dir: Path,
) -> tuple[str, str | None, str | None, str | None] | None:
    return _queue_reconciliation.terminal_report_data(
        reaction_dir,
        load_report_payload_fn=_load_report_payload,
    )


def _apply_terminal_reconciliation(
    entry: QueueEntry,
    *,
    status: str,
    run_id: str | None,
    finished_at: str | None,
    error: str | None = None,
) -> QueueEntry:
    return _queue_reconciliation.apply_terminal_reconciliation(
        entry,
        status=status,
        run_id=run_id,
        finished_at=finished_at,
        error=error,
        now_iso_fn=_now_iso,
    )


@dataclass(frozen=True)
class _QueueReconciliationDeps:
    load_state: Any
    queue_entry_id: Any
    queue_entry_reaction_dir: Any
    queue_entry_status: Any
    active_lock_pid: Any
    queue_lock: Any
    apply_terminal_reconciliation: Any
    load_entries: Any
    read_worker_pid: Any
    save_entries: Any
    terminal_report_data: Any


def _queue_reconciliation_deps() -> _QueueReconciliationDeps:
    return _QueueReconciliationDeps(
        load_state=load_state,
        queue_entry_id=queue_entry_id,
        queue_entry_reaction_dir=queue_entry_reaction_dir,
        queue_entry_status=queue_entry_status,
        active_lock_pid=_active_lock_pid,
        queue_lock=_core_queue_store.queue_lock,
        apply_terminal_reconciliation=_apply_terminal_reconciliation,
        load_entries=_load_entries,
        read_worker_pid=_read_worker_pid,
        save_entries=_save_entries,
        terminal_report_data=_terminal_report_data,
    )


def reconcile_orphaned_running_entries(
    allowed_root: Path,
    *,
    ignore_worker_pid: bool = False,
) -> int:
    """Reconcile queue entries stuck as running after worker/process loss.

    If the queue worker is not active, any queue entry still marked ``running``
    is orphaned. Prefer terminal state from ``run_state.json``; if that file has
    already been removed, fall back to ``run_report.json`` before re-queueing.
    """
    return _queue_reconciliation.reconcile_orphaned_running_entries(
        allowed_root,
        ignore_worker_pid=ignore_worker_pid,
        deps=_queue_reconciliation_deps(),
        logger=logger,
    )


# -- Duplicate detection --------------------------------------------------


class DuplicateEntryError(ValueError):
    """Raised when enqueueing a reaction_dir that already has an active entry."""

    def __init__(
        self,
        reaction_dir: str,
        existing: QueueEntry,
    ) -> None:
        self.existing = _normalize_entry(existing)
        status = queue_entry_status(self.existing) or "?"
        qid = queue_entry_id(self.existing) or "?"
        super().__init__(
            f"Reaction directory already queued: {reaction_dir} "
            f"(queue_id={qid}, status={status}). "
            f"Use --force to re-enqueue a completed/failed job, or cancel the existing entry first."
        )


def _find_active_entry(
    entries: Sequence[_QueueEntryT], reaction_dir: str
) -> Optional[_QueueEntryT]:
    """Find an active (pending/running) entry for the given reaction_dir."""
    for entry in entries:
        if (
            queue_entry_reaction_dir(entry) == reaction_dir
            and queue_entry_status(entry) in _ACTIVE_STATUSES
        ):
            return entry
    return None


def _find_terminal_entry(
    entries: Sequence[_QueueEntryT], reaction_dir: str
) -> Optional[_QueueEntryT]:
    """Find the most recent terminal entry for the given reaction_dir."""
    for entry in reversed(entries):
        if (
            queue_entry_reaction_dir(entry) == reaction_dir
            and queue_entry_status(entry) in _TERMINAL_STATUSES
        ):
            return entry
    return None


def _find_entry_by_queue_id(
    entries: Sequence[_QueueEntryT], queue_id: str
) -> Optional[_QueueEntryT]:
    """Find a queue entry by queue_id."""
    for entry in entries:
        if queue_entry_id(entry) == queue_id:
            return entry
    return None


# -- Public API -----------------------------------------------------------


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
    """Add a reaction directory to the queue.

    Duplicate prevention rules:
    - If the reaction_dir already has a **pending** or **running** entry,
      always reject (real duplicate / accidental re-submit).
    - If the reaction_dir has a **completed/failed/cancelled** entry:
      - Without ``force``: reject (accidental re-submit).
      - With ``force``: allow (intentional re-run / retry).
    """
    resolved = str(Path(reaction_dir).expanduser().resolve())
    reconcile_orphaned_running_entries(allowed_root)

    def append_entry(entries: list[QueueEntry]) -> tuple[QueueEntry, bool]:
        # Check for active duplicate — always blocked
        active = _find_active_entry(entries, resolved)
        if active is not None:
            raise DuplicateEntryError(resolved, active)

        # Check for terminal duplicate — blocked unless force
        if not force:
            terminal = _find_terminal_entry(entries, resolved)
            if terminal is not None:
                raise DuplicateEntryError(resolved, terminal)

        entry = QueueEntry(
            queue_id=timestamped_token("q", token_bytes=4),
            app_name=QUEUE_APP_NAME,
            task_id=_normalize_text(task_id) or timestamped_token("orca", token_bytes=4),
            task_kind=_normalize_text(task_kind) or QUEUE_TASK_KIND,
            engine=QUEUE_ENGINE,
            status=QueueStatus.PENDING,
            priority=priority,
            enqueued_at=_now_iso(),
            metadata=_entry_metadata(
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
    """Return the highest-priority pending entry (lowest priority number) and mark it running."""
    def dequeue(entries: list[QueueEntry]) -> tuple[QueueEntry | None, bool]:
        pending = [
            (i, e)
            for i, e in enumerate(entries)
            if queue_entry_status(e) == QueueStatus.PENDING.value
        ]
        if not pending:
            return None, False

        # Preserve FIFO order within the same priority using the persisted list order.
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
    return _update_terminal(allowed_root, queue_id, QueueStatus.COMPLETED.value, run_id=run_id)


def mark_failed(
    allowed_root: Path,
    queue_id: str,
    *,
    error: str | None = None,
    run_id: str | None = None,
) -> bool:
    """Mark a queue entry as failed."""
    return _update_terminal(
        allowed_root, queue_id, QueueStatus.FAILED.value, error=error, run_id=run_id
    )


def mark_cancelled(allowed_root: Path, queue_id: str) -> bool:
    """Mark a running queue entry as cancelled after the worker stops it."""
    return _update_running_entry_state(
        allowed_root,
        queue_id,
        status=QueueStatus.CANCELLED.value,
        finished_at=_now_iso(),
        cancel_requested=False,
    )


def requeue_running_entry(allowed_root: Path, queue_id: str) -> bool:
    """Return a running queue entry back to pending during worker shutdown."""
    return _update_running_entry_state(
        allowed_root,
        queue_id,
        status=QueueStatus.PENDING.value,
        started_at=None,
        cancel_requested=False,
    )


def cancel(allowed_root: Path, queue_id: str) -> Optional[QueueEntry]:
    """Cancel a queue entry.

    - pending → immediately set to cancelled.
    - running → set cancel_requested=True (worker will send SIGTERM).
    - terminal → no-op, returns None.
    """
    def cancel_entry(entries: list[QueueEntry]) -> tuple[QueueEntry | None, bool]:
        for idx, current in enumerate(entries):
            if current.queue_id != queue_id:
                continue

            if current.status == QueueStatus.PENDING:
                entry = _cancel_pending_entry(current, finished_at=_now_iso())
                entries[idx] = entry
                logger.info("Cancelled pending entry: %s", queue_id)
                return entry, True
            if current.status == QueueStatus.RUNNING:
                entry = replace(current, cancel_requested=True)
                entries[idx] = entry
                logger.info("Cancel requested for running entry: %s", queue_id)
                return entry, True

            logger.debug(
                "Cannot cancel entry in terminal state: %s (%s)",
                queue_id,
                current.status.value,
            )
            return None, False
        return None, False

    return cast(QueueEntry | None, _mutate_entries(allowed_root, cancel_entry))


def cancel_all_pending(allowed_root: Path) -> int:
    """Cancel all pending entries. Returns the number cancelled."""
    def cancel_pending(entries: list[QueueEntry]) -> tuple[int, bool]:
        count = 0
        now = _now_iso()
        for idx, current in enumerate(entries):
            if current.status == QueueStatus.PENDING:
                entries[idx] = _cancel_pending_entry(current, finished_at=now)
                count += 1
        return count, count > 0

    count = int(_mutate_entries(allowed_root, cancel_pending))
    logger.info("Cancelled %d pending entries", count)
    return count


def list_queue(
    allowed_root: Path,
    *,
    status_filter: str | None = None,
) -> list[QueueEntry]:
    """List queue entries, optionally filtered by status."""
    entries = cast(list[QueueEntry], _mutate_entries(allowed_root, lambda entries: (entries, False)))
    if status_filter:
        normalized_filter = _normalize_text(status_filter).lower()
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
            lambda entries: (_find_active_entry(entries, resolved), False),
        ),
    )


def get_cancel_requested(allowed_root: Path, queue_id: str) -> bool:
    """Check if a running entry has a cancel request."""
    return bool(
        _mutate_entries(
            allowed_root,
            lambda entries: (
                bool(entry.cancel_requested)
                if (entry := _find_entry_by_queue_id(entries, queue_id)) is not None
                else False,
                False,
            ),
        )
    )


def clear_terminal(allowed_root: Path, *, keep_last: int = 0) -> int:
    """Remove completed/failed/cancelled entries. Returns count removed.

    If keep_last > 0, keeps the N most recent terminal entries.
    """
    def clear_in_place(entries: list[QueueEntry]) -> tuple[int, bool]:
        active = [e for e in entries if e.status.value in _ACTIVE_STATUSES]
        terminal = [e for e in entries if e.status.value in _TERMINAL_STATUSES]
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


# -- Internal helpers -----------------------------------------------------


def _update_terminal(
    allowed_root: Path,
    queue_id: str,
    status: str,
    *,
    error: str | None = None,
    run_id: str | None = None,
) -> bool:
    def update(entries: list[QueueEntry]) -> tuple[bool, bool]:
        for idx, current in enumerate(entries):
            if current.queue_id != queue_id:
                continue
            metadata = dict(current.metadata)
            if error is not None:
                updated_error = error
            else:
                updated_error = current.error
            if run_id is not None:
                metadata["run_id"] = run_id
            entry = replace(
                current,
                status=QueueStatus(status),
                finished_at=_now_iso(),
                error=updated_error,
                metadata=metadata,
            )
            entries[idx] = entry
            logger.info("Entry %s → %s", queue_id, status)
            return True, True
        return False, False

    return bool(_mutate_entries(allowed_root, update))


def _cancel_pending_entry(entry: QueueEntry, *, finished_at: str) -> QueueEntry:
    return replace(entry, status=QueueStatus.CANCELLED, finished_at=finished_at)


def _update_running_entry_state(
    allowed_root: Path,
    queue_id: str,
    *,
    status: str,
    started_at: object = _UNSET,
    finished_at: object = _UNSET,
    cancel_requested: bool | None = None,
) -> bool:
    def update(entries: list[QueueEntry]) -> tuple[bool, bool]:
        for idx, current in enumerate(entries):
            if current.queue_id != queue_id:
                continue
            if current.status != QueueStatus.RUNNING:
                return False, False
            updates: dict[str, Any] = {"status": QueueStatus(status)}
            if started_at is not _UNSET:
                updates["started_at"] = cast(str | None, started_at) or ""
            if finished_at is not _UNSET:
                updates["finished_at"] = cast(str | None, finished_at) or ""
            if cancel_requested is not None:
                updates["cancel_requested"] = cancel_requested
            entry = replace(current, **updates)
            entries[idx] = entry
            logger.info("Entry %s → %s", queue_id, status)
            return True, True
        return False, False

    return bool(_mutate_entries(allowed_root, update))
