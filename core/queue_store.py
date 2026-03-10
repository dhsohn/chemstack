"""Persistent task queue backed by a JSON file with file-based locking.

Queue entries are stored in ``{allowed_root}/queue.json`` and protected by
``{allowed_root}/queue.lock`` using the shared ``lock_utils`` infrastructure.
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, cast
from uuid import uuid4

from .lock_utils import (
    acquire_file_lock,
    current_process_start_ticks,
    is_process_alive,
    parse_lock_info,
    process_start_ticks,
)
from .state_store import atomic_write_text
from .statuses import QueueStatus
from .types import QueueEntry

logger = logging.getLogger(__name__)

QUEUE_FILE_NAME = "queue.json"
QUEUE_LOCK_NAME = "queue.lock"

# Terminal statuses — entries in these states are "done" and cannot transition.
_TERMINAL_STATUSES = frozenset({
    QueueStatus.COMPLETED.value,
    QueueStatus.FAILED.value,
    QueueStatus.CANCELLED.value,
})

# Active statuses — entries that occupy a slot or are waiting to run.
_ACTIVE_STATUSES = frozenset({
    QueueStatus.PENDING.value,
    QueueStatus.RUNNING.value,
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _queue_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_FILE_NAME


def _lock_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_LOCK_NAME


# -- Lock helpers ---------------------------------------------------------


def _queue_lock_active_error(lock_pid: int, lock_info: dict, lock_path: Path) -> RuntimeError:
    return RuntimeError(
        f"Queue lock is held by active process (pid={lock_pid}). Lock: {lock_path}"
    )


def _queue_lock_unreadable_error(lock_path: Path) -> RuntimeError:
    return RuntimeError(f"Queue lock file unreadable. Remove manually: {lock_path}")


def _queue_lock_stale_remove_error(lock_pid: int, lock_path: Path, exc: OSError) -> RuntimeError:
    return RuntimeError(
        f"Failed to remove stale queue lock (pid={lock_pid}): {lock_path}. error={exc}"
    )


@contextmanager
def _acquire_queue_lock(allowed_root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    lp = _lock_path(allowed_root)
    payload = {"pid": os.getpid(), "started_at": _now_iso()}
    ticks = current_process_start_ticks()
    if ticks is not None:
        payload["process_start_ticks"] = ticks

    with acquire_file_lock(
        lock_path=lp,
        lock_payload_obj=payload,
        parse_lock_info_fn=parse_lock_info,
        is_process_alive_fn=is_process_alive,
        process_start_ticks_fn=process_start_ticks,
        logger=logger,
        acquired_log_template="Queue lock acquired: %s",
        released_log_template="Queue lock released: %s",
        stale_pid_reuse_log_template=(
            "Stale queue lock (PID reuse, pid=%d, expected=%d, observed=%d): %s"
        ),
        stale_lock_log_template="Stale queue lock (pid=%d), removing: %s",
        timeout_seconds=timeout_seconds,
        active_lock_error_builder=_queue_lock_active_error,
        unreadable_lock_error_builder=_queue_lock_unreadable_error,
        stale_remove_error_builder=_queue_lock_stale_remove_error,
    ):
        yield


# -- Low-level persistence ------------------------------------------------


def _load_entries(allowed_root: Path) -> List[QueueEntry]:
    qp = _queue_path(allowed_root)
    if not qp.exists():
        return []
    try:
        raw = json.loads(qp.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to parse queue file, starting fresh: %s", qp)
        return []
    if not isinstance(raw, list):
        return []
    return [cast(QueueEntry, e) for e in raw if isinstance(e, dict)]


def _save_entries(allowed_root: Path, entries: List[QueueEntry]) -> None:
    qp = _queue_path(allowed_root)
    atomic_write_text(qp, json.dumps(entries, ensure_ascii=True, indent=2))


# -- Duplicate detection --------------------------------------------------


class DuplicateEntryError(ValueError):
    """Raised when enqueueing a reaction_dir that already has an active entry."""

    def __init__(self, reaction_dir: str, existing: QueueEntry) -> None:
        self.existing = existing
        status = existing.get("status", "?")
        qid = existing.get("queue_id", "?")
        super().__init__(
            f"Reaction directory already queued: {reaction_dir} "
            f"(queue_id={qid}, status={status}). "
            f"Use --force to re-enqueue a completed/failed job, or cancel the existing entry first."
        )


def _find_active_entry(entries: List[QueueEntry], reaction_dir: str) -> Optional[QueueEntry]:
    """Find an active (pending/running) entry for the given reaction_dir."""
    for entry in entries:
        if entry.get("reaction_dir") == reaction_dir and entry.get("status") in _ACTIVE_STATUSES:
            return entry
    return None


def _find_terminal_entry(entries: List[QueueEntry], reaction_dir: str) -> Optional[QueueEntry]:
    """Find the most recent terminal entry for the given reaction_dir."""
    for entry in reversed(entries):
        if entry.get("reaction_dir") == reaction_dir and entry.get("status") in _TERMINAL_STATUSES:
            return entry
    return None


# -- Public API -----------------------------------------------------------


def enqueue(
    allowed_root: Path,
    reaction_dir: str,
    *,
    priority: int = 10,
    force: bool = False,
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

    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)

        # Check for active duplicate — always blocked
        active = _find_active_entry(entries, resolved)
        if active is not None:
            raise DuplicateEntryError(resolved, active)

        # Check for terminal duplicate — blocked unless force
        if not force:
            terminal = _find_terminal_entry(entries, resolved)
            if terminal is not None:
                raise DuplicateEntryError(resolved, terminal)

        entry: QueueEntry = {
            "queue_id": f"q_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}",
            "reaction_dir": resolved,
            "status": QueueStatus.PENDING.value,
            "priority": priority,
            "enqueued_at": _now_iso(),
            "started_at": None,
            "finished_at": None,
            "cancel_requested": False,
            "run_id": None,
            "error": None,
            "force": force,
        }
        entries.append(entry)
        _save_entries(allowed_root, entries)

    logger.info("Enqueued: %s (queue_id=%s, force=%s)", resolved, entry["queue_id"], force)
    return entry


def dequeue_next(allowed_root: Path) -> Optional[QueueEntry]:
    """Return the highest-priority pending entry (lowest priority number) and mark it running."""
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        pending = [
            (i, e) for i, e in enumerate(entries)
            if e.get("status") == QueueStatus.PENDING.value
        ]
        if not pending:
            return None

        # Sort by priority (lower = higher priority), then enqueued_at
        pending.sort(key=lambda t: (t[1].get("priority", 10), t[1].get("enqueued_at", "")))
        idx, entry = pending[0]

        entry["status"] = QueueStatus.RUNNING.value
        entry["started_at"] = _now_iso()
        entries[idx] = entry
        _save_entries(allowed_root, entries)

    logger.info("Dequeued: %s (queue_id=%s)", entry.get("reaction_dir"), entry.get("queue_id"))
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
    return _update_terminal(allowed_root, queue_id, QueueStatus.FAILED.value, error=error, run_id=run_id)


def cancel(allowed_root: Path, queue_id: str) -> Optional[QueueEntry]:
    """Cancel a queue entry.

    - pending → immediately set to cancelled.
    - running → set cancel_requested=True (worker will send SIGTERM).
    - terminal → no-op, returns None.
    """
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        for entry in entries:
            if entry.get("queue_id") != queue_id:
                continue

            status = entry.get("status", "")
            if status == QueueStatus.PENDING.value:
                entry["status"] = QueueStatus.CANCELLED.value
                entry["finished_at"] = _now_iso()
                _save_entries(allowed_root, entries)
                logger.info("Cancelled pending entry: %s", queue_id)
                return entry
            elif status == QueueStatus.RUNNING.value:
                entry["cancel_requested"] = True
                _save_entries(allowed_root, entries)
                logger.info("Cancel requested for running entry: %s", queue_id)
                return entry
            else:
                logger.debug("Cannot cancel entry in terminal state: %s (%s)", queue_id, status)
                return None
    return None


def cancel_all_pending(allowed_root: Path) -> int:
    """Cancel all pending entries. Returns the number cancelled."""
    count = 0
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        now = _now_iso()
        for entry in entries:
            if entry.get("status") == QueueStatus.PENDING.value:
                entry["status"] = QueueStatus.CANCELLED.value
                entry["finished_at"] = now
                count += 1
        if count:
            _save_entries(allowed_root, entries)
    logger.info("Cancelled %d pending entries", count)
    return count


def list_queue(
    allowed_root: Path,
    *,
    status_filter: str | None = None,
) -> List[QueueEntry]:
    """List queue entries, optionally filtered by status."""
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
    if status_filter:
        entries = [e for e in entries if e.get("status") == status_filter]
    return entries


def get_cancel_requested(allowed_root: Path, queue_id: str) -> bool:
    """Check if a running entry has a cancel request."""
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
    for entry in entries:
        if entry.get("queue_id") == queue_id:
            return bool(entry.get("cancel_requested", False))
    return False


def clear_terminal(allowed_root: Path, *, keep_last: int = 0) -> int:
    """Remove completed/failed/cancelled entries. Returns count removed.

    If keep_last > 0, keeps the N most recent terminal entries.
    """
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        active = [e for e in entries if e.get("status") in _ACTIVE_STATUSES]
        terminal = [e for e in entries if e.get("status") in _TERMINAL_STATUSES]

        if keep_last > 0:
            # Sort terminal by finished_at descending, keep newest
            terminal.sort(key=lambda e: e.get("finished_at") or "", reverse=True)
            kept = terminal[:keep_last]
            removed_count = len(terminal) - len(kept)
            entries = active + kept
        else:
            removed_count = len(terminal)
            entries = active

        _save_entries(allowed_root, entries)
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
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        for entry in entries:
            if entry.get("queue_id") != queue_id:
                continue
            entry["status"] = status
            entry["finished_at"] = _now_iso()
            if error is not None:
                entry["error"] = error
            if run_id is not None:
                entry["run_id"] = run_id
            _save_entries(allowed_root, entries)
            logger.info("Entry %s → %s", queue_id, status)
            return True
    return False
