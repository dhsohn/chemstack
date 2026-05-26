"""Persistent task queue backed by a JSON file with file-based locking.

Queue entries are stored in ``{allowed_root}/queue.json`` and protected by
``{allowed_root}/queue.lock`` using the shared ``lock_utils`` infrastructure.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, List, Optional, cast

from chemstack.core.queue import store as _core_queue_store
from chemstack.core.utils.persistence import atomic_write_json, now_utc_iso, timestamped_token

from .lock_utils import (
    acquire_file_lock,
    is_process_alive,
    parse_lock_info,
    process_start_ticks,
)
from ..core.app_ids import CHEMSTACK_ORCA_APP_NAME
from .process_tracking import active_run_lock_pid, current_process_lock_payload, read_pid_file
from . import queue_entry_model as _queue_entry_model
from . import queue_backend_adapter as _queue_backend_adapter_module
from . import queue_reconciliation as _queue_reconciliation
from .state import load_state, report_json_path
from .statuses import QueueStatus
from .types import QueueEntry

logger = logging.getLogger(__name__)

QUEUE_FILE_NAME = "queue.json"
QUEUE_LOCK_NAME = "queue.lock"
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


class QueueStoreCorruptError(RuntimeError):
    """Raised when the queue file exists but cannot be safely loaded."""


def _now_iso() -> str:
    return now_utc_iso()


def _normalize_text(value: object | None) -> str:
    return _queue_entry_model.normalize_text(value)


def _normalize_entry(entry: QueueEntry) -> QueueEntry:
    return _queue_entry_model.normalize_entry(entry)


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


def _queue_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_FILE_NAME


def _lock_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_LOCK_NAME


def _chem_core_queue_module() -> Any | None:
    return _core_queue_store


def _queue_backend_adapter() -> _queue_backend_adapter_module.QueueBackendAdapter:
    return _queue_backend_adapter_module.QueueBackendAdapter(
        queue_path=_queue_path,
        corrupt_error=QueueStoreCorruptError,
        atomic_write_json=atomic_write_json,
        backend_module=_chem_core_queue_module,
        normalize_entry=_normalize_entry,
    )


# -- Lock helpers ---------------------------------------------------------


def _queue_lock_active_error(lock_pid: int, lock_info: dict, lock_path: Path) -> RuntimeError:
    return RuntimeError(f"Queue lock is held by active process (pid={lock_pid}). Lock: {lock_path}")


def _queue_lock_unreadable_error(lock_path: Path) -> RuntimeError:
    return RuntimeError(f"Queue lock file unreadable. Remove manually: {lock_path}")


def _queue_lock_stale_remove_error(lock_pid: int, lock_path: Path, exc: OSError) -> RuntimeError:
    return RuntimeError(
        f"Failed to remove stale queue lock (pid={lock_pid}): {lock_path}. error={exc}"
    )


@contextmanager
def _acquire_queue_lock(allowed_root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    lp = _lock_path(allowed_root)
    payload = current_process_lock_payload()

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
    return list(_queue_backend_adapter().load_entries(allowed_root))


def _save_entries(allowed_root: Path, entries: List[QueueEntry]) -> None:
    _queue_backend_adapter().save_entries(allowed_root, entries)


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
) -> None:
    _queue_reconciliation.apply_terminal_reconciliation(
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
    _active_lock_pid: Any
    _acquire_queue_lock: Any
    _apply_terminal_reconciliation: Any
    _load_entries: Any
    _read_worker_pid: Any
    _save_entries: Any
    _terminal_report_data: Any


def _queue_reconciliation_deps() -> _QueueReconciliationDeps:
    return _QueueReconciliationDeps(
        load_state=load_state,
        queue_entry_id=queue_entry_id,
        queue_entry_reaction_dir=queue_entry_reaction_dir,
        queue_entry_status=queue_entry_status,
        _active_lock_pid=_active_lock_pid,
        _acquire_queue_lock=_acquire_queue_lock,
        _apply_terminal_reconciliation=_apply_terminal_reconciliation,
        _load_entries=_load_entries,
        _read_worker_pid=_read_worker_pid,
        _save_entries=_save_entries,
        _terminal_report_data=_terminal_report_data,
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
        if (
            queue_entry_reaction_dir(entry) == reaction_dir
            and queue_entry_status(entry) in _ACTIVE_STATUSES
        ):
            return entry
    return None


def _find_terminal_entry(entries: List[QueueEntry], reaction_dir: str) -> Optional[QueueEntry]:
    """Find the most recent terminal entry for the given reaction_dir."""
    for entry in reversed(entries):
        if (
            queue_entry_reaction_dir(entry) == reaction_dir
            and queue_entry_status(entry) in _TERMINAL_STATUSES
        ):
            return entry
    return None


def _find_entry_by_queue_id(entries: List[QueueEntry], queue_id: str) -> Optional[QueueEntry]:
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
            "queue_id": timestamped_token("q", token_bytes=4),
            "app_name": QUEUE_APP_NAME,
            "task_id": _normalize_text(task_id) or timestamped_token("orca", token_bytes=4),
            "task_kind": _normalize_text(task_kind) or QUEUE_TASK_KIND,
            "engine": QUEUE_ENGINE,
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
            "metadata": _entry_metadata(
                reaction_dir=resolved,
                force=force,
                extra=metadata,
            ),
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
            (i, e)
            for i, e in enumerate(entries)
            if queue_entry_status(e) == QueueStatus.PENDING.value
        ]
        if not pending:
            return None

        # Preserve FIFO order within the same priority using the persisted list order.
        pending.sort(key=lambda t: (queue_entry_priority(t[1]), t[0]))
        idx, entry = pending[0]
        entry = _normalize_entry(entry)

        entry["status"] = QueueStatus.RUNNING.value
        entry["started_at"] = _now_iso()
        entries[idx] = entry
        _save_entries(allowed_root, entries)

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
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        for idx, current in enumerate(entries):
            if current.get("queue_id") != queue_id:
                continue

            entry = _normalize_entry(current)
            status = entry.get("status", "")
            if status == QueueStatus.PENDING.value:
                entry = _cancel_pending_entry(entry, finished_at=_now_iso())
                entries[idx] = entry
                _save_entries(allowed_root, entries)
                logger.info("Cancelled pending entry: %s", queue_id)
                return entry
            if status == QueueStatus.RUNNING.value:
                entry["cancel_requested"] = True
                entries[idx] = entry
                _save_entries(allowed_root, entries)
                logger.info("Cancel requested for running entry: %s", queue_id)
                return entry

            logger.debug("Cannot cancel entry in terminal state: %s (%s)", queue_id, status)
            return None
        return None


def cancel_all_pending(allowed_root: Path) -> int:
    """Cancel all pending entries. Returns the number cancelled."""
    count = 0
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        now = _now_iso()
        for idx, current in enumerate(entries):
            if queue_entry_status(current) == QueueStatus.PENDING.value:
                entries[idx] = _cancel_pending_entry(current, finished_at=now)
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
    entries = [_normalize_entry(entry) for entry in entries]
    if status_filter:
        normalized_filter = _normalize_text(status_filter).lower()
        entries = [e for e in entries if queue_entry_status(e) == normalized_filter]
    return entries


def has_pending_entries(allowed_root: Path) -> bool:
    """Return True when at least one pending entry exists."""
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
    return any(queue_entry_status(entry) == QueueStatus.PENDING.value for entry in entries)


def get_active_entry_for_reaction_dir(allowed_root: Path, reaction_dir: str) -> QueueEntry | None:
    """Return the active queue entry for a reaction_dir, if one exists."""
    resolved = str(Path(reaction_dir).expanduser().resolve())
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
    entry = _find_active_entry(entries, resolved)
    if entry is None:
        return None
    return _normalize_entry(entry)


def get_cancel_requested(allowed_root: Path, queue_id: str) -> bool:
    """Check if a running entry has a cancel request."""
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
    entry = _find_entry_by_queue_id(entries, queue_id)
    if entry is None:
        return False
    return bool(_normalize_entry(entry).get("cancel_requested", False))


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
        for idx, current in enumerate(entries):
            if current.get("queue_id") != queue_id:
                continue
            entry = _normalize_entry(current)
            entry["status"] = status
            entry["finished_at"] = _now_iso()
            if error is not None:
                entry["error"] = error
            if run_id is not None:
                entry["run_id"] = run_id
            entries[idx] = entry
            _save_entries(allowed_root, entries)
            logger.info("Entry %s → %s", queue_id, status)
            return True
        return False


def _cancel_pending_entry(entry: QueueEntry, *, finished_at: str) -> QueueEntry:
    normalized = _normalize_entry(entry)
    normalized["status"] = QueueStatus.CANCELLED.value
    normalized["finished_at"] = finished_at
    return normalized


def _update_running_entry_state(
    allowed_root: Path,
    queue_id: str,
    *,
    status: str,
    started_at: object = _UNSET,
    finished_at: object = _UNSET,
    cancel_requested: bool | None = None,
) -> bool:
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        for idx, current in enumerate(entries):
            if queue_entry_id(current) != queue_id:
                continue
            if queue_entry_status(current) != QueueStatus.RUNNING.value:
                return False
            entry = _normalize_entry(current)
            entry["status"] = status
            if started_at is not _UNSET:
                entry["started_at"] = cast(Optional[str], started_at)
            if finished_at is not _UNSET:
                entry["finished_at"] = cast(Optional[str], finished_at)
            if cancel_requested is not None:
                entry["cancel_requested"] = cancel_requested
            entries[idx] = entry
            _save_entries(allowed_root, entries)
            logger.info("Entry %s → %s", queue_id, status)
            return True
        return False
