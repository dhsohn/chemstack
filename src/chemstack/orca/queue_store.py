"""Persistent task queue backed by a JSON file with file-based locking.

Queue entries are stored in ``{allowed_root}/queue.json`` and protected by
``{allowed_root}/queue.lock`` using the shared ``lock_utils`` infrastructure.
"""

from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any, Iterator, List, Optional, cast

from .lock_utils import (
    acquire_file_lock,
    is_process_alive,
    parse_lock_info,
    process_start_ticks,
)
from ..core.app_ids import CHEMSTACK_ORCA_APP_NAME
from .persistence_utils import atomic_write_json, now_utc_iso, timestamped_token
from .process_tracking import active_run_lock_pid, current_process_lock_payload, read_pid_file
from .state_store import load_state, report_json_path
from .statuses import QueueStatus, RunStatus
from .types import QueueEntry

logger = logging.getLogger(__name__)

QUEUE_FILE_NAME = "queue.json"
QUEUE_LOCK_NAME = "queue.lock"
WORKER_PID_FILE_NAME = "queue_worker.pid"
QUEUE_APP_NAME = CHEMSTACK_ORCA_APP_NAME
QUEUE_ENGINE = "orca"
QUEUE_TASK_KIND = "orca_run_inp"

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
_UNSET = object()


def _now_iso() -> str:
    return now_utc_iso()


def _normalize_text(value: object | None) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _normalize_priority(value: object, *, default: int = 10) -> int:
    try:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return int(value)
    except (TypeError, ValueError):
        pass
    return default


def _normalize_optional_text(value: object | None) -> str | None:
    text = _normalize_text(value)
    if not text or text.lower() == "none":
        return None
    return text


def _normalize_metadata(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items()}


def _normalize_entry(entry: QueueEntry) -> QueueEntry:
    normalized = cast(QueueEntry, dict(entry))
    metadata = _normalize_metadata(normalized.get("metadata"))
    reaction_dir = _normalize_text(metadata.get("reaction_dir")) or _normalize_text(normalized.get("reaction_dir"))
    force = _normalize_bool(metadata.get("force", normalized.get("force", False)))
    run_id = _normalize_optional_text(metadata.get("run_id")) or _normalize_optional_text(normalized.get("run_id"))

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

    normalized["app_name"] = _normalize_text(normalized.get("app_name")) or QUEUE_APP_NAME
    task_id = _normalize_text(normalized.get("task_id")) or _normalize_text(normalized.get("queue_id"))
    if task_id:
        normalized["task_id"] = task_id
    normalized["task_kind"] = _normalize_text(normalized.get("task_kind")) or QUEUE_TASK_KIND
    normalized["engine"] = _normalize_text(normalized.get("engine")) or QUEUE_ENGINE
    normalized["priority"] = _normalize_priority(normalized.get("priority"), default=10)
    normalized["status"] = _normalize_text(normalized.get("status")).lower()
    normalized["started_at"] = _normalize_optional_text(normalized.get("started_at"))
    normalized["finished_at"] = _normalize_optional_text(normalized.get("finished_at"))
    normalized["error"] = _normalize_optional_text(normalized.get("error"))
    normalized["metadata"] = metadata
    return normalized


def _entry_metadata(
    *,
    reaction_dir: str,
    force: bool,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata = _normalize_metadata(extra)
    metadata.setdefault("reaction_dir", reaction_dir)
    metadata.setdefault("force", force)
    return metadata


def queue_entry_metadata(entry: QueueEntry) -> dict[str, Any]:
    return dict(_normalize_metadata(_normalize_entry(entry).get("metadata")))


def queue_entry_run_id(entry: QueueEntry) -> str | None:
    return _normalize_optional_text(_normalize_entry(entry).get("run_id"))


def queue_entry_id(entry: QueueEntry) -> str:
    return _normalize_text(_normalize_entry(entry).get("queue_id"))


def queue_entry_task_id(entry: QueueEntry) -> str | None:
    task_id = _normalize_text(_normalize_entry(entry).get("task_id"))
    return task_id or None


def queue_entry_status(entry: QueueEntry) -> str:
    return _normalize_text(_normalize_entry(entry).get("status")).lower()


def queue_entry_reaction_dir(entry: QueueEntry) -> str:
    normalized = _normalize_entry(entry)
    metadata = _normalize_metadata(normalized.get("metadata"))
    return _normalize_text(metadata.get("reaction_dir")) or _normalize_text(normalized.get("reaction_dir"))


def queue_entry_force(entry: QueueEntry) -> bool:
    normalized = _normalize_entry(entry)
    metadata = _normalize_metadata(normalized.get("metadata"))
    return _normalize_bool(metadata.get("force", normalized.get("force", False)))


def queue_entry_priority(entry: QueueEntry) -> int:
    return _normalize_priority(_normalize_entry(entry).get("priority"), default=10)


def queue_entry_app_name(entry: QueueEntry) -> str:
    return _normalize_text(_normalize_entry(entry).get("app_name")) or QUEUE_APP_NAME


def _queue_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_FILE_NAME


def _lock_path(allowed_root: Path) -> Path:
    return allowed_root / QUEUE_LOCK_NAME


@lru_cache(maxsize=1)
def _chem_core_queue_module() -> Any | None:
    try:
        return import_module("chemstack.core.queue.store")
    except ModuleNotFoundError as exc:
        if exc.name not in {
            "chemstack",
            "chemstack.core",
            "chemstack.core.queue",
            "chemstack.core.queue.store",
        }:
            raise
        repo_root = Path(__file__).resolve().parents[2] / "src"
        if not repo_root.is_dir():
            return None
        repo_root_text = str(repo_root)
        if repo_root_text not in sys.path:
            sys.path.insert(0, repo_root_text)
        try:
            return import_module("chemstack.core.queue.store")
        except ModuleNotFoundError:
            return None


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


def _to_chem_core_entry(entry: QueueEntry, *, backend: Any) -> Any:
    normalized = _normalize_entry(entry)
    metadata = queue_entry_metadata(normalized)
    run_id = queue_entry_run_id(normalized)
    if run_id is not None:
        metadata.setdefault("run_id", run_id)

    status_text = queue_entry_status(normalized) or QueueStatus.PENDING.value
    try:
        status = backend.QueueStatus(status_text)
    except ValueError:
        status = backend.QueueStatus(QueueStatus.PENDING.value)
    return backend.QueueEntry(
        queue_id=queue_entry_id(normalized),
        app_name=queue_entry_app_name(normalized),
        task_id=queue_entry_task_id(normalized) or queue_entry_id(normalized),
        task_kind=_normalize_text(normalized.get("task_kind")) or QUEUE_TASK_KIND,
        engine=_normalize_text(normalized.get("engine")) or QUEUE_ENGINE,
        status=status,
        priority=queue_entry_priority(normalized),
        enqueued_at=_normalize_text(normalized.get("enqueued_at")),
        started_at=_normalize_text(normalized.get("started_at")),
        finished_at=_normalize_text(normalized.get("finished_at")),
        cancel_requested=bool(normalized.get("cancel_requested", False)),
        error=_normalize_text(normalized.get("error")),
        metadata=metadata,
    )


def _backend_compat_entry_dict(entry: QueueEntry, *, backend: Any) -> dict[str, Any]:
    normalized = _normalize_entry(entry)
    serialized = dict(backend._entry_to_dict(_to_chem_core_entry(normalized, backend=backend)))

    reaction_dir = queue_entry_reaction_dir(normalized)
    if reaction_dir:
        serialized["reaction_dir"] = reaction_dir
    serialized["force"] = queue_entry_force(normalized)
    serialized["started_at"] = normalized.get("started_at")
    serialized["finished_at"] = normalized.get("finished_at")
    serialized["error"] = normalized.get("error")
    serialized["run_id"] = queue_entry_run_id(normalized)
    return serialized


def _save_entries(allowed_root: Path, entries: List[QueueEntry]) -> None:
    normalized_entries = [_normalize_entry(entry) for entry in entries]
    backend = _chem_core_queue_module()
    if backend is None:
        atomic_write_json(_queue_path(allowed_root), normalized_entries, ensure_ascii=True, indent=2)
        return
    serialized_entries = [
        _backend_compat_entry_dict(entry, backend=backend)
        for entry in normalized_entries
    ]
    atomic_write_json(_queue_path(allowed_root), serialized_entries, ensure_ascii=True, indent=2)


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
    path = report_json_path(reaction_dir)
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to parse run report: %s", path)
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def _terminal_report_data(reaction_dir: Path) -> tuple[str, str | None, str | None, str | None] | None:
    report = _load_report_payload(reaction_dir)
    if report is None:
        return None

    final_result = report.get("final_result")
    final_dict = final_result if isinstance(final_result, dict) else {}
    status = str(final_dict.get("status") or report.get("status") or "").strip().lower()
    if status not in {QueueStatus.COMPLETED.value, QueueStatus.FAILED.value}:
        return None

    run_id_text = str(report.get("run_id", "")).strip()
    finished_at_text = str(final_dict.get("completed_at") or report.get("updated_at") or "").strip()
    error_text = None
    if status == QueueStatus.FAILED.value:
        reason = str(final_dict.get("reason", "")).strip()
        if reason:
            error_text = reason

    return (
        status,
        run_id_text or None,
        finished_at_text or None,
        error_text,
    )


def _apply_terminal_reconciliation(
    entry: QueueEntry,
    *,
    status: str,
    run_id: str | None,
    finished_at: str | None,
    error: str | None = None,
) -> None:
    entry["status"] = status
    entry["finished_at"] = finished_at or entry.get("finished_at") or _now_iso()
    if run_id is not None:
        entry["run_id"] = run_id
    if error is not None:
        entry["error"] = error
    elif status == QueueStatus.COMPLETED.value:
        entry["error"] = None


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
    if not ignore_worker_pid and _read_worker_pid(allowed_root) is not None:
        return 0

    changed = 0
    with _acquire_queue_lock(allowed_root):
        entries = _load_entries(allowed_root)
        for entry in entries:
            if queue_entry_status(entry) != QueueStatus.RUNNING.value:
                continue

            rdir = queue_entry_reaction_dir(entry)
            if not rdir:
                continue
            reaction_dir = Path(rdir)

            if _active_lock_pid(reaction_dir) is not None:
                continue

            queue_id = queue_entry_id(entry) or "?"
            state = load_state(reaction_dir)
            run_status = str(state.get("status", "")).strip().lower() if state else ""

            if state is not None and run_status == RunStatus.COMPLETED.value:
                final_result = state.get("final_result")
                final_dict = final_result if isinstance(final_result, dict) else {}
                _apply_terminal_reconciliation(
                    entry,
                    status=QueueStatus.COMPLETED.value,
                    run_id=str(state.get("run_id", "")).strip() or None,
                    finished_at=str(final_dict.get("completed_at") or state.get("updated_at") or "").strip() or None,
                )
                logger.info("Reconciled orphaned entry %s -> completed", queue_id)
                changed += 1
                continue

            if state is not None and run_status == RunStatus.FAILED.value:
                final_result = state.get("final_result")
                final_dict = final_result if isinstance(final_result, dict) else {}
                _apply_terminal_reconciliation(
                    entry,
                    status=QueueStatus.FAILED.value,
                    run_id=str(state.get("run_id", "")).strip() or None,
                    finished_at=str(final_dict.get("completed_at") or state.get("updated_at") or "").strip() or None,
                    error=str(final_dict.get("reason", "")).strip() or "orphaned_worker_crash",
                )
                logger.info("Reconciled orphaned entry %s -> failed", queue_id)
                changed += 1
                continue

            report_data = _terminal_report_data(reaction_dir)
            if report_data is not None:
                status, run_id, finished_at, error = report_data
                _apply_terminal_reconciliation(
                    entry,
                    status=status,
                    run_id=run_id,
                    finished_at=finished_at,
                    error=error,
                )
                logger.info("Reconciled orphaned entry %s -> %s (from run_report)", queue_id, status)
                changed += 1
                continue

            entry["status"] = QueueStatus.PENDING.value
            entry["started_at"] = None
            logger.info("Reconciled orphaned entry %s -> pending (re-queue)", queue_id)
            changed += 1

        if changed:
            _save_entries(allowed_root, entries)
    return changed


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
        if queue_entry_reaction_dir(entry) == reaction_dir and queue_entry_status(entry) in _ACTIVE_STATUSES:
            return entry
    return None


def _find_terminal_entry(entries: List[QueueEntry], reaction_dir: str) -> Optional[QueueEntry]:
    """Find the most recent terminal entry for the given reaction_dir."""
    for entry in reversed(entries):
        if queue_entry_reaction_dir(entry) == reaction_dir and queue_entry_status(entry) in _TERMINAL_STATUSES:
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
            "queue_id": timestamped_token("q"),
            "app_name": QUEUE_APP_NAME,
            "task_id": _normalize_text(task_id) or timestamped_token("orca"),
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
            (i, e) for i, e in enumerate(entries)
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
    return _update_terminal(allowed_root, queue_id, QueueStatus.FAILED.value, error=error, run_id=run_id)


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
