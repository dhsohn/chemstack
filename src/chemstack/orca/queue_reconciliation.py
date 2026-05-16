from __future__ import annotations

import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .statuses import QueueStatus, RunStatus
from .types import QueueEntry


def load_report_payload(
    reaction_dir: Path,
    *,
    report_json_path_fn: Callable[[Path], Path],
    logger: logging.Logger,
) -> dict | None:
    path = report_json_path_fn(reaction_dir)
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


def terminal_report_data(
    reaction_dir: Path,
    *,
    load_report_payload_fn: Callable[[Path], dict | None],
) -> tuple[str, str | None, str | None, str | None] | None:
    report = load_report_payload_fn(reaction_dir)
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


def apply_terminal_reconciliation(
    entry: QueueEntry,
    *,
    status: str,
    run_id: str | None,
    finished_at: str | None,
    error: str | None = None,
    now_iso_fn: Callable[[], str],
) -> None:
    entry["status"] = status
    entry["finished_at"] = finished_at or entry.get("finished_at") or now_iso_fn()
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
    deps: Any,
    logger: logging.Logger,
) -> int:
    """Reconcile queue entries stuck as running after worker/process loss."""
    if not ignore_worker_pid and deps._read_worker_pid(allowed_root) is not None:
        return 0

    changed = 0
    with deps._acquire_queue_lock(allowed_root):
        entries = deps._load_entries(allowed_root)
        for entry in entries:
            if deps.queue_entry_status(entry) != QueueStatus.RUNNING.value:
                continue
            changed += _reconcile_entry(entry, deps=deps, logger=logger)

        if changed:
            deps._save_entries(allowed_root, entries)
    return changed


def _reconcile_entry(
    entry: QueueEntry,
    *,
    deps: Any,
    logger: logging.Logger,
) -> int:
    rdir = deps.queue_entry_reaction_dir(entry)
    if not rdir:
        return 0
    reaction_dir = Path(rdir)

    if deps._active_lock_pid(reaction_dir) is not None:
        return 0

    queue_id = deps.queue_entry_id(entry) or "?"
    state = deps.load_state(reaction_dir)
    run_status = str(state.get("status", "")).strip().lower() if state else ""

    if state is not None and run_status == RunStatus.COMPLETED.value:
        _apply_state_terminal(
            entry,
            state,
            status=QueueStatus.COMPLETED.value,
            default_error=None,
            deps=deps,
        )
        logger.info("Reconciled orphaned entry %s -> completed", queue_id)
        return 1

    if state is not None and run_status == RunStatus.FAILED.value:
        _apply_state_terminal(
            entry,
            state,
            status=QueueStatus.FAILED.value,
            default_error="orphaned_worker_crash",
            deps=deps,
        )
        logger.info("Reconciled orphaned entry %s -> failed", queue_id)
        return 1

    report_data = deps._terminal_report_data(reaction_dir)
    if report_data is not None:
        status, run_id, finished_at, error = report_data
        deps._apply_terminal_reconciliation(
            entry,
            status=status,
            run_id=run_id,
            finished_at=finished_at,
            error=error,
        )
        logger.info("Reconciled orphaned entry %s -> %s (from run_report)", queue_id, status)
        return 1

    entry["status"] = QueueStatus.PENDING.value
    entry["started_at"] = None
    logger.info("Reconciled orphaned entry %s -> pending (re-queue)", queue_id)
    return 1


def _apply_state_terminal(
    entry: QueueEntry,
    state: dict[str, Any],
    *,
    status: str,
    default_error: str | None,
    deps: Any,
) -> None:
    final_result = state.get("final_result")
    final_dict = final_result if isinstance(final_result, dict) else {}
    error = None
    if default_error is not None:
        error = str(final_dict.get("reason", "")).strip() or default_error
    deps._apply_terminal_reconciliation(
        entry,
        status=status,
        run_id=str(state.get("run_id", "")).strip() or None,
        finished_at=str(final_dict.get("completed_at") or state.get("updated_at") or "").strip() or None,
        error=error,
    )
