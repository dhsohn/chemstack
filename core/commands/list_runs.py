"""Unified list command — display all simulations from queue and run state."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import load_config
from ..queue_store import clear_terminal, list_queue, reconcile_orphaned_running_entries
from ..run_snapshot import RunSnapshot, collect_run_snapshots, elapsed_text
from ..state_store import STATE_FILE_NAME, load_state
from ..statuses import QueueStatus, RunStatus
from ..types import QueueEntry
from ._helpers import _to_resolved_local

logger = logging.getLogger(__name__)

_STATUS_ICONS = {
    QueueStatus.PENDING.value: "\u23f3",
    RunStatus.CREATED.value: "\U0001f195",
    "running": "\u25b6",
    RunStatus.RETRYING.value: "\U0001f504",
    QueueStatus.COMPLETED.value: "\u2705",
    QueueStatus.FAILED.value: "\u274c",
    QueueStatus.CANCELLED.value: "\u26d4",
}

ALL_FILTER_STATUSES = [
    "pending", "created", "running", "retrying",
    "completed", "failed", "cancelled",
]

_TERMINAL_RUN_STATUSES = frozenset({RunStatus.COMPLETED.value, RunStatus.FAILED.value})
_ACTIVE_QUEUE_STATUSES = frozenset({QueueStatus.PENDING.value, QueueStatus.RUNNING.value})


def _status_icon(status: str) -> str:
    return _STATUS_ICONS.get(status, "?")


def _format_elapsed(start_iso: str, end_iso: str | None) -> str:
    """Return a human-readable elapsed string."""
    try:
        start = datetime.fromisoformat(start_iso)
    except (ValueError, TypeError):
        return "-"
    if end_iso:
        try:
            end = datetime.fromisoformat(end_iso)
        except (ValueError, TypeError):
            end = datetime.now(timezone.utc)
    else:
        end = datetime.now(timezone.utc)
    secs = max(0, (end - start).total_seconds())
    return elapsed_text(secs)


def _resolved_path_text(path_text: str) -> str:
    text = str(path_text).strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def _optional_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def _match_queue_snapshot(
    entry: QueueEntry,
    *,
    snapshot_by_run_id: dict[str, RunSnapshot],
    snapshot_by_dir: dict[str, RunSnapshot],
) -> RunSnapshot | None:
    run_id = _optional_text(entry.get("run_id"))
    if run_id:
        return snapshot_by_run_id.get(run_id)

    status = str(entry.get("status", "")).strip().lower()
    if status not in _ACTIVE_QUEUE_STATUSES:
        return None

    reaction_dir = _resolved_path_text(entry.get("reaction_dir", ""))
    if not reaction_dir:
        return None
    return snapshot_by_dir.get(reaction_dir)


def _queue_entry_represents_snapshot(entry: QueueEntry, snapshot: RunSnapshot | None) -> bool:
    if snapshot is None:
        return False

    run_id = _optional_text(entry.get("run_id"))
    if run_id and run_id == snapshot.run_id:
        return True

    status = str(entry.get("status", "")).strip().lower()
    if status not in _ACTIVE_QUEUE_STATUSES:
        return False

    reaction_dir = _resolved_path_text(entry.get("reaction_dir", ""))
    return bool(reaction_dir) and reaction_dir == _resolved_path_text(str(snapshot.reaction_dir))


def _build_queue_row(entry: QueueEntry, snapshot: RunSnapshot | None) -> tuple[dict[str, str], bool]:
    status = str(entry.get("status", "?"))
    icon = _status_icon(status)

    if snapshot is not None:
        elapsed = snapshot.elapsed_text
        inp = snapshot.selected_inp_name if snapshot.selected_inp_name != "-" else ""
        attempts = str(snapshot.attempts)
        if status == QueueStatus.RUNNING.value and snapshot.status != RunStatus.RUNNING.value:
            status = snapshot.status
            icon = _status_icon(status)
    else:
        elapsed = _format_elapsed(
            entry.get("enqueued_at", ""),
            entry.get("finished_at"),
        )
        inp = ""
        attempts = "-"

    row = {
        "icon": icon,
        "id": str(entry.get("queue_id", "?")),
        "status": status,
        "pri": str(entry.get("priority", "-")),
        "dir": Path(str(entry.get("reaction_dir", ""))).name if entry.get("reaction_dir", "") else "?",
        "elapsed": elapsed,
        "inp": inp,
        "attempts": attempts,
    }
    return row, _queue_entry_represents_snapshot(entry, snapshot)


def _build_standalone_row(snapshot: RunSnapshot) -> dict[str, str]:
    status = snapshot.status
    return {
        "icon": _status_icon(status),
        "id": snapshot.run_id or snapshot.name,
        "status": status,
        "pri": "-",
        "dir": snapshot.name,
        "elapsed": snapshot.elapsed_text,
        "inp": snapshot.selected_inp_name if snapshot.selected_inp_name != "-" else "",
        "attempts": str(snapshot.attempts),
    }


def _collect_unified(allowed_root: Path) -> list[dict[str, str]]:
    """Merge queue entries and run snapshots into unified display rows."""
    reconcile_orphaned_running_entries(allowed_root)
    queue_entries = list_queue(allowed_root)
    snapshots = collect_run_snapshots(allowed_root)

    snapshot_by_dir: dict[str, RunSnapshot] = {}
    snapshot_by_run_id = {
        s.run_id: s
        for s in snapshots
        if s.run_id
    }
    for snapshot in snapshots:
        snapshot_by_dir[_resolved_path_text(str(snapshot.reaction_dir))] = snapshot

    represented_snapshot_keys: set[str] = set()
    rows: list[dict[str, str]] = []

    # Process queue entries first
    for entry in queue_entries:
        snap = _match_queue_snapshot(
            entry,
            snapshot_by_run_id=snapshot_by_run_id,
            snapshot_by_dir=snapshot_by_dir,
        )
        row, represents_snapshot = _build_queue_row(entry, snap)
        rows.append(row)
        if snap is not None and represents_snapshot:
            represented_snapshot_keys.add(snap.key)

    # Add standalone runs (not managed by queue)
    for snap in snapshots:
        if snap.key in represented_snapshot_keys:
            continue
        rows.append(_build_standalone_row(snap))

    return rows


def _print_table(rows: list[dict[str, str]]) -> None:
    """Print unified rows as a formatted terminal table."""
    headers = ["", "ID", "STATUS", "PRI", "DIRECTORY", "ELAPSED", "INP", "ATTEMPTS"]
    keys = ["icon", "id", "status", "pri", "dir", "elapsed", "inp", "attempts"]

    table_rows = [[r[k] for k in keys] for r in rows]

    widths = [len(h) for h in headers]
    for row in table_rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("\u2500" * (sum(widths) + 2 * (len(widths) - 1)))
    for row in table_rows:
        print(fmt.format(*row))


def _summary_text(rows: list[dict[str, str]]) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        status = row["status"]
        counts[status] = counts.get(status, 0) + 1

    summary_parts = [f"{counts[status]} {status}" for status in ALL_FILTER_STATUSES if counts.get(status)]
    return f"Simulations: {len(rows)} total ({', '.join(summary_parts)})"


def cmd_list(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)

    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    # Handle 'clear' subaction
    if getattr(args, "action", None) == "clear":
        return _cmd_clear(allowed_root)

    rows = _collect_unified(allowed_root)
    filter_status = getattr(args, "filter", None)

    if filter_status:
        rows = [r for r in rows if r["status"] == filter_status]

    if not rows:
        print("No simulations found.")
        return 0

    print(f"{_summary_text(rows)}\n")

    _print_table(rows)
    return 0


def _cmd_clear(allowed_root: Path) -> int:
    """Remove completed/failed/cancelled simulations from the list."""
    # 1. Clear terminal queue entries
    queue_count = clear_terminal(allowed_root)

    # 2. Clear standalone terminal run_state.json files
    run_count = 0
    for state_path in allowed_root.rglob(STATE_FILE_NAME):
        state = load_state(state_path.parent)
        if state is None:
            continue
        status = str(state.get("status", "")).strip().lower()
        if status in _TERMINAL_RUN_STATUSES:
            try:
                state_path.unlink()
                run_count += 1
            except OSError as exc:
                logger.warning("Failed to remove %s: %s", state_path, exc)

    total = queue_count + run_count
    print(f"Cleared {total} completed/failed/cancelled entries.")
    if queue_count:
        print(f"  queue entries: {queue_count}")
    if run_count:
        print(f"  run states: {run_count}")
    return 0
