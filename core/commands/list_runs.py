"""Unified list command — display all simulations from queue and run state."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import load_config
from ..queue_store import clear_terminal, list_queue
from ..run_snapshot import RunSnapshot, collect_run_snapshots, elapsed_text
from ..state_store import STATE_FILE_NAME, load_state
from ..statuses import QueueStatus, RunStatus
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


def _collect_unified(allowed_root: Path) -> list[dict[str, str]]:
    """Merge queue entries and run snapshots into unified display rows."""
    queue_entries = list_queue(allowed_root)
    snapshots = collect_run_snapshots(allowed_root)

    # Index snapshots by resolved reaction_dir
    snap_by_dir: dict[str, RunSnapshot] = {}
    for s in snapshots:
        snap_by_dir[str(s.reaction_dir)] = s

    queued_dirs: set[str] = set()
    rows: list[dict[str, str]] = []

    # Process queue entries first
    for entry in queue_entries:
        rdir = entry.get("reaction_dir", "")
        queued_dirs.add(rdir)
        snap = snap_by_dir.get(rdir)

        status = entry.get("status", "?")
        icon = _status_icon(status)
        entry_id = entry.get("queue_id", "?")
        priority = str(entry.get("priority", "-"))
        directory = Path(rdir).name if rdir else "?"

        if snap:
            el = snap.elapsed_text
            inp = snap.selected_inp_name if snap.selected_inp_name != "-" else ""
            attempts = str(snap.attempts)
            # If queue says running but run_state says retrying, prefer retrying
            if status == QueueStatus.RUNNING.value and snap.status == RunStatus.RETRYING.value:
                status = RunStatus.RETRYING.value
                icon = _status_icon(status)
        else:
            el = _format_elapsed(
                entry.get("enqueued_at", ""),
                entry.get("finished_at"),
            )
            inp = ""
            attempts = "-"

        rows.append({
            "icon": icon,
            "id": entry_id,
            "status": status,
            "pri": priority,
            "dir": directory,
            "elapsed": el,
            "inp": inp,
            "attempts": attempts,
        })

    # Add standalone runs (not managed by queue)
    for snap in snapshots:
        rdir = str(snap.reaction_dir)
        if rdir in queued_dirs:
            continue

        status = snap.status
        rows.append({
            "icon": _status_icon(status),
            "id": snap.run_id or snap.name,
            "status": status,
            "pri": "-",
            "dir": snap.name,
            "elapsed": snap.elapsed_text,
            "inp": snap.selected_inp_name if snap.selected_inp_name != "-" else "",
            "attempts": str(snap.attempts),
        })

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

    # Summary line
    counts: dict[str, int] = {}
    for r in rows:
        s = r["status"]
        counts[s] = counts.get(s, 0) + 1

    summary_parts = [f"{counts[s]} {s}" for s in ALL_FILTER_STATUSES if counts.get(s)]
    print(f"Simulations: {len(rows)} total ({', '.join(summary_parts)})\n")

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
