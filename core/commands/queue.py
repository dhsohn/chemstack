"""CLI commands for the queue subsystem."""

from __future__ import annotations

import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..cancellation import CancelTargetError, cancel_target
from ..config import load_config
from ..queue_store import (
    DuplicateEntryError,
    cancel_all_pending,
    clear_terminal,
    enqueue,
    list_queue,
)
from ..queue_worker import QueueWorker, read_worker_pid
from ..statuses import QueueStatus
from ..types import QueueEntry
from ..telegram_notifier import notify_queue_enqueued_event
from ..types import QueueEnqueuedNotification
from ._helpers import _validate_reaction_dir

logger = logging.getLogger(__name__)


def _status_icon(status: str) -> str:
    return {
        QueueStatus.PENDING.value: "\u23f3",
        QueueStatus.RUNNING.value: "\u25b6",
        QueueStatus.COMPLETED.value: "\u2705",
        QueueStatus.FAILED.value: "\u274c",
        QueueStatus.CANCELLED.value: "\u26d4",
    }.get(status, "?")


def _format_elapsed(enqueued_at: str, finished_at: str | None) -> str:
    """Return a human-readable elapsed string since enqueue time."""
    from datetime import datetime, timezone
    try:
        start = datetime.fromisoformat(enqueued_at)
    except (ValueError, TypeError):
        return "-"
    if finished_at:
        try:
            end = datetime.fromisoformat(finished_at)
        except (ValueError, TypeError):
            end = datetime.now(timezone.utc)
    else:
        end = datetime.now(timezone.utc)
    secs = max(0, (end - start).total_seconds())
    if secs < 60:
        return f"{int(secs)}s"
    if secs < 3600:
        return f"{int(secs // 60)}m"
    if secs < 86400:
        return f"{int(secs // 3600)}h {int((secs % 3600) // 60)}m"
    return f"{int(secs // 86400)}d {int((secs % 86400) // 3600)}h"


def _print_queue_table(entries: list[QueueEntry]) -> None:
    """Print queue entries as a formatted terminal table."""
    headers = ["", "QUEUE ID", "STATUS", "PRI", "DIRECTORY", "ELAPSED"]
    rows: list[list[str]] = []
    for entry in entries:
        status = entry.get("status", "?")
        icon = _status_icon(status)
        elapsed = _format_elapsed(
            entry.get("enqueued_at", ""),
            entry.get("finished_at"),
        )
        rows.append([
            icon,
            entry.get("queue_id", "?"),
            status,
            str(entry.get("priority", "?")),
            Path(entry.get("reaction_dir", "")).name,
            elapsed,
        ])

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("\u2500" * (sum(widths) + 2 * (len(widths) - 1)))
    for row in rows:
        print(fmt.format(*row))


def _emit_entry(entry: QueueEntry, as_json: bool) -> None:
    if as_json:
        print(json.dumps(entry, ensure_ascii=True, indent=2))
        return
    icon = _status_icon(entry.get("status", ""))
    print(f"  {icon} {entry.get('queue_id', '?')}  {entry.get('status', '?'):10s}  "
          f"pri={entry.get('priority', '?')}  {Path(entry.get('reaction_dir', '')).name}")


# -- Subcommands ----------------------------------------------------------


def cmd_queue_add(args: Any) -> int:
    cfg = load_config(args.config)
    try:
        reaction_dir = _validate_reaction_dir(cfg, args.reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    try:
        entry = enqueue(
            allowed_root,
            str(reaction_dir),
            priority=args.priority,
            force=args.force,
        )
    except DuplicateEntryError as exc:
        logger.error("%s", exc)
        return 1

    if getattr(args, "json", False):
        print(json.dumps(entry, ensure_ascii=True, indent=2))
    else:
        print(f"Enqueued: {entry['queue_id']}")
        print(f"  reaction_dir: {entry['reaction_dir']}")
        print(f"  priority: {entry['priority']}")
        if args.force:
            print("  force: true (intentional re-run)")

    notification: QueueEnqueuedNotification = {
        "queue_id": entry["queue_id"],
        "reaction_dir": entry["reaction_dir"],
        "priority": entry["priority"],
        "force": entry.get("force", False),
        "enqueued_at": entry.get("enqueued_at", ""),
    }
    notify_queue_enqueued_event(cfg.telegram, notification)

    return 0


def cmd_queue_list(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()

    status_filter = getattr(args, "filter", None)
    entries = list_queue(allowed_root, status_filter=status_filter)

    if getattr(args, "json", False):
        print(json.dumps(entries, ensure_ascii=True, indent=2))
        return 0

    if not entries:
        print("Queue is empty.")
        return 0

    counts: dict[str, int] = {}
    for e in entries:
        s = e.get("status", "?")
        counts[s] = counts.get(s, 0) + 1
    summary_parts = [f"{counts.get(s.value, 0)} {s.value}" for s in QueueStatus if counts.get(s.value)]
    print(f"Queue: {len(entries)} total ({', '.join(summary_parts)})\n")

    _print_queue_table(entries)
    return 0


def cmd_queue_cancel(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()

    target = args.target
    if target == "all-pending":
        count = cancel_all_pending(allowed_root)
        print(f"Cancelled {count} pending entries.")
        return 0

    try:
        result = cancel_target(allowed_root, target)
    except CancelTargetError as exc:
        logger.error("%s", exc)
        return 1
    if result is None:
        logger.error("Cannot cancel: target not found or already in terminal state: %s", target)
        return 1

    if result.action == "cancelled":
        label = result.queue_id or target
        print(f"Cancelled: {label}")
    else:
        if result.source == "queue":
            print(f"Cancel requested for running job: {result.queue_id or target}")
            print("  The worker will terminate the ORCA process shortly.")
        else:
            print(f"Cancel requested for running simulation: {Path(result.reaction_dir).name}")
            if result.pid is not None:
                print(f"  pid: {result.pid}")
    return 0


def cmd_queue_clear(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    count = clear_terminal(allowed_root)
    print(f"Cleared {count} completed/failed/cancelled entries.")
    return 0


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()

    # Check if a worker is already running
    existing_pid = read_worker_pid(allowed_root)
    if existing_pid is not None:
        logger.error("Worker already running (pid=%d). Stop it first.", existing_pid)
        return 1

    if getattr(args, "daemon", False):
        return _start_daemon(args)

    worker = QueueWorker(
        cfg,
        args.config,
        max_concurrent=args.max_concurrent,
    )
    return worker.run()


def cmd_queue_stop(args: Any) -> int:
    """Stop a running worker daemon by sending SIGTERM."""
    import signal as _signal

    cfg = load_config(args.config)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()

    pid = read_worker_pid(allowed_root)
    if pid is None:
        print("No worker is running.")
        return 0

    try:
        import os
        os.kill(pid, _signal.SIGTERM)
        print(f"Sent SIGTERM to worker (pid={pid}).")
    except ProcessLookupError:
        print("Worker process not found (stale PID file).")
        try:
            (allowed_root / "queue_worker.pid").unlink()
        except OSError:
            pass
    except PermissionError:
        logger.error("Permission denied sending signal to pid=%d", pid)
        return 1
    return 0


def _start_daemon(args: Any) -> int:
    """Fork the worker into a background daemon process."""
    log_dir = Path(args.config).expanduser().resolve().parent
    if log_dir.name == "config":
        log_dir = log_dir.parent / "logs"
    else:
        log_dir = Path.home() / "orca_auto" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"queue_worker_{time.strftime('%Y%m%d_%H%M%S')}.log"
    cmd = [
        sys.executable, "-m", "core.cli",
        "--config", args.config,
        "--log-file", str(log_file),
        "queue", "worker",
        "--max-concurrent", str(args.max_concurrent),
    ]

    with log_file.open("w", encoding="utf-8") as handle:
        proc = subprocess.Popen(
            cmd,
            stdout=handle,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )

    time.sleep(0.3)
    if proc.poll() is not None:
        print(f"Worker failed to start. Check log: {log_file}")
        return 1

    print(f"Worker started (pid={proc.pid})")
    print(f"  log: {log_file}")
    return 0
