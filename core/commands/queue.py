"""CLI commands for the queue subsystem."""

from __future__ import annotations

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
    enqueue,
)
from ..queue_worker import QueueWorker, read_worker_pid
from ..telegram_notifier import notify_queue_enqueued_event
from ..types import QueueEnqueuedNotification
from ._helpers import _validate_reaction_dir

logger = logging.getLogger(__name__)


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
