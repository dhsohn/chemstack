"""CLI commands for the queue subsystem."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..cancellation import CancelTargetError, cancel_target
from ..config import load_config
from ..queue_store import cancel_all_pending
from ..queue_worker import QueueWorker, read_worker_pid, start_worker_daemon

logger = logging.getLogger(__name__)


# -- Subcommands ----------------------------------------------------------


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
    configured_max_concurrent = max(1, int(cfg.runtime.max_concurrent))

    # Check if a worker is already running
    existing_pid = read_worker_pid(allowed_root)
    if existing_pid is not None:
        logger.error("Worker already running (pid=%d). Stop it first.", existing_pid)
        return 1

    if getattr(args, "daemon", False):
        result = start_worker_daemon(args.config)
        if result.status != "started":
            print(f"Worker failed to start. Check log: {result.log_file}")
            return 1
        print(f"Worker started (pid={result.pid})")
        print(f"  log: {result.log_file}")
        return 0

    worker = QueueWorker(
        cfg,
        args.config,
        max_concurrent=configured_max_concurrent,
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
