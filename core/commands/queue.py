"""CLI commands for the queue subsystem."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from ..cancellation import CancelTargetError, cancel_target
from ..config import load_config
from ..queue_store import cancel_all_pending
from ..queue_worker import QueueWorker, read_worker_pid

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
    auto_organize = bool(getattr(cfg, "behavior", None) and cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False

    # Check if a worker is already running
    existing_pid = read_worker_pid(allowed_root)
    if existing_pid is not None:
        logger.error(
            "Worker already running (pid=%d). Check the active systemd service or foreground worker.",
            existing_pid,
        )
        return 1

    worker = QueueWorker(
        cfg,
        args.config,
        max_concurrent=configured_max_concurrent,
        auto_organize=auto_organize,
    )
    return worker.run()
