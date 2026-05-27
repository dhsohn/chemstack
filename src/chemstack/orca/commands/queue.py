"""CLI commands for the queue subsystem."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from chemstack.core.commands.queue import run_queue_worker_command
from chemstack.core.queue.worker import resolve_worker_auto_organize

from ..cancellation import CancelTargetError, cancel_target
from ..config import load_config
from ..queue_adapter import cancel_all_pending
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
    return run_queue_worker_command(
        args,
        load_config_fn=load_config,
        config_path_fn=lambda worker_args: str(worker_args.config),
        existing_pid_fn=lambda cfg: read_worker_pid(
            Path(cfg.runtime.allowed_root).expanduser().resolve()
        ),
        existing_pid_report_fn=lambda pid: logger.error(
            "Worker already running (pid=%d). Check the active systemd service.",
            pid,
        ),
        max_concurrent_fn=lambda cfg: max(1, int(cfg.runtime.max_concurrent)),
        worker_factory=lambda cfg, config_path, **kwargs: QueueWorker(
            cfg,
            config_path,
            auto_organize=resolve_worker_auto_organize(cfg, args),
            **kwargs,
        ),
    )
