"""CLI commands for the queue subsystem."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from chemstack.core.commands.queue import run_queue_worker_command
from chemstack.core.queue.worker import resolve_worker_auto_organize

from ..config import load_config
from ..queue_worker import QueueWorker, read_worker_pid

logger = logging.getLogger(__name__)


# -- Subcommands ----------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.orca.commands.queue")
    parser.add_argument("--config", required=True)
    parser.add_argument("--auto-organize", action="store_true")
    parser.add_argument("--no-auto-organize", action="store_true")
    return parser


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


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
