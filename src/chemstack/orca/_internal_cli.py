from __future__ import annotations

import argparse
from typing import Callable

from .commands import queue as queue_cmd
from .commands._helpers import default_config_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.orca._internal_cli")
    parser.add_argument("--config", default=default_config_path(), help="Path to chemstack.yaml")
    sub = parser.add_subparsers(dest="command", required=True)

    queue_parser = sub.add_parser("queue", help="Internal ORCA queue management")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    worker = queue_sub.add_parser("worker", help="Run the ORCA queue worker")
    auto_group = worker.add_mutually_exclusive_group()
    auto_group.add_argument(
        "--auto-organize",
        action="store_true",
        help="Automatically move completed runs into organized_root after execution",
    )
    auto_group.add_argument(
        "--no-auto-organize",
        action="store_true",
        help="Disable automatic organization for this worker invocation",
    )

    cancel = queue_sub.add_parser("cancel", help="Cancel a queued or running job")
    cancel.add_argument("target", help="queue_id, job_dir, run_id, or 'all-pending'")
    return parser


def cmd_queue_cancel(args: argparse.Namespace) -> int:
    return int(queue_cmd.cmd_queue_cancel(args))


def cmd_queue_worker(args: argparse.Namespace) -> int:
    return int(queue_cmd.cmd_queue_worker(args))


def _cmd_queue(args: argparse.Namespace) -> int:
    if args.queue_command == "worker":
        return int(cmd_queue_worker(args))
    if args.queue_command == "cancel":
        return int(cmd_queue_cancel(args))
    raise ValueError(f"Unsupported queue subcommand: {args.queue_command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    command_map: dict[str, Callable[[argparse.Namespace], int]] = {
        "queue": _cmd_queue,
    }
    return int(command_map[args.command](args))


if __name__ == "__main__":
    raise SystemExit(main())
