from __future__ import annotations

import argparse
from typing import Callable

from chemstack import cli_queue

from .commands._helpers import default_config_path
from .commands import init as init_cmd
from .commands.list_runs import cmd_list as _engine_cmd_list
from .commands.monitor import cmd_monitor
from .commands import organize as organize_cmd
from .commands.queue import cmd_queue_cancel as _engine_cmd_queue_cancel
from .commands import run_inp as run_inp_cmd
from .commands import summary as summary_cmd
from .cli_logging import configure_logging


def cmd_init(args: argparse.Namespace) -> int:
    return int(init_cmd.cmd_init(args))


def cmd_run_inp(args: argparse.Namespace) -> int:
    return int(run_inp_cmd.cmd_run_inp(args))


def cmd_list(args: argparse.Namespace) -> int:
    if getattr(args, "action", None) == "clear":
        return int(_engine_cmd_list(args))
    status = getattr(args, "filter", None)
    return cli_queue.cmd_queue_list(
        argparse.Namespace(
            action=None,
            workflow_root=None,
            chemstack_config=args.config,
            limit=0,
            refresh=False,
            engine=["orca"],
            status=[status] if status else None,
            kind=["job"],
            json=False,
        )
    )


def cmd_queue_cancel(args: argparse.Namespace) -> int:
    target = str(getattr(args, "target", "")).strip()
    if target == "all-pending":
        return int(_engine_cmd_queue_cancel(args))
    return cli_queue.cmd_queue_cancel(
        argparse.Namespace(
            target=target,
            workflow_root=None,
            chemstack_config=args.config,
            json=False,
        )
    )


def cmd_organize(args: argparse.Namespace) -> int:
    return int(organize_cmd.cmd_organize(args))


def cmd_summary(args: argparse.Namespace) -> int:
    return int(summary_cmd.cmd_summary(args))


def cmd_queue(args: argparse.Namespace) -> int:
    _queue_sub_map: dict[str, Callable[[argparse.Namespace], int]] = {
        "cancel": cmd_queue_cancel,
    }
    handler = _queue_sub_map.get(args.queue_command)
    if handler is None:
        print("Usage: python -m chemstack.orca.cli queue cancel <target>")
        return 1
    return int(handler(args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.orca.cli")
    parser.add_argument(
        "--config",
        default=default_config_path(),
        help="Path to chemstack.yaml",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument("--log-file", default=None, help="Write logs to file (with rotation, max 10MB x 5)")
    sub = parser.add_subparsers(dest="command", required=True)

    def _add_run_dir_parser(name: str, *, help_text: str) -> argparse.ArgumentParser:
        run_parser = sub.add_parser(name, help=help_text)
        run_parser.add_argument("path", help="Job directory under the configured allowed_root")
        run_parser.add_argument("--force", action="store_true", help="Force re-run even if existing output is completed")
        run_parser.add_argument("--priority", type=int, default=10, help="Queue priority when submission is enqueued (lower = higher, default 10)")
        run_parser.add_argument("--max-cores", type=int, default=None, help="Override max cores recorded for this queued run")
        run_parser.add_argument("--max-memory-gb", type=int, default=None, help="Override max memory (GB) recorded for this queued run")
        return run_parser

    init = sub.add_parser("init", help="Interactively create or update the config file")
    init.add_argument("--force", action="store_true", help="Overwrite existing config without confirmation")

    _add_run_dir_parser("run-dir", help_text="Queue an ORCA job directory")

    list_cmd = sub.add_parser("list", help="Show status of all simulations (queue + standalone)")
    list_cmd.add_argument("action", nargs="?", default=None, choices=["clear"],
                          help="Optional action: 'clear' removes completed/failed/cancelled entries")
    list_cmd.add_argument("--filter", default=None,
                          choices=["pending", "created", "running", "retrying", "completed", "failed", "cancelled"],
                          help="Filter by status")

    sub.add_parser("monitor", help="Send Telegram alerts for newly discovered DFT results or scan failures")
    summary = sub.add_parser("summary", help="Send a periodic Telegram digest of current run/workstation state")
    summary.add_argument("--no-send", action="store_true", default=False, help="Print summary without sending Telegram")

    organize = sub.add_parser("organize")
    organize.add_argument("--reaction-dir", default=None, help="Single job directory to organize")
    organize.add_argument("--root", default=None, help="Root directory to scan (mutually exclusive with --reaction-dir)")
    organize.add_argument("--apply", action="store_true", default=False, help="Actually move files (default is dry-run)")
    organize.add_argument("--rebuild-index", action="store_true", default=False, help="Rebuild JSONL index from organized directories")

    # -- queue subcommand with its own sub-subcommands --------------------
    queue_parser = sub.add_parser("queue", help="Manage the task queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    q_cancel = queue_sub.add_parser("cancel", help="Cancel a queued or running job")
    q_cancel.add_argument("target", help="queue_id, job_dir, or run_id to cancel; or 'all-pending'")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    configure_logging(args)

    command_map: dict[str, Callable[[argparse.Namespace], int]] = {
        "init": cmd_init,
        "run-dir": cmd_run_inp,
        "list": cmd_list,
        "monitor": cmd_monitor,
        "summary": cmd_summary,
        "organize": cmd_organize,
        "queue": cmd_queue,
    }
    handler = command_map[args.command]
    return int(handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
