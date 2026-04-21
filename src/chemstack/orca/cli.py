from __future__ import annotations

import argparse
import logging
import logging.handlers
import os
import sys
from typing import Callable

from chemstack import cli as unified_cli

from .commands._helpers import default_config_path
from .commands.list_runs import cmd_list as _engine_cmd_list
from .commands.monitor import cmd_monitor
from .commands.queue import (
    cmd_queue_cancel as _engine_cmd_queue_cancel,
    cmd_queue_worker as _engine_cmd_queue_worker,
)
from .telegram_bot import run_bot as _run_bot

_CHEMSTACK_HANDLER_ATTR = "_chemstack_managed_handler"
_DIRECT_WORKER_ENV_VAR = "CHEMSTACK_QUEUE_WORKER_DIRECT"


def cmd_bot(args: argparse.Namespace) -> int:
    from .config import load_config
    cfg = load_config(args.config)
    return int(_run_bot(cfg))


def _shared_list_argv(*, config_path: str, engine: str, status: str | None = None) -> list[str]:
    argv = [
        "queue",
        "list",
        "--engine",
        engine,
        "--kind",
        "job",
        "--chemstack-config",
        config_path,
    ]
    if status:
        argv.extend(["--status", status])
    return argv


def _shared_config_argv(config_path: str | None) -> list[str]:
    if not config_path:
        return []
    return ["--chemstack-config", config_path]


def _shared_orca_logging_argv(args: argparse.Namespace) -> list[str]:
    argv: list[str] = []
    if bool(getattr(args, "verbose", False)):
        argv.append("--verbose")
    log_file = getattr(args, "log_file", None)
    if log_file:
        argv.extend(["--log-file", str(log_file)])
    return argv


def cmd_init(args: argparse.Namespace) -> int:
    argv = ["init", "orca", *_shared_config_argv(args.config), *_shared_orca_logging_argv(args)]
    if bool(getattr(args, "force", False)):
        argv.append("--force")
    return int(unified_cli.main(argv))


def cmd_run_inp(args: argparse.Namespace) -> int:
    argv = [
        "run-dir",
        "orca",
        *_shared_config_argv(args.config),
        *_shared_orca_logging_argv(args),
        args.path,
        "--priority",
        str(args.priority),
    ]
    if bool(getattr(args, "force", False)):
        argv.append("--force")
    max_cores = getattr(args, "max_cores", None)
    if max_cores is not None:
        argv.extend(["--max-cores", str(max_cores)])
    max_memory_gb = getattr(args, "max_memory_gb", None)
    if max_memory_gb is not None:
        argv.extend(["--max-memory-gb", str(max_memory_gb)])
    return int(unified_cli.main(argv))


cmd_run_dir = cmd_run_inp


def cmd_list(args: argparse.Namespace) -> int:
    if getattr(args, "action", None) == "clear":
        return int(_engine_cmd_list(args))
    return int(
        unified_cli.main(
            _shared_list_argv(
                config_path=args.config,
                engine="orca",
                status=getattr(args, "filter", None),
            )
        )
    )


def cmd_queue_cancel(args: argparse.Namespace) -> int:
    target = str(getattr(args, "target", "")).strip()
    if target == "all-pending":
        return int(_engine_cmd_queue_cancel(args))
    return int(
        unified_cli.main(
            [
                "queue",
                "cancel",
                target,
                "--chemstack-config",
                args.config,
            ]
        )
    )


def cmd_queue_worker(args: argparse.Namespace) -> int:
    if os.getenv(_DIRECT_WORKER_ENV_VAR) == "1":
        return int(_engine_cmd_queue_worker(args))
    argv = [
        "queue",
        "worker",
        "--app",
        "orca",
        "--chemstack-config",
        args.config,
    ]
    if bool(getattr(args, "auto_organize", False)):
        argv.append("--auto-organize")
    elif bool(getattr(args, "no_auto_organize", False)):
        argv.append("--no-auto-organize")
    return int(unified_cli.main(argv))


def cmd_organize(args: argparse.Namespace) -> int:
    argv = ["organize", "orca", *_shared_config_argv(args.config), *_shared_orca_logging_argv(args)]
    if getattr(args, "reaction_dir", None):
        argv.extend(["--reaction-dir", args.reaction_dir])
    if getattr(args, "root", None):
        argv.extend(["--root", args.root])
    if bool(getattr(args, "apply", False)):
        argv.append("--apply")
    if bool(getattr(args, "rebuild_index", False)):
        argv.append("--rebuild-index")
    return int(unified_cli.main(argv))


def cmd_summary(args: argparse.Namespace) -> int:
    argv = ["summary", "orca", *_shared_config_argv(args.config), *_shared_orca_logging_argv(args)]
    if bool(getattr(args, "no_send", False)):
        argv.append("--no-send")
    return int(unified_cli.main(argv))


def cmd_queue(args: argparse.Namespace) -> int:
    _queue_sub_map: dict[str, Callable[[argparse.Namespace], int]] = {
        "cancel": cmd_queue_cancel,
        "worker": cmd_queue_worker,
    }
    handler = _queue_sub_map.get(args.queue_command)
    if handler is None:
        print("Usage: python -m chemstack.orca.cli queue {cancel|worker}")
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

    sub.add_parser("bot", help="Start Telegram bot (long polling)")

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

    q_worker = queue_sub.add_parser("worker", help="Run the queue worker in the foreground")
    auto_group = q_worker.add_mutually_exclusive_group()
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

    return parser


def _configure_logging(args: argparse.Namespace) -> None:
    """Set up logging based on CLI flags."""
    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    log_file = getattr(args, "log_file", None)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    _remove_managed_handlers(root_logger)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    if log_file:
        handler: logging.Handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
    else:
        handler = logging.StreamHandler(sys.stderr)

    handler.setFormatter(formatter)
    setattr(handler, _CHEMSTACK_HANDLER_ATTR, True)
    root_logger.addHandler(handler)


def _remove_managed_handlers(root_logger: logging.Logger) -> None:
    for handler in list(root_logger.handlers):
        if not getattr(handler, _CHEMSTACK_HANDLER_ATTR, False):
            continue
        root_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args)

    command_map: dict[str, Callable[[argparse.Namespace], int]] = {
        "init": cmd_init,
        "run-dir": cmd_run_inp,
        "list": cmd_list,
        "bot": cmd_bot,
        "monitor": cmd_monitor,
        "summary": cmd_summary,
        "organize": cmd_organize,
        "queue": cmd_queue,
    }
    handler = command_map[args.command]
    return int(handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
