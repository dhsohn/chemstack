from __future__ import annotations

import argparse
import logging
import logging.handlers
import sys

from .commands._helpers import (
    CONFIG_ENV_VAR as _CONFIG_ENV_VAR,
    default_config_path as _default_config_path,
)
from .commands.init import cmd_init as _cmd_init
from .commands.list_runs import cmd_list as _cmd_list
from .commands.monitor import cmd_monitor as _cmd_monitor
from .commands.organize import cmd_organize as _cmd_organize
from .commands.summary import cmd_summary as _cmd_summary
from .commands.queue import (
    cmd_queue_add as _cmd_queue_add,
    cmd_queue_cancel as _cmd_queue_cancel,
    cmd_queue_clear as _cmd_queue_clear,
    cmd_queue_list as _cmd_queue_list,
    cmd_queue_stop as _cmd_queue_stop,
    cmd_queue_worker as _cmd_queue_worker,
)
from .commands.run_inp import (
    _retry_inp_path as _retry_inp_path_impl,
    _select_latest_inp as _select_latest_inp_impl,
    cmd_run_inp as _cmd_run_inp,
)
from .orca_runner import OrcaRunner
from .telegram_bot import run_bot as _run_bot

CONFIG_ENV_VAR = _CONFIG_ENV_VAR
default_config_path = _default_config_path
_retry_inp_path = _retry_inp_path_impl
_select_latest_inp = _select_latest_inp_impl


def cmd_run_inp(args: argparse.Namespace) -> int:
    return int(_cmd_run_inp(args, runner_cls=OrcaRunner))


def cmd_list(args: argparse.Namespace) -> int:
    return int(_cmd_list(args))


def cmd_init(args: argparse.Namespace) -> int:
    return int(_cmd_init(args))


def cmd_organize(args: argparse.Namespace) -> int:
    return int(_cmd_organize(args))


def cmd_monitor(args: argparse.Namespace) -> int:
    return int(_cmd_monitor(args))


def cmd_summary(args: argparse.Namespace) -> int:
    return int(_cmd_summary(args))


def cmd_bot(args: argparse.Namespace) -> int:
    from .config import load_config
    cfg = load_config(args.config)
    return int(_run_bot(cfg))


def cmd_queue(args: argparse.Namespace) -> int:
    _queue_sub_map = {
        "add": _cmd_queue_add,
        "list": _cmd_queue_list,
        "cancel": _cmd_queue_cancel,
        "clear": _cmd_queue_clear,
        "worker": _cmd_queue_worker,
        "stop": _cmd_queue_stop,
    }
    handler = _queue_sub_map.get(args.queue_command)
    if handler is None:
        print("Usage: orca_auto queue {add|list|cancel|clear|worker|stop}")
        return 1
    return int(handler(args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orca_auto")
    parser.add_argument("--config", default=default_config_path(), help="Path to orca_auto.yaml")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    parser.add_argument("--json-log", action="store_true", help="Emit structured JSON log lines")
    parser.add_argument("--log-file", default=None, help="Write logs to file (with rotation, max 10MB x 5)")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init", help="Interactively create or update the config file")
    init.add_argument("--force", action="store_true", help="Overwrite existing config without confirmation")

    run_inp = sub.add_parser("run-inp")
    run_inp.add_argument("--reaction-dir", required=True, help="Directory under the configured allowed_root containing input files")
    run_inp.add_argument("--force", action="store_true", help="Force re-run even if existing output is completed")
    run_inp.add_argument("--json", action="store_true")
    run_inp.add_argument("--foreground", action="store_true", help="Run in the foreground")

    list_cmd = sub.add_parser("list", help="Show status of all simulations")
    list_cmd.add_argument("--filter", default=None, choices=["created", "running", "retrying", "completed", "failed"],
                          help="Filter by status")
    list_cmd.add_argument("--json", action="store_true")

    sub.add_parser("bot", help="Start Telegram bot (long polling)")

    sub.add_parser("monitor", help="Send Telegram alerts for newly detected run/result events")
    summary = sub.add_parser("summary", help="Send a periodic Telegram digest of current workstation activity")
    summary.add_argument("--no-send", action="store_true", default=False, help="Print summary without sending Telegram")

    organize = sub.add_parser("organize")
    organize.add_argument("--reaction-dir", default=None, help="Single reaction directory to organize")
    organize.add_argument("--root", default=None, help="Root directory to scan (mutually exclusive with --reaction-dir)")
    organize.add_argument("--apply", action="store_true", default=False, help="Actually move files (default is dry-run)")
    organize.add_argument("--rebuild-index", action="store_true", default=False, help="Rebuild JSONL index from organized directories")
    organize.add_argument("--json", action="store_true")

    # -- queue subcommand with its own sub-subcommands --------------------
    queue_parser = sub.add_parser("queue", help="Manage the task queue")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    q_add = queue_sub.add_parser("add", help="Add a reaction directory to the queue")
    q_add.add_argument("--reaction-dir", required=True, help="Directory under allowed_root")
    q_add.add_argument("--priority", type=int, default=10, help="Priority (lower = higher, default 10)")
    q_add.add_argument("--force", action="store_true", help="Allow re-enqueue of completed/failed jobs (intentional retry)")
    q_add.add_argument("--json", action="store_true")

    q_list = queue_sub.add_parser("list", help="Show queue entries")
    q_list.add_argument("--filter", default=None, choices=["pending", "running", "completed", "failed", "cancelled"],
                        help="Filter by status")
    q_list.add_argument("--json", action="store_true")

    q_cancel = queue_sub.add_parser("cancel", help="Cancel a queued or running job")
    q_cancel.add_argument("target", help="queue_id, reaction_dir, or run_id to cancel; or 'all-pending'")

    queue_sub.add_parser("clear", help="Remove completed/failed/cancelled entries from the queue")

    q_worker = queue_sub.add_parser("worker", help="Start the queue worker")
    q_worker.add_argument("--max-concurrent", type=int, default=4, help="Max concurrent jobs (default 4)")
    q_worker.add_argument("--daemon", action="store_true", help="Run worker in background")

    queue_sub.add_parser("stop", help="Stop the running worker daemon")

    return parser


def _configure_logging(args: argparse.Namespace) -> None:
    """Set up logging based on CLI flags."""
    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    use_json = getattr(args, "json_log", False)
    log_file = getattr(args, "log_file", None)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if use_json:
        from .json_logger import JSONFormatter
        formatter: logging.Formatter = JSONFormatter()
    else:
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
    root_logger.addHandler(handler)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    _configure_logging(args)

    command_map = {
        "init": cmd_init,
        "run-inp": cmd_run_inp,
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
