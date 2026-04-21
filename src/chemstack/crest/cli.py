from __future__ import annotations

import argparse
import os
from typing import Callable

from chemstack import cli as unified_cli

from .commands.queue import cmd_queue_worker as _engine_cmd_queue_worker
from .commands.reindex import cmd_reindex
from .config import default_config_path

_DIRECT_WORKER_ENV_VAR = "CHEMSTACK_QUEUE_WORKER_DIRECT"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.cli")
    parser.add_argument("--config", default=default_config_path(), help="Path to chemstack.yaml containing CREST settings")
    sub = parser.add_subparsers(dest="command", required=True)

    run_dir = sub.add_parser("run-dir", help="Queue a CREST job directory")
    run_dir.add_argument("path", help="Job directory under allowed_root")
    run_dir.add_argument("--priority", type=int, default=10, help="Queue priority (lower = earlier)")

    init = sub.add_parser("init", help="Create a CREST job scaffold")
    init.add_argument("--root", required=True, help="Job directory to create under allowed_root")

    sub.add_parser("list", help="Show queued CREST jobs")
    organize = sub.add_parser("organize", help="Plan or apply organization into crest_outputs")
    organize.add_argument("--root", default=None, help="Root under allowed_root to scan for completed jobs")
    organize.add_argument("--apply", action="store_true", help="Move completed job directories into organized_root")
    reindex = sub.add_parser("reindex", help="Rebuild the job location index from artifacts")
    reindex.add_argument("--root", default=None, help="Optional root to scan instead of both configured roots")
    summary = sub.add_parser("summary", help="Show summary by job_id or job directory")
    summary.add_argument("target", help="job_id or job directory path")
    summary.add_argument("--json", action="store_true", help="Print combined index/state/report JSON")

    queue_parser = sub.add_parser("queue", help="Queue management")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    worker = queue_sub.add_parser("worker", help="Run the queue worker")
    worker.add_argument(
        "--once",
        action="store_true",
        help="Process at most one pending job and exit",
    )
    auto_group = worker.add_mutually_exclusive_group()
    auto_group.add_argument(
        "--auto-organize",
        action="store_true",
        help="Automatically move terminal jobs into organized_root after execution",
    )
    auto_group.add_argument(
        "--no-auto-organize",
        action="store_true",
        help="Disable automatic organization for this worker invocation",
    )
    cancel = queue_sub.add_parser("cancel", help="Cancel a queued or running job")
    cancel.add_argument("target", help="queue_id or job_id")
    return parser


def _shared_config_argv(config_path: str | None) -> list[str]:
    if not config_path:
        return []
    return ["--chemstack-config", config_path]


def cmd_init(args: argparse.Namespace) -> int:
    argv = ["init", "crest", *_shared_config_argv(args.config), "--root", args.root]
    return int(unified_cli.main(argv))


def cmd_run_dir(args: argparse.Namespace) -> int:
    argv = ["run-dir", "crest", *_shared_config_argv(args.config), args.path]
    argv.extend(["--priority", str(args.priority)])
    return int(unified_cli.main(argv))


def cmd_list(args: argparse.Namespace) -> int:
    return int(
        unified_cli.main(
            [
                "queue",
                "list",
                "--engine",
                "crest",
                "--kind",
                "job",
                "--chemstack-config",
                args.config,
            ]
        )
    )


def cmd_organize(args: argparse.Namespace) -> int:
    argv = ["organize", "crest", *_shared_config_argv(args.config)]
    if getattr(args, "job_dir", None):
        argv.extend(["--job-dir", args.job_dir])
    if getattr(args, "root", None):
        argv.extend(["--root", args.root])
    if bool(getattr(args, "apply", False)):
        argv.append("--apply")
    return int(unified_cli.main(argv))


def cmd_summary(args: argparse.Namespace) -> int:
    argv = ["summary", "crest", *_shared_config_argv(args.config), args.target]
    if bool(getattr(args, "json", False)):
        argv.append("--json")
    return int(unified_cli.main(argv))


def cmd_queue_cancel(args: argparse.Namespace) -> int:
    return int(
        unified_cli.main(
            [
                "queue",
                "cancel",
                str(getattr(args, "target", "")).strip(),
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
        "crest",
        "--chemstack-config",
        args.config,
    ]
    if bool(getattr(args, "once", False)):
        argv.append("--once")
    if bool(getattr(args, "auto_organize", False)):
        argv.append("--auto-organize")
    elif bool(getattr(args, "no_auto_organize", False)):
        argv.append("--no-auto-organize")
    return int(unified_cli.main(argv))


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
        "init": cmd_init,
        "run-dir": cmd_run_dir,
        "list": cmd_list,
        "organize": cmd_organize,
        "reindex": cmd_reindex,
        "queue": _cmd_queue,
        "summary": cmd_summary,
    }
    return int(command_map[args.command](args))


if __name__ == "__main__":
    raise SystemExit(main())
