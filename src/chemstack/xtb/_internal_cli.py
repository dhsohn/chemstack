from __future__ import annotations

import argparse
from typing import Callable

from .commands import init as scaffold_cmd
from .commands import list_jobs as list_cmd
from .commands import organize as organize_cmd
from .commands import queue as queue_cmd
from .commands import reindex as reindex_cmd
from .commands import run_dir as run_dir_cmd
from .commands import summary as summary_cmd
from .config import default_config_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.xtb._internal_cli")
    parser.add_argument("--config", default=default_config_path(), help="Path to chemstack.yaml containing xTB settings")
    sub = parser.add_subparsers(dest="command", required=True)

    scaffold = sub.add_parser("scaffold", help="Create an internal xTB job scaffold")
    scaffold.add_argument("--root", required=True, help="Job directory to create under allowed_root")
    scaffold.add_argument(
        "--job-type",
        default="path_search",
        choices=["path_search", "opt", "sp", "ranking"],
        help="Scaffold type to create",
    )

    run_dir = sub.add_parser("run-dir", help="Queue an xTB job directory")
    run_dir.add_argument("path", help="Job directory under allowed_root")
    run_dir.add_argument("--priority", type=int, default=10, help="Queue priority (lower = earlier)")

    sub.add_parser("list", help="Show queued xTB jobs")

    organize = sub.add_parser("organize", help="Plan or apply organization into xtb_outputs")
    organize.add_argument("--job-dir", default=None, help="Single job directory to organize")
    organize.add_argument("--root", default=None, help="Root under allowed_root to scan for completed jobs")
    organize.add_argument("--apply", action="store_true", help="Move completed job directories into organized_root")

    reindex = sub.add_parser("reindex", help="Rebuild the job location index from artifacts")
    reindex.add_argument("--root", default=None, help="Optional root to scan instead of both configured roots")

    summary = sub.add_parser("summary", help="Show summary by job_id or job directory")
    summary.add_argument("target", help="job_id or job directory path")
    summary.add_argument("--json", action="store_true", help="Print combined index/state/report JSON")

    queue_parser = sub.add_parser("queue", help="Queue management")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    worker = queue_sub.add_parser("worker", help="Run the xTB queue worker")
    worker.add_argument("--once", action="store_true", help="Process at most one pending job and exit")
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


def cmd_scaffold(args: argparse.Namespace) -> int:
    return int(scaffold_cmd.cmd_init(args))


def cmd_run_dir(args: argparse.Namespace) -> int:
    return int(run_dir_cmd.cmd_run_dir(args))


def cmd_list(args: argparse.Namespace) -> int:
    return int(list_cmd.cmd_list(args))


def cmd_organize(args: argparse.Namespace) -> int:
    return int(organize_cmd.cmd_organize(args))


def cmd_summary(args: argparse.Namespace) -> int:
    return int(summary_cmd.cmd_summary(args))


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
        "scaffold": cmd_scaffold,
        "run-dir": cmd_run_dir,
        "list": cmd_list,
        "organize": cmd_organize,
        "reindex": reindex_cmd.cmd_reindex,
        "queue": _cmd_queue,
        "summary": cmd_summary,
    }
    return int(command_map[args.command](args))


if __name__ == "__main__":
    raise SystemExit(main())
