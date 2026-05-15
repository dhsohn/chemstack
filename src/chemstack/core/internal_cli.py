from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass


CommandHandler = Callable[[argparse.Namespace], int]


@dataclass(frozen=True)
class EngineInternalCliSpec:
    module_name: str
    engine_label: str
    config_path: str
    scaffold_job_type_choices: tuple[str, ...] = ()
    scaffold_default_job_type: str = ""


def build_engine_internal_parser(spec: EngineInternalCliSpec) -> argparse.ArgumentParser:
    label = spec.engine_label
    parser = argparse.ArgumentParser(prog=f"python -m {spec.module_name}")
    parser.add_argument("--config", default=spec.config_path, help=f"Path to chemstack.yaml containing {label} settings")
    sub = parser.add_subparsers(dest="command", required=True)

    scaffold = sub.add_parser("scaffold", help=f"Create an internal {label} job scaffold")
    scaffold.add_argument("--root", required=True, help="Job directory to create under allowed_root")
    if spec.scaffold_job_type_choices:
        scaffold.add_argument(
            "--job-type",
            default=spec.scaffold_default_job_type or spec.scaffold_job_type_choices[0],
            choices=list(spec.scaffold_job_type_choices),
            help="Scaffold type to create",
        )

    run_dir = sub.add_parser("run-dir", help=f"Queue a {label} job directory")
    run_dir.add_argument("path", help="Job directory under allowed_root")
    run_dir.add_argument("--priority", type=int, default=10, help="Queue priority (lower = earlier)")

    sub.add_parser("list", help=f"Show queued {label} jobs")

    organize = sub.add_parser("organize", help=f"Plan or apply {label} job organization")
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

    worker = queue_sub.add_parser("worker", help=f"Run the {label} queue worker")
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


def dispatch_engine_internal_command(
    args: argparse.Namespace,
    *,
    command_handlers: dict[str, CommandHandler],
    queue_worker_handler: CommandHandler,
    queue_cancel_handler: CommandHandler,
) -> int:
    if args.command == "queue":
        if args.queue_command == "worker":
            return int(queue_worker_handler(args))
        if args.queue_command == "cancel":
            return int(queue_cancel_handler(args))
        raise ValueError(f"Unsupported queue subcommand: {args.queue_command}")
    return int(command_handlers[args.command](args))
