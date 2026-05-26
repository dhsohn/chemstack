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


def build_engine_internal_parser(spec: EngineInternalCliSpec) -> argparse.ArgumentParser:
    label = spec.engine_label
    parser = argparse.ArgumentParser(prog=f"python -m {spec.module_name}")
    parser.add_argument("--config", default=spec.config_path, help=f"Path to chemstack.yaml containing {label} settings")
    sub = parser.add_subparsers(dest="command", required=True)

    run_dir = sub.add_parser("run-dir", help=f"Queue a {label} job directory")
    run_dir.add_argument("path", help="Job directory under allowed_root")
    run_dir.add_argument("--priority", type=int, default=10, help="Queue priority (lower = earlier)")

    queue_parser = sub.add_parser("queue", help="Queue management")
    queue_sub = queue_parser.add_subparsers(dest="queue_command", required=True)

    queue_sub.add_parser("worker", help=f"Run the {label} queue worker")

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
        return dispatch_engine_internal_queue_command(
            args,
            queue_worker_handler=queue_worker_handler,
            queue_cancel_handler=queue_cancel_handler,
        )
    return int(command_handlers[args.command](args))


def dispatch_engine_internal_queue_command(
    args: argparse.Namespace,
    *,
    queue_worker_handler: CommandHandler,
    queue_cancel_handler: CommandHandler,
) -> int:
    if args.queue_command == "worker":
        return int(queue_worker_handler(args))
    if args.queue_command == "cancel":
        return int(queue_cancel_handler(args))
    raise ValueError(f"Unsupported queue subcommand: {args.queue_command}")


def run_engine_internal_cli(
    argv: list[str] | None,
    *,
    build_parser_fn: Callable[[], argparse.ArgumentParser],
    command_handlers: dict[str, CommandHandler],
    queue_worker_handler: CommandHandler,
    queue_cancel_handler: CommandHandler,
) -> int:
    args = build_parser_fn().parse_args(argv)
    return dispatch_engine_internal_command(
        args,
        command_handlers=command_handlers,
        queue_worker_handler=queue_worker_handler,
        queue_cancel_handler=queue_cancel_handler,
    )
