from __future__ import annotations

import argparse

from chemstack.core.internal_cli import (
    EngineInternalCliSpec,
    build_engine_internal_parser,
    dispatch_engine_internal_command,
)

from .commands import init as scaffold_cmd
from .commands import list_jobs as list_cmd
from .commands import organize as organize_cmd
from .commands import queue as queue_cmd
from .commands import reindex as reindex_cmd
from .commands import run_dir as run_dir_cmd
from .commands import summary as summary_cmd
from .config import default_config_path


def build_parser() -> argparse.ArgumentParser:
    return build_engine_internal_parser(
        EngineInternalCliSpec(
            module_name="chemstack.crest._internal_cli",
            engine_label="CREST",
            config_path=default_config_path(),
        )
    )


def cmd_scaffold(args: argparse.Namespace) -> int:
    return int(scaffold_cmd.cmd_init(args))


def cmd_run_dir(args: argparse.Namespace) -> int:
    return int(run_dir_cmd.cmd_run_dir(args))


def cmd_list(args: argparse.Namespace) -> int:
    return int(list_cmd.cmd_list(args))


def cmd_organize(args: argparse.Namespace) -> int:
    return int(organize_cmd.cmd_organize(args))


def cmd_reindex(args: argparse.Namespace) -> int:
    return int(reindex_cmd.cmd_reindex(args))


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

    return dispatch_engine_internal_command(
        args,
        command_handlers={
            "scaffold": cmd_scaffold,
            "run-dir": cmd_run_dir,
            "list": cmd_list,
            "organize": cmd_organize,
            "reindex": cmd_reindex,
            "summary": cmd_summary,
        },
        queue_worker_handler=cmd_queue_worker,
        queue_cancel_handler=cmd_queue_cancel,
    )


if __name__ == "__main__":
    raise SystemExit(main())
