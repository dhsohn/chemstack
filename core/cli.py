from __future__ import annotations

import argparse
import logging
import sys

from .commands._helpers import CONFIG_ENV_VAR, default_config_path
from .commands.organize import cmd_organize as _cmd_organize
from .commands.run_inp import (
    _retry_inp_path,
    _select_latest_inp,
    cmd_run_inp as _cmd_run_inp,
    cmd_status as _cmd_status,
)
from .orca_runner import OrcaRunner


def cmd_status(args: argparse.Namespace) -> int:
    return int(_cmd_status(args))


def cmd_run_inp(args: argparse.Namespace) -> int:
    return int(_cmd_run_inp(args, runner_cls=OrcaRunner))


def cmd_organize(args: argparse.Namespace) -> int:
    return int(_cmd_organize(args))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="orca_auto")
    parser.add_argument("--config", default=default_config_path(), help="Path to orca_auto.yaml")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    sub = parser.add_subparsers(dest="command", required=True)

    run_inp = sub.add_parser("run-inp")
    run_inp.add_argument("--reaction-dir", required=True, help="Directory under the configured allowed_root containing input files")
    run_inp.add_argument("--max-retries", type=int, default=None)
    run_inp.add_argument("--force", action="store_true", help="Force re-run even if existing output is completed")
    run_inp.add_argument("--json", action="store_true")
    run_inp.add_argument("--foreground", action="store_true", help="Run in the foreground")
    run_inp.add_argument("--background", action="store_true", help="Run in the background (default launcher behavior)")

    status = sub.add_parser("status")
    status.add_argument("--reaction-dir", required=True, help="Directory under the configured allowed_root")
    status.add_argument("--json", action="store_true")

    organize = sub.add_parser("organize")
    organize.add_argument("--reaction-dir", default=None, help="Single reaction directory to organize")
    organize.add_argument("--root", default=None, help="Root directory to scan (mutually exclusive with --reaction-dir)")
    organize.add_argument("--apply", action="store_true", default=False, help="Actually move files (default is dry-run)")
    organize.add_argument("--rebuild-index", action="store_true", default=False, help="Rebuild JSONL index from organized directories")
    organize.add_argument("--json", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    log_level = logging.DEBUG if getattr(args, "verbose", False) else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    command_map = {
        "run-inp": cmd_run_inp,
        "status": cmd_status,
        "organize": cmd_organize,
    }
    handler = command_map[args.command]
    return int(handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
