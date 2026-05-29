from __future__ import annotations

import argparse
from importlib import metadata

from chemstack.cli_parser_commands import (
    add_init_parser,
    add_monitor_parser,
    add_organize_parser,
    add_run_dir_parser,
    add_scaffold_parser,
    add_summary_parser,
)
from chemstack.cli_parser_queue import add_queue_parser
from chemstack.cli_parser_systemd import add_service_parser, add_systemd_parser


def _chemstack_version() -> str:
    try:
        return metadata.version("chemstack")
    except metadata.PackageNotFoundError:
        return "0.0.0+unknown"


_EXAMPLES_EPILOG = """\
examples:
  chemstack init
  chemstack run-dir /home/user/orca_runs/sample_rxn
  chemstack queue list --engine orca
  chemstack queue cancel <target>
  chemstack summary --no-send
  chemstack service status
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chemstack",
        epilog=_EXAMPLES_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_chemstack_version()}",
    )
    parser.add_argument(
        "--no-color",
        action="store_true",
        help="Disable ANSI color in terminal output (also honors NO_COLOR)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_queue_parser(subparsers)
    add_run_dir_parser(subparsers)
    add_init_parser(subparsers)
    add_scaffold_parser(subparsers)
    add_organize_parser(subparsers)
    add_summary_parser(subparsers)
    add_monitor_parser(subparsers)
    add_systemd_parser(subparsers)
    add_service_parser(subparsers)
    return parser


__all__ = ["build_parser"]
