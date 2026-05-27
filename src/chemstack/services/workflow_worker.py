from __future__ import annotations

import argparse
import os

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.flow.cli_workflow import cmd_workflow_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.services.workflow_worker")
    parser.add_argument(
        "--workflow-root",
        required=True,
        help="Root that directly contains workflow workspaces.",
    )
    parser.add_argument(
        "--chemstack-config",
        default=str(os.getenv(CHEMSTACK_CONFIG_ENV_VAR, "")).strip() or None,
        help="Path to shared chemstack.yaml.",
    )
    parser.add_argument(
        "--no-submit",
        action="store_true",
        help="Only sync/append stages; do not submit newly actionable stages.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one orchestration cycle.",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional cycle limit; 0 means run forever.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=30.0,
        help="Sleep interval between orchestration cycles.",
    )
    parser.add_argument(
        "--lock-timeout-seconds",
        type=float,
        default=5.0,
        help="How long to wait for the worker lock.",
    )
    parser.add_argument(
        "--refresh-registry",
        action="store_true",
        help="Reindex the workflow registry before the first cycle.",
    )
    parser.add_argument(
        "--refresh-each-cycle",
        action="store_true",
        help="Reindex the workflow registry before every cycle.",
    )
    parser.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(cmd_workflow_worker(args))


if __name__ == "__main__":
    raise SystemExit(main())
