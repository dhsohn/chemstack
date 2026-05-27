from __future__ import annotations

import argparse

from chemstack.orca.commands.queue import cmd_queue_worker


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.orca.runtime.queue_worker")
    parser.add_argument("--config", required=True)
    parser.add_argument("--auto-organize", action="store_true")
    parser.add_argument("--no-auto-organize", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
