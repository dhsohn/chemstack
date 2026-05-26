from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    from chemstack.flow.cli_parsers import build_parser as _build_parser

    return _build_parser()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
