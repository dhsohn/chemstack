from __future__ import annotations

from chemstack import cli as unified_cli


def main() -> int:
    return int(unified_cli.main(["summary"]))


if __name__ == "__main__":
    raise SystemExit(main())
