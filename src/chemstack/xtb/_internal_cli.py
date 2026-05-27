from __future__ import annotations

import argparse

from chemstack.core.internal_cli import (
    EngineInternalCliSpec,
    build_engine_internal_parser,
    run_engine_internal_cli,
)

from .commands import queue as queue_cmd
from .config import default_config_path


def build_parser() -> argparse.ArgumentParser:
    return build_engine_internal_parser(
        EngineInternalCliSpec(
            module_name="chemstack.xtb._internal_cli",
            engine_label="xTB",
            config_path=default_config_path(),
        )
    )


def main(argv: list[str] | None = None) -> int:
    return run_engine_internal_cli(
        argv,
        build_parser_fn=build_parser,
        queue_worker_handler=queue_cmd.cmd_queue_worker,
    )


if __name__ == "__main__":
    raise SystemExit(main())
