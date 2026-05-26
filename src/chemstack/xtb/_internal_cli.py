from __future__ import annotations

import argparse

from chemstack.core.internal_cli import (
    EngineInternalCliSpec,
    build_engine_internal_parser,
    run_engine_internal_cli,
)

from .commands import init as init_cmd
from .commands import list_jobs as list_cmd
from .commands import queue as queue_cmd
from .commands import reindex as reindex_cmd
from .commands import run_dir as run_dir_cmd
from .commands import summary as summary_cmd
from .config import default_config_path


def build_parser() -> argparse.ArgumentParser:
    return build_engine_internal_parser(
        EngineInternalCliSpec(
            module_name="chemstack.xtb._internal_cli",
            engine_label="xTB",
            config_path=default_config_path(),
            scaffold_job_type_choices=("path_search", "opt", "sp", "ranking"),
            scaffold_default_job_type="path_search",
        )
    )


def main(argv: list[str] | None = None) -> int:
    return run_engine_internal_cli(
        argv,
        build_parser_fn=build_parser,
        command_handlers={
            "scaffold": init_cmd.cmd_init,
            "run-dir": run_dir_cmd.cmd_run_dir,
            "list": list_cmd.cmd_list,
            "reindex": reindex_cmd.cmd_reindex,
            "summary": summary_cmd.cmd_summary,
        },
        queue_worker_handler=queue_cmd.cmd_queue_worker,
        queue_cancel_handler=queue_cmd.cmd_queue_cancel,
    )


if __name__ == "__main__":
    raise SystemExit(main())
