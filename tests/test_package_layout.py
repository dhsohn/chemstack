from __future__ import annotations

import importlib

from core import cli as core_cli
from core.commands.run_inp import cmd_run_inp as core_cmd_run_inp
from orca_auto import cli as orca_cli
from orca_auto.__main__ import main as package_main
from orca_auto.commands.run_inp import cmd_run_inp


def test_orca_auto_cli_reexports_core_main() -> None:
    assert orca_cli.main is core_cli.main


def test_orca_auto_package_entrypoint_uses_cli_main() -> None:
    assert package_main is core_cli.main


def test_orca_auto_command_module_reexports_core_command() -> None:
    assert cmd_run_inp is core_cmd_run_inp


def test_orca_auto_command_wrappers_import_expected_symbols() -> None:
    expected_symbols = {
        "_helpers": "default_config_path",
        "init": "cmd_init",
        "list_runs": "cmd_list",
        "monitor": "cmd_monitor",
        "organize": "cmd_organize",
        "queue": "cmd_queue_worker",
        "run_inp": "cmd_run_inp",
        "run_job": "cmd_run_job",
        "summary": "cmd_summary",
    }

    for module_name, symbol_name in expected_symbols.items():
        module = importlib.import_module(f"orca_auto.commands.{module_name}")
        assert getattr(module, symbol_name) is not None
