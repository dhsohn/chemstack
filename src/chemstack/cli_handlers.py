from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from chemstack.cli_common import (
    _configure_orca_logging,
    _dependency,
    _engine_config_for_command,
)
from chemstack.cli_errors import emit_error
from chemstack.flow.run_dir_layout import inspect_workflow_run_dir
from chemstack.core.utils import normalize_text


def cmd_init(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.init import cmd_init as _cmd_orca_init

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_init(args))


def cmd_orca_run_dir(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.run_inp import cmd_run_inp as _cmd_orca_run_dir

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_run_dir(args))


def cmd_orca_organize(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.organize import cmd_organize as _cmd_orca_organize

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_organize(args))


def cmd_workflow_scaffold(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.flow.scaffold import cmd_scaffold as _cmd_workflow_scaffold

    return int(_cmd_workflow_scaffold(args))


def _detect_run_dir_app(args: argparse.Namespace) -> str:
    raw_path = normalize_text(getattr(args, "path", None))
    if not raw_path:
        raise ValueError("run-dir requires a target directory path")

    target = Path(raw_path).expanduser().resolve()
    if not target.exists():
        raise ValueError(f"run-dir target not found: {target}")
    if not target.is_dir():
        raise ValueError(f"run-dir target is not a directory: {target}")

    if (target / "workflow.json").is_file():
        return "workflow"

    workflow_layout = inspect_workflow_run_dir(target)
    orca_input_present = any(candidate.is_file() for candidate in target.glob("*.inp"))

    if workflow_layout.has_manifest:
        return "workflow"
    if orca_input_present:
        return "orca"

    raise ValueError(
        "Could not infer run-dir target type from directory. "
        "Expected flow.yaml for workflow inputs, or *.inp for ORCA."
    )


def cmd_run_dir(args: Any, *, deps: Any | None = None) -> int:
    detect_run_dir_app = _dependency(deps, "_detect_run_dir_app", _detect_run_dir_app)
    try:
        run_dir_app = detect_run_dir_app(args)
    except ValueError as exc:
        emit_error(exc)
        return 1

    args.run_dir_app = run_dir_app
    if run_dir_app == "workflow":
        args.workflow_dir = getattr(args, "path")
        workflow_run_dir = _dependency(deps, "cmd_workflow_run_dir", cmd_workflow_run_dir)
        return int(workflow_run_dir(args))
    if getattr(args, "priority", None) is None:
        args.priority = 10
    orca_run_dir = _dependency(deps, "cmd_orca_run_dir", cmd_orca_run_dir)
    return int(orca_run_dir(args))


def cmd_workflow_run_dir(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.flow.cli_run_dir import cmd_run_dir as _cmd_workflow_run_dir

    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    shared_config = engine_config_for_command(args)
    if shared_config:
        args.chemstack_config = shared_config
    return int(_cmd_workflow_run_dir(args))


def cmd_summary(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.summary import cmd_summary as _cmd_combined_summary

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_combined_summary(args))


def cmd_orca_monitor(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.monitor import cmd_monitor as _cmd_orca_monitor

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_monitor(args))
