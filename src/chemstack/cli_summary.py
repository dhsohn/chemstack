from __future__ import annotations

import argparse
from typing import Any

from chemstack.cli_common import (
    _configure_orca_logging,
    _dependency,
    _engine_config_for_command,
)
from chemstack.flow.submitters.common import normalize_text


def cmd_orca_summary(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.summary import cmd_summary as _cmd_orca_summary

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_summary(args))


def cmd_summary(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    summary_app = normalize_text(getattr(args, "summary_app", None)).lower() or "combined"
    if summary_app == "orca":
        orca_summary = _dependency(deps, "cmd_orca_summary", cmd_orca_summary)
        return int(orca_summary(args))

    from chemstack.summary import cmd_summary as _cmd_combined_summary

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_combined_summary(args))
