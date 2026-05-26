from __future__ import annotations

import argparse
from typing import Any

from chemstack.cli_common import (
    _configure_orca_logging,
    _dependency,
    _engine_config_for_command,
)


def cmd_orca_monitor(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    from chemstack.orca.commands.monitor import cmd_monitor as _cmd_orca_monitor

    configure_logging = _dependency(deps, "_configure_orca_logging", _configure_orca_logging)
    engine_config_for_command = _dependency(
        deps, "_engine_config_for_command", _engine_config_for_command
    )
    configure_logging(args)
    args.config = engine_config_for_command(args)
    return int(_cmd_orca_monitor(args))
