from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from orca_auto.core.app_ids import ORCA_AUTO_CONFIG_ENV_VAR
from orca_auto.core.config.files import (
    discover_shared_config_path,
    shared_workflow_root_from_config,
)
from orca_auto.core.utils.coercion import normalize_text
from orca_auto.flow.templates import normalize_workflow_template_id


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _repo_root_for_subprocess() -> str | None:
    root = _repo_root()
    if (root / "src" / "orca_auto").is_dir():
        return str(root)
    return None


def _discover_shared_config_path(explicit: str | None) -> str | None:
    return discover_shared_config_path(explicit, _repo_root(), env_var=ORCA_AUTO_CONFIG_ENV_VAR)


def _discover_workflow_root(explicit: str | Path | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return None


def _effective_shared_config_text(args: argparse.Namespace) -> str:
    return normalize_text(getattr(args, "orca_auto_config", None)) or normalize_text(
        getattr(args, "config", None)
    )


def _workflow_root_for_args(args: Any, *, config_path: str | None = None) -> str | None:
    explicit_root = _discover_workflow_root(getattr(args, "workflow_root", None))
    if explicit_root:
        return explicit_root
    config_text = normalize_text(config_path) or _discover_shared_config_path(
        _effective_shared_config_text(args)
    )
    return shared_workflow_root_from_config(config_text)


def _engine_config_for_command(args: argparse.Namespace) -> str | None:
    config_path = _discover_shared_config_path(_effective_shared_config_text(args))
    if not config_path:
        return None
    return str(Path(config_path).expanduser().resolve())


def _shared_orca_auto_config(args: Any) -> str | None:
    explicit = normalize_text(getattr(args, "orca_auto_config", None))
    if explicit:
        return str(Path(explicit).expanduser().resolve())
    return _discover_shared_config_path(None)


def _normalize_workflow_type(value: Any) -> str:
    return normalize_workflow_template_id(normalize_text(value))


def _configure_orca_logging(args: argparse.Namespace) -> None:
    from orca_auto.orca.cli_logging import configure_logging

    configure_logging(
        argparse.Namespace(
            verbose=bool(getattr(args, "verbose", False)),
            log_file=getattr(args, "log_file", None),
        )
    )
