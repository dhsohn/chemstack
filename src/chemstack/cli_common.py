from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.flow.submitters.common import normalize_text


def _dependency(deps: Any | None, name: str, fallback: Any) -> Any:
    if deps is not None and hasattr(deps, name):
        return getattr(deps, name)
    return fallback


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _repo_root_for_subprocess() -> str | None:
    root = _repo_root()
    if (root / "src" / "chemstack").is_dir():
        return str(root)
    return None


def _discover_shared_config_path(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())

    env_text = normalize_text(os.getenv(CHEMSTACK_CONFIG_ENV_VAR))
    if env_text:
        return str(Path(env_text).expanduser().resolve())

    candidates = [
        _repo_root() / "config" / "chemstack.yaml",
        Path.home() / "chemstack" / "config" / "chemstack.yaml",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate.expanduser().resolve())
    return None


def _discover_workflow_root(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return None


def _effective_shared_config_text(args: argparse.Namespace) -> str:
    return (
        normalize_text(getattr(args, "chemstack_config", None))
        or normalize_text(getattr(args, "config", None))
        or normalize_text(getattr(args, "global_config", None))
    )


def _workflow_root_for_args(args: Any, *, deps: Any | None = None) -> str | None:
    discover_workflow_root = _dependency(deps, "_discover_workflow_root", _discover_workflow_root)
    discover_shared_config_path = _dependency(
        deps, "_discover_shared_config_path", _discover_shared_config_path
    )
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    workflow_root_from_config = _dependency(
        deps, "shared_workflow_root_from_config", shared_workflow_root_from_config
    )

    explicit_root = discover_workflow_root(getattr(args, "workflow_root", None))
    if explicit_root:
        return explicit_root
    config_path = discover_shared_config_path(effective_shared_config_text(args))
    return workflow_root_from_config(config_path)


def _engine_config_for_command(args: argparse.Namespace, *, deps: Any | None = None) -> str | None:
    discover_shared_config_path = _dependency(
        deps, "_discover_shared_config_path", _discover_shared_config_path
    )
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )

    config_path = discover_shared_config_path(effective_shared_config_text(args))
    if not config_path:
        return None
    return str(Path(config_path).expanduser().resolve())


def _configure_orca_logging(args: argparse.Namespace) -> None:
    from chemstack.orca.cli import _configure_logging as _configure_orca_logging_impl

    _configure_orca_logging_impl(
        argparse.Namespace(
            verbose=bool(getattr(args, "verbose", False)),
            log_file=getattr(args, "log_file", None),
        )
    )
