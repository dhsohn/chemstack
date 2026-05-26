from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR
from chemstack.core.config.files import (
    default_config_path_from_repo_root,
    shared_workflow_root_from_config,
)
from chemstack.core.utils.coercion import normalize_text as _coerce_text


def _normalize_text(value: Any) -> str:
    return _coerce_text(value)


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


def _project_root(*, deps: Any | None = None) -> Path:
    path_cls = _dependency(deps, "Path", Path)
    return path_cls(__file__).resolve().parents[1]


def _resolve_existing_path(path_text: str, *, deps: Any | None = None) -> Path | None:
    normalize = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    text = normalize(path_text)
    if not text:
        return None
    try:
        candidate = path_cls(text).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() else None


def _discover_shared_config_path(explicit: str | None) -> str | None:
    explicit_text = _normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())

    env_text = _normalize_text(os.getenv(CHEMSTACK_CONFIG_ENV_VAR))
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


def _discover_workflow_root(explicit: str | Path | None) -> str | None:
    explicit_text = _normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return None


def _discover_configured_workflow_root(
    explicit: str | Path | None,
    *,
    config_path: str | Path | None = None,
    deps: Any | None = None,
) -> str | None:
    normalize = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)
    workflow_root_from_config = _dependency(
        deps, "shared_workflow_root_from_config", shared_workflow_root_from_config
    )
    default_config_path = _dependency(
        deps, "default_config_path_from_repo_root", default_config_path_from_repo_root
    )
    project_root = _dependency(deps, "_project_root", _project_root)

    explicit_text = normalize(explicit)
    if explicit_text:
        return str(path_cls(explicit_text).expanduser().resolve())
    config_text = normalize(config_path)
    if config_text:
        return workflow_root_from_config(config_text)
    return workflow_root_from_config(default_config_path(project_root()))


def _effective_shared_config_text(args: argparse.Namespace) -> str:
    return (
        _normalize_text(getattr(args, "chemstack_config", None))
        or _normalize_text(getattr(args, "config", None))
        or _normalize_text(getattr(args, "global_config", None))
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


def _shared_chemstack_config(args: Any, *, deps: Any | None = None) -> str | None:
    normalize = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)
    resolve_existing_path = _dependency(deps, "_resolve_existing_path", _resolve_existing_path)
    default_config_path = _dependency(
        deps, "default_config_path_from_repo_root", default_config_path_from_repo_root
    )
    project_root = _dependency(deps, "_project_root", _project_root)

    explicit = normalize(getattr(args, "chemstack_config", None))
    if explicit:
        return str(path_cls(explicit).expanduser().resolve())
    default_config = resolve_existing_path(default_config_path(project_root()))
    return str(default_config) if default_config is not None else None


def _workflow_root_from_args(
    args: Any, *, config_path: str | None = None, deps: Any | None = None
) -> str | None:
    discover_workflow_root = _dependency(
        deps, "_discover_configured_workflow_root", _discover_configured_workflow_root
    )
    return discover_workflow_root(getattr(args, "workflow_root", None), config_path=config_path)


def _normalize_workflow_type(value: Any, *, deps: Any | None = None) -> str:
    normalize = _dependency(deps, "_normalize_text", _normalize_text)
    text = normalize(value).lower().replace("-", "_")
    aliases = {
        "reaction": "reaction_ts_search",
        "reaction_ts": "reaction_ts_search",
        "reaction_ts_search": "reaction_ts_search",
        "reaction_tssearch": "reaction_ts_search",
        "conformer": "conformer_screening",
        "conformer_screening": "conformer_screening",
        "conformer_screen": "conformer_screening",
        "screening": "conformer_screening",
    }
    normalized = aliases.get(text, "")
    if normalized:
        return normalized
    raise ValueError("workflow_type must be one of: reaction_ts_search, conformer_screening")


def _configure_orca_logging(args: argparse.Namespace) -> None:
    from chemstack.orca.cli_logging import configure_logging

    configure_logging(
        argparse.Namespace(
            verbose=bool(getattr(args, "verbose", False)),
            log_file=getattr(args, "log_file", None),
        )
    )
