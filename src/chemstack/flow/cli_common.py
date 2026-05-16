from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.config.files import (
    default_config_path_from_repo_root,
    shared_workflow_root_from_config,
)


def _dependency(deps: Any | None, name: str, fallback: Any) -> Any:
    if deps is not None and hasattr(deps, name):
        return getattr(deps, name)
    return fallback


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _project_root(*, deps: Any | None = None) -> Path:
    path_cls = _dependency(deps, "Path", Path)
    return path_cls(__file__).resolve().parents[2]


def _resolve_existing_path(path_text: str, *, deps: Any | None = None) -> Path | None:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)

    text = normalize_text(path_text)
    if not text:
        return None
    try:
        candidate = path_cls(text).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() else None


def _discover_workflow_root(
    explicit: str | Path | None,
    *,
    config_path: str | Path | None = None,
    deps: Any | None = None,
) -> str | None:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)
    workflow_root_from_config = _dependency(
        deps, "shared_workflow_root_from_config", shared_workflow_root_from_config
    )
    default_config_path = _dependency(
        deps, "default_config_path_from_repo_root", default_config_path_from_repo_root
    )
    project_root = _dependency(deps, "_project_root", _project_root)

    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(path_cls(explicit_text).expanduser().resolve())
    config_text = normalize_text(config_path)
    if config_text:
        return workflow_root_from_config(config_text)
    return workflow_root_from_config(default_config_path(project_root()))


def _shared_chemstack_config(args: Any, *, deps: Any | None = None) -> str | None:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    path_cls = _dependency(deps, "Path", Path)
    resolve_existing_path = _dependency(deps, "_resolve_existing_path", _resolve_existing_path)
    default_config_path = _dependency(
        deps, "default_config_path_from_repo_root", default_config_path_from_repo_root
    )
    project_root = _dependency(deps, "_project_root", _project_root)

    explicit = normalize_text(getattr(args, "chemstack_config", None))
    if explicit:
        return str(path_cls(explicit).expanduser().resolve())
    default_config = resolve_existing_path(default_config_path(project_root()))
    return str(default_config) if default_config is not None else None


def _workflow_root_from_args(
    args: Any, *, config_path: str | None = None, deps: Any | None = None
) -> str | None:
    discover_workflow_root = _dependency(deps, "_discover_workflow_root", _discover_workflow_root)
    return discover_workflow_root(getattr(args, "workflow_root", None), config_path=config_path)


def _normalize_workflow_type(value: Any, *, deps: Any | None = None) -> str:
    normalize_text = _dependency(deps, "_normalize_text", _normalize_text)
    text = normalize_text(value).lower().replace("-", "_")
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
