from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CONFIG_ENV_VAR, CHEMSTACK_REPO_ROOT_ENV_VAR
from chemstack.core.config.files import (
    default_config_path_from_repo_root,
    shared_workflow_root_from_config,
)

from ._activity_model import ActivitySourceRequest, ResolvedActivitySources
from .submitters.common import normalize_text


def coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def resolve_existing_path(path_text: str) -> Path | None:
    text = normalize_text(path_text)
    if not text:
        return None
    try:
        candidate = Path(text).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() else None


def discover_workflow_root(explicit: str | Path | None, *, deps: Any) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return shared_workflow_root_from_config(default_config_path_from_repo_root(deps._project_root()))


def discover_sibling_config(
    explicit: str | None,
    *,
    app_name: str,
    deps: Any,
) -> str | None:
    del app_name
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())

    env_text = normalize_text(os.getenv(CHEMSTACK_CONFIG_ENV_VAR))
    if env_text:
        return str(Path(env_text).expanduser().resolve())

    root = deps._project_root()
    candidates = [
        root / "config" / "chemstack.yaml",
        Path.home() / "chemstack" / "config" / "chemstack.yaml",
    ]
    for candidate in candidates:
        resolved = deps._resolve_existing_path(str(candidate))
        if resolved is not None:
            return str(resolved)
    return None


def discover_orca_config(explicit: str | None, *, deps: Any) -> str | None:
    return deps._discover_sibling_config(
        explicit,
        app_name="chemstack",
    )


def shared_config_hint(*configs: str | None) -> str | None:
    for config in configs:
        text = normalize_text(config)
        if text:
            return text
    return None


def resolve_activity_source_request(
    request: ActivitySourceRequest,
    *,
    deps: Any,
) -> ResolvedActivitySources:
    shared_hint = deps._shared_config_hint(
        request.orca_config,
        request.crest_config,
        request.xtb_config,
    )
    explicit_workflow_root = normalize_text(request.workflow_root)
    resolved_workflow_root: str | None
    if explicit_workflow_root:
        resolved_workflow_root = str(Path(explicit_workflow_root).expanduser().resolve())
    elif shared_hint:
        resolved_workflow_root = shared_workflow_root_from_config(shared_hint)
    else:
        resolved_workflow_root = deps._discover_workflow_root(None)
    resolved_crest_config = deps._discover_sibling_config(
        request.crest_config or shared_hint,
        app_name="chemstack_crest",
    )
    resolved_xtb_config = deps._discover_sibling_config(
        request.xtb_config or shared_hint,
        app_name="chemstack_xtb",
    )
    resolved_orca_config = deps._discover_orca_config(
        request.orca_config or shared_hint
    )
    return ResolvedActivitySources(
        workflow_root=resolved_workflow_root,
        crest_config=resolved_crest_config,
        xtb_config=resolved_xtb_config,
        orca_config=resolved_orca_config,
    )


def discover_orca_repo_root(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    env_text = normalize_text(os.getenv(CHEMSTACK_REPO_ROOT_ENV_VAR))
    if env_text:
        return str(Path(env_text).expanduser().resolve())
    return None
