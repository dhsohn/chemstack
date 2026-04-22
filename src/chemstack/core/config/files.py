from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

import yaml

CHEMSTACK_CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"
DEFAULT_CONFIG_FILENAME = "chemstack.yaml"
DEFAULT_SHARED_ADMISSION_DIRNAME = "admission"


def default_config_path_from_repo_root(
    repo_root: Path,
    *,
    env_var: str = CHEMSTACK_CONFIG_ENV_VAR,
) -> str:
    env_path = os.getenv(env_var, "").strip()
    if env_path:
        return env_path

    repo_default = repo_root / "config" / DEFAULT_CONFIG_FILENAME
    if repo_default.exists():
        return str(repo_default)

    home_default = Path.home() / "chemstack" / "config" / DEFAULT_CONFIG_FILENAME
    if home_default.exists():
        return str(home_default)

    return str(repo_default)


def engine_config_mapping(
    raw: dict[str, Any],
    engine: str,
    *,
    inherit_keys: Iterable[str] = ("behavior", "resources", "telegram"),
) -> dict[str, Any]:
    section = raw.get(engine)
    if not isinstance(section, dict):
        return raw

    resolved = dict(section)
    for key in inherit_keys:
        inherited = raw.get(key)
        if key not in resolved and isinstance(inherited, dict):
            resolved[key] = dict(inherited)
    return resolved


def default_shared_admission_root(config_path: Path) -> str:
    return str(config_path.expanduser().resolve().parent / DEFAULT_SHARED_ADMISSION_DIRNAME)


def workflow_root_from_mapping(raw: dict[str, Any] | None) -> str:
    if not isinstance(raw, dict):
        return ""

    workflow_raw = raw.get("workflow", {})
    if not isinstance(workflow_raw, dict):
        return ""

    root_text = str(
        workflow_raw.get("root")
        or workflow_raw.get("workflow_root")
        or ""
    ).strip()
    if not root_text:
        return ""
    return str(Path(root_text).expanduser().resolve())


def shared_workflow_root_from_config(config_path: str | Path | None) -> str | None:
    if config_path is None:
        return None

    try:
        path = Path(config_path).expanduser().resolve()
    except OSError:
        return None
    if not path.exists():
        return None

    try:
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None

    root_text = workflow_root_from_mapping(parsed)
    if not root_text:
        return None
    return root_text
