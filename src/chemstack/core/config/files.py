from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

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
