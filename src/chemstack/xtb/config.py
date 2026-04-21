from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.config.files import (
    default_config_path_from_repo_root,
    default_shared_admission_root,
    engine_config_mapping,
)
from chemstack.core.config import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"
_REMOVED_RUNTIME_SCHEDULER_KEYS = frozenset(
    {"max_concurrent", "admission_root", "admission_limit", "admission_max_concurrent"}
)


@dataclass(frozen=True)
class PathsConfig:
    xtb_executable: str = ""


@dataclass(frozen=True)
class BehaviorConfig:
    auto_organize_on_terminal: bool = False


@dataclass(frozen=True)
class AppConfig:
    runtime: CommonRuntimeConfig
    paths: PathsConfig = field(default_factory=PathsConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)
    resources: CommonResourceConfig = field(default_factory=CommonResourceConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


def default_config_path() -> str:
    repo_root = Path(__file__).resolve().parents[3]
    return default_config_path_from_repo_root(repo_root, env_var=CONFIG_ENV_VAR)


def _as_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _raise_removed_runtime_scheduler_keys(path: Path, removed_keys: list[str]) -> None:
    keys = ", ".join(f"runtime.{key}" for key in removed_keys)
    raise ValueError(f"Config uses unsupported runtime keys: {keys} ({path})")


def load_config(config_path: str | None = None) -> AppConfig:
    path = Path(config_path or default_config_path()).expanduser().resolve()
    if not path.exists():
        raise ValueError(
            f"Config file not found: {path}. Copy config/chemstack.yaml.example to this path and edit the xtb section."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config file is invalid: {path}")

    raw = engine_config_mapping(raw, "xtb", inherit_keys=("behavior", "resources", "telegram", "scheduler"))
    scheduler_raw = raw.get("scheduler", {}) if isinstance(raw.get("scheduler"), dict) else {}
    runtime_raw = raw.get("runtime", {}) if isinstance(raw.get("runtime"), dict) else {}
    paths_raw = raw.get("paths", {}) if isinstance(raw.get("paths"), dict) else {}
    behavior_raw = raw.get("behavior", {}) if isinstance(raw.get("behavior"), dict) else {}
    resources_raw = raw.get("resources", {}) if isinstance(raw.get("resources"), dict) else {}
    telegram_raw = raw.get("telegram", {}) if isinstance(raw.get("telegram"), dict) else {}
    removed_runtime_scheduler_keys = sorted(_REMOVED_RUNTIME_SCHEDULER_KEYS.intersection(runtime_raw.keys()))
    if removed_runtime_scheduler_keys:
        _raise_removed_runtime_scheduler_keys(path, removed_runtime_scheduler_keys)

    allowed_root = _as_str(runtime_raw.get("allowed_root"))
    if not allowed_root:
        raise ValueError(f"Config is missing runtime.allowed_root: {path}")

    scheduler_enabled = bool(scheduler_raw)
    shared_max_active_simulations = max(1, _as_int(scheduler_raw.get("max_active_simulations"), 4))
    shared_admission_root = _as_str(
        scheduler_raw.get("admission_root"),
        default_shared_admission_root(path) if scheduler_enabled else allowed_root,
    )
    organized_root = _as_str(runtime_raw.get("organized_root"), str(Path(allowed_root).parent / "xtb_outputs"))
    max_concurrent = shared_max_active_simulations
    admission_root = shared_admission_root
    admission_limit = shared_max_active_simulations

    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=allowed_root,
            organized_root=organized_root,
            max_concurrent=max_concurrent,
            admission_root=admission_root,
            admission_limit=admission_limit,
        ),
        paths=PathsConfig(
            xtb_executable=_as_str(paths_raw.get("xtb_executable")),
        ),
        behavior=BehaviorConfig(
            auto_organize_on_terminal=_as_bool(behavior_raw.get("auto_organize_on_terminal"), False),
        ),
        resources=CommonResourceConfig(
            max_cores_per_task=max(1, _as_int(resources_raw.get("max_cores_per_task"), 8)),
            max_memory_gb_per_task=max(1, _as_int(resources_raw.get("max_memory_gb_per_task"), 32)),
        ),
        telegram=TelegramConfig(
            bot_token=_as_str(telegram_raw.get("bot_token")),
            chat_id=_as_str(telegram_raw.get("chat_id")),
        ),
    )
