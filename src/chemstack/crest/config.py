from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.config.files import (
    default_config_path_from_repo_root,
    default_shared_admission_root,
    workflow_root_from_mapping,
)
from chemstack.core.config import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"


@dataclass(frozen=True)
class PathsConfig:
    crest_executable: str = ""


@dataclass(frozen=True)
class BehaviorConfig:
    auto_organize_on_terminal: bool = False


@dataclass(frozen=True)
class AppConfig:
    runtime: CommonRuntimeConfig
    workflow_root: str = ""
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


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_config(config_path: str | None = None) -> AppConfig:
    path = Path(config_path or default_config_path()).expanduser().resolve()
    if not path.exists():
        raise ValueError(
            f"Config file not found: {path}. Copy config/chemstack.yaml.example to this path and edit the workflow section."
        )

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Config file is invalid: {path}")

    scheduler_raw = raw.get("scheduler", {}) if isinstance(raw.get("scheduler"), dict) else {}
    workflow_raw = raw.get("workflow", {}) if isinstance(raw.get("workflow"), dict) else {}
    workflow_paths_raw = workflow_raw.get("paths", {}) if isinstance(workflow_raw.get("paths"), dict) else {}
    behavior_raw = raw.get("behavior", {}) if isinstance(raw.get("behavior"), dict) else {}
    resources_raw = raw.get("resources", {}) if isinstance(raw.get("resources"), dict) else {}
    telegram_raw = raw.get("telegram", {}) if isinstance(raw.get("telegram"), dict) else {}

    workflow_root = workflow_root_from_mapping(raw)
    if not workflow_root:
        raise ValueError(f"Config is missing workflow.root: {path}")

    shared_max_active_simulations = max(1, _as_int(scheduler_raw.get("max_active_simulations"), 4))
    shared_admission_root = _as_str(
        scheduler_raw.get("admission_root"),
        default_shared_admission_root(path),
    )
    max_concurrent = shared_max_active_simulations
    admission_root = shared_admission_root
    admission_limit = shared_max_active_simulations

    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=workflow_root,
            organized_root=workflow_root,
            max_concurrent=max_concurrent,
            admission_root=admission_root,
            admission_limit=admission_limit,
        ),
        workflow_root=workflow_root,
        paths=PathsConfig(
            crest_executable=_as_str(workflow_paths_raw.get("crest_executable")),
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
            timeout_seconds=max(0.1, _as_float(telegram_raw.get("timeout_seconds"), TelegramConfig.timeout_seconds)),
            max_attempts=max(1, _as_int(telegram_raw.get("max_attempts"), TelegramConfig.max_attempts)),
            retry_backoff_seconds=max(
                0.0,
                _as_float(telegram_raw.get("retry_backoff_seconds"), TelegramConfig.retry_backoff_seconds),
            ),
        ),
    )
