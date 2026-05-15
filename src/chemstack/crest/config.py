from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from chemstack.core.config import engines as _engine_config
from chemstack.core.config.files import (
    default_config_path_from_repo_root,
)
from chemstack.core.config import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"
_as_bool = _engine_config.as_bool
_as_float = _engine_config.as_float
_as_int = _engine_config.as_int
_as_str = _engine_config.as_str


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


def load_config(config_path: str | None = None) -> AppConfig:
    return _engine_config.load_workflow_engine_config(
        config_path,
        default_config_path_fn=default_config_path,
        executable_key="crest_executable",
        paths_cls=PathsConfig,
        behavior_cls=BehaviorConfig,
        app_config_cls=AppConfig,
    )
