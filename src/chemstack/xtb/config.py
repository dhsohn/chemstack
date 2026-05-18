from __future__ import annotations

from dataclasses import dataclass, field

from chemstack.core.config import engines as _engine_config
from chemstack.core.config import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

CONFIG_ENV_VAR = "CHEMSTACK_CONFIG"
_as_bool = _engine_config.as_bool
_as_float = _engine_config.as_float
_as_int = _engine_config.as_int
_as_str = _engine_config.as_str


@dataclass(frozen=True)
class PathsConfig:
    xtb_executable: str = ""


@dataclass(frozen=True)
class BehaviorConfig:
    pass


@dataclass(frozen=True)
class AppConfig:
    runtime: CommonRuntimeConfig
    workflow_root: str = ""
    paths: PathsConfig = field(default_factory=PathsConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)
    resources: CommonResourceConfig = field(default_factory=CommonResourceConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


_CONFIG_SPEC = _engine_config.WorkflowEngineConfigSpec(
    module_file=__file__,
    env_var=CONFIG_ENV_VAR,
    executable_key="xtb_executable",
    paths_cls=PathsConfig,
    behavior_cls=BehaviorConfig,
    app_config_cls=AppConfig,
)


def default_config_path() -> str:
    return _CONFIG_SPEC.default_config_path()


def load_config(config_path: str | None = None) -> AppConfig:
    return _CONFIG_SPEC.load_config(config_path, default_config_path_fn=default_config_path)
