from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml

from chemstack.core.config import CommonResourceConfig, TelegramConfig
from chemstack.core.config import engines as _config_engines
from chemstack.core.config.files import (
    default_shared_admission_root,
    engine_config_mapping,
    workflow_root_from_mapping,
)

from .config_validation import (
    _as_int,
    _as_str,
    _validate_config,
)

logger = logging.getLogger(__name__)

_CONFIG_TEMPLATE_RELATIVE_PATH = Path("config") / "chemstack.yaml.example"
_TEMPLATE_ALLOWED_ROOT = "/path/to/orca_runs"
_TEMPLATE_ORGANIZED_ROOT = "/path/to/orca_outputs"
_TEMPLATE_ORCA_EXECUTABLE = "/path/to/orca/orca"
_REMOVED_RUNTIME_SCHEDULER_KEYS = frozenset(
    {
        "max_concurrent",
        "admission_root",
        "admission_limit",
        "admission_max_concurrent",
        "default_max_attempts",
    }
)


def _config_template_path() -> Path:
    repo_root = Path(__file__).resolve().parents[3]
    return repo_root / _CONFIG_TEMPLATE_RELATIVE_PATH


def _default_organized_root(allowed_root: str) -> str:
    allowed = Path(allowed_root).expanduser()
    if not allowed.is_absolute():
        return ""
    return str(allowed.parent / "orca_outputs")


def _missing_config_error(path: Path) -> ValueError:
    template_path = _config_template_path()
    return ValueError(
        "Config file not found: "
        f"{path}. Copy {template_path} to {path} and set explicit Linux paths for "
        "runtime.allowed_root, runtime.organized_root, and paths.orca_executable."
    )


def _missing_required_settings_error(path: Path, missing_keys: list[str]) -> ValueError:
    keys = ", ".join(missing_keys)
    return ValueError(
        "Config is missing required settings: "
        f"{keys}. chemstack no longer assumes personal defaults like ~/orca_runs or "
        f"~/opt/orca/orca. Update {path} with explicit Linux paths."
    )


def _placeholder_settings_error(path: Path, placeholder_keys: list[str]) -> ValueError:
    keys = ", ".join(placeholder_keys)
    return ValueError(
        "Config still contains template placeholder paths in "
        f"{keys}. Edit {path} and replace /path/to/... values with your real Linux paths."
    )


def _removed_runtime_scheduler_keys_error(path: Path, removed_keys: list[str]) -> ValueError:
    keys = ", ".join(f"runtime.{key}" for key in removed_keys)
    return ValueError(f"Config uses unsupported runtime keys: {keys} ({path})")


@dataclass
class CommonRuntimeConfig:
    allowed_root: str = ""
    organized_root: str = ""
    # max retry count, not total execution count
    default_max_retries: int = 2
    max_concurrent: int = 4
    admission_root: str = ""
    admission_limit: int | None = None

    def __post_init__(self) -> None:
        self.default_max_retries = max(0, _as_int(self.default_max_retries, 2))
        self.max_concurrent = max(1, _as_int(self.max_concurrent, 4))
        if not self.organized_root and self.allowed_root:
            self.organized_root = _default_organized_root(self.allowed_root)
        if not self.admission_root and self.allowed_root:
            self.admission_root = self.allowed_root
        if self.admission_limit is not None:
            try:
                if isinstance(self.admission_limit, bool):
                    normalized_limit = int(self.admission_limit)
                elif isinstance(self.admission_limit, (int, str)):
                    normalized_limit = int(self.admission_limit)
                else:
                    raise TypeError("Unsupported admission_limit type")
            except (TypeError, ValueError):
                normalized_limit = self.max_concurrent
            self.admission_limit = normalized_limit
            if self.admission_limit < 1:
                self.admission_limit = max(1, self.max_concurrent)

    @property
    def resolved_admission_root(self) -> str:
        return self.admission_root or self.allowed_root

    @property
    def resolved_admission_limit(self) -> int:
        if self.admission_limit is not None:
            return max(1, int(self.admission_limit))
        return max(1, int(self.max_concurrent))

    def to_common_runtime_config(self) -> CommonRuntimeConfig:
        return CommonRuntimeConfig(
            allowed_root=self.allowed_root,
            organized_root=self.organized_root,
            default_max_retries=self.default_max_retries,
            max_concurrent=self.max_concurrent,
            admission_root=self.admission_root,
            admission_limit=self.admission_limit,
        )


RuntimeConfig = CommonRuntimeConfig


@dataclass
class PathsConfig:
    orca_executable: str = ""


@dataclass
class BehaviorConfig:
    auto_organize_on_terminal: bool = False


@dataclass
class AppConfig:
    runtime: CommonRuntimeConfig = field(default_factory=CommonRuntimeConfig)
    workflow_root: str = ""
    paths: PathsConfig = field(default_factory=PathsConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)
    resources: CommonResourceConfig = field(default_factory=CommonResourceConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


def _load_raw_config(path: Path) -> Dict[str, Any]:
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
            if isinstance(parsed, dict):
                return parsed
        return {}
    raise _missing_config_error(path)


def _section_mapping(raw: Dict[str, Any], key: str) -> Dict[str, Any]:
    section = raw.get(key, {})
    return section if isinstance(section, dict) else {}


def _reject_unsupported_runtime_keys(path: Path, runtime_raw: Dict[str, Any]) -> None:
    if "platform_mode" in runtime_raw:
        raise ValueError(
            "runtime.platform_mode is removed. chemstack is Linux-only; delete this unsupported key from config."
        )
    removed_runtime_scheduler_keys = sorted(
        _REMOVED_RUNTIME_SCHEDULER_KEYS.intersection(runtime_raw.keys())
    )
    if removed_runtime_scheduler_keys:
        raise _removed_runtime_scheduler_keys_error(path, removed_runtime_scheduler_keys)


def _required_runtime_paths(
    path: Path,
    runtime_raw: Dict[str, Any],
    paths_raw: Dict[str, Any],
) -> tuple[str, str]:
    allowed_root = _as_str(runtime_raw.get("allowed_root"), "")
    orca_executable = _as_str(paths_raw.get("orca_executable"), "")
    missing_keys: list[str] = []
    if not allowed_root:
        missing_keys.append("runtime.allowed_root")
    if not orca_executable:
        missing_keys.append("paths.orca_executable")
    if missing_keys:
        raise _missing_required_settings_error(path, missing_keys)
    return allowed_root, orca_executable


def _scheduler_runtime_settings(
    path: Path,
    scheduler_raw: Dict[str, Any],
    allowed_root: str,
) -> tuple[int, str, int | None]:
    scheduler_enabled = bool(scheduler_raw)
    shared_max_active_simulations = _as_int(
        scheduler_raw.get("max_active_simulations"),
        RuntimeConfig.max_concurrent,
    )
    if shared_max_active_simulations < 1:
        raise ValueError("scheduler.max_active_simulations must be an integer >= 1.")
    shared_admission_root = _as_str(
        scheduler_raw.get("admission_root"),
        default_shared_admission_root(path) if scheduler_enabled else allowed_root,
    )
    admission_limit = shared_max_active_simulations if scheduler_enabled else None
    return shared_max_active_simulations, shared_admission_root, admission_limit


def _build_telegram_config(telegram_raw: Dict[str, Any]) -> TelegramConfig:
    return TelegramConfig(
        bot_token=_as_str(telegram_raw.get("bot_token"), ""),
        chat_id=str(telegram_raw.get("chat_id", "")).strip(),
        timeout_seconds=_config_engines.as_float(
            telegram_raw.get("timeout_seconds"), TelegramConfig.timeout_seconds
        ),
        max_attempts=_as_int(telegram_raw.get("max_attempts"), TelegramConfig.max_attempts),
        retry_backoff_seconds=_config_engines.as_float(
            telegram_raw.get("retry_backoff_seconds"),
            TelegramConfig.retry_backoff_seconds,
        ),
    )


def _placeholder_keys(cfg: AppConfig) -> list[str]:
    placeholder_keys: list[str] = []
    if cfg.runtime.allowed_root == _TEMPLATE_ALLOWED_ROOT:
        placeholder_keys.append("runtime.allowed_root")
    if cfg.runtime.organized_root == _TEMPLATE_ORGANIZED_ROOT:
        placeholder_keys.append("runtime.organized_root")
    if cfg.paths.orca_executable == _TEMPLATE_ORCA_EXECUTABLE:
        placeholder_keys.append("paths.orca_executable")
    return placeholder_keys


def load_config(config_path: str) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    raw = _load_raw_config(path)
    workflow_root = _as_str(workflow_root_from_mapping(raw), "")
    raw = engine_config_mapping(
        raw, "orca", inherit_keys=("behavior", "resources", "telegram", "scheduler")
    )
    scheduler_raw = _section_mapping(raw, "scheduler")
    runtime_raw = _section_mapping(raw, "runtime")
    paths_raw = _section_mapping(raw, "paths")
    behavior_raw = _section_mapping(raw, "behavior")
    telegram_raw = _section_mapping(raw, "telegram")
    resources_raw = _section_mapping(raw, "resources")

    _reject_unsupported_runtime_keys(path, runtime_raw)
    allowed_root, orca_executable = _required_runtime_paths(path, runtime_raw, paths_raw)
    organized_root = _as_str(
        runtime_raw.get("organized_root"),
        _default_organized_root(allowed_root),
    )
    default_max_retries = _as_int(
        runtime_raw.get("default_max_retries"),
        RuntimeConfig.default_max_retries,
    )
    max_concurrent, admission_root, admission_limit = _scheduler_runtime_settings(
        path,
        scheduler_raw,
        allowed_root,
    )
    telegram_cfg = _build_telegram_config(telegram_raw)

    cfg = AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=allowed_root,
            organized_root=organized_root,
            default_max_retries=max(0, default_max_retries),
            max_concurrent=max_concurrent,
            admission_root=admission_root,
            admission_limit=admission_limit,
        ),
        workflow_root=workflow_root,
        paths=PathsConfig(
            orca_executable=orca_executable,
        ),
        behavior=BehaviorConfig(
            auto_organize_on_terminal=_config_engines.as_bool(
                behavior_raw.get("auto_organize_on_terminal"),
                False,
            ),
        ),
        resources=CommonResourceConfig(
            max_cores_per_task=max(1, _as_int(resources_raw.get("max_cores_per_task"), 8)),
            max_memory_gb_per_task=max(1, _as_int(resources_raw.get("max_memory_gb_per_task"), 32)),
        ),
        telegram=telegram_cfg,
    )
    placeholder_keys = _placeholder_keys(cfg)
    if placeholder_keys:
        raise _placeholder_settings_error(path, placeholder_keys)

    _validate_config(cfg)

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, admission_root=%s, orca_executable=%s, max_concurrent=%d, admission_limit=%d",
        cfg.runtime.allowed_root,
        cfg.runtime.organized_root,
        cfg.runtime.resolved_admission_root,
        cfg.paths.orca_executable,
        cfg.runtime.max_concurrent,
        cfg.runtime.resolved_admission_limit,
    )
    return cfg
