from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

import yaml
from .config_validation import (
    _as_int,
    _as_str,
    _validate_config,
)

logger = logging.getLogger(__name__)

_CONFIG_TEMPLATE_RELATIVE_PATH = Path("config") / "orca_auto.yaml.example"
_TEMPLATE_ALLOWED_ROOT = "/path/to/orca_runs"
_TEMPLATE_ORGANIZED_ROOT = "/path/to/orca_outputs"
_TEMPLATE_ORCA_EXECUTABLE = "/path/to/orca/orca"


def _config_template_path() -> Path:
    return Path(__file__).resolve().parents[1] / _CONFIG_TEMPLATE_RELATIVE_PATH


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
        f"{keys}. orca_auto no longer assumes personal defaults like ~/orca_runs or "
        f"~/opt/orca/orca. Update {path} with explicit Linux paths."
    )


def _placeholder_settings_error(path: Path, placeholder_keys: list[str]) -> ValueError:
    keys = ", ".join(placeholder_keys)
    return ValueError(
        "Config still contains template placeholder paths in "
        f"{keys}. Edit {path} and replace /path/to/... values with your real Linux paths."
    )


@dataclass
class RuntimeConfig:
    allowed_root: str = ""
    organized_root: str = ""
    # max retry count, not total execution count
    default_max_retries: int = 2
    max_concurrent: int = 4
    admission_root: str = ""
    admission_max_concurrent: int = 4

    def __post_init__(self) -> None:
        if not self.organized_root and self.allowed_root:
            self.organized_root = _default_organized_root(self.allowed_root)
        if not self.admission_root and self.allowed_root:
            self.admission_root = self.allowed_root
        if self.admission_max_concurrent < 1:
            self.admission_max_concurrent = max(1, self.max_concurrent)


@dataclass
class PathsConfig:
    orca_executable: str = ""


@dataclass
class TelegramConfig:
    bot_token: str = ""
    chat_id: str = ""
    enabled: bool = False

    def __post_init__(self) -> None:
        self.enabled = bool(self.bot_token and self.chat_id)


@dataclass
class AppConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    telegram: TelegramConfig = field(default_factory=TelegramConfig)


def load_config(config_path: str) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    raw: Dict[str, Any] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
            if isinstance(parsed, dict):
                raw = parsed
    else:
        raise _missing_config_error(path)

    runtime_raw = raw.get("runtime", {}) if isinstance(raw.get("runtime", {}), dict) else {}
    paths_raw = raw.get("paths", {}) if isinstance(raw.get("paths", {}), dict) else {}
    telegram_raw = raw.get("telegram", {}) if isinstance(raw.get("telegram", {}), dict) else {}

    if "platform_mode" in runtime_raw:
        raise ValueError(
            "runtime.platform_mode is removed. orca_auto is Linux-only; delete this legacy key from config."
        )

    allowed_root = _as_str(runtime_raw.get("allowed_root"), "")
    orca_executable = _as_str(paths_raw.get("orca_executable"), "")
    missing_keys: list[str] = []
    if not allowed_root:
        missing_keys.append("runtime.allowed_root")
    if not orca_executable:
        missing_keys.append("paths.orca_executable")
    if missing_keys:
        raise _missing_required_settings_error(path, missing_keys)

    organized_root = _as_str(
        runtime_raw.get("organized_root"),
        _default_organized_root(allowed_root),
    )
    default_max_retries = _as_int(
        runtime_raw.get("default_max_retries"),
        RuntimeConfig.default_max_retries,
    )
    max_concurrent = _as_int(
        runtime_raw.get("max_concurrent"),
        RuntimeConfig.max_concurrent,
    )
    if max_concurrent < 1:
        raise ValueError("runtime.max_concurrent must be an integer >= 1.")
    admission_root = _as_str(
        runtime_raw.get("admission_root"),
        allowed_root,
    )
    admission_max_concurrent = _as_int(
        runtime_raw.get("admission_max_concurrent"),
        max_concurrent,
    )
    if admission_max_concurrent < 1:
        raise ValueError("runtime.admission_max_concurrent must be an integer >= 1.")

    telegram_cfg = TelegramConfig(
        bot_token=_as_str(telegram_raw.get("bot_token"), ""),
        chat_id=str(telegram_raw.get("chat_id", "")).strip(),
    )

    cfg = AppConfig(
        runtime=RuntimeConfig(
            allowed_root=allowed_root,
            organized_root=organized_root,
            default_max_retries=max(0, default_max_retries),
            max_concurrent=max_concurrent,
            admission_root=admission_root,
            admission_max_concurrent=admission_max_concurrent,
        ),
        paths=PathsConfig(
            orca_executable=orca_executable,
        ),
        telegram=telegram_cfg,
    )
    placeholder_keys: list[str] = []
    if cfg.runtime.allowed_root == _TEMPLATE_ALLOWED_ROOT:
        placeholder_keys.append("runtime.allowed_root")
    if cfg.runtime.organized_root == _TEMPLATE_ORGANIZED_ROOT:
        placeholder_keys.append("runtime.organized_root")
    if cfg.paths.orca_executable == _TEMPLATE_ORCA_EXECUTABLE:
        placeholder_keys.append("paths.orca_executable")
    if placeholder_keys:
        raise _placeholder_settings_error(path, placeholder_keys)

    _validate_config(cfg)

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, admission_root=%s, orca_executable=%s, max_concurrent=%d, admission_max_concurrent=%d",
        cfg.runtime.allowed_root,
        cfg.runtime.organized_root,
        cfg.runtime.admission_root,
        cfg.paths.orca_executable,
        cfg.runtime.max_concurrent,
        cfg.runtime.admission_max_concurrent,
    )
    return cfg
