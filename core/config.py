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


def _default_allowed_root() -> str:
    return str(Path.home() / "orca_runs")


def _default_organized_root() -> str:
    return str(Path.home() / "orca_outputs")


def _default_orca_executable() -> str:
    return str(Path.home() / "opt" / "orca" / "orca")


@dataclass
class RuntimeConfig:
    allowed_root: str = ""
    organized_root: str = ""
    # max retry count, not total execution count
    default_max_retries: int = 2

    def __post_init__(self) -> None:
        if not self.allowed_root:
            self.allowed_root = _default_allowed_root()
        if not self.organized_root:
            self.organized_root = _default_organized_root()


@dataclass
class PathsConfig:
    orca_executable: str = ""

    def __post_init__(self) -> None:
        if not self.orca_executable:
            self.orca_executable = _default_orca_executable()


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
        logger.warning("Config file not found, using defaults: %s", path)

    runtime_raw = raw.get("runtime", {}) if isinstance(raw.get("runtime", {}), dict) else {}
    paths_raw = raw.get("paths", {}) if isinstance(raw.get("paths", {}), dict) else {}
    telegram_raw = raw.get("telegram", {}) if isinstance(raw.get("telegram", {}), dict) else {}

    if "platform_mode" in runtime_raw:
        raise ValueError(
            "runtime.platform_mode is removed. orca_auto is Linux-only; delete this legacy key from config."
        )

    allowed_root = _as_str(
        runtime_raw.get("allowed_root"),
        _default_allowed_root(),
    )
    organized_root = _as_str(
        runtime_raw.get("organized_root"),
        _default_organized_root(),
    )
    default_max_retries = _as_int(
        runtime_raw.get("default_max_retries"),
        RuntimeConfig.default_max_retries,
    )

    telegram_cfg = TelegramConfig(
        bot_token=_as_str(telegram_raw.get("bot_token"), ""),
        chat_id=str(telegram_raw.get("chat_id", "")).strip(),
    )

    cfg = AppConfig(
        runtime=RuntimeConfig(
            allowed_root=allowed_root,
            organized_root=organized_root,
            default_max_retries=max(0, default_max_retries),
        ),
        paths=PathsConfig(
            orca_executable=_as_str(paths_raw.get("orca_executable"), _default_orca_executable()),
        ),
        telegram=telegram_cfg,
    )
    _validate_config(cfg)

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, orca_executable=%s",
        cfg.runtime.allowed_root, cfg.runtime.organized_root, cfg.paths.orca_executable,
    )
    return cfg
