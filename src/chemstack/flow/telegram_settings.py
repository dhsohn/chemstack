"""Settings resolution for the chemstack_flow Telegram bot."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import CHEMSTACK_REPO_ROOT_ENV_VAR
from chemstack.core.config import TelegramConfig
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.core.notifications import load_telegram_config_from_file

from . import _activity_sources


@dataclass(frozen=True)
class TelegramBotSettings:
    telegram: TelegramConfig
    workflow_root: str | None
    crest_config: str | None
    xtb_config: str | None
    orca_config: str | None
    orca_repo_root: str | None

    @property
    def enabled(self) -> bool:
        return self.telegram.enabled


def _env_text(getenv: Callable[..., str | None], name: str) -> str:
    return str(getenv(name, "") or "").strip()


def settings_from_env(
    *,
    activity_sources: Any = _activity_sources,
    getenv: Callable[..., str | None] = os.getenv,
) -> TelegramBotSettings:
    shared_config = activity_sources.discover_shared_config(None)
    return TelegramBotSettings(
        telegram=TelegramConfig(
            bot_token=_env_text(getenv, "CHEMSTACK_FLOW_TELEGRAM_BOT_TOKEN"),
            chat_id=_env_text(getenv, "CHEMSTACK_FLOW_TELEGRAM_CHAT_ID"),
        ),
        workflow_root=activity_sources.discover_workflow_root(None),
        crest_config=shared_config,
        xtb_config=shared_config,
        orca_config=shared_config,
        orca_repo_root=_env_text(getenv, CHEMSTACK_REPO_ROOT_ENV_VAR) or None,
    )


def telegram_from_config_path(
    config_path: str | None,
    *,
    path_cls: Any = Path,
    load_telegram_config: Callable[[str | None], TelegramConfig] = load_telegram_config_from_file,
) -> TelegramConfig:
    config_text = str(config_path or "").strip()
    if config_text:
        try:
            path_cls(config_text).expanduser().resolve()
        except OSError:
            return TelegramConfig()
    return load_telegram_config(config_path)


def settings_from_config(
    config_path: str | None = None,
    *,
    activity_sources: Any = _activity_sources,
    getenv: Callable[..., str | None] = os.getenv,
    path_cls: Any = Path,
    load_telegram_config: Callable[[str | None], TelegramConfig] | None = None,
    workflow_root_from_config: Callable[
        [str | None], str | None
    ] = shared_workflow_root_from_config,
) -> TelegramBotSettings:
    shared_config = activity_sources.discover_shared_config(config_path)
    if load_telegram_config is None:
        telegram = telegram_from_config_path(shared_config, path_cls=path_cls)
    else:
        telegram = load_telegram_config(shared_config)
    if not telegram.enabled:
        telegram = TelegramConfig(
            bot_token=_env_text(getenv, "CHEMSTACK_FLOW_TELEGRAM_BOT_TOKEN"),
            chat_id=_env_text(getenv, "CHEMSTACK_FLOW_TELEGRAM_CHAT_ID"),
        )
    workflow_root = workflow_root_from_config(
        shared_config
    ) or activity_sources.discover_workflow_root(None)
    return TelegramBotSettings(
        telegram=telegram,
        workflow_root=workflow_root,
        crest_config=shared_config,
        xtb_config=shared_config,
        orca_config=shared_config,
        orca_repo_root=_env_text(getenv, CHEMSTACK_REPO_ROOT_ENV_VAR) or None,
    )


__all__ = [
    "TelegramBotSettings",
    "settings_from_config",
    "settings_from_env",
    "telegram_from_config_path",
]
