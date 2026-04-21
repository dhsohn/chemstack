# ruff: noqa: E402

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.core.config import TelegramConfig

from chemstack.flow import cli
from chemstack.flow import telegram_bot as bot


def _settings() -> bot.TelegramBotSettings:
    return bot.TelegramBotSettings(
        telegram=TelegramConfig(bot_token="bot-token", chat_id="chat-id"),
        workflow_root="/tmp/workflow_root",
        crest_auto_config="/tmp/chemstack.yaml",
        xtb_auto_config="/tmp/chemstack.yaml",
        orca_auto_config="/tmp/chemstack.yaml",
        orca_auto_repo_root=None,
    )


def test_handle_list_formats_unified_activity_rows(monkeypatch) -> None:
    monkeypatch.setattr(
        bot,
        "list_activities",
        lambda **kwargs: {
            "activities": [
                {"label": "wf-a", "activity_id": "wf-a", "engine": "xtb", "status": "running", "source": "chem_flow"},
                {"label": "mol-b", "activity_id": "crest-q-1", "engine": "crest", "status": "pending", "source": "crest_auto"},
            ]
        },
    )

    text = bot._handle_list(_settings(), "")

    assert "<b>Activities</b> (2)" in text
    assert "wf-a" in text
    assert "crest_auto" in text


def test_handle_cancel_routes_through_activity_control(monkeypatch) -> None:
    monkeypatch.setattr(
        bot,
        "cancel_activity",
        lambda **kwargs: {
            "label": "wf-a",
            "activity_id": "wf-a",
            "status": "cancel_requested",
        },
    )

    text = bot._handle_cancel(_settings(), "wf-a")

    assert "wf-a" in text
    assert "cancel_requested" in text


def test_handle_help_mentions_only_supported_commands() -> None:
    text = bot._handle_help(_settings(), "")

    assert "/list" in text
    assert "/cancel" in text
    assert "/help" in text
    assert "/cron" not in text


def test_settings_from_env_uses_autodiscovery(monkeypatch) -> None:
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_CHAT_ID", "chat-id")
    monkeypatch.setattr(bot, "_discover_workflow_root", lambda explicit: "/tmp/wf")
    monkeypatch.setattr(
        bot,
        "_discover_sibling_config",
        lambda explicit, *, app_name: "/tmp/chemstack.yaml",
    )

    settings = bot.settings_from_env()

    assert settings.telegram.bot_token == "bot-token"
    assert settings.telegram.chat_id == "chat-id"
    assert settings.workflow_root == "/tmp/wf"
    assert settings.crest_auto_config == "/tmp/chemstack.yaml"
    assert settings.xtb_auto_config == "/tmp/chemstack.yaml"
    assert settings.orca_auto_config == "/tmp/chemstack.yaml"


def test_cmd_bot_and_parser(monkeypatch) -> None:
    import chemstack.flow.telegram_bot as imported_bot

    monkeypatch.setattr(imported_bot, "run_bot", lambda: 7)
    assert cli.cmd_bot(SimpleNamespace()) == 7

    parser = cli.build_parser()
    args = parser.parse_args(["bot"])
    assert args.command == "bot"
    assert args.func is cli.cmd_bot
