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
                {
                    "label": "wf-a",
                    "activity_id": "wf-a",
                    "kind": "workflow",
                    "engine": "orca",
                    "status": "running",
                    "source": "chem_flow",
                },
                {
                    "label": "mol-b",
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "running",
                    "source": "crest_auto",
                    "metadata": {
                        "job_dir": "/tmp/crest/workflow_jobs/wf-a/stage_01_crest",
                    },
                },
                {
                    "label": "ts-1",
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "source": "chemstack_orca",
                    "metadata": {
                        "reaction_dir": "/tmp/orca/standalone/ts-1",
                    },
                },
            ]
        },
    )

    text = bot._handle_list(_settings(), "")

    assert "<b>active_simulations</b>: <code>2</code>" in text
    assert "- <code>wf-a</code> kind=<code>workflow</code> engine=<code>orca</code>" in text
    assert "\xa0\xa0- <code>crest-q-1</code> kind=<code>job</code> engine=<code>crest</code>" in text
    assert "- <code>orca-q-1</code> kind=<code>job</code> engine=<code>orca</code>" in text


def test_handle_list_filter_keeps_workflow_parent_for_visible_child(monkeypatch) -> None:
    monkeypatch.setattr(
        bot,
        "list_activities",
        lambda **kwargs: {
            "activities": [
                {
                    "label": "wf-a",
                    "activity_id": "wf-a",
                    "kind": "workflow",
                    "engine": "orca",
                    "status": "running",
                    "source": "chem_flow",
                },
                {
                    "label": "mol-b",
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "source": "crest_auto",
                    "metadata": {
                        "job_dir": "/tmp/crest/workflow_jobs/wf-a/stage_01_crest",
                    },
                },
            ]
        },
    )

    text = bot._handle_list(_settings(), "pending")

    assert "<b>active_simulations</b>: <code>0</code>" in text
    assert "- <code>wf-a</code> kind=<code>workflow</code>" in text
    assert "\xa0\xa0- <code>crest-q-1</code> kind=<code>job</code> engine=<code>crest</code> status=<code>pending</code>" in text


def test_handle_list_shows_all_workflow_child_jobs(monkeypatch) -> None:
    child_rows = [
        {
            "label": f"ts-{index}",
            "activity_id": f"orca-q-{index}",
            "kind": "job",
            "engine": "orca",
            "status": "running",
            "source": "chemstack_orca",
            "metadata": {
                "reaction_dir": f"/tmp/orca/workflow_jobs/wf-a/stage_03_orca/case_{index:03d}",
            },
        }
        for index in range(1, 10)
    ]
    monkeypatch.setattr(
        bot,
        "list_activities",
        lambda **kwargs: {
            "activities": [
                {
                    "label": "wf-a",
                    "activity_id": "wf-a",
                    "kind": "workflow",
                    "engine": "orca",
                    "status": "running",
                    "source": "chem_flow",
                },
                *child_rows,
            ]
        },
    )

    text = bot._handle_list(_settings(), "")

    assert "<b>active_simulations</b>: <code>9</code>" in text
    assert text.count("\xa0\xa0- <code>orca-q-") == 9
    assert "- <code>wf-a</code> kind=<code>workflow</code> engine=<code>orca</code>" in text


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


def test_settings_from_config_uses_shared_telegram_section(tmp_path: Path) -> None:
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "workflow:",
                "  root: /tmp/workflows",
                "telegram:",
                '  bot_token: "bot-token"',
                '  chat_id: "chat-id"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    settings = bot.settings_from_config(str(config_path))

    assert settings.telegram.bot_token == "bot-token"
    assert settings.telegram.chat_id == "chat-id"
    assert settings.workflow_root == str(Path("/tmp/workflows").resolve())
    assert settings.crest_auto_config == str(config_path.resolve())
    assert settings.xtb_auto_config == str(config_path.resolve())
    assert settings.orca_auto_config == str(config_path.resolve())


def test_cmd_bot_and_parser(monkeypatch) -> None:
    import chemstack.flow.telegram_bot as imported_bot

    monkeypatch.setattr(imported_bot, "run_bot", lambda: 7)
    assert cli.cmd_bot(SimpleNamespace()) == 7

    parser = cli.build_parser()
    args = parser.parse_args(["bot"])
    assert args.command == "bot"
    assert args.func is cli.cmd_bot
