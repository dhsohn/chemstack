# ruff: noqa: E402

from __future__ import annotations

import io
import json
import sys
import urllib.error
from email.message import Message
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

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
                    "engine": "workflow",
                    "status": "running",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "current_engine": "orca",
                        "request_parameters": {"crest_mode": "nci"},
                    },
                },
                {
                    "label": "mol-b",
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "running",
                    "source": "crest_auto",
                    "submitted_at": "2026-04-26T01:10:00+00:00",
                    "updated_at": "2026-04-26T01:10:00+00:00",
                    "metadata": {
                        "task_kind": "conformer_search",
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
                    "submitted_at": "2026-04-26T01:20:00+00:00",
                    "updated_at": "2026-04-26T01:20:00+00:00",
                    "metadata": {
                        "task_kind": "irc",
                        "reaction_dir": "/tmp/orca/standalone/ts-1",
                    },
                },
            ]
        },
    )

    text = bot._handle_list(_settings(), "")

    assert "active_simulations: 2" in text
    assert "Status" in text and "Name" in text and "Detail" in text and "ID" in text and "Elapsed" in text
    assert "wf-a" in text
    assert "ts_search(nci)" in text
    assert "crest-q-1" not in text
    assert "orca-q-1" in text
    assert "IRC" in text


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
                    "engine": "workflow",
                    "status": "running",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "current_engine": "crest",
                    },
                },
                {
                    "label": "mol-b",
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "source": "crest_auto",
                    "submitted_at": "2026-04-26T01:10:00+00:00",
                    "updated_at": "2026-04-26T01:10:00+00:00",
                    "metadata": {
                        "task_kind": "conformer_search",
                        "mode": "nci",
                        "job_dir": "/tmp/crest/workflow_jobs/wf-a/stage_01_crest",
                    },
                },
            ]
        },
    )

    text = bot._handle_list(_settings(), "pending")

    assert "active_simulations: 0" in text
    assert "wf-a" in text
    assert "crest-q-1" in text
    assert "conformer_search(nci)" in text


def test_handle_list_uses_global_active_simulation_count_from_full_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        bot,
        "list_activities",
        lambda **kwargs: {
            "activities": [
                {
                    "label": "hidden-run",
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "source": "chemstack_orca",
                },
                {
                    "label": "visible-pending",
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "source": "crest_auto",
                },
            ],
            "sources": {"orca_auto_config": "/tmp/chemstack.yaml"},
        },
    )

    def _fake_count(items, *, config_path=None):
        captured["items"] = list(items)
        captured["config_path"] = config_path
        return 4

    monkeypatch.setattr(bot, "count_global_active_simulations", _fake_count)

    text = bot._handle_list(_settings(), "pending")

    assert "active_simulations: 4" in text
    assert len(captured["items"]) == 2
    assert captured["config_path"] == "/tmp/chemstack.yaml"
    assert "crest-q-1" in text
    assert "conformer_search" in text


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
                    "engine": "workflow",
                    "status": "running",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "current_engine": "orca",
                    },
                },
                *child_rows,
            ]
        },
    )

    text = bot._handle_list(_settings(), "")

    assert "active_simulations: 9" in text
    assert text.count("orca-q-") == 9
    assert "wf-a" in text
    assert "ts_search" in text


def test_handle_list_clear_uses_shared_clear_activity_control(monkeypatch) -> None:
    monkeypatch.setattr(
        bot,
        "clear_activities",
        lambda **kwargs: {
            "total_cleared": 4,
            "cleared": {
                "workflows": 1,
                "xtb_queue_entries": 1,
                "crest_queue_entries": 0,
                "orca_queue_entries": 1,
                "orca_run_states": 1,
            },
        },
    )

    text = bot._handle_list(_settings(), "clear")

    assert "Cleared 4 completed/failed/cancelled entries." in text
    assert "workflows: 1" in text
    assert "xTB queue entries: 1" in text
    assert "ORCA queue entries: 1" in text
    assert "ORCA run states: 1" in text


def test_handle_list_reports_empty_activity_results(monkeypatch) -> None:
    monkeypatch.setattr(bot, "list_activities", lambda **kwargs: {"activities": []})
    monkeypatch.setattr(bot, "count_global_active_simulations", lambda items, *, config_path=None: 0)

    text = bot._handle_list(_settings(), "running")

    assert "active_simulations: 0" in text
    assert "No matching activities." in text


def test_activity_counter_config_path_falls_back_to_settings() -> None:
    settings = bot.TelegramBotSettings(
        telegram=TelegramConfig(bot_token="bot-token", chat_id="chat-id"),
        workflow_root=None,
        crest_auto_config="",
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config=None,
        orca_auto_repo_root=None,
    )

    assert bot._activity_counter_config_path({"sources": {}}, settings=settings) == "/tmp/xtb.yaml"
    assert (
        bot._activity_counter_config_path(
            {"sources": {"crest_auto_config": " /tmp/crest.yaml "}},
            settings=settings,
        )
        == "/tmp/crest.yaml"
    )


def test_send_preformatted_response_wraps_chunks_in_pre(monkeypatch) -> None:
    sent: list[tuple[str, str | None]] = []

    def fake_send(token: str, chat_id: str, text: str, *, parse_mode: str | None = "HTML") -> bool:
        sent.append((text, parse_mode))
        return True

    monkeypatch.setattr(bot, "_send_message", fake_send)

    text = "\n".join(f"line-{index} {'x' * 20}" for index in range(8))

    assert bot._send_preformatted_response("bot-token", "chat-id", text, limit=80)
    assert len(sent) > 1
    assert all(mode == "HTML" for _chunk, mode in sent)
    assert all(chunk.startswith("<pre>") and chunk.endswith("</pre>") for chunk, _mode in sent)


def test_message_chunks_rejects_non_positive_limit_and_splits_long_line() -> None:
    with pytest.raises(ValueError, match="positive"):
        bot._message_chunks("hello", limit=0)

    assert bot._message_chunks("abcdef", limit=2) == ["ab", "cd", "ef"]


def test_send_response_returns_false_when_all_send_attempts_fail(monkeypatch) -> None:
    monkeypatch.setattr(bot, "_send_message", lambda *args, **kwargs: False)

    assert bot._send_response("bot-token", "chat-id", "<b>hello</b>", parse_mode="HTML") is False


def test_send_preformatted_response_falls_back_to_plain_text_and_reports_failure(monkeypatch) -> None:
    sent_modes: list[str | None] = []

    def fake_send(token: str, chat_id: str, text: str, *, parse_mode: str | None = "HTML") -> bool:
        sent_modes.append(parse_mode)
        return parse_mode is None

    monkeypatch.setattr(bot, "_send_message", fake_send)

    assert bot._send_preformatted_response("bot-token", "chat-id", "hello")
    assert sent_modes == ["HTML", None]

    monkeypatch.setattr(bot, "_send_message", lambda *args, **kwargs: False)
    assert bot._send_preformatted_response("bot-token", "chat-id", "hello") is False

    with pytest.raises(ValueError, match="exceed wrapper"):
        bot._send_preformatted_response("bot-token", "chat-id", "hello", limit=10)


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


def test_handle_cancel_usage_and_error_paths(monkeypatch) -> None:
    assert "Usage:" in bot._handle_cancel(_settings(), "")

    monkeypatch.setattr(
        bot,
        "cancel_activity",
        lambda **kwargs: (_ for _ in ()).throw(LookupError("missing <target>")),
    )

    assert "missing &lt;target&gt;" in bot._handle_cancel(_settings(), "wf-missing")


def test_handle_help_mentions_only_supported_commands() -> None:
    text = bot._handle_help(_settings(), "")

    assert "/list" in text
    assert "/list clear" in text
    assert "/cancel" in text
    assert "/help" in text
    assert "/cron" not in text


def test_send_response_splits_long_messages(monkeypatch) -> None:
    sent: list[tuple[str, str | None]] = []

    def fake_send(token: str, chat_id: str, text: str, *, parse_mode: str | None = "HTML") -> bool:
        sent.append((text, parse_mode))
        return True

    monkeypatch.setattr(bot, "_send_message", fake_send)

    text = "\n".join(f"<code>line-{index}</code> {'x' * 28}" for index in range(8))

    assert bot._send_response("bot-token", "chat-id", text, parse_mode="HTML", limit=80)
    assert len(sent) > 1
    assert all(len(chunk) <= 80 for chunk, _mode in sent)
    assert all(mode == "HTML" for _chunk, mode in sent)


def test_send_response_falls_back_to_plain_text_when_html_send_fails(monkeypatch) -> None:
    sent_modes: list[str | None] = []

    def fake_send(token: str, chat_id: str, text: str, *, parse_mode: str | None = "HTML") -> bool:
        sent_modes.append(parse_mode)
        return parse_mode is None

    monkeypatch.setattr(bot, "_send_message", fake_send)

    assert bot._send_response("bot-token", "chat-id", "<b>hello</b>", parse_mode="HTML")
    assert sent_modes == ["HTML", None]


def test_send_message_truncates_text_and_omits_parse_mode_when_none(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_api_call(token: str, method: str, payload: dict[str, Any], **kwargs: Any) -> object:
        captured.update({"token": token, "method": method, "payload": payload})
        return {"message_id": 1}

    monkeypatch.setattr(bot, "_api_call", fake_api_call)

    assert bot._send_message("bot-token", "chat-id", "x" * 5000, parse_mode=None)
    assert captured["token"] == "bot-token"
    assert captured["method"] == "sendMessage"
    assert captured["payload"]["chat_id"] == "chat-id"
    assert len(captured["payload"]["text"]) == bot._MAX_MESSAGE_LENGTH
    assert "parse_mode" not in captured["payload"]


def test_api_call_handles_success_api_error_http_error_and_generic_error(monkeypatch) -> None:
    class Response:
        def __init__(self, payload: dict[str, Any]) -> None:
            self.payload = payload

        def __enter__(self) -> "Response":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(self.payload).encode("utf-8")

    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: Response({"ok": True, "result": {"id": 1}}),
    )
    assert bot._api_call("token", "method") == {"id": 1}

    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: Response({"ok": False, "description": "bad"}),
    )
    assert bot._api_call("token", "method") is None

    def raise_http_error(request: object, *, timeout: int) -> object:
        headers: Message[str, str] = Message()
        raise urllib.error.HTTPError(
            url="https://example.test",
            code=429,
            msg="too many",
            hdrs=headers,
            fp=io.BytesIO(b"rate limited"),
        )

    monkeypatch.setattr(bot, "urlopen_with_ipv4_fallback", raise_http_error)
    assert bot._api_call("token", "method") is None

    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    assert bot._api_call("token", "method") is None


def test_set_bot_commands_delegates_to_api_call(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_api_call(token: str, method: str, payload: dict[str, Any]) -> None:
        captured.update({"token": token, "method": method, "payload": payload})

    monkeypatch.setattr(bot, "_api_call", fake_api_call)

    bot._set_bot_commands("bot-token")

    assert captured["token"] == "bot-token"
    assert captured["method"] == "setMyCommands"
    assert [item["command"] for item in captured["payload"]["commands"]] == ["list", "cancel", "help"]


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


def test_telegram_from_config_path_handles_empty_missing_invalid_and_missing_section(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert not bot._telegram_from_config_path("").enabled
    assert not bot._telegram_from_config_path(str(tmp_path / "missing.yaml")).enabled

    bad_yaml = tmp_path / "bad.yaml"
    bad_yaml.write_text("telegram: [", encoding="utf-8")
    assert not bot._telegram_from_config_path(str(bad_yaml)).enabled

    non_mapping = tmp_path / "list.yaml"
    non_mapping.write_text("- item\n", encoding="utf-8")
    assert not bot._telegram_from_config_path(str(non_mapping)).enabled

    no_telegram = tmp_path / "no-telegram.yaml"
    no_telegram.write_text("workflow:\n  root: /tmp/wf\n", encoding="utf-8")
    assert not bot._telegram_from_config_path(str(no_telegram)).enabled

    class BadPath:
        def __init__(self, _value: object) -> None:
            pass

        def expanduser(self) -> "BadPath":
            raise OSError("bad path")

    monkeypatch.setattr(bot, "Path", BadPath)
    assert not bot._telegram_from_config_path("/bad").enabled


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


def test_settings_from_config_falls_back_to_environment_when_config_telegram_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text("workflow:\n  root: /tmp/workflows\n", encoding="utf-8")
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "env-token")
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_CHAT_ID", "env-chat")

    settings = bot.settings_from_config(str(config_path))

    assert settings.enabled is True
    assert settings.telegram.bot_token == "env-token"
    assert settings.telegram.chat_id == "env-chat"


def test_run_bot_disabled_settings_returns_error() -> None:
    settings = bot.TelegramBotSettings(
        telegram=TelegramConfig(),
        workflow_root=None,
        crest_auto_config=None,
        xtb_auto_config=None,
        orca_auto_config=None,
        orca_auto_repo_root=None,
    )

    assert bot.run_bot(settings) == 1


def test_run_bot_processes_known_unknown_and_handler_error_updates(monkeypatch) -> None:
    settings = _settings()
    sent: list[tuple[str, str | None]] = []
    calls = {"polls": 0}

    def fake_api_call(token: str, method: str, payload: dict[str, Any] | None = None, **kwargs: Any) -> Any:
        if method == "setMyCommands":
            return True
        if method == "getUpdates":
            calls["polls"] += 1
            if calls["polls"] > 1:
                raise KeyboardInterrupt
            return [
                "skip",
                {"update_id": 1, "message": "skip"},
                {"update_id": 2, "message": {"chat": {"id": "other"}, "text": "/help"}},
                {"update_id": 3, "message": {"chat": {"id": "chat-id"}, "text": "hello"}},
                {"update_id": 4, "message": {"chat": {"id": "chat-id"}, "text": "/unknown"}},
                {"update_id": 5, "message": {"chat": {"id": "chat-id"}, "text": "/list"}},
                {"update_id": 6, "message": {"chat": {"id": "chat-id"}, "text": "/boom"}},
            ]
        return None

    def fake_send_response(
        token: str,
        chat_id: str,
        text: str,
        *,
        parse_mode: str | None = "HTML",
        limit: int = bot._MAX_MESSAGE_LENGTH,
    ) -> bool:
        sent.append((text, parse_mode))
        return True

    monkeypatch.setitem(
        bot._HANDLERS,
        "boom",
        lambda _settings, _args: (_ for _ in ()).throw(RuntimeError("bad <boom>")),
    )
    monkeypatch.setitem(bot._HANDLERS, "list", lambda _settings, _args: "list body")
    monkeypatch.setattr(bot, "_api_call", fake_api_call)
    monkeypatch.setattr(bot, "_send_response", fake_send_response)
    monkeypatch.setattr(bot, "_send_preformatted_response", fake_send_response)

    assert bot.run_bot(settings) == 0
    assert any("Unknown command: /unknown" in text for text, _mode in sent)
    assert any(text == "list body" and mode == "HTML" for text, mode in sent)
    assert any("Error: bad &lt;boom&gt;" in text for text, _mode in sent)


def test_cmd_bot_and_parser(monkeypatch) -> None:
    import chemstack.flow.telegram_bot as imported_bot

    monkeypatch.setattr(imported_bot, "run_bot", lambda: 7)
    assert cli.cmd_bot(SimpleNamespace()) == 7

    parser = cli.build_parser()
    args = parser.parse_args(["bot"])
    assert args.command == "bot"
    assert args.func is cli.cmd_bot
