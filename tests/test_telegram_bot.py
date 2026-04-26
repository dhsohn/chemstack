"""Telegram bot handler tests."""

import io
import json
import tempfile
import urllib.error
import unittest
from email.message import Message
from pathlib import Path
from typing import cast
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from chemstack.orca import telegram_bot as bot
from chemstack.orca.cancellation import CancelTargetError
from chemstack.orca.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from chemstack.orca.queue_store import enqueue, mark_completed
from chemstack.orca.state_store import STATE_FILE_NAME
from chemstack.orca.telegram_bot import _handle_cancel, _handle_cron, _handle_help, _handle_list


def _write_running_state(reaction_dir: Path, *, run_id: str, pid: int) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "rxn.inp"),
        "max_retries": 2,
        "status": "running",
        "started_at": "2026-03-01T00:00:00+00:00",
        "updated_at": "2026-03-01T01:00:00+00:00",
        "attempts": [{"index": 1}],
        "final_result": None,
    }
    (reaction_dir / STATE_FILE_NAME).write_text(json.dumps(state), encoding="utf-8")
    (reaction_dir / "run.lock").write_text(json.dumps({"pid": pid}), encoding="utf-8")


class TestTelegramBotHandlers(unittest.TestCase):
    def _make_cfg(self, allowed_root: str) -> AppConfig:
        return AppConfig(
            runtime=RuntimeConfig(allowed_root=allowed_root),
            paths=PathsConfig(),
            telegram=TelegramConfig(bot_token="fake", chat_id="123"),
        )

    def _make_run(
        self,
        reaction_dir: Path,
        *,
        status: str = "completed",
        run_id: str | None = None,
        inp_name: str = "rxn.inp",
    ) -> None:
        reaction_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "run_id": run_id or f"run_{reaction_dir.name}",
            "reaction_dir": str(reaction_dir),
            "selected_inp": str(reaction_dir / inp_name),
            "max_retries": 2,
            "status": status,
            "started_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T01:00:00+00:00",
            "attempts": [{"index": 1}],
            "final_result": {"status": status},
        }
        (reaction_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")

    def test_handle_list_empty(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "")
        self.assertIn("No simulations found", result)

    def test_handle_list_with_runs(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running")
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "")
        self.assertIn("rxn1", result)
        self.assertIn("rxn2", result)
        self.assertIn("(2)", result)

    def test_handle_list_filter(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            self._make_run(allowed / "rxn1", status="completed")
            self._make_run(allowed / "rxn2", status="running")
            cfg = self._make_cfg(str(allowed))
            result = _handle_list(cfg, "running")
        self.assertIn("rxn2", result)
        self.assertNotIn("rxn1", result)
        self.assertIn("(1)", result)

    def test_handle_list_shows_newer_standalone_run_beside_old_queue_history(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()
            reaction_dir = allowed / "rxn1"
            reaction_dir.mkdir()
            entry = enqueue(allowed, str(reaction_dir))
            self.assertTrue(mark_completed(allowed, entry["queue_id"], run_id="run_old"))
            self._make_run(reaction_dir, status="completed", run_id="run_new", inp_name="rerun.inp")
            cfg = self._make_cfg(str(allowed))

            result = _handle_list(cfg, "")

        self.assertIn("(2)", result)
        self.assertEqual(result.count("rxn1"), 2)
        self.assertIn("rerun.inp", result)

    def test_handle_list_clear_clears_tracked_terminal_runs_with_legacy_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            allowed = root / "orca_runs"
            organized = root / "organized" / "project" / "rxn_tracked"
            legacy_dir = allowed / "legacy" / "rxn_legacy"
            running_dir = allowed / "live" / "rxn_running"
            allowed.mkdir()
            self._make_run(organized, status="completed", run_id="run_tracked", inp_name="tracked.inp")
            self._make_run(legacy_dir, status="failed", run_id="run_legacy")
            self._make_run(running_dir, status="running", run_id="run_running")
            (allowed / "job_locations.json").write_text(
                json.dumps(
                    [
                        {
                            "job_id": "job_tracked",
                            "app_name": "orca_auto",
                            "job_type": "orca_opt",
                            "status": "completed",
                            "original_run_dir": str(allowed / "project" / "rxn_tracked"),
                            "molecule_key": "rxn_tracked",
                            "selected_input_xyz": str(organized / "tracked.inp"),
                            "organized_output_dir": str(organized),
                            "latest_known_path": str(organized),
                            "resource_request": {},
                            "resource_actual": {},
                        }
                    ],
                    ensure_ascii=True,
                    indent=2,
                ),
                encoding="utf-8",
            )
            cfg = self._make_cfg(str(allowed))

            result = _handle_list(cfg, "clear")

            self.assertIn("Cleared 2 entries", result)
            self.assertIn("run states: 2", result)
            self.assertFalse((organized / "run_state.json").exists())
            self.assertFalse((legacy_dir / "run_state.json").exists())
            self.assertTrue((running_dir / "run_state.json").exists())

    def test_handle_help(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        result = _handle_help(cfg, "")
        self.assertIn("/list", result)
        self.assertIn("/cancel &lt;target&gt;", result)
        self.assertIn("/help", result)
        self.assertNotIn("/queue", result)

    def test_handle_list_missing_root(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent_root_xyz")
        result = _handle_list(cfg, "")
        self.assertIn("not found", result)

    @patch("chemstack.orca.cancellation.os.kill")
    @patch("chemstack.orca.process_tracking.is_process_alive", return_value=True)
    def test_handle_cancel_direct_run(self, mock_alive, mock_kill) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            reaction_dir = allowed / "group_a" / "rxn1"
            _write_running_state(reaction_dir, run_id="run_direct_1", pid=4321)
            cfg = self._make_cfg(str(allowed))

            result = _handle_cancel(cfg, "group_a/rxn1")

        self.assertIn("Cancel requested for running simulation", result)
        self.assertIn("rxn1", result)
        mock_alive.assert_called_once_with(4321)
        mock_kill.assert_called_once()

    def test_handle_cancel_usage(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        result = _handle_cancel(cfg, "")
        self.assertIn("/cancel &lt;target&gt;", result)

    def test_handle_cancel_error_and_terminal_paths(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        with patch(
            "chemstack.orca.telegram_bot.cancel_target",
            side_effect=CancelTargetError("bad <target>"),
        ):
            self.assertIn("bad &lt;target&gt;", _handle_cancel(cfg, "bad"))

        with patch("chemstack.orca.telegram_bot.cancel_target", return_value=None):
            self.assertIn("Cannot cancel", _handle_cancel(cfg, "done"))

        with patch(
            "chemstack.orca.telegram_bot.cancel_target",
            return_value=SimpleNamespace(action="cancelled", source="queue", queue_id="q-1", reaction_dir=""),
        ):
            self.assertIn("Cancelled", _handle_cancel(cfg, "q-1"))

        with patch(
            "chemstack.orca.telegram_bot.cancel_target",
            return_value=SimpleNamespace(action="requested", source="queue", queue_id="q-2", reaction_dir=""),
        ):
            self.assertIn("running job", _handle_cancel(cfg, "q-2"))

    def test_handle_cron_reports_empty_crontab(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")

        with patch(
            "chemstack.orca.telegram_bot.subprocess.run",
            return_value=SimpleNamespace(stdout=""),
        ):
            result = _handle_cron(cfg, "")

        self.assertIn("No crontab entries found", result)

    def test_handle_cron_formats_managed_entries(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        crontab = "\n".join(
            [
                "# ORCA_AUTO_CRON_START",
                "0 9,21 * * * /opt/chemstack/cron_dft_summary --config /tmp/chemstack.yaml",
                "0 * * * * /opt/chemstack/cron_custom --config /tmp/chemstack.yaml",
                "# ORCA_AUTO_CRON_END",
            ]
        )

        with patch(
            "chemstack.orca.telegram_bot.subprocess.run",
            return_value=SimpleNamespace(stdout=crontab),
        ):
            result = _handle_cron(cfg, "")

        self.assertIn("Cron Jobs", result)
        self.assertIn("dft_summary", result)
        self.assertIn("Twice daily", result)
        self.assertIn("custom", result)

    def test_handle_list_clear_reports_nothing_to_clear(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()
            cfg = self._make_cfg(str(allowed))
            with patch("chemstack.orca.telegram_bot._clear_terminal_entries", return_value=(0, 0)):
                result = _handle_list(cfg, "clear")

        self.assertIn("Nothing to clear", result)

    def test_handle_cron_reports_read_failure_and_missing_managed_block(self) -> None:
        cfg = self._make_cfg("/tmp/nonexistent")
        with patch(
            "chemstack.orca.telegram_bot.subprocess.run",
            side_effect=RuntimeError("no crontab"),
        ):
            self.assertIn("Failed to read crontab", _handle_cron(cfg, ""))

        with patch(
            "chemstack.orca.telegram_bot.subprocess.run",
            return_value=SimpleNamespace(stdout="0 * * * * /bin/echo hi\n"),
        ):
            self.assertIn("No chemstack cron jobs", _handle_cron(cfg, ""))


class _TelegramApiResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def __enter__(self) -> "_TelegramApiResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


def _make_cfg(allowed_root: str = "/tmp/nonexistent") -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(allowed_root=allowed_root),
        paths=PathsConfig(),
        telegram=TelegramConfig(bot_token="fake-token", chat_id="123"),
    )


def test_api_call_handles_success_api_error_http_error_and_generic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: _TelegramApiResponse({"ok": True, "result": {"message_id": 1}}),
    )
    assert bot._api_call("token", "sendMessage") == {"message_id": 1}

    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: _TelegramApiResponse({"ok": False, "description": "bad"}),
    )
    assert bot._api_call("token", "sendMessage") is None

    def raise_http_error(request: object, *, timeout: int) -> object:
        headers: Message[str, str] = Message()
        raise urllib.error.HTTPError(
            url="https://example.test",
            code=500,
            msg="server error",
            hdrs=headers,
            fp=io.BytesIO(b"server failed"),
        )

    monkeypatch.setattr(bot, "urlopen_with_ipv4_fallback", raise_http_error)
    assert bot._api_call("token", "sendMessage") is None

    monkeypatch.setattr(
        bot,
        "urlopen_with_ipv4_fallback",
        lambda request, *, timeout: (_ for _ in ()).throw(RuntimeError("offline")),
    )
    assert bot._api_call("token", "sendMessage") is None


def test_send_message_truncates_and_set_commands_delegates(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, str, dict[str, object]]] = []

    def fake_api_call(token: str, method: str, payload: dict[str, object]) -> object:
        captured.append((token, method, payload))
        return {"ok": True}

    monkeypatch.setattr(bot, "_api_call", fake_api_call)

    assert bot._send_message("token", "123", "x" * 5000, parse_mode=None)
    assert captured[0][0] == "token"
    assert captured[0][1] == "sendMessage"
    assert len(str(captured[0][2]["text"])) == bot._MAX_MESSAGE_LENGTH
    assert "parse_mode" not in captured[0][2]

    bot._set_bot_commands("token")
    assert captured[1][1] == "setMyCommands"
    commands = cast(list[dict[str, str]], captured[1][2]["commands"])
    assert [item["command"] for item in commands] == [
        "list",
        "cancel",
        "cron",
        "help",
    ]


def test_handle_list_formats_rows_without_input_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cfg = _make_cfg(str(tmp_path))
    monkeypatch.setattr(
        bot,
        "_collect_unified",
        lambda allowed_root: [
            {
                "dir": "rxn <one>",
                "status": "running",
                "elapsed": "1m",
                "inp": "",
            }
        ],
    )

    result = bot._handle_list(cfg, "")

    assert "rxn &lt;one&gt;" in result
    assert "<code>" not in result


def test_run_bot_disabled_and_polling_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    disabled_cfg = AppConfig(
        runtime=RuntimeConfig(allowed_root="/tmp"),
        paths=PathsConfig(),
        telegram=TelegramConfig(),
    )
    assert bot.run_bot(disabled_cfg) == 1

    cfg = _make_cfg("/tmp")
    sent: list[str] = []
    calls = {"polls": 0}

    def fake_api_call(
        token: str,
        method: str,
        payload: dict[str, object] | None = None,
        *,
        timeout: int = 35,
    ) -> object:
        if method == "setMyCommands":
            return True
        if method == "getUpdates":
            calls["polls"] += 1
            if calls["polls"] > 1:
                raise KeyboardInterrupt
            return [
                "skip",
                {"update_id": 1, "message": "skip"},
                {"update_id": 2, "message": {"chat": {"id": "999"}, "text": "/help"}},
                {"update_id": 3, "message": {"chat": {"id": "123"}, "text": "hello"}},
                {"update_id": 4, "message": {"chat": {"id": "123"}, "text": "/unknown"}},
                {"update_id": 5, "message": {"chat": {"id": "123"}, "text": "/list running"}},
                {"update_id": 6, "message": {"chat": {"id": "123"}, "text": "/boom"}},
            ]
        return None

    monkeypatch.setattr(bot, "_api_call", fake_api_call)

    def fake_send_message(token: str, chat_id: str, text: str, **kwargs: object) -> bool:
        sent.append(text)
        return True

    monkeypatch.setattr(bot, "_send_message", fake_send_message)
    monkeypatch.setitem(bot._HANDLERS, "list", lambda cfg, args: f"list:{args}")
    monkeypatch.setitem(
        bot._HANDLERS,
        "boom",
        lambda cfg, args: (_ for _ in ()).throw(RuntimeError("handler failed")),
    )

    assert bot.run_bot(cfg) == 0
    assert any("Unknown command" in text for text in sent)
    assert "list:running" in sent
    assert any("Error: handler failed" in text for text in sent)


def test_run_bot_poll_errors_sleep_and_continue(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_cfg("/tmp")
    calls = {"polls": 0}
    sleeps: list[float] = []

    def fake_api_call(
        token: str,
        method: str,
        payload: dict[str, object] | None = None,
        *,
        timeout: int = 35,
    ) -> object:
        if method == "setMyCommands":
            return True
        calls["polls"] += 1
        if calls["polls"] == 1:
            raise RuntimeError("temporary")
        raise KeyboardInterrupt

    monkeypatch.setattr(bot, "_api_call", fake_api_call)
    monkeypatch.setattr(bot.time, "sleep", lambda seconds: sleeps.append(seconds))

    assert bot.run_bot(cfg) == 0
    assert sleeps == [5]

if __name__ == "__main__":
    unittest.main()
