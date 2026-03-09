"""Telegram notifier module tests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from core.config import TelegramConfig
from core.dft_monitor import MonitorResult, ScanReport
from core.types import RetryNotification
from core.telegram_notifier import (
    escape_html,
    _status_icon,
    format_scan_report,
    format_retry_event,
    notify_scan_report,
    notify_retry_event,
    send_message,
)


def _enabled_config() -> TelegramConfig:
    return TelegramConfig(bot_token="123:ABC", chat_id="999")


def _disabled_config() -> TelegramConfig:
    return TelegramConfig(bot_token="", chat_id="")


def _sample_report() -> ScanReport:
    return ScanReport(
        new_results=[
            MonitorResult(
                formula="CH4",
                method_basis="B3LYP/def2-SVP",
                energy="E = -40.518380 Eh",
                status="completed",
                calc_type="opt",
                path="orca_outputs/opt/CH4/calc.out",
                note="",
            ),
            MonitorResult(
                formula="C6H6",
                method_basis="PBE0/def2-TZVP",
                energy="E = -232.123456 Eh",
                status="failed",
                calc_type="opt+freq",
                path="orca_outputs/opt/C6H6/calc.out",
                note=" (NOT CONVERGED)",
            ),
        ],
        scanned_files=10,
    )


def _sample_retry_event() -> RetryNotification:
    return {
        "reaction_dir": "/tmp/rxn<demo>",
        "selected_inp": "/tmp/rxn<demo>/rxn.inp",
        "failed_inp": "/tmp/rxn<demo>/rxn.inp",
        "failed_out": "/tmp/rxn<demo>/rxn.out",
        "next_inp": "/tmp/rxn<demo>/rxn.retry01.inp",
        "attempt_index": 1,
        "retry_number": 1,
        "max_retries": 2,
        "analyzer_status": "error_scf",
        "analyzer_reason": "scf_not_converged",
        "patch_actions": ["route_add_tightscf_slowconv", "geometry_restart_from_rxn.xyz"],
        "resumed": False,
    }


class TestEscapeHtml:
    def test_special_chars(self) -> None:
        assert escape_html("<b>&test</b>") == "&lt;b&gt;&amp;test&lt;/b&gt;"

    def test_plain_text(self) -> None:
        assert escape_html("hello") == "hello"


class TestStatusIcon:
    def test_known_statuses(self) -> None:
        assert _status_icon("completed") == "\u2705"
        assert _status_icon("running") == "\u23f3"
        assert _status_icon("failed") == "\u274c"

    def test_unknown_status(self) -> None:
        assert _status_icon("unknown") == "\u2753"


class TestFormatScanReport:
    def test_empty_report_returns_none(self) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        assert format_scan_report(report) is None

    def test_format_with_results(self) -> None:
        report = _sample_report()
        text = format_scan_report(report)
        assert text is not None
        assert "DFT Calculation Alert" in text
        assert "2 new" in text
        assert "CH4" in text
        assert "C6H6" in text
        assert "B3LYP/def2-SVP" in text
        assert "NOT CONVERGED" in text


class TestFormatRetryEvent:
    def test_format_contains_failure_and_restart_context(self) -> None:
        text = format_retry_event(_sample_retry_event())
        assert "ORCA Auto Retry" in text
        assert "retry 1/2 is starting" in text
        assert "error_scf" in text
        assert "scf_not_converged" in text
        assert "rxn.inp" in text
        assert "rxn.retry01.inp" in text
        assert "TightSCF + SlowConv" in text
        assert "geometry restart from rxn.xyz" in text
        assert "&lt;demo&gt;" in text


class TestSendMessage:
    def test_disabled_config_returns_false(self) -> None:
        assert send_message(_disabled_config(), "test") is False

    @patch("core.telegram_notifier.urllib.request.urlopen")
    def test_success(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"ok": True}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = send_message(_enabled_config(), "hello")
        assert result is True
        mock_urlopen.assert_called_once()

        # Verify sent payload
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        body = json.loads(req.data.decode("utf-8"))
        assert body["chat_id"] == "999"
        assert body["text"] == "hello"
        assert body["parse_mode"] == "HTML"

    @patch("core.telegram_notifier.urllib.request.urlopen")
    def test_api_error(self, mock_urlopen: MagicMock) -> None:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"ok": False}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        assert send_message(_enabled_config(), "hello") is False


class TestNotifyScanReport:
    @patch("core.telegram_notifier.send_message", return_value=True)
    def test_sends_when_results_exist(self, mock_send: MagicMock) -> None:
        report = _sample_report()
        result = notify_scan_report(_enabled_config(), report)
        assert result is True
        mock_send.assert_called_once()

    @patch("core.telegram_notifier.send_message")
    def test_skips_empty_report(self, mock_send: MagicMock) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        result = notify_scan_report(_enabled_config(), report)
        assert result is False
        mock_send.assert_not_called()


class TestNotifyRetryEvent:
    @patch("core.telegram_notifier.send_message", return_value=True)
    def test_sends_retry_message(self, mock_send: MagicMock) -> None:
        result = notify_retry_event(_enabled_config(), _sample_retry_event())
        assert result is True
        mock_send.assert_called_once()

    @patch("core.telegram_notifier.send_message")
    def test_skips_when_disabled(self, mock_send: MagicMock) -> None:
        result = notify_retry_event(_disabled_config(), _sample_retry_event())
        assert result is False
        mock_send.assert_not_called()
