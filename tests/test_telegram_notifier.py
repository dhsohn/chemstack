"""Telegram notifier module tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from chemstack.orca.config import TelegramConfig
from chemstack.orca.dft_monitor import MonitorResult, ScanReport
from chemstack.orca.types import QueueEnqueuedNotification, RetryNotification, RunFinishedNotification, RunStartedNotification
from chemstack.orca.telegram_notifier import (
    escape_html,
    _status_icon,
    format_monitor_message,
    format_run_finished_event,
    format_run_started_event,
    format_retry_event,
    format_queue_enqueued_event,
    has_monitor_updates,
    notify_monitor_report,
    notify_retry_event,
    notify_run_finished_event,
    notify_run_started_event,
    notify_queue_enqueued_event,
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


def _sample_started_event() -> RunStartedNotification:
    return {
        "reaction_dir": "/tmp/rxn<demo>",
        "selected_inp": "/tmp/rxn<demo>/rxn.inp",
        "current_inp": "/tmp/rxn<demo>/rxn.inp",
        "run_id": "run_20260310_demo",
        "attempt_index": 1,
        "max_retries": 2,
        "status": "running",
        "attempt_started_at": "2026-03-10T00:00:00+00:00",
        "resumed": False,
    }


def _sample_finished_event() -> RunFinishedNotification:
    return {
        "reaction_dir": "/tmp/rxn<demo>",
        "selected_inp": "/tmp/rxn<demo>/rxn.inp",
        "run_id": "run_20260310_demo",
        "status": "completed",
        "analyzer_status": "completed",
        "reason": "normal_termination",
        "attempt_count": 2,
        "max_retries": 2,
        "completed_at": "2026-03-10T00:05:00+00:00",
        "last_out_path": "/tmp/rxn<demo>/rxn.retry01.out",
        "resumed": False,
        "skipped_execution": False,
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
        assert _status_icon("retrying") == "\U0001f504"
        assert _status_icon("failed") == "\u274c"

    def test_unknown_status(self) -> None:
        assert _status_icon("unknown") == "\u2753"


class TestMonitorFormatting:
    def test_empty_report_returns_none(self) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        assert has_monitor_updates(report) is False

    def test_format_with_results(self) -> None:
        report = _sample_report()
        text = format_monitor_message(report)
        assert has_monitor_updates(report) is True
        assert "chemstack monitor" in text
        assert "New Calculations Detected" in text
        assert "CH4" in text
        assert "C6H6" in text
        assert "B3LYP/def2-SVP" in text
        assert "NOT CONVERGED" in text


class TestFormatRetryEvent:
    def test_format_contains_failure_and_restart_context(self) -> None:
        text = format_retry_event(_sample_retry_event())
        assert "ChemStack ORCA Retry" in text
        assert "retry 1/2 is starting" in text
        assert "error_scf" in text
        assert "scf_not_converged" in text
        assert "rxn.inp" in text
        assert "rxn.retry01.inp" in text
        assert "TightSCF + SlowConv" in text
        assert "geometry restart from rxn.xyz" in text
        assert "&lt;demo&gt;" in text


class TestFormatRunStartedEvent:
    def test_format_contains_start_context(self) -> None:
        text = format_run_started_event(_sample_started_event())
        assert "ChemStack ORCA Started" in text
        assert "#1" in text
        assert "running" in text
        assert "rxn.inp" in text
        assert "&lt;demo&gt;" in text


class TestFormatRunFinishedEvent:
    def test_format_contains_terminal_context(self) -> None:
        text = format_run_finished_event(_sample_finished_event())
        assert "ChemStack ORCA Completed" in text
        assert "normal_termination" in text
        assert "completed" in text
        assert "rxn.retry01.out" in text
        assert "&lt;demo&gt;" in text


class TestSendMessage:
    def test_disabled_config_returns_false(self) -> None:
        assert send_message(_disabled_config(), "test") is False

    @patch("chemstack.orca.telegram_notifier.build_telegram_transport")
    def test_success(self, mock_build_transport: MagicMock) -> None:
        fake_transport = MagicMock()
        fake_transport.send_text.return_value = SimpleNamespace(
            sent=True,
            skipped=False,
            status_code=200,
            response_text='{"ok":true}',
            error="",
        )
        mock_build_transport.return_value = fake_transport

        result = send_message(_enabled_config(), "hello")
        assert result is True
        mock_build_transport.assert_called_once()
        fake_transport.send_text.assert_called_once_with("hello", parse_mode="HTML")

    @patch("chemstack.orca.telegram_notifier.build_telegram_transport")
    def test_api_error(self, mock_build_transport: MagicMock) -> None:
        fake_transport = MagicMock()
        fake_transport.send_text.return_value = SimpleNamespace(
            sent=False,
            skipped=False,
            status_code=503,
            response_text="busy",
            error="telegram_http_503",
        )
        mock_build_transport.return_value = fake_transport
        assert send_message(_enabled_config(), "hello") is False

    @patch("chemstack.orca.telegram_notifier.build_telegram_transport")
    def test_custom_timeout_flows_through_shared_transport(self, mock_build_transport: MagicMock) -> None:
        fake_transport = MagicMock()
        fake_transport.send_text.return_value = SimpleNamespace(
            sent=True,
            skipped=False,
            status_code=200,
            response_text='{"ok":true}',
            error="",
        )
        mock_build_transport.return_value = fake_transport
        config = TelegramConfig(bot_token="123:ABC", chat_id="999", timeout_seconds=1.5)

        assert send_message(config, "hello") is True
        built_config = mock_build_transport.call_args.args[0]
        assert built_config.timeout_seconds == 1.5


class TestNotifyMonitorReport:
    @patch("chemstack.orca.telegram_notifier.send_message", return_value=True)
    def test_sends_when_results_exist(self, mock_send: MagicMock) -> None:
        report = _sample_report()
        result = notify_monitor_report(_enabled_config(), report)
        assert result is True
        mock_send.assert_called_once()

    @patch("chemstack.orca.telegram_notifier.send_message")
    def test_skips_empty_report(self, mock_send: MagicMock) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        result = notify_monitor_report(_enabled_config(), report)
        assert result is False
        mock_send.assert_not_called()


class TestNotifyRetryEvent:
    @patch("chemstack.orca.telegram_notifier.send_message", return_value=True)
    def test_sends_retry_message(self, mock_send: MagicMock) -> None:
        result = notify_retry_event(_enabled_config(), _sample_retry_event())
        assert result is True
        mock_send.assert_called_once()

    @patch("chemstack.orca.telegram_notifier.send_message")
    def test_skips_when_disabled(self, mock_send: MagicMock) -> None:
        result = notify_retry_event(_disabled_config(), _sample_retry_event())
        assert result is False
        mock_send.assert_not_called()


class TestNotifyRunStartedEvent:
    @patch("chemstack.orca.telegram_notifier.send_message", return_value=True)
    def test_sends_started_message(self, mock_send: MagicMock) -> None:
        result = notify_run_started_event(_enabled_config(), _sample_started_event())
        assert result is True
        mock_send.assert_called_once()

    @patch("chemstack.orca.telegram_notifier.send_message")
    def test_skips_when_disabled(self, mock_send: MagicMock) -> None:
        result = notify_run_started_event(_disabled_config(), _sample_started_event())
        assert result is False
        mock_send.assert_not_called()


class TestNotifyRunFinishedEvent:
    @patch("chemstack.orca.telegram_notifier.send_message", return_value=True)
    def test_sends_finished_message(self, mock_send: MagicMock) -> None:
        result = notify_run_finished_event(_enabled_config(), _sample_finished_event())
        assert result is True
        mock_send.assert_called_once()

    @patch("chemstack.orca.telegram_notifier.send_message")
    def test_skips_when_disabled(self, mock_send: MagicMock) -> None:
        result = notify_run_finished_event(_disabled_config(), _sample_finished_event())
        assert result is False
        mock_send.assert_not_called()


def _sample_queue_enqueued_event() -> QueueEnqueuedNotification:
    return {
        "queue_id": "q_20260310_abc12345",
        "reaction_dir": "/tmp/orca_runs/rxn<demo>",
        "priority": 5,
        "force": False,
        "enqueued_at": "2026-03-10T00:00:00+00:00",
    }


class TestFormatQueueEnqueuedEvent:
    def test_format_contains_queue_context(self) -> None:
        text = format_queue_enqueued_event(_sample_queue_enqueued_event())
        assert "ChemStack ORCA Queued" in text
        assert "q_20260310_abc12345" in text
        assert "Priority" in text
        assert "5" in text
        assert "&lt;demo&gt;" in text

    def test_format_force_mode(self) -> None:
        event = _sample_queue_enqueued_event()
        event["force"] = True
        text = format_queue_enqueued_event(event)
        assert "force re-enqueue" in text


class TestNotifyQueueEnqueuedEvent:
    @patch("chemstack.orca.telegram_notifier.send_message", return_value=True)
    def test_sends_enqueued_message(self, mock_send: MagicMock) -> None:
        result = notify_queue_enqueued_event(_enabled_config(), _sample_queue_enqueued_event())
        assert result is True
        mock_send.assert_called_once()

    @patch("chemstack.orca.telegram_notifier.send_message")
    def test_skips_when_disabled(self, mock_send: MagicMock) -> None:
        result = notify_queue_enqueued_event(_disabled_config(), _sample_queue_enqueued_event())
        assert result is False
        mock_send.assert_not_called()
