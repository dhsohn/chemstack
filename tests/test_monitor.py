"""Monitor command tests."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.commands.monitor import (
    _build_message,
    _format_dft_section,
    _format_failure_section,
)
from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from core.dft_monitor import MonitorResult, ParseFailure, ScanReport


def _sample_report(n: int = 1) -> ScanReport:
    results = [
        MonitorResult(
            formula="C6H6",
            method_basis="B3LYP/6-31G(d)",
            energy="E = -232.123456 Eh",
            status="completed",
            calc_type="opt",
            path="orca_runs/rxn/calc.out",
            note="",
        )
        for _ in range(n)
    ]
    return ScanReport(new_results=results, scanned_files=5)


def _sample_running_report() -> ScanReport:
    return ScanReport(
        new_results=[
            MonitorResult(
                formula="C6H6",
                method_basis="B3LYP/6-31G(d)",
                energy="E = -232.123456 Eh",
                status="running",
                calc_type="neb",
                path="orca_runs/rxn/calc.out",
                note="",
            )
        ],
        scanned_files=5,
    )


class TestFormatDftSection:
    def test_empty_report_returns_none(self) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        assert _format_dft_section(report) is None

    def test_dft_section_content(self) -> None:
        report = _sample_report()
        result = _format_dft_section(report)
        assert result is not None
        assert "New Calculations Detected" in result
        assert "C6H6" in result
        assert "B3LYP/6-31G(d)" in result

    def test_running_only_results_are_suppressed(self) -> None:
        report = _sample_running_report()
        assert _format_dft_section(report) is None


class TestFormatFailureSection:
    def test_no_failures_returns_none(self) -> None:
        report = ScanReport(new_results=[], scanned_files=5)
        assert _format_failure_section(report) is None

    def test_failure_section_content(self) -> None:
        report = ScanReport(
            new_results=[],
            failures=[
                ParseFailure(
                    path="orca_runs/job/calc.out",
                    error="invalid literal for float()",
                    error_type="ValueError",
                ),
            ],
            scanned_files=5,
        )
        result = _format_failure_section(report)
        assert result is not None
        assert "Scan Parse Failures" in result
        assert "ValueError" in result


class TestBuildMessage:
    def test_contains_header_scope_and_divider(self) -> None:
        message = _build_message(ScanReport(new_results=[], scanned_files=0))
        assert "orca_auto monitor" in message
        assert "\u2500" in message
        assert "Filesystem discovery only" in message
        assert "run-inp alerts" in message
        assert "summary" in message

    def test_header_uses_local_timezone_like_summary(self) -> None:
        previous_tz = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "Asia/Seoul"
            time.tzset()
            message = _build_message(ScanReport(new_results=[], scanned_files=0))
        finally:
            if previous_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous_tz
            time.tzset()

        assert "<code>" in message
        assert "KST" in message

    def test_includes_dft_and_failure_sections(self) -> None:
        report = ScanReport(
            new_results=_sample_report().new_results,
            failures=[ParseFailure(path="job/calc.out", error="bad encoding", error_type="UnicodeDecodeError")],
            scanned_files=1,
        )
        message = _build_message(report)
        assert "New Calculations Detected" in message
        assert "Scan Parse Failures" in message
        assert "UnicodeDecodeError" in message


class TestRunMonitor:
    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_does_not_send_when_no_discoveries(
        self,
        mock_index_cls: MagicMock,
        mock_monitor_cls: MagicMock,
        mock_send: MagicMock,
    ) -> None:
        mock_monitor = MagicMock()
        mock_monitor.scan.return_value = ScanReport(new_results=[], scanned_files=0)
        mock_monitor_cls.return_value = mock_monitor

        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()

            from core.commands.monitor import _run_monitor

            cfg = AppConfig(
                runtime=RuntimeConfig(allowed_root=str(allowed)),
                paths=PathsConfig(orca_executable="/usr/bin/orca"),
                telegram=TelegramConfig(bot_token="fake", chat_id="123"),
            )
            result = _run_monitor(cfg)

        assert result == 0
        mock_send.assert_not_called()

    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_sends_when_new_dft_discovery_exists(
        self,
        mock_index_cls: MagicMock,
        mock_monitor_cls: MagicMock,
        mock_send: MagicMock,
    ) -> None:
        mock_monitor = MagicMock()
        mock_monitor.scan.return_value = _sample_report()
        mock_monitor_cls.return_value = mock_monitor

        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()

            from core.commands.monitor import _run_monitor

            cfg = AppConfig(
                runtime=RuntimeConfig(allowed_root=str(allowed)),
                paths=PathsConfig(orca_executable="/usr/bin/orca"),
                telegram=TelegramConfig(bot_token="fake", chat_id="123"),
            )
            result = _run_monitor(cfg)

        assert result == 0
        mock_send.assert_called_once()
        sent_text = mock_send.call_args[0][1]
        assert "Filesystem discovery only" in sent_text
        assert "New Calculations Detected" in sent_text

    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_does_not_send_when_only_running_dft_updates_exist(
        self,
        mock_index_cls: MagicMock,
        mock_monitor_cls: MagicMock,
        mock_send: MagicMock,
    ) -> None:
        mock_monitor = MagicMock()
        mock_monitor.scan.return_value = _sample_running_report()
        mock_monitor_cls.return_value = mock_monitor

        with tempfile.TemporaryDirectory() as td:
            allowed = Path(td) / "orca_runs"
            allowed.mkdir()

            from core.commands.monitor import _run_monitor

            cfg = AppConfig(
                runtime=RuntimeConfig(allowed_root=str(allowed)),
                paths=PathsConfig(orca_executable="/usr/bin/orca"),
                telegram=TelegramConfig(bot_token="fake", chat_id="123"),
            )
            result = _run_monitor(cfg)

        assert result == 0
        mock_send.assert_not_called()
