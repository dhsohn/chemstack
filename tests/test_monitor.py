"""Monitor command tests."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.commands.monitor import (
    _build_message,
    _format_dft_section,
    _format_overall_summary,
    _format_running_section,
)
from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from core.dft_monitor import MonitorResult, ScanReport
from core.types import RunInfo


def _make_run(
    reaction_dir: Path,
    *,
    status: str = "completed",
    started_at: str = "2026-03-01T00:00:00+00:00",
    updated_at: str = "2026-03-01T01:00:00+00:00",
) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": f"run_{reaction_dir.name}",
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "calc.inp"),
        "max_retries": 2,
        "status": status,
        "started_at": started_at,
        "updated_at": updated_at,
        "attempts": [{"index": 1}],
        "final_result": {"status": status},
    }
    (reaction_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")


def _sample_run_info(status: str = "running", attempts: int = 1) -> RunInfo:
    return RunInfo(
        dir="rxn/ts_opt",
        status=status,
        elapsed=3600.0,
        elapsed_text="1h 00m",
        inp="ts_opt.inp",
        attempts=attempts,
        started_at="2026-03-01T00:00:00+00:00",
    )


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


class TestFormatRunningSection:
    def test_no_active_returns_none(self) -> None:
        runs = [_sample_run_info(status="completed")]
        assert _format_running_section(runs) is None

    def test_running_section_content(self) -> None:
        runs = [_sample_run_info(status="running")]
        result = _format_running_section(runs)
        assert result is not None
        assert "Running" in result
        assert "(1)" in result
        assert "rxn/ts_opt" in result
        assert "ts_opt.inp" in result
        assert "1h 00m" in result

    def test_retry_shows_attempt_count(self) -> None:
        runs = [_sample_run_info(status="retrying", attempts=3)]
        result = _format_running_section(runs)
        assert result is not None
        assert "attempt #3" in result


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
        assert "E = -232.123456 Eh" in result
        assert "OPT" in result


class TestFormatOverallSummary:
    def test_empty_runs(self) -> None:
        result = _format_overall_summary([])
        assert "total 0" in result
        assert "No runs" in result

    def test_mixed_statuses(self) -> None:
        runs = [
            _sample_run_info(status="running"),
            _sample_run_info(status="completed"),
            _sample_run_info(status="completed"),
            _sample_run_info(status="failed"),
        ]
        result = _format_overall_summary(runs)
        assert "total 4" in result
        assert "running 1" in result
        assert "completed 2" in result
        assert "failed 1" in result


class TestBuildMessage:
    def test_contains_header_and_divider(self) -> None:
        runs: list[RunInfo] = []
        report = ScanReport(new_results=[], scanned_files=0)
        message = _build_message(runs, report)
        assert "orca_auto monitor" in message
        assert "\u2500" in message
        assert "Overview" in message

    def test_includes_running_and_dft_sections(self) -> None:
        runs = [_sample_run_info(status="running")]
        report = _sample_report()
        message = _build_message(runs, report)
        assert "Running" in message
        assert "New Calculations Detected" in message
        assert "Overview" in message


class TestRunMonitor:
    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_sends_message_with_runs(
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
            _make_run(allowed / "rxn1", status="running")

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
        assert "orca_auto monitor" in sent_text

    def test_returns_1_when_telegram_disabled(self) -> None:
        from core.commands.monitor import _run_monitor
        cfg = AppConfig(
            runtime=RuntimeConfig(allowed_root="/tmp/nonexistent"),
            paths=PathsConfig(orca_executable="/usr/bin/orca"),
            telegram=TelegramConfig(bot_token="", chat_id=""),
        )
        assert _run_monitor(cfg) == 1
