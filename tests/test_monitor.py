"""Monitor command tests."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.commands.monitor import (
    _build_message,
    _detect_run_events,
    _format_dft_section,
    _format_failure_section,
    _format_overview_line,
    _format_run_event_section,
)
from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from core.dft_monitor import MonitorResult, ParseFailure, ScanReport
from core.run_snapshot import RunSnapshot


def _make_run(
    reaction_dir: Path,
    *,
    status: str = "completed",
    attempts: int = 1,
    started_at: str = "2026-03-01T00:00:00+00:00",
    updated_at: str = "2026-03-01T01:00:00+00:00",
    completed_at: str = "",
    reason: str = "",
) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    final_result: dict[str, str] | None = None
    if status in {"completed", "failed"}:
        final_result = {
            "status": status,
            "reason": reason or status,
            "completed_at": completed_at or updated_at,
            "last_out_path": str(reaction_dir / "calc.out"),
        }
    state = {
        "run_id": f"run_{reaction_dir.name}",
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / "calc.inp"),
        "max_retries": 2,
        "status": status,
        "started_at": started_at,
        "updated_at": updated_at,
        "attempts": [{"index": idx + 1} for idx in range(attempts)],
        "final_result": final_result,
    }
    (reaction_dir / "calc.inp").write_text("! Opt\n", encoding="utf-8")
    (reaction_dir / "run_state.json").write_text(json.dumps(state), encoding="utf-8")


def _sample_snapshot(status: str = "running", attempts: int = 1, reason: str = "") -> RunSnapshot:
    return RunSnapshot(
        key="run_rxn_ts_opt",
        name="rxn/ts_opt",
        reaction_dir=Path("/tmp/rxn/ts_opt"),
        run_id="run_rxn_ts_opt",
        status=status,
        started_at="2026-03-01T00:00:00+00:00",
        updated_at="2026-03-01T01:00:00+00:00",
        completed_at="2026-03-01T01:00:00+00:00" if status in {"completed", "failed"} else "",
        selected_inp_name="ts_opt.inp",
        attempts=attempts,
        latest_out_path=None,
        final_reason=reason,
        elapsed=3600.0,
        elapsed_text="1h 00m",
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


class TestDetectRunEvents:
    def test_no_events_without_baseline(self) -> None:
        events = _detect_run_events({}, [_sample_snapshot(status="completed")], has_baseline=False)
        assert events == []

    def test_completed_transition_detected(self) -> None:
        previous = {"run_rxn_ts_opt": {"status": "running", "attempts": 1, "updated_at": "old"}}
        events = _detect_run_events(previous, [_sample_snapshot(status="completed")], has_baseline=True)
        assert len(events) == 1
        assert events[0].kind == "completed"

    def test_retry_transition_detected_when_attempts_increase(self) -> None:
        previous = {"run_rxn_ts_opt": {"status": "retrying", "attempts": 1, "updated_at": "old"}}
        events = _detect_run_events(previous, [_sample_snapshot(status="retrying", attempts=2)], has_baseline=True)
        assert len(events) == 1
        assert events[0].kind == "retrying"


class TestFormatRunEventSection:
    def test_none_when_no_matching_events(self) -> None:
        assert _format_run_event_section([], "completed", "Completed") is None

    def test_completed_section_content(self) -> None:
        events = _detect_run_events(
            {"run_rxn_ts_opt": {"status": "running", "attempts": 1, "updated_at": "old"}},
            [_sample_snapshot(status="completed", reason="normal_termination")],
            has_baseline=True,
        )
        result = _format_run_event_section(events, "completed", "Completed")
        assert result is not None
        assert "Completed" in result
        assert "rxn/ts_opt" in result
        assert "normal_termination" in result

    def test_retry_section_shows_attempt_count(self) -> None:
        events = _detect_run_events(
            {"run_rxn_ts_opt": {"status": "running", "attempts": 1, "updated_at": "old"}},
            [_sample_snapshot(status="retrying", attempts=2)],
            has_baseline=True,
        )
        result = _format_run_event_section(events, "retrying", "Retries")
        assert result is not None
        assert "attempt #2" in result


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
        assert "Parse Failures" in result
        assert "ValueError" in result


class TestFormatOverview:
    def test_empty_runs(self) -> None:
        result = _format_overview_line([])
        assert "Overview" in result
        assert "No runs" in result

    def test_mixed_statuses(self) -> None:
        snapshots = [
            _sample_snapshot(status="running"),
            _sample_snapshot(status="completed"),
            _sample_snapshot(status="failed"),
        ]
        result = _format_overview_line(snapshots)
        assert "running 1" in result
        assert "completed 1" in result
        assert "failed 1" in result


class TestBuildMessage:
    def test_contains_header_and_divider(self) -> None:
        message = _build_message([], [], ScanReport(new_results=[], scanned_files=0))
        assert "orca_auto monitor" in message
        assert "\u2500" in message
        assert "Overview" in message

    def test_header_uses_local_timezone_like_summary(self) -> None:
        previous_tz = os.environ.get("TZ")
        try:
            os.environ["TZ"] = "Asia/Seoul"
            time.tzset()
            message = _build_message([], [], ScanReport(new_results=[], scanned_files=0))
        finally:
            if previous_tz is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous_tz
            time.tzset()

        assert "<code>" in message
        assert "KST" in message

    def test_includes_event_and_dft_sections(self) -> None:
        snapshots = [_sample_snapshot(status="completed")]
        events = _detect_run_events(
            {"run_rxn_ts_opt": {"status": "running", "attempts": 1, "updated_at": "old"}},
            snapshots,
            has_baseline=True,
        )
        message = _build_message(snapshots, events, _sample_report())
        assert "Completed" in message
        assert "New Calculations Detected" in message
        assert "Overview" in message

    def test_includes_failure_section(self) -> None:
        report = ScanReport(
            new_results=[],
            failures=[ParseFailure(path="job/calc.out", error="bad encoding", error_type="UnicodeDecodeError")],
            scanned_files=1,
        )
        message = _build_message([], [], report)
        assert "Parse Failures" in message
        assert "UnicodeDecodeError" in message


class TestRunMonitor:
    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_does_not_send_when_no_events(
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
        mock_send.assert_not_called()

    @patch("core.commands.monitor.send_message", return_value=True)
    @patch("core.commands.monitor.DFTMonitor")
    @patch("core.commands.monitor.DFTIndex")
    def test_sends_when_run_status_transitions(
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
            run_dir = allowed / "rxn1"
            _make_run(run_dir, status="running")

            from core.commands.monitor import _monitor_state_path, _run_monitor

            _monitor_state_path(allowed).write_text(
                json.dumps(
                    {
                        "version": 1,
                        "runs": {
                            "run_rxn1": {
                                "status": "running",
                                "attempts": 1,
                                "updated_at": "2026-03-01T00:30:00+00:00",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            _make_run(run_dir, status="completed", reason="normal_termination")

            cfg = AppConfig(
                runtime=RuntimeConfig(allowed_root=str(allowed)),
                paths=PathsConfig(orca_executable="/usr/bin/orca"),
                telegram=TelegramConfig(bot_token="fake", chat_id="123"),
            )
            result = _run_monitor(cfg)

        assert result == 0
        mock_send.assert_called_once()
        sent_text = mock_send.call_args[0][1]
        assert "Completed" in sent_text
        assert "rxn1" in sent_text

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
            _make_run(allowed / "rxn1", status="running")

            from core.commands.monitor import _run_monitor

            cfg = AppConfig(
                runtime=RuntimeConfig(allowed_root=str(allowed)),
                paths=PathsConfig(orca_executable="/usr/bin/orca"),
                telegram=TelegramConfig(bot_token="fake", chat_id="123"),
            )
            result = _run_monitor(cfg)

        assert result == 0
        mock_send.assert_not_called()

    def test_returns_1_when_telegram_disabled(self) -> None:
        from core.commands.monitor import _run_monitor

        cfg = AppConfig(
            runtime=RuntimeConfig(allowed_root="/tmp/nonexistent"),
            paths=PathsConfig(orca_executable="/usr/bin/orca"),
            telegram=TelegramConfig(bot_token="", chat_id=""),
        )
        assert _run_monitor(cfg) == 1
