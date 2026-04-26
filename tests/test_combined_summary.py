from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from chemstack import summary as combined_summary
from chemstack.orca.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig
from chemstack.orca.run_snapshot import RunSnapshot


class _FrozenDateTime(datetime):
    @classmethod
    def frozen_now(cls) -> datetime:
        return cls(2026, 4, 26, 4, 30, 0, tzinfo=timezone.utc)

    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        current = cls.frozen_now()
        if tz is None:
            return current
        return current.astimezone(tz)


def _cfg(allowed_root: Path, *, telegram_enabled: bool = True) -> AppConfig:
    telegram = (
        TelegramConfig(bot_token="token", chat_id="1234")
        if telegram_enabled
        else TelegramConfig()
    )
    return AppConfig(
        runtime=RuntimeConfig(
            allowed_root=str(allowed_root),
            organized_root=str(allowed_root.parent / "outputs"),
        ),
        paths=PathsConfig(orca_executable="/opt/orca/orca"),
        telegram=telegram,
    )


def _snapshot(
    reaction_dir: Path,
    *,
    name: str,
    status: str = "running",
    selected_inp_name: str = "calc.inp",
    final_reason: str = "",
) -> RunSnapshot:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    return RunSnapshot(
        key=f"key-{name}",
        name=name,
        reaction_dir=reaction_dir,
        run_id=f"run-{name}",
        status=status,
        started_at="2026-04-26T00:00:00+00:00",
        updated_at="2026-04-26T03:30:00+00:00",
        completed_at="2026-04-26T03:30:00+00:00",
        selected_inp_name=selected_inp_name,
        attempts=1,
        latest_out_path=None,
        final_reason=final_reason,
        elapsed=0.0,
        elapsed_text="0m",
    )


def test_build_summary_message_includes_workflow_sections(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    cfg = _cfg(allowed_root)
    running_snapshot = _snapshot(allowed_root / "TS01", name="TS01", status="running", selected_inp_name="tsopt.inp")
    failed_snapshot = _snapshot(
        allowed_root / "OPT01",
        name="OPT01",
        status="failed",
        selected_inp_name="opt.inp",
        final_reason="SCF did not converge",
    )
    workflow_summaries = [
        {
            "workflow_id": "wf_running",
            "template_name": "reaction_ts_search",
            "status": "running",
            "reaction_key": "rxn-001",
            "stage_count": 4,
            "stage_summaries": [
                {
                    "stage_id": "stage-02",
                    "stage_kind": "orca_stage",
                    "task_kind": "optts_freq",
                    "engine": "orca",
                    "status": "running",
                    "task_status": "running",
                }
            ],
            "submission_summary": {
                "submitted_count": 3,
                "failed_count": 0,
                "skipped_count": 1,
            },
        },
        {
            "workflow_id": "wf_failed",
            "template_name": "conformer_screening",
            "status": "failed",
            "reaction_key": "conf-009",
            "stage_count": 2,
            "stage_summaries": [
                {
                    "stage_id": "stage-01",
                    "stage_kind": "crest_stage",
                    "task_kind": "conformer_search",
                    "engine": "crest",
                    "status": "failed",
                    "task_status": "failed",
                }
            ],
            "submission_summary": {
                "submitted_count": 1,
                "failed_count": 1,
                "skipped_count": 0,
            },
        },
    ]

    with patch("chemstack.summary.datetime", _FrozenDateTime), patch(
        "chemstack.summary.orca_summary.collect_run_snapshots",
        return_value=[running_snapshot, failed_snapshot],
    ), patch(
        "chemstack.summary.orca_summary._scan_cwd_process_counts",
        return_value={},
    ), patch(
        "chemstack.summary._workflow_summary_rows",
        return_value=("/tmp/workflows", workflow_summaries),
    ), patch(
        "chemstack.summary._activity_rows",
        return_value=[{"app": "orca"}],
    ), patch(
        "chemstack.summary.count_global_active_simulations",
        return_value=4,
    ), patch(
        "chemstack.summary.orca_summary._count_active_orca_processes",
        return_value=7,
    ), patch(
        "chemstack.summary.orca_summary._format_running_section",
        return_value="⏳ <b>Active Runs</b>\n\nmocked running section",
    ), patch(
        "chemstack.summary.orca_summary._format_attention_section",
        return_value="⚠️ <b>Attention</b>\n\nmocked attention section",
    ):
        message = combined_summary._build_summary_message(cfg, config_path="/tmp/chemstack.yaml")

    assert "Active ORCA processes: 7" in message
    assert "Active simulations: 4" in message
    assert "🧭 Workflows:" in message
    assert "▶ running 1" in message
    assert "❌ failed 1" in message
    assert "Active Workflows" in message
    assert "Workflow Attention" in message
    assert "wf_running" in message
    assert "wf_failed" in message
    assert "optts_freq" in message
    assert "conformer_search" in message
    assert "mocked running section" in message
    assert "mocked attention section" in message


def test_run_summary_and_cmd_summary_cover_send_paths(tmp_path: Path, capsys) -> None:
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()

    disabled_cfg = _cfg(allowed_root, telegram_enabled=False)
    enabled_cfg = _cfg(allowed_root, telegram_enabled=True)

    with patch("chemstack.summary._build_summary_message", return_value="combined payload"):
        assert combined_summary._run_summary(disabled_cfg, config_path="config.yml", send=True) == 1
        assert "combined payload" in capsys.readouterr().out

    with patch("chemstack.summary._build_summary_message", return_value="combined payload"), patch(
        "chemstack.summary.send_message",
        return_value=True,
    ):
        assert combined_summary._run_summary(enabled_cfg, config_path="config.yml", send=True) == 0

    with patch("chemstack.summary._build_summary_message", return_value="combined payload"), patch(
        "chemstack.summary.send_message",
        return_value=False,
    ):
        assert combined_summary._run_summary(enabled_cfg, config_path="config.yml", send=True) == 1

    args = SimpleNamespace(config="config.yml", no_send=True)
    with patch("chemstack.summary.load_config", return_value=enabled_cfg) as mocked_load, patch(
        "chemstack.summary._run_summary",
        return_value=7,
    ) as mocked_run:
        assert combined_summary.cmd_summary(args) == 7

    mocked_load.assert_called_once_with("config.yml")
    mocked_run.assert_called_once_with(enabled_cfg, config_path="config.yml", send=False)
