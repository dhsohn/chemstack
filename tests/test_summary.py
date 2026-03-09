from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from core.commands.summary import _build_summary_text, _run_summary
from core.config import AppConfig, PathsConfig, RuntimeConfig, TelegramConfig


def _cfg(allowed_root: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(allowed_root=str(allowed_root), organized_root=str(allowed_root.parent / "outputs")),
        paths=PathsConfig(orca_executable="/opt/orca/orca"),
        telegram=TelegramConfig(bot_token="token", chat_id="1234"),
    )


def _write_state(
    reaction_dir: Path,
    *,
    run_id: str,
    status: str,
    started_at: str,
    updated_at: str,
    completed_at: str = "",
) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    inp_path = reaction_dir / "calc.inp"
    inp_path.write_text("! Opt\n", encoding="utf-8")

    final_result: dict[str, str] | None
    if status == "completed":
        final_result = {
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": completed_at or updated_at,
            "last_out_path": str(reaction_dir / "calc.out"),
        }
    elif status == "failed":
        final_result = {
            "status": "failed",
            "analyzer_status": "failed",
            "reason": "error_termination",
            "completed_at": completed_at or updated_at,
            "last_out_path": str(reaction_dir / "calc.out"),
        }
    else:
        final_result = None

    state = {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(inp_path),
        "status": status,
        "started_at": started_at,
        "updated_at": updated_at,
        "attempts": [],
        "final_result": final_result,
    }
    (reaction_dir / "run_state.json").write_text(
        json.dumps(state, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def test_build_summary_text_includes_running_failed_and_completed_sections(tmp_path: Path) -> None:
    allowed = tmp_path / "orca_runs"
    allowed.mkdir()

    running = allowed / "rxn_running"
    _write_state(
        running,
        run_id="run_running",
        status="running",
        started_at="2026-03-08T00:00:00+00:00",
        updated_at="2026-03-08T01:00:00+00:00",
    )
    (running / "run.lock").write_text("{}", encoding="utf-8")
    (running / "calc.out").write_text(
        "\n".join([
            "GEOMETRY OPTIMIZATION CYCLE 12",
            "FINAL SINGLE POINT ENERGY      -123.456789",
            "Max. no of cycles        MaxIter  .... 174",
            "SCF still running",
        ]),
        encoding="utf-8",
    )

    failed = allowed / "rxn_failed"
    _write_state(
        failed,
        run_id="run_failed",
        status="failed",
        started_at="2026-03-07T00:00:00+00:00",
        updated_at="2026-03-07T02:00:00+00:00",
    )
    (failed / "calc.out").write_text("FATAL ERROR\n", encoding="utf-8")

    completed = allowed / "rxn_completed"
    _write_state(
        completed,
        run_id="run_completed",
        status="completed",
        started_at="2026-03-06T00:00:00+00:00",
        updated_at="2026-03-06T03:00:00+00:00",
        completed_at="2026-03-06T03:00:00+00:00",
    )
    (completed / "calc.out").write_text("ORCA TERMINATED NORMALLY\n", encoding="utf-8")

    with patch("core.commands.summary._count_active_orca_processes", return_value=2), patch(
        "core.commands.summary._scan_cwd_process_counts",
        return_value={running.resolve(): 11},
    ):
        text = _build_summary_text(_cfg(allowed))

    assert "[ORCA DFT 중간결과 요약]" in text
    assert "summary: running=1 completed=1 failed=1 other=0" in text
    assert "active_orca_processes: 2" in text
    assert "[running details] showing 1 / 1" in text
    assert "cycle=12" in text
    assert "E=-123.456789 Eh" in text
    assert "proc=11" in text
    assert "ETA≈" in text
    assert "maxiter=174" in text
    assert "rate=" in text
    assert "note: run.lock present" in text
    assert "[failed suspects]" in text
    assert "run_failed" in text
    assert "[recent completed] showing 1 / 1" in text
    assert "run_completed" in text


def test_run_summary_no_send_prints_and_returns_zero(tmp_path: Path, capsys) -> None:
    allowed = tmp_path / "orca_runs"
    allowed.mkdir()
    _write_state(
        allowed / "rxn_running",
        run_id="run_running",
        status="running",
        started_at="2026-03-08T00:00:00+00:00",
        updated_at="2026-03-08T01:00:00+00:00",
    )
    (allowed / "rxn_running" / "calc.out").write_text(
        "FINAL SINGLE POINT ENERGY      -10.000000\n",
        encoding="utf-8",
    )

    with patch("core.commands.summary._count_active_orca_processes", return_value=0), patch(
        "core.commands.summary._scan_cwd_process_counts",
        return_value={},
    ):
        rc = _run_summary(_cfg(allowed), send=False)

    captured = capsys.readouterr()
    assert rc == 0
    assert "[ORCA DFT 중간결과 요약]" in captured.out
