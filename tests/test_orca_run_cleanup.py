from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from chemstack.orca import run_cleanup
from chemstack.orca.run_snapshot import RunSnapshot


def _snapshot(
    reaction_dir: Path,
    *,
    run_id: str = "run_1",
    name: str = "rxn",
    status: str = "running",
    selected_inp_name: str = "calc.inp",
) -> RunSnapshot:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    return RunSnapshot(
        key=f"key-{name}",
        name=name,
        reaction_dir=reaction_dir,
        run_id=run_id,
        status=status,
        started_at="2026-03-10T00:00:00+00:00",
        updated_at="2026-03-10T01:00:00+00:00",
        completed_at="2026-03-10T01:00:00+00:00",
        selected_inp_name=selected_inp_name,
        attempts=2,
        latest_out_path=None,
        final_reason="",
        elapsed=3600.0,
        elapsed_text="1h 00m",
    )


def _write_state(
    reaction_dir: Path,
    *,
    run_id: str,
    status: str,
    inp_name: str = "calc.inp",
) -> None:
    reaction_dir.mkdir(parents=True, exist_ok=True)
    state = {
        "run_id": run_id,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(reaction_dir / inp_name),
        "max_retries": 2,
        "status": status,
        "started_at": "2026-03-10T00:00:00+00:00",
        "updated_at": "2026-03-10T01:00:00+00:00",
        "attempts": [{"index": 1}],
        "final_result": {"status": status},
    }
    (reaction_dir / "run_state.json").write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")


def test_resolved_path_text_handles_blank_and_resolve_failure(tmp_path: Path) -> None:
    assert run_cleanup._resolved_path_text("") == ""

    original = str(tmp_path / "job")
    with patch("pathlib.Path.resolve", side_effect=OSError("boom")):
        assert run_cleanup._resolved_path_text(original) == original


def test_clear_terminal_run_states_clears_tracked_and_untracked_terminal_states(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_dir = tmp_path / "organized" / "project" / "rxn_tracked"
    untracked_dir = allowed_root / "untracked" / "rxn_failed"
    running_dir = allowed_root / "live" / "rxn_running"
    allowed_root.mkdir()

    _write_state(organized_dir, run_id="run_tracked", status="completed", inp_name="tracked.inp")
    _write_state(untracked_dir, run_id="run_failed", status="failed")
    _write_state(running_dir, run_id="run_running", status="running")
    (allowed_root / "job_locations.json").write_text(
        json.dumps(
            [
                {
                    "job_id": "job_tracked",
                    "app_name": "chemstack_orca",
                    "job_type": "orca_opt",
                    "status": "completed",
                    "original_run_dir": str(allowed_root / "project" / "rxn_tracked"),
                    "molecule_key": "rxn_tracked",
                    "selected_input_xyz": str(organized_dir / "tracked.inp"),
                    "organized_output_dir": str(organized_dir),
                    "latest_known_path": str(organized_dir),
                    "resource_request": {},
                    "resource_actual": {},
                }
            ],
            ensure_ascii=True,
            indent=2,
        ),
        encoding="utf-8",
    )

    cleared = run_cleanup.clear_terminal_run_states(allowed_root)

    assert cleared == 2
    assert not (organized_dir / "run_state.json").exists()
    assert not (untracked_dir / "run_state.json").exists()
    assert (running_dir / "run_state.json").exists()


def test_clear_terminal_run_states_skips_missing_files_and_warns_on_unlink_error(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    missing_snapshot = _snapshot(allowed_root / "missing", run_id="run_missing", name="missing", status="completed")
    failed_snapshot = _snapshot(allowed_root / "failed", run_id="run_failed", name="failed", status="failed")
    success_snapshot = _snapshot(allowed_root / "success", run_id="run_success", name="success", status="completed")
    running_snapshot = _snapshot(allowed_root / "running", run_id="run_running", name="running", status="running")

    failed_state = failed_snapshot.reaction_dir / "run_state.json"
    success_state = success_snapshot.reaction_dir / "run_state.json"
    running_state = running_snapshot.reaction_dir / "run_state.json"
    failed_state.write_text("{}", encoding="utf-8")
    success_state.write_text("{}", encoding="utf-8")
    running_state.write_text("{}", encoding="utf-8")

    original_unlink = Path.unlink

    def fake_unlink(self: Path, *args, **kwargs) -> None:
        if self == failed_state:
            raise OSError("cannot remove")
        return original_unlink(self, *args, **kwargs)

    with patch(
        "chemstack.orca.run_cleanup.collect_run_snapshots",
        return_value=[missing_snapshot, failed_snapshot, success_snapshot, running_snapshot],
    ), patch(
        "pathlib.Path.unlink",
        new=fake_unlink,
    ), patch("chemstack.orca.run_cleanup.logger.warning") as warning:
        cleared = run_cleanup.clear_terminal_run_states(allowed_root)

    assert cleared == 1
    warning.assert_called_once()
    assert failed_state.exists()
    assert not success_state.exists()
    assert running_state.exists()


def test_clear_terminal_entries_reports_queue_and_run_state_counts(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    allowed_root.mkdir()

    with patch("chemstack.orca.run_cleanup.clear_terminal", return_value=2), patch(
        "chemstack.orca.run_cleanup.clear_terminal_run_states",
        return_value=3,
    ):
        assert run_cleanup.clear_terminal_entries(allowed_root) == (2, 3)
