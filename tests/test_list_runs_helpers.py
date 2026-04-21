from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from chemstack.orca.commands import list_runs
from chemstack.orca.config import AppConfig, PathsConfig, RuntimeConfig
from chemstack.orca.run_snapshot import RunSnapshot


def _cfg(allowed_root: Path) -> AppConfig:
    return AppConfig(
        runtime=RuntimeConfig(allowed_root=str(allowed_root), organized_root=str(allowed_root.parent / "outputs")),
        paths=PathsConfig(orca_executable="/usr/bin/orca"),
    )


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
    assert list_runs._resolved_path_text("") == ""

    original = str(tmp_path / "job")
    with patch("pathlib.Path.resolve", side_effect=OSError("boom")):
        assert list_runs._resolved_path_text(original) == original


def test_match_queue_snapshot_prefers_run_id_and_filters_inactive_entries(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    snapshot = _snapshot(reaction_dir, run_id="run_1")
    by_id = {"run_1": snapshot}
    by_dir = {str(reaction_dir.resolve()): snapshot}

    assert list_runs._match_queue_snapshot(
        {"run_id": "run_1", "status": "pending"},
        snapshot_by_run_id=by_id,
        snapshot_by_dir=by_dir,
    ) is snapshot

    assert list_runs._match_queue_snapshot(
        {"status": "completed", "reaction_dir": str(reaction_dir)},
        snapshot_by_run_id=by_id,
        snapshot_by_dir=by_dir,
    ) is None

    assert list_runs._match_queue_snapshot(
        {"status": "pending", "reaction_dir": ""},
        snapshot_by_run_id=by_id,
        snapshot_by_dir=by_dir,
    ) is None

    assert list_runs._match_queue_snapshot(
        {"status": "pending", "reaction_dir": str(reaction_dir)},
        snapshot_by_run_id=by_id,
        snapshot_by_dir=by_dir,
    ) is snapshot


def test_match_queue_snapshot_falls_back_to_directory_when_run_id_is_none(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    snapshot = _snapshot(reaction_dir, run_id="run_1")
    by_id = {"run_1": snapshot}
    by_dir = {str(reaction_dir.resolve()): snapshot}

    assert list_runs._match_queue_snapshot(
        {"run_id": None, "status": "running", "reaction_dir": str(reaction_dir)},
        snapshot_by_run_id=by_id,
        snapshot_by_dir=by_dir,
    ) is snapshot


def test_queue_entry_represents_snapshot_covers_id_status_and_directory_branches(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    snapshot = _snapshot(reaction_dir, run_id="run_1")

    assert list_runs._queue_entry_represents_snapshot({"run_id": "run_1"}, snapshot) is True
    assert list_runs._queue_entry_represents_snapshot({"run_id": "run_2"}, snapshot) is False
    assert list_runs._queue_entry_represents_snapshot({"status": "completed"}, snapshot) is False
    assert (
        list_runs._queue_entry_represents_snapshot(
            {"status": "running", "reaction_dir": str(reaction_dir)},
            snapshot,
        )
        is True
    )
    assert (
        list_runs._queue_entry_represents_snapshot(
            {"status": "running", "reaction_dir": ""},
            snapshot,
        )
        is False
    )
    assert list_runs._queue_entry_represents_snapshot({"status": "running"}, None) is False


def test_collect_unified_keeps_snapshot_without_run_id_as_standalone(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = allowed_root / "rxn"
    snapshot = _snapshot(reaction_dir, run_id="", name="rxn")

    with patch("chemstack.orca.commands.list_runs.reconcile_orphaned_running_entries"), patch(
        "chemstack.orca.commands.list_runs.list_queue",
        return_value=[],
    ), patch(
        "chemstack.orca.commands.list_runs.collect_run_snapshots",
        return_value=[snapshot],
    ):
        rows = list_runs._collect_unified(allowed_root)

    assert rows == [
        {
            "icon": list_runs._status_icon("running"),
            "id": "rxn",
            "status": "running",
            "pri": "-",
            "dir": "rxn",
            "elapsed": "1h 00m",
            "inp": "calc.inp",
            "attempts": "2",
        }
    ]


def test_print_table_outputs_headers_and_rows(capsys) -> None:
    rows = [
        {
            "icon": "▶",
            "id": "run_1",
            "status": "running",
            "pri": "5",
            "dir": "rxn",
            "elapsed": "1h 00m",
            "inp": "calc.inp",
            "attempts": "2",
        }
    ]

    list_runs._print_table(rows)

    output = capsys.readouterr().out
    assert "STATUS" in output
    assert "run_1" in output
    assert "calc.inp" in output


def test_cmd_list_returns_one_when_allowed_root_is_missing(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path / "missing")
    args = SimpleNamespace(config="config.yml", action=None, filter=None)

    with patch("chemstack.orca.commands.list_runs.load_config", return_value=cfg):
        assert list_runs.cmd_list(args) == 1


def test_cmd_list_clear_delegates_to_clear_action(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    allowed_root.mkdir()
    cfg = _cfg(allowed_root)
    args = SimpleNamespace(config="config.yml", action="clear", filter=None)

    with patch("chemstack.orca.commands.list_runs.load_config", return_value=cfg), patch(
        "chemstack.orca.commands.list_runs._cmd_clear",
        return_value=7,
    ) as clear_cmd:
        assert list_runs.cmd_list(args) == 7

    clear_cmd.assert_called_once_with(allowed_root.resolve())


def test_clear_terminal_run_states_clears_tracked_and_legacy_terminal_states(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    organized_dir = tmp_path / "organized" / "project" / "rxn_tracked"
    legacy_dir = allowed_root / "legacy" / "rxn_legacy"
    running_dir = allowed_root / "live" / "rxn_running"
    allowed_root.mkdir()

    _write_state(organized_dir, run_id="run_tracked", status="completed", inp_name="tracked.inp")
    _write_state(legacy_dir, run_id="run_legacy", status="failed")
    _write_state(running_dir, run_id="run_running", status="running")
    (allowed_root / "job_locations.json").write_text(
        json.dumps(
            [
                {
                    "job_id": "job_tracked",
                    "app_name": "orca_auto",
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

    cleared = list_runs._clear_terminal_run_states(allowed_root)

    assert cleared == 2
    assert not (organized_dir / "run_state.json").exists()
    assert not (legacy_dir / "run_state.json").exists()
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
        "chemstack.orca.commands.list_runs.collect_run_snapshots",
        return_value=[missing_snapshot, failed_snapshot, success_snapshot, running_snapshot],
    ), patch(
        "pathlib.Path.unlink",
        new=fake_unlink,
    ), patch("chemstack.orca.commands.list_runs.logger.warning") as warning:
        cleared = list_runs._clear_terminal_run_states(allowed_root)

    assert cleared == 1
    warning.assert_called_once()
    assert failed_state.exists()
    assert not success_state.exists()
    assert running_state.exists()


def test_cmd_clear_reports_combined_counts(tmp_path: Path, capsys) -> None:
    allowed_root = tmp_path / "orca_runs"
    allowed_root.mkdir()

    with patch("chemstack.orca.commands.list_runs._clear_terminal_entries", return_value=(2, 3)):
        rc = list_runs._cmd_clear(allowed_root)

    assert rc == 0
    output = capsys.readouterr().out
    assert "Cleared 5 completed/failed/cancelled entries." in output
    assert "queue entries: 2" in output
    assert "run states: 3" in output
