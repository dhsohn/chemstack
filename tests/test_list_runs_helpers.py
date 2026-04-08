from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from core.commands import list_runs
from core.config import AppConfig, PathsConfig, RuntimeConfig
from core.run_snapshot import RunSnapshot


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

    with patch("core.commands.list_runs.reconcile_orphaned_running_entries"), patch(
        "core.commands.list_runs.list_queue",
        return_value=[],
    ), patch(
        "core.commands.list_runs.collect_run_snapshots",
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

    with patch("core.commands.list_runs.load_config", return_value=cfg):
        assert list_runs.cmd_list(args) == 1


def test_cmd_list_clear_delegates_to_clear_action(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    allowed_root.mkdir()
    cfg = _cfg(allowed_root)
    args = SimpleNamespace(config="config.yml", action="clear", filter=None)

    with patch("core.commands.list_runs.load_config", return_value=cfg), patch(
        "core.commands.list_runs._cmd_clear",
        return_value=7,
    ) as clear_cmd:
        assert list_runs.cmd_list(args) == 7

    clear_cmd.assert_called_once_with(allowed_root.resolve())


def test_cmd_clear_skips_missing_state_and_warns_on_unlink_error(tmp_path: Path, capsys) -> None:
    allowed_root = tmp_path / "orca_runs"
    skipped_dir = allowed_root / "skipped"
    failed_dir = allowed_root / "failed"
    skipped_dir.mkdir(parents=True)
    failed_dir.mkdir(parents=True)
    skipped_state = skipped_dir / "run_state.json"
    failed_state = failed_dir / "run_state.json"
    skipped_state.write_text("{}", encoding="utf-8")
    failed_state.write_text("{}", encoding="utf-8")

    original_unlink = Path.unlink

    def fake_load_state(path: Path):
        if path == skipped_dir:
            return None
        if path == failed_dir:
            return {"status": "completed"}
        return None

    def fake_unlink(self: Path, *args, **kwargs) -> None:
        if self == failed_state:
            raise OSError("cannot remove")
        return original_unlink(self, *args, **kwargs)

    with patch("core.commands.list_runs.clear_terminal", return_value=0), patch(
        "core.commands.list_runs.load_state",
        side_effect=fake_load_state,
    ), patch(
        "pathlib.Path.unlink",
        new=fake_unlink,
    ), patch("core.commands.list_runs.logger.warning") as warning:
        rc = list_runs._cmd_clear(allowed_root)

    assert rc == 0
    warning.assert_called_once()
    assert "Cleared 0 completed/failed/cancelled entries." in capsys.readouterr().out
