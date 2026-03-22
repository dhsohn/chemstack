from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from core import run_snapshot
from core.run_snapshot import (
    RunSnapshot,
    _compute_elapsed,
    _latest_out_path,
    collect_run_snapshots,
    elapsed_text,
    parse_iso_utc,
    sort_snapshots_by_completed,
    sort_snapshots_by_started,
)


class _FrozenDateTime(datetime):
    @classmethod
    def now(cls, tz=None):  # type: ignore[override]
        current = cls(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)
        if tz is None:
            return current
        return current.astimezone(tz)


def _snapshot(
    reaction_dir: Path,
    *,
    name: str,
    started_at: str,
    updated_at: str = "",
    completed_at: str = "",
) -> RunSnapshot:
    return RunSnapshot(
        key=f"key-{name}",
        name=name,
        reaction_dir=reaction_dir,
        run_id=f"run-{name}",
        status="running",
        started_at=started_at,
        updated_at=updated_at,
        completed_at=completed_at,
        selected_inp_name="calc.inp",
        attempts=1,
        latest_out_path=None,
        final_reason="",
        elapsed=0.0,
        elapsed_text="0s",
    )


def test_parse_iso_utc_handles_invalid_z_naive_and_offset_values() -> None:
    assert parse_iso_utc(None) is None
    assert parse_iso_utc("not-a-timestamp") is None

    parsed_z = parse_iso_utc("2026-01-10T12:00:00Z")
    assert parsed_z == datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

    parsed_naive = parse_iso_utc("2026-01-10T12:00:00")
    assert parsed_naive == datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)

    parsed_offset = parse_iso_utc("2026-01-10T21:00:00+09:00")
    assert parsed_offset == datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc)


def test_elapsed_text_formats_negative_hour_minute_and_second_ranges() -> None:
    assert elapsed_text(-1.0) == "-"
    assert elapsed_text(3661.0) == "1h 01m"
    assert elapsed_text(125.0) == "2m 05s"
    assert elapsed_text(9.0) == "9s"


def test_compute_elapsed_handles_missing_started_terminal_and_running(monkeypatch) -> None:
    assert _compute_elapsed({"status": "running"}) == -1.0

    completed_elapsed = _compute_elapsed(
        {
            "status": "completed",
            "started_at": "2026-01-10T10:00:00+00:00",
            "updated_at": "2026-01-10T11:30:00+00:00",
        }
    )
    assert completed_elapsed == 5400.0

    monkeypatch.setattr(run_snapshot, "datetime", _FrozenDateTime)
    running_elapsed = _compute_elapsed(
        {
            "status": "running",
            "started_at": "2026-01-10T11:45:00+00:00",
        }
    )
    assert running_elapsed == 900.0


def test_latest_out_path_prefers_final_result_when_resolvable(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    final_out = reaction_dir / "final.out"
    final_out.write_text("done", encoding="utf-8")
    attempt_out = reaction_dir / "attempt.out"
    attempt_out.write_text("attempt", encoding="utf-8")

    resolved = _latest_out_path(
        reaction_dir,
        {
            "final_result": {"last_out_path": "final.out"},
            "attempts": [{"out_path": "attempt.out"}],
        },
    )

    assert resolved == final_out.resolve()


def test_latest_out_path_uses_latest_valid_attempt_after_skipping_invalid_entries(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    attempt_out = reaction_dir / "attempt.out"
    attempt_out.write_text("attempt", encoding="utf-8")

    resolved = _latest_out_path(
        reaction_dir,
        {
            "final_result": {"last_out_path": "missing.out"},
            "attempts": [
                "invalid",
                {"out_path": ""},
                {"other": "value"},
                {"out_path": "attempt.out"},
            ],
        },
    )

    assert resolved == attempt_out.resolve()


def test_latest_out_path_falls_back_to_latest_output_in_directory(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    older = reaction_dir / "older.out"
    newer = reaction_dir / "newer.out"
    older.write_text("old", encoding="utf-8")
    newer.write_text("new", encoding="utf-8")
    older.touch()
    newer.touch()
    older_mtime = datetime(2026, 1, 10, 11, 0, 0, tzinfo=timezone.utc).timestamp()
    newer_mtime = datetime(2026, 1, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    older.touch()
    newer.touch()
    import os

    os.utime(older, (older_mtime, older_mtime))
    os.utime(newer, (newer_mtime, newer_mtime))

    resolved = _latest_out_path(
        reaction_dir,
        {"final_result": None, "attempts": [{"out_path": "missing.out"}, 123]},
    )

    assert resolved == newer


def test_collect_run_snapshots_returns_empty_for_missing_root(tmp_path: Path) -> None:
    assert collect_run_snapshots(tmp_path / "missing") == []


def test_collect_run_snapshots_skips_state_files_that_fail_to_load(
    tmp_path: Path,
    monkeypatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = allowed_root / "rxn"
    reaction_dir.mkdir(parents=True)
    (reaction_dir / "run_state.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(run_snapshot, "load_state", lambda _reaction_dir: None)

    assert collect_run_snapshots(allowed_root) == []


def test_collect_run_snapshots_builds_basic_snapshot_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = allowed_root / "group" / "rxn"
    reaction_dir.mkdir(parents=True)
    (reaction_dir / "run_state.json").write_text("{}", encoding="utf-8")
    out_path = reaction_dir / "calc.out"
    out_path.write_text("done", encoding="utf-8")

    state = {
        "run_id": "run-123",
        "status": "RUNNING",
        "started_at": "2026-01-10T10:00:00+00:00",
        "updated_at": "2026-01-10T11:01:01+00:00",
        "selected_inp": str(reaction_dir / "calc.inp"),
        "attempts": [{"out_path": str(out_path)}, {"out_path": str(out_path)}],
        "final_result": {
            "completed_at": "2026-01-10T11:01:01+00:00",
            "reason": "  terminated_normally  ",
        },
    }

    monkeypatch.setattr(run_snapshot, "load_state", lambda _reaction_dir: state)
    monkeypatch.setattr(run_snapshot, "_compute_elapsed", lambda _state: 3661.0)

    snapshots = collect_run_snapshots(allowed_root)

    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot.key == "run-123"
    assert snapshot.name == "group/rxn"
    assert snapshot.reaction_dir == reaction_dir
    assert snapshot.run_id == "run-123"
    assert snapshot.status == "running"
    assert snapshot.started_at == "2026-01-10T10:00:00+00:00"
    assert snapshot.updated_at == "2026-01-10T11:01:01+00:00"
    assert snapshot.completed_at == "2026-01-10T11:01:01+00:00"
    assert snapshot.selected_inp_name == "calc.inp"
    assert snapshot.attempts == 2
    assert snapshot.latest_out_path == out_path.resolve()
    assert snapshot.final_reason == "terminated_normally"
    assert snapshot.elapsed == 3661.0
    assert snapshot.elapsed_text == "1h 01m"


def test_sort_snapshots_by_started_handles_invalid_timestamps(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    ordered = sort_snapshots_by_started(
        [
            _snapshot(reaction_dir, name="invalid", started_at="not-a-time"),
            _snapshot(reaction_dir, name="later", started_at="2026-01-10T12:00:00Z"),
            _snapshot(reaction_dir, name="earlier", started_at="2026-01-10T11:00:00+00:00"),
        ]
    )

    assert [snapshot.name for snapshot in ordered] == ["earlier", "later", "invalid"]


def test_sort_snapshots_by_completed_uses_updated_at_fallback_and_invalid_last(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    ordered = sort_snapshots_by_completed(
        [
            _snapshot(
                reaction_dir,
                name="fallback",
                started_at="2026-01-10T09:00:00+00:00",
                updated_at="2026-01-10T12:30:00+00:00",
                completed_at="bad",
            ),
            _snapshot(
                reaction_dir,
                name="completed",
                started_at="2026-01-10T09:30:00+00:00",
                updated_at="2026-01-10T12:00:00+00:00",
                completed_at="2026-01-10T12:00:00+00:00",
            ),
            _snapshot(
                reaction_dir,
                name="invalid",
                started_at="2026-01-10T08:00:00+00:00",
                updated_at="also-bad",
                completed_at="",
            ),
        ]
    )

    assert [snapshot.name for snapshot in ordered] == ["fallback", "completed", "invalid"]
