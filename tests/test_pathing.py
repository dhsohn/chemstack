from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import core.pathing as pathing


class _Candidate:
    def __init__(self, resolved: Path, *, exists: bool, resolve_error: OSError | None = None) -> None:
        self._resolved = resolved
        self._exists = exists
        self._resolve_error = resolve_error

    def resolve(self) -> Path:
        if self._resolve_error is not None:
            raise self._resolve_error
        return self._resolved

    def exists(self) -> bool:
        return self._exists


def test_is_rejected_windows_path_detects_windows_and_wsl_forms() -> None:
    assert pathing.is_rejected_windows_path(r"C:\orca\runs\calc.inp")
    assert pathing.is_rejected_windows_path("/mnt/c/orca/runs/calc.inp")
    assert not pathing.is_rejected_windows_path("/home/user/orca/runs/calc.inp")


def test_is_subpath_reports_true_and_false_paths(tmp_path: Path) -> None:
    root = tmp_path / "root"
    inside = root / "nested" / "calc.inp"
    outside = tmp_path / "outside" / "calc.inp"
    inside.parent.mkdir(parents=True)
    outside.parent.mkdir(parents=True)
    inside.write_text("! Opt\n", encoding="utf-8")
    outside.write_text("! Opt\n", encoding="utf-8")

    assert pathing.is_subpath(inside, root)
    assert not pathing.is_subpath(outside, root)


def test_artifact_candidates_cover_blank_absolute_and_relative_inputs(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    absolute = tmp_path / "calc.out"

    assert pathing.artifact_candidates("   ", reaction_dir) == []
    assert pathing.artifact_candidates(str(absolute), reaction_dir) == [
        absolute,
        reaction_dir / absolute.name,
    ]
    assert pathing.artifact_candidates("nested/calc.out", reaction_dir) == [
        reaction_dir / "nested" / "calc.out",
        reaction_dir / "calc.out",
    ]


def test_resolve_artifact_path_returns_existing_candidate_and_none_when_missing(tmp_path: Path) -> None:
    reaction_dir = tmp_path / "rxn"
    reaction_dir.mkdir()
    local_out = reaction_dir / "calc.out"
    local_out.write_text("done\n", encoding="utf-8")

    assert pathing.resolve_artifact_path("calc.out", reaction_dir) == local_out.resolve()
    assert pathing.resolve_artifact_path(str(local_out), reaction_dir) == local_out.resolve()
    assert pathing.resolve_artifact_path("missing.out", reaction_dir) is None


def test_resolve_artifact_path_skips_resolve_errors_and_duplicate_resolved_candidates(tmp_path: Path) -> None:
    resolved = (tmp_path / "calc.out").resolve()

    with patch(
        "core.pathing.artifact_candidates",
        return_value=[
            _Candidate(resolved, exists=False, resolve_error=OSError("boom")),
            _Candidate(resolved, exists=False),
            _Candidate(resolved, exists=True),
        ],
    ):
        assert pathing.resolve_artifact_path("calc.out", tmp_path) is None

    with patch(
        "core.pathing.artifact_candidates",
        return_value=[_Candidate(resolved, exists=True)],
    ):
        assert pathing.resolve_artifact_path("calc.out", tmp_path) == resolved
