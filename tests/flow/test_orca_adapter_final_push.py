from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orca_auto.core.indexing import JobLocationRecord
from orca_auto.flow.adapters import (
    _orca_local_lookup,
    _orca_path_helpers,
    _orca_tracking,
)
from orca_auto.flow.adapters import (
    orca as orca_adapter,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def _write_xyz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "2",
                "comment",
                "H 0.0 0.0 0.0",
                "H 0.0 0.0 0.74",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _patch_resolve_for_names(
    monkeypatch: pytest.MonkeyPatch,
    sample_path: Path,
    bad_names: set[str],
) -> None:
    path_type = type(sample_path)
    original_resolve = path_type.resolve

    def fake_resolve(self: Path, strict: bool = False) -> Path:
        if self.name in bad_names:
            raise OSError("boom")
        return original_resolve(self, strict=strict)

    monkeypatch.setattr(path_type, "resolve", fake_resolve)


def test_basic_path_helpers_cover_remaining_low_level_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert _orca_path_helpers.direct_dir_target_impl("   ") is None

    class ExplodingResolvePath:
        def expanduser(self) -> ExplodingResolvePath:
            return self

        def resolve(self) -> Path:
            raise OSError("resolve failed")

    with monkeypatch.context() as inner:
        inner.setattr(_orca_path_helpers, "Path", lambda _raw: ExplodingResolvePath())
        assert _orca_path_helpers.resolve_candidate_path_impl("broken") is None

    class ExplodingExpanduserPath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> Path:
            raise OSError("expand failed")

    with monkeypatch.context() as inner:
        inner.setattr(_orca_path_helpers, "Path", lambda raw: ExplodingExpanduserPath(raw))
        assert (
            _orca_path_helpers.resolve_artifact_path_impl("relative.txt", tmp_path)
            == "relative.txt"
        )


def test_resolve_job_dir_and_record_organized_dir_skip_oserror_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_dir = tmp_path / "organized_ok"
    valid_dir.mkdir()
    bad_latest = tmp_path / "bad_latest"
    record = JobLocationRecord(
        job_id="job_refresh",
        app_name="orca_auto_orca",
        job_type="orca_opt",
        status="running",
        original_run_dir="",
        organized_output_dir=str(valid_dir),
        latest_known_path=str(bad_latest),
    )

    monkeypatch.setattr(
        _orca_local_lookup, "resolve_job_location", lambda _index_root, _target: record
    )
    _patch_resolve_for_names(monkeypatch, valid_dir, {"bad_latest"})

    resolved_dir, resolved_record = _orca_local_lookup.resolve_job_dir_impl(
        tmp_path, "job_refresh"
    )

    assert resolved_record is record
    assert resolved_dir == valid_dir.resolve()
    assert _orca_local_lookup.record_organized_dir_impl(record) == valid_dir.resolve()


def test_find_queue_entry_covers_target_queue_id_and_not_found(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = tmp_path / "rxn_queue"
    reaction_dir.mkdir()
    _write_json(
        allowed_root / _orca_local_lookup.QUEUE_FILE_NAME,
        [
            {
                "queue_id": "q_target",
                "task_id": "task_target",
                "metadata": {
                    "run_id": "run_target",
                    "reaction_dir": str(reaction_dir),
                },
            }
        ],
    )

    entry = _orca_local_lookup.find_queue_entry_impl(
        allowed_root=allowed_root,
        target="q_target",
        queue_id="",
        run_id="",
        reaction_dir="",
    )

    assert entry is not None
    assert entry["queue_id"] == "q_target"
    assert (
        _orca_local_lookup.find_queue_entry_impl(
            allowed_root=allowed_root,
            target="missing",
            queue_id="",
            run_id="",
            reaction_dir="",
        )
        is None
    )


def test_find_organized_record_covers_bad_paths_target_match_reaction_dir_and_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    organized_root = tmp_path / "orca_outputs"
    target_dir = organized_root / "opt" / "H2" / "run_target"
    target_dir.mkdir(parents=True)
    reaction_dir = tmp_path / "reaction_match"
    reaction_dir.mkdir()

    _write_jsonl(
        organized_root / "index" / _orca_local_lookup.RECORDS_FILE_NAME,
        [
            {"run_id": "bad_reaction", "reaction_dir": str(tmp_path / "bad_reaction")},
            {"run_id": "bad_organized", "organized_path": "bad_organized"},
            {"run_id": "target_run", "organized_path": "opt/H2/run_target"},
            {"run_id": "reaction_run", "reaction_dir": str(reaction_dir)},
        ],
    )
    _patch_resolve_for_names(monkeypatch, organized_root, {"bad_reaction", "bad_organized"})

    found_target = _orca_local_lookup.find_organized_record_impl(
        organized_root=organized_root,
        target="target_run",
        run_id="",
        reaction_dir="",
    )
    assert found_target is not None
    assert found_target["run_id"] == "target_run"

    found_reaction = _orca_local_lookup.find_organized_record_impl(
        organized_root=organized_root,
        target="missing",
        run_id="",
        reaction_dir=str(reaction_dir),
    )
    assert found_reaction is not None
    assert found_reaction["run_id"] == "reaction_run"

    assert (
        _orca_local_lookup.find_organized_record_impl(
            organized_root=organized_root,
            target="missing",
            run_id="",
            reaction_dir=str(tmp_path / "other"),
        )
        is None
    )


def test_directory_and_artifact_path_helpers_cover_oserror_fallbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_resolve_for_names(
        monkeypatch,
        tmp_path,
        {"bad_reaction_dir", "bad_organized_dir", "bad_abs.txt", "bad_rel.txt", "bad_stub"},
    )

    assert (
        _orca_local_lookup.organized_dir_from_record_impl(
            tmp_path,
            {"reaction_dir": str(tmp_path / "bad_reaction_dir")},
        )
        is None
    )
    assert (
        _orca_local_lookup.organized_dir_from_record_impl(
            tmp_path,
            {"organized_path": "bad_organized_dir"},
        )
        is None
    )

    bad_abs = tmp_path / "bad_abs.txt"
    assert _orca_path_helpers.resolve_artifact_path_impl(str(bad_abs), None) == str(bad_abs)
    assert _orca_path_helpers.resolve_artifact_path_impl("bad_rel.txt", tmp_path) == str(
        tmp_path / "bad_rel.txt"
    )

    assert (
        _orca_local_lookup.load_tracked_organized_ref_impl(
            JobLocationRecord(
                job_id="job_stub_empty",
                app_name="orca_auto_orca",
                job_type="orca_opt",
                status="running",
                original_run_dir="",
            ),
            None,
        )
        == {}
    )
    assert (
        _orca_local_lookup.load_tracked_organized_ref_impl(
            JobLocationRecord(
                job_id="job_stub_bad",
                app_name="orca_auto_orca",
                job_type="orca_opt",
                status="running",
                original_run_dir=str(tmp_path / "bad_stub"),
            ),
            None,
        )
        == {}
    )


def test_iter_existing_dirs_skips_oserror_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    good_dir = tmp_path / "good_dir"
    bad_dir = tmp_path / "bad_dir"
    good_dir.mkdir()
    bad_dir.mkdir()
    _patch_resolve_for_names(monkeypatch, good_dir, {"bad_dir"})

    assert _orca_path_helpers.iter_existing_dirs_impl(bad_dir, good_dir) == [good_dir.resolve()]


def test_prefer_orca_optimized_xyz_returns_unresolved_preferred_candidate_on_resolve_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_dir = tmp_path / "run_dir"
    current_dir.mkdir()
    selected_inp = current_dir / "job.inp"
    preferred_xyz = current_dir / "job.xyz"
    selected_inp.write_text("! Opt\n", encoding="utf-8")
    _write_xyz(preferred_xyz)
    _patch_resolve_for_names(monkeypatch, current_dir, {"job.xyz"})

    chosen = _orca_path_helpers.prefer_orca_optimized_xyz_impl(
        selected_inp=str(selected_inp),
        selected_input_xyz="",
        current_dir=current_dir,
        organized_dir=None,
        latest_known_path="",
        last_out_path="",
    )

    assert chosen == str(preferred_xyz)


def test_prefer_orca_optimized_xyz_handles_source_glob_and_duplicate_fallbacks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExplodingSourcePath:
        def resolve(self) -> Path:
            raise OSError("source resolve failed")

    class ExplodingResolveItem:
        def __init__(self, text: str) -> None:
            self.text = text

        def is_file(self) -> bool:
            return True

        def stat(self) -> SimpleNamespace:
            return SimpleNamespace(st_mtime=1)

        def resolve(self) -> Path:
            raise OSError("item resolve failed")

        def __str__(self) -> str:
            return self.text

    class GlobErrorDir:
        def glob(self, _pattern: str) -> list[ExplodingResolveItem]:
            raise OSError("glob failed")

    class DuplicateDir:
        def __init__(self, item: ExplodingResolveItem) -> None:
            self.item = item

        def glob(self, _pattern: str) -> list[ExplodingResolveItem]:
            return [self.item, self.item]

    source_path = ExplodingSourcePath()
    candidate = ExplodingResolveItem("/tmp/final.xyz")
    monkeypatch.setattr(
        _orca_path_helpers,
        "resolve_candidate_path_impl",
        lambda value: source_path if value == "selected_source" else None,
    )
    monkeypatch.setattr(
        _orca_path_helpers,
        "iter_existing_dirs_impl",
        lambda *_args: [GlobErrorDir(), DuplicateDir(candidate)],
    )

    chosen = _orca_path_helpers.prefer_orca_optimized_xyz_impl(
        selected_inp="",
        selected_input_xyz="selected_source",
        current_dir=None,
        organized_dir=None,
        latest_known_path="",
        last_out_path="",
    )

    assert chosen == "/tmp/final.xyz"


def test_load_orca_artifact_contract_uses_target_when_no_paths_are_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_orca_tracking, "load_orca_contract_payload_impl", lambda **kwargs: None)
    monkeypatch.setattr(_orca_tracking, "tracked_runtime_context_impl", lambda **kwargs: None)
    monkeypatch.setattr(
        _orca_tracking,
        "tracked_artifact_context_impl",
        lambda **kwargs: (None, None, {}, {}, {}),
    )
    monkeypatch.setattr(
        _orca_local_lookup, "resolve_job_dir_impl", lambda index_root, target: (None, None)
    )
    monkeypatch.setattr(_orca_local_lookup, "find_queue_entry_impl", lambda **kwargs: None)
    monkeypatch.setattr(_orca_path_helpers, "direct_dir_target_impl", lambda target: None)
    monkeypatch.setattr(_orca_path_helpers, "resolve_candidate_path_impl", lambda value: None)

    contract = orca_adapter.load_orca_artifact_contract(target="dangling_target")

    assert contract.latest_known_path == "dangling_target"
    assert contract.status == "unknown"
