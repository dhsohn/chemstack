from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest


from chemstack.core.indexing import JobLocationRecord

from chemstack.flow.adapters import orca as orca_adapter


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


def _module_not_found(name: str) -> ModuleNotFoundError:
    error = ModuleNotFoundError(f"No module named '{name}'")
    error.name = name
    return error


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


def test_import_and_basic_path_helpers_cover_remaining_low_level_edges(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import_calls: list[str] = []

    def fake_import(module_name: str) -> object:
        import_calls.append(module_name)
        raise _module_not_found("chemstack")

    monkeypatch.setattr(orca_adapter, "import_module", fake_import)

    assert orca_adapter._import_orca_auto_module("chemstack.orca.job_locations") is None
    assert import_calls == ["chemstack.orca.job_locations"]

    assert orca_adapter._direct_dir_target("   ") is None

    class ExplodingResolvePath:
        def expanduser(self) -> ExplodingResolvePath:
            return self

        def resolve(self) -> Path:
            raise OSError("resolve failed")

    with monkeypatch.context() as inner:
        inner.setattr(orca_adapter, "Path", lambda _raw: ExplodingResolvePath())
        assert orca_adapter._resolve_candidate_path("broken") is None

    class ExplodingExpanduserPath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> Path:
            raise OSError("expand failed")

    with monkeypatch.context() as inner:
        inner.setattr(orca_adapter, "Path", lambda raw: ExplodingExpanduserPath(raw))
        assert orca_adapter._resolve_artifact_path("relative.txt", tmp_path) == "relative.txt"


def test_resolve_job_dir_and_record_organized_dir_skip_oserror_candidates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_dir = tmp_path / "organized_ok"
    valid_dir.mkdir()
    bad_latest = tmp_path / "bad_latest"
    record = JobLocationRecord(
        job_id="job_refresh",
        app_name="chemstack_orca",
        job_type="orca_opt",
        status="running",
        original_run_dir="",
        organized_output_dir=str(valid_dir),
        latest_known_path=str(bad_latest),
    )

    monkeypatch.setattr(orca_adapter, "resolve_job_location", lambda _index_root, _target: record)
    _patch_resolve_for_names(monkeypatch, valid_dir, {"bad_latest"})

    resolved_dir, resolved_record = orca_adapter._resolve_job_dir(tmp_path, "job_refresh")

    assert resolved_record is record
    assert resolved_dir == valid_dir.resolve()
    assert orca_adapter._record_organized_dir(record) == valid_dir.resolve()


def test_find_queue_entry_covers_target_queue_id_and_not_found(tmp_path: Path) -> None:
    allowed_root = tmp_path / "orca_runs"
    reaction_dir = tmp_path / "rxn_queue"
    reaction_dir.mkdir()
    _write_json(
        allowed_root / orca_adapter.QUEUE_FILE_NAME,
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

    entry = orca_adapter._find_queue_entry(
        allowed_root=allowed_root,
        target="q_target",
        queue_id="",
        run_id="",
        reaction_dir="",
    )

    assert entry is not None
    assert entry["queue_id"] == "q_target"
    assert (
        orca_adapter._find_queue_entry(
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
        organized_root / "index" / orca_adapter.RECORDS_FILE_NAME,
        [
            {"run_id": "bad_reaction", "reaction_dir": str(tmp_path / "bad_reaction")},
            {"run_id": "bad_organized", "organized_path": "bad_organized"},
            {"run_id": "target_run", "organized_path": "opt/H2/run_target"},
            {"run_id": "reaction_run", "reaction_dir": str(reaction_dir)},
        ],
    )
    _patch_resolve_for_names(monkeypatch, organized_root, {"bad_reaction", "bad_organized"})

    found_target = orca_adapter._find_organized_record(
        organized_root=organized_root,
        target="target_run",
        run_id="",
        reaction_dir="",
    )
    assert found_target is not None
    assert found_target["run_id"] == "target_run"

    found_reaction = orca_adapter._find_organized_record(
        organized_root=organized_root,
        target="missing",
        run_id="",
        reaction_dir=str(reaction_dir),
    )
    assert found_reaction is not None
    assert found_reaction["run_id"] == "reaction_run"

    assert (
        orca_adapter._find_organized_record(
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
        orca_adapter._organized_dir_from_record(
            tmp_path,
            {"reaction_dir": str(tmp_path / "bad_reaction_dir")},
        )
        is None
    )
    assert (
        orca_adapter._organized_dir_from_record(
            tmp_path,
            {"organized_path": "bad_organized_dir"},
        )
        is None
    )

    bad_abs = tmp_path / "bad_abs.txt"
    assert orca_adapter._resolve_artifact_path(str(bad_abs), None) == str(bad_abs)
    assert orca_adapter._resolve_artifact_path("bad_rel.txt", tmp_path) == str(tmp_path / "bad_rel.txt")

    assert (
        orca_adapter._load_tracked_organized_ref(
            JobLocationRecord(
                job_id="job_stub_empty",
                app_name="chemstack_orca",
                job_type="orca_opt",
                status="running",
                original_run_dir="",
            ),
            None,
        )
        == {}
    )
    assert (
        orca_adapter._load_tracked_organized_ref(
            JobLocationRecord(
                job_id="job_stub_bad",
                app_name="chemstack_orca",
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

    assert orca_adapter._iter_existing_dirs(bad_dir, good_dir) == [good_dir.resolve()]


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

    chosen = orca_adapter._prefer_orca_optimized_xyz(
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
        orca_adapter,
        "_resolve_candidate_path",
        lambda value: source_path if value == "selected_source" else None,
    )
    monkeypatch.setattr(
        orca_adapter,
        "_iter_existing_dirs",
        lambda *_args: [GlobErrorDir(), DuplicateDir(candidate)],
    )

    chosen = orca_adapter._prefer_orca_optimized_xyz(
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
    monkeypatch.setattr(orca_adapter, "_tracked_contract_payload", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_runtime_context", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_tracked_artifact_context", lambda **kwargs: (None, None, {}, {}, {}))
    monkeypatch.setattr(orca_adapter, "_resolve_job_dir", lambda index_root, target: (None, None))
    monkeypatch.setattr(orca_adapter, "_find_queue_entry", lambda **kwargs: None)
    monkeypatch.setattr(orca_adapter, "_direct_dir_target", lambda target: None)
    monkeypatch.setattr(orca_adapter, "_resolve_candidate_path", lambda value: None)

    contract = orca_adapter.load_orca_artifact_contract(target="dangling_target")

    assert contract.latest_known_path == "dangling_target"
    assert contract.status == "unknown"
