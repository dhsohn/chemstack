from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.commands import engine_reindex
from chemstack.core.indexing import JobLocationRecord, get_job_location, upsert_job_location


def _cfg(
    allowed_root: Path,
    organized_root: Path,
    *,
    workflow_root: Path | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_root=str(workflow_root or ""),
        runtime=SimpleNamespace(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
        ),
    )


def _record(job_id: str, job_dir: Path, *, status: str = "completed") -> JobLocationRecord:
    return JobLocationRecord(
        job_id=job_id,
        app_name="chemstack.test",
        job_type="test",
        status=status,
        original_run_dir=str(job_dir),
        latest_known_path=str(job_dir),
    )


def test_scan_roots_prefers_explicit_root_and_discovers_workflow_runtime_roots(
    tmp_path: Path,
) -> None:
    explicit = tmp_path / "explicit"
    explicit.mkdir()
    workflow_root = tmp_path / "workflows"
    stage = workflow_root / "run-1" / "02_xtb"
    stage.mkdir(parents=True)
    cfg = _cfg(tmp_path / "allowed", tmp_path / "organized", workflow_root=workflow_root)

    assert engine_reindex.scan_roots(cfg, str(explicit), engine="xtb") == [explicit.resolve()]
    assert engine_reindex.scan_roots(cfg, None, engine="xtb") == [stage.resolve()]


def test_iter_candidate_dirs_finds_supported_artifact_files(tmp_path: Path) -> None:
    root = tmp_path / "root"
    for dirname, filename in (
        ("from-state", "job_state.json"),
        ("from-report", "job_report.json"),
        ("from-organized", "organized_ref.json"),
    ):
        job_dir = root / dirname
        job_dir.mkdir(parents=True)
        (job_dir / filename).write_text("{}", encoding="utf-8")
    (root / "ignored").mkdir()

    assert engine_reindex.iter_candidate_dirs(root) == {
        (root / "from-state").resolve(),
        (root / "from-report").resolve(),
        (root / "from-organized").resolve(),
    }


def test_cmd_reindex_uses_report_state_organized_ref_job_id_precedence(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    root = tmp_path / "index"
    root.mkdir()
    job_dir = tmp_path / "candidate-dir"
    job_dir.mkdir()
    upsert_job_location(root, _record("report-id", job_dir, status="existing"))
    existing_seen: list[str | None] = []

    def record_from_artifacts(**kwargs: Any) -> JobLocationRecord:
        existing_seen.append(kwargs["existing"].job_id if kwargs["existing"] is not None else None)
        return _record("new-id", kwargs["job_dir"], status="indexed")

    deps = engine_reindex.ReindexDeps(
        load_config=lambda _config: _cfg(root, root),
        load_state=lambda _job_dir: {"job_id": "state-id"},
        load_report_json=lambda _job_dir: {"job_id": "report-id"},
        load_organized_ref=lambda _job_dir: {"job_id": "organized-id"},
        index_root_for_path=lambda _cfg_obj, _job_dir: root,
        record_from_artifacts=record_from_artifacts,
        _scan_roots=lambda _cfg_obj, _raw_root: [tmp_path],
        _iter_candidate_dirs=lambda _scan_root: {job_dir},
    )

    exit_code = engine_reindex.cmd_reindex(SimpleNamespace(config=None, root=None), engine="xtb", deps=deps)

    assert exit_code == 0
    assert existing_seen == ["report-id"]
    assert get_job_location(root, "new-id") is not None
    assert "indexed: 1" in capsys.readouterr().out


def test_cmd_reindex_falls_back_to_state_organized_ref_and_directory_name_for_candidate_id(
    tmp_path: Path,
) -> None:
    root = tmp_path / "index"
    root.mkdir()
    job_dirs = [tmp_path / "state-dir", tmp_path / "organized-dir", tmp_path / "directory-id"]
    for job_dir in job_dirs:
        job_dir.mkdir()
    for job_id, job_dir in (
        ("state-id", job_dirs[0]),
        ("organized-id", job_dirs[1]),
        ("directory-id", job_dirs[2]),
    ):
        upsert_job_location(root, _record(job_id, job_dir, status="existing"))
    existing_seen: dict[str, str | None] = {}

    def load_state(job_dir: Path) -> dict[str, str]:
        return {"job_id": "state-id"} if job_dir.name == "state-dir" else {}

    def load_organized_ref(job_dir: Path) -> dict[str, str]:
        return {"job_id": "organized-id"} if job_dir.name == "organized-dir" else {}

    def record_from_artifacts(**kwargs: Any) -> JobLocationRecord:
        job_dir = kwargs["job_dir"]
        existing = kwargs["existing"]
        existing_seen[job_dir.name] = existing.job_id if existing is not None else None
        return _record(f"indexed-{job_dir.name}", job_dir)

    deps = engine_reindex.ReindexDeps(
        load_config=lambda _config: _cfg(root, root),
        load_state=load_state,
        load_report_json=lambda _job_dir: {},
        load_organized_ref=load_organized_ref,
        index_root_for_path=lambda _cfg_obj, _job_dir: root,
        record_from_artifacts=record_from_artifacts,
        _scan_roots=lambda _cfg_obj, _raw_root: [tmp_path],
        _iter_candidate_dirs=lambda _scan_root: set(job_dirs),
    )

    assert engine_reindex.cmd_reindex(SimpleNamespace(config=None, root=None), engine="xtb", deps=deps) == 0
    assert existing_seen == {
        "directory-id": "directory-id",
        "organized-dir": "organized-id",
        "state-dir": "state-id",
    }


def test_cmd_reindex_prints_error_when_no_roots_are_available(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    deps = engine_reindex.ReindexDeps(
        load_config=lambda _config: _cfg(tmp_path / "missing", tmp_path / "also-missing"),
        load_state=lambda _job_dir: {},
        load_report_json=lambda _job_dir: {},
        load_organized_ref=lambda _job_dir: {},
        index_root_for_path=lambda _cfg_obj, _job_dir: tmp_path,
        record_from_artifacts=lambda **_kwargs: None,
        _scan_roots=lambda _cfg_obj, _raw_root: [],
        _iter_candidate_dirs=lambda _root: set(),
    )

    assert engine_reindex.cmd_reindex(SimpleNamespace(config=None, root=None), engine="xtb", deps=deps) == 1
    assert capsys.readouterr().out == "error: no reindex roots available\n"


def test_scan_roots_skips_missing_default_roots(tmp_path: Path) -> None:
    organized = tmp_path / "organized"
    organized.mkdir()
    cfg = _cfg(tmp_path / "missing", organized)

    assert engine_reindex.scan_roots(cfg, None, engine="xtb") == [organized.resolve()]
