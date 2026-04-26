from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.scaffold import (
    ScaffoldFile,
    print_scaffold_report,
    resolve_scaffold_job_dir,
    write_scaffold_files,
)


def test_resolve_scaffold_job_dir_uses_workflow_local_runs_root(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "internal" / "xtb" / "runs"
    allowed_root.mkdir(parents=True)
    cfg = SimpleNamespace(
        workflow_root=str(workflow_root),
        runtime=SimpleNamespace(allowed_root=str(tmp_path / "fallback")),
    )

    job_dir = resolve_scaffold_job_dir(
        allowed_root / "job-001",
        cfg,
        engine="xtb",
        engine_label="xTB",
    )

    assert job_dir == (allowed_root / "job-001").resolve()
    assert job_dir.is_dir()


def test_resolve_scaffold_job_dir_uses_configured_allowed_root(tmp_path: Path) -> None:
    allowed_root = tmp_path / "runs"
    allowed_root.mkdir()
    cfg = SimpleNamespace(
        workflow_root="",
        runtime=SimpleNamespace(allowed_root=str(allowed_root)),
    )

    job_dir = resolve_scaffold_job_dir(
        allowed_root / "job-001",
        cfg,
        engine="xtb",
        engine_label="xTB",
    )

    assert job_dir == (allowed_root / "job-001").resolve()
    assert job_dir.is_dir()


def test_resolve_scaffold_job_dir_rejects_paths_outside_workflow_runs_root(
    tmp_path: Path,
) -> None:
    workflow_root = tmp_path / "workflow_root"
    (workflow_root / "wf_001" / "internal" / "xtb" / "runs").mkdir(parents=True)
    cfg = SimpleNamespace(
        workflow_root=str(workflow_root),
        runtime=SimpleNamespace(allowed_root=str(tmp_path / "fallback")),
    )

    with pytest.raises(ValueError, match="workflow-local xTB runs root"):
        resolve_scaffold_job_dir(
            tmp_path / "outside-job",
            cfg,
            engine="xtb",
            engine_label="xTB",
        )


def test_write_scaffold_files_and_report_preserve_created_skipped_order(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    existing = tmp_path / "existing.txt"
    existing.write_text("original\n", encoding="utf-8")

    result = write_scaffold_files(
        [
            ScaffoldFile(tmp_path / "fresh.txt", "fresh\n", "fresh.txt"),
            ScaffoldFile(existing, "new\n", "existing.txt"),
        ]
    )
    print_scaffold_report(tmp_path, result, metadata=(("job_type", "opt"),))

    output = capsys.readouterr().out
    assert result.created == ("fresh.txt",)
    assert result.skipped == ("existing.txt",)
    assert existing.read_text(encoding="utf-8") == "original\n"
    assert "job_type: opt" in output
    assert "created: 1" in output
    assert "skipped: 1" in output
    assert output.index("created_file: fresh.txt") < output.index("skipped_file: existing.txt")
