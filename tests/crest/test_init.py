from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from chemstack.crest.commands import init as init_cmd


def _write_config(tmp_path: Path) -> tuple[Path, Path]:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "01_crest"
    allowed_root.mkdir(parents=True)
    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "workflow": {
                    "root": str(workflow_root),
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path, allowed_root


def test_cmd_init_requires_root(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config_path, _ = _write_config(tmp_path)

    exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root="",
        )
    )

    captured = capsys.readouterr().out
    assert exit_code == 1
    assert "error: init requires --root" in captured


def test_cmd_init_scaffolds_crest_job_and_reports_skips(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config_path, allowed_root = _write_config(tmp_path)
    job_dir = allowed_root / "crest-job"

    first_exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root=str(job_dir),
        )
    )

    first_output = capsys.readouterr().out
    assert first_exit_code == 0
    assert f"job_dir: {job_dir.resolve()}" in first_output
    assert "created: 3" in first_output
    assert "skipped: 0" in first_output
    assert "created_file: input.xyz" in first_output
    assert "created_file: crest_job.yaml" in first_output
    assert "created_file: README.md" in first_output

    assert (job_dir / "input.xyz").exists()
    assert (job_dir / "README.md").exists()
    manifest = yaml.safe_load((job_dir / "crest_job.yaml").read_text(encoding="utf-8"))
    assert manifest["mode"] == "standard"
    assert manifest["input_xyz"] == "input.xyz"

    second_exit_code = init_cmd.cmd_init(
        Namespace(
            config=str(config_path),
            root=str(job_dir),
        )
    )

    second_output = capsys.readouterr().out
    assert second_exit_code == 0
    assert "created: 0" in second_output
    assert "skipped: 3" in second_output
    assert "skipped_file: input.xyz" in second_output
    assert "skipped_file: crest_job.yaml" in second_output
    assert "skipped_file: README.md" in second_output
