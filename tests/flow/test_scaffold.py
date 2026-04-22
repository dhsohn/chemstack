from __future__ import annotations

from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from chemstack.flow import scaffold


def test_cmd_scaffold_creates_reaction_workflow_scaffold(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workflow_dir = tmp_path / "reaction_workflow"

    rc = scaffold.cmd_scaffold(
        Namespace(
            root=str(workflow_dir),
            workflow_type="reaction_ts_search",
        )
    )

    output = capsys.readouterr().out
    manifest = yaml.safe_load((workflow_dir / "flow.yaml").read_text(encoding="utf-8"))
    readme = (workflow_dir / "README.md").read_text(encoding="utf-8")

    assert rc == 0
    assert (workflow_dir / "reactant.xyz").exists()
    assert (workflow_dir / "product.xyz").exists()
    assert (workflow_dir / "README.md").exists()
    assert manifest["workflow_type"] == "reaction_ts_search"
    assert manifest["crest_mode"] == "standard"
    assert manifest["max_crest_candidates"] == 3
    assert "max_xtb_stages" not in manifest
    assert "max_orca_stages" not in manifest
    assert "workflow_type: reaction_ts_search" in output
    assert "crest_mode: standard" in output
    assert "created_file: reactant.xyz" in output
    assert "created_file: product.xyz" in output
    assert "chemstack scaffold ts_search" in readme
    assert "crest_mode: nci" in readme
    assert "queues one ORCA OptTS child job for each xTB ts_guess" in readme


def test_cmd_scaffold_is_idempotent_for_conformer_workflow(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "conformer_workflow"

    first_rc = scaffold.cmd_scaffold(
        Namespace(
            root=str(workflow_dir),
            workflow_type="conformer_screening",
        )
    )
    assert first_rc == 0
    manifest = yaml.safe_load((workflow_dir / "flow.yaml").read_text(encoding="utf-8"))
    readme = (workflow_dir / "README.md").read_text(encoding="utf-8")
    assert manifest["max_orca_stages"] == 20
    assert "20 retained CREST conformers" in readme
    capsys.readouterr()

    custom_input = "1\ncustom\nHe 0.0 0.0 0.0\n"
    (workflow_dir / "input.xyz").write_text(custom_input, encoding="utf-8")

    second_rc = scaffold.cmd_scaffold(
        Namespace(
            root=str(workflow_dir),
            workflow_type="conformer_screening",
        )
    )

    output = capsys.readouterr().out
    assert second_rc == 0
    assert "created: 0" in output
    assert "skipped: 3" in output
    assert "skipped_file: input.xyz" in output
    assert (workflow_dir / "input.xyz").read_text(encoding="utf-8") == custom_input


def test_cmd_scaffold_rejects_invalid_crest_mode(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    workflow_dir = tmp_path / "bad_mode"

    rc = scaffold.cmd_scaffold(
        Namespace(
            root=str(workflow_dir),
            workflow_type="reaction_ts_search",
            crest_mode="fast",
        )
    )

    output = capsys.readouterr().out
    assert rc == 1
    assert "error: unsupported crest_mode: fast" in output
