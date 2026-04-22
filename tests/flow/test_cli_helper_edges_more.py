# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow import cli


def test_cli_path_and_manifest_helper_edges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    existing_file = tmp_path / "existing.xyz"
    existing_file.write_text("x", encoding="utf-8")

    assert (cli._project_root() / "chemstack").is_dir()
    assert cli._resolve_existing_path("   ") is None
    assert cli._resolve_existing_path(str(existing_file)) == existing_file.resolve()
    assert cli._resolve_existing_path(str(tmp_path / "missing.xyz")) is None

    class _ExplodingPath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> "_ExplodingPath":
            return self

        def resolve(self) -> Path:
            raise OSError(self.raw)

    monkeypatch.setattr(cli, "Path", _ExplodingPath)
    assert cli._resolve_existing_path("boom") is None


def test_cli_option_and_workflow_root_helpers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert cli._discover_workflow_root("~/workflow-root").endswith("workflow-root")

    captured_root: Path | None = None

    def fake_default_config_path(repo_root: Path) -> Path:
        nonlocal captured_root
        captured_root = repo_root
        return Path("/tmp/chemstack.yaml")

    monkeypatch.setattr(cli, "default_config_path_from_repo_root", fake_default_config_path)
    monkeypatch.setattr(cli, "shared_workflow_root_from_config", lambda path: f"resolved:{path}")
    assert cli._discover_workflow_root(None) == "resolved:/tmp/chemstack.yaml"
    assert captured_root == cli._project_root()

    with pytest.raises(ValueError, match="workflow_type must be one of"):
        cli._normalize_workflow_type("unknown")

    assert cli._resolve_text_option_with_section("  cli-value  ", {}, "key", {}, "section_key", "fallback") == "cli-value"
    assert cli._resolve_int_option(7, {}, "key", 1) == 7
    assert cli._resolve_int_option_with_section(9, {}, "key", {}, "section_key", 1) == 9


def test_cli_run_dir_manifest_and_path_resolution_edges(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workflow_dir = tmp_path / "workflow_dir"
    workflow_dir.mkdir()

    monkeypatch.setattr(cli, "WORKFLOW_MANIFEST_FILENAMES", ("flow.json",))

    (workflow_dir / "flow.json").write_text('{"workflow_type": "reaction_ts_search"}', encoding="utf-8")
    assert cli._load_run_dir_manifest(workflow_dir) == {"workflow_type": "reaction_ts_search"}

    (workflow_dir / "flow.json").write_text("null", encoding="utf-8")
    assert cli._load_run_dir_manifest(workflow_dir) == {}

    (workflow_dir / "flow.json").write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="Run directory manifest must contain a mapping"):
        cli._load_run_dir_manifest(workflow_dir)

    assert cli._resolve_manifest_file_value(workflow_dir, None) == ""
    assert cli._resolve_manifest_file_value(workflow_dir, "inputs/input.xyz") == str(
        (workflow_dir / "inputs" / "input.xyz").resolve()
    )
    assert cli._resolve_run_dir_path(
        workflow_dir,
        explicit=None,
        manifest={"input_xyz": "inputs/input.xyz"},
        key="input_xyz",
        default_names=("unused.xyz",),
    ) == str((workflow_dir / "inputs" / "input.xyz").resolve())


def test_cmd_run_dir_reports_invalid_directory_and_unknown_layout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing_dir = tmp_path / "missing"
    invalid_args = SimpleNamespace(
        workflow_dir=str(missing_dir),
        workflow_type=None,
        workflow_root=None,
        reactant_xyz=None,
        product_xyz=None,
        input_xyz=None,
        crest_mode=None,
        priority=None,
        max_cores=None,
        max_memory_gb=None,
        max_crest_candidates=None,
        max_xtb_stages=None,
        max_orca_stages=None,
        orca_route_line=None,
        charge=None,
        multiplicity=None,
        json=False,
    )
    assert cli.cmd_run_dir(invalid_args) == 1
    assert "workflow_dir does not exist or is not a directory" in capsys.readouterr().out

    empty_workflow_dir = tmp_path / "empty_workflow"
    empty_workflow_dir.mkdir()
    unknown_args = SimpleNamespace(**{**invalid_args.__dict__, "workflow_dir": str(empty_workflow_dir)})
    assert cli.cmd_run_dir(unknown_args) == 1
    assert "Could not infer workflow type from workflow_dir" in capsys.readouterr().out


def test_cmd_run_dir_for_conformer_uses_nested_crest_section(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "conformer_nested"
    workflow_dir.mkdir()
    (workflow_dir / "input.xyz").write_text("x", encoding="utf-8")
    (workflow_dir / "flow.yaml").write_text(
        "\n".join(
            [
                "workflow_type: conformer_screening",
                "resources:",
                "  max_cores: 10",
                "crest:",
                "  mode: nci",
                "  energy_window: 6.0",
                "orca:",
                '  route_line: "! opt"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli, "_discover_workflow_root", lambda explicit: "/tmp/workflow_root")

    def fake_create_conformer_screening_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "workflow_id": "wf_conformer_nested",
            "metadata": {"workspace_dir": "/tmp/workflows/wf_conformer_nested"},
            "stages": [{}],
        }

    monkeypatch.setattr(cli, "create_conformer_screening_workflow", fake_create_conformer_screening_workflow)

    args = SimpleNamespace(
        workflow_dir=str(workflow_dir),
        workflow_type=None,
        workflow_root=None,
        reactant_xyz=None,
        product_xyz=None,
        input_xyz=None,
        crest_mode=None,
        priority=None,
        max_cores=None,
        max_memory_gb=None,
        max_crest_candidates=None,
        max_xtb_stages=None,
        max_orca_stages=None,
        orca_route_line=None,
        charge=None,
        multiplicity=None,
        json=False,
    )

    assert cli.cmd_run_dir(args) == 0
    assert "workflow_id: wf_conformer_nested" in capsys.readouterr().out
    assert captured["crest_mode"] == "nci"
    assert captured["crest_job_manifest"] == {"mode": "nci", "energy_window": 6.0}
    assert captured["max_cores"] == 10
    assert captured["orca_route_line"] == "! opt"


def test_cmd_activity_list_json_and_cancel_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "list_activities",
        lambda **kwargs: {
            "count": 1,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "chem_flow",
                }
            ],
        },
    )
    assert cli.cmd_activity_list(
        SimpleNamespace(
            workflow_root="/tmp/wf",
            limit=0,
            refresh=False,
            chemstack_config="/tmp/chemstack.yaml",
            orca_auto_config=None,
            orca_auto_repo_root=None,
            json=True,
        )
    ) == 0
    assert json.loads(capsys.readouterr().out)["count"] == 1

    monkeypatch.setattr(
        cli,
        "cancel_activity",
        lambda **kwargs: {
            "activity_id": "xtb-q-1",
            "engine": "xtb",
            "source": "xtb_auto",
            "label": "rxn-a",
            "status": "cancel_requested",
            "cancel_target": "xtb-q-1",
        },
    )
    assert cli.cmd_activity_cancel(
        SimpleNamespace(
            target="xtb-q-1",
            workflow_root=None,
            chemstack_config="/tmp/chemstack.yaml",
            crest_auto_executable="crest_auto",
            crest_auto_repo_root=None,
            xtb_auto_executable="xtb_auto",
            xtb_auto_repo_root=None,
            orca_auto_executable="orca_auto",
            orca_auto_repo_root=None,
            json=False,
        )
    ) == 0
    stdout = capsys.readouterr().out
    assert "activity_id: xtb-q-1" in stdout
    assert "engine: xtb" in stdout
    assert "source: xtb_auto" in stdout
    assert "label: rxn-a" in stdout
    assert "status: cancel_requested" in stdout
    assert "cancel_target: xtb-q-1" in stdout
