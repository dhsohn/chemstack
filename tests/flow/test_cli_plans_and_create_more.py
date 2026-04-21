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


class _FakeSerializable:
    def __init__(self, **payload: Any) -> None:
        self.__dict__.update(payload)

    def to_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


def _plan_payload(template_name: str, *, include_source: bool = True) -> dict[str, Any]:
    payload = {
        "workflow_id": f"wf_{template_name}",
        "template_name": template_name,
        "status": "planned",
        "reaction_key": "rxn_1",
        "metadata": {"workspace_dir": "/tmp/workflows/wf_1"},
        "stages": [
            {
                "stage_id": "stage_1",
                "task": {
                    "engine": "orca",
                    "task_kind": "opt",
                    "payload": {
                        "selected_input_xyz": "/tmp/input.xyz",
                        "reaction_dir": "/tmp/reaction_dir",
                    },
                    "enqueue_payload": {"command": "python -m chemstack.orca.cli run-dir /tmp/reaction_dir"},
                },
            },
            {
                "stage_id": "stage_2",
                "task": {
                    "engine": "orca",
                    "task_kind": "freq",
                    "payload": {
                        "selected_input_xyz": "/tmp/fallback.xyz",
                        "suggested_command": "python -m chemstack.orca.cli run-dir /tmp/fallback.xyz",
                    },
                },
            },
        ],
    }
    if include_source:
        payload["source_job_id"] = "source_1"
    return payload


def _create_payload(template_name: str) -> dict[str, Any]:
    return {
        "workflow_id": f"wf_create_{template_name}",
        "template_name": template_name,
        "metadata": {"workspace_dir": "/tmp/workflows/wf_create"},
        "stages": [{}, {}],
    }


def test_cmd_xtb_inspect_supports_text_and_json(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    contract = _FakeSerializable(
        job_id="xtb-job-1",
        job_type="path_search",
        status="completed",
        reason="done",
        job_dir="/tmp/xtb-job-1",
        latest_known_path="/tmp/xtb-job-1",
        organized_output_dir="/tmp/organized",
        reaction_key="rxn_1",
        selected_input_xyz="/tmp/input.xyz",
        candidate_details=[{"rank": 1}],
        selected_candidate_paths=["/tmp/selected.xyz"],
        analysis_summary={"status": "ok"},
    )
    monkeypatch.setattr(cli, "load_xtb_artifact_contract", lambda **kwargs: contract)

    assert cli.cmd_xtb_inspect(SimpleNamespace(xtb_index_root="/tmp/xtb", target="xtb-job-1", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "job_id: xtb-job-1" in stdout
    assert "candidate_count: 1" in stdout
    assert "selected_candidate_paths: ['/tmp/selected.xyz']" in stdout
    assert "analysis_summary: {'status': 'ok'}" in stdout

    assert cli.cmd_xtb_inspect(SimpleNamespace(xtb_index_root="/tmp/xtb", target="xtb-job-1", json=True)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["job_id"] == "xtb-job-1"
    assert payload["job_type"] == "path_search"


def test_cmd_xtb_candidates_json_mode_serializes_candidates(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    contract = _FakeSerializable(job_id="xtb-job-2", job_type="path_search", reaction_key="rxn_2")
    candidates = [
        _FakeSerializable(
            source_job_id="xtb-job-2",
            source_job_type="path_search",
            reaction_key="rxn_2",
            selected_input_xyz="/tmp/input.xyz",
            rank=1,
            kind="ts_guess",
            artifact_path="/tmp/candidate.xyz",
            selected=True,
            metadata={},
        )
    ]
    monkeypatch.setattr(cli, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(cli, "select_xtb_downstream_inputs", lambda *args, **kwargs: candidates)

    assert (
        cli.cmd_xtb_candidates(
            SimpleNamespace(
                xtb_index_root="/tmp/xtb",
                target="xtb-job-2",
                preferred_kinds=None,
                max_candidates=3,
                include_unselected=False,
                json=True,
            )
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["source_job_id"] == "xtb-job-2"
    assert payload["candidate_count"] == 1
    assert payload["candidates"][0]["artifact_path"] == "/tmp/candidate.xyz"


def test_cmd_crest_inspect_text_mode_formats_retained_paths(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    contract = _FakeSerializable(
        job_id="crest-job-2",
        mode="standard",
        status="completed",
        reason="done",
        job_dir="/tmp/crest-job-2",
        latest_known_path="/tmp/crest-job-2",
        organized_output_dir="/tmp/crest-organized",
        molecule_key="mol-2",
        selected_input_xyz="/tmp/input.xyz",
        retained_conformer_count=2,
        retained_conformer_paths=["/tmp/conf-1.xyz", "/tmp/conf-2.xyz"],
    )
    monkeypatch.setattr(cli, "load_crest_artifact_contract", lambda **kwargs: contract)

    assert cli.cmd_crest_inspect(SimpleNamespace(crest_index_root="/tmp/crest", target="crest-job-2", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "job_id: crest-job-2" in stdout
    assert "retained_conformer_count: 2" in stdout
    assert "retained_conformer_paths: ['/tmp/conf-1.xyz', '/tmp/conf-2.xyz']" in stdout


@pytest.mark.parametrize(
    ("builder_name", "command", "args", "payload", "expected_texts"),
    [
        (
            "build_reaction_ts_search_plan_from_target",
            cli.cmd_workflow_reaction_ts_search,
            {
                "xtb_index_root": "/tmp/xtb",
                "target": "xtb-job-1",
                "max_orca_stages": 2,
                "include_unselected": False,
                "workspace_root": "/tmp/workflows",
                "charge": 0,
                "multiplicity": 1,
                "max_cores": 8,
                "max_memory_gb": 16,
                "orca_route_line": "! Opt",
                "priority": 5,
            },
            _plan_payload("reaction_ts_search"),
            [
                "workflow_id: wf_reaction_ts_search",
                "source_job_id: source_1",
                "reaction_dir=/tmp/reaction_dir",
                "enqueue_command=python -m chemstack.orca.cli run-dir /tmp/reaction_dir",
                "suggested_command=python -m chemstack.orca.cli run-dir /tmp/fallback.xyz",
            ],
        ),
        (
            "build_conformer_screening_plan_from_target",
            cli.cmd_workflow_conformer_screening,
            {
                "crest_index_root": "/tmp/crest",
                "target": "crest-job-2",
                "max_orca_stages": 2,
                "workspace_root": "/tmp/workflows",
                "charge": 0,
                "multiplicity": 1,
                "max_cores": 8,
                "max_memory_gb": 16,
                "orca_route_line": "! Opt",
                "priority": 5,
            },
            _plan_payload("conformer_screening"),
            [
                "workflow_id: wf_conformer_screening",
                "source_job_id: source_1",
                "reaction_dir=/tmp/reaction_dir",
            ],
        ),
        (
            "build_conformer_screening_plan_from_target",
            cli.cmd_workflow_conformer_screening,
            {
                "crest_index_root": "/tmp/crest",
                "target": "crest-job-nci",
                "max_orca_stages": 2,
                "workspace_root": "/tmp/workflows",
                "charge": 0,
                "multiplicity": 1,
                "max_cores": 8,
                "max_memory_gb": 16,
                "orca_route_line": "! Opt",
                "priority": 5,
            },
            _plan_payload("conformer_screening"),
            [
                "workflow_id: wf_conformer_screening",
                "source_job_id: source_1",
                "reaction_dir=/tmp/reaction_dir",
            ],
        ),
    ],
)
def test_workflow_plan_commands_render_text_and_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    builder_name: str,
    command: Any,
    args: dict[str, Any],
    payload: dict[str, Any],
    expected_texts: list[str],
) -> None:
    monkeypatch.setattr(cli, builder_name, lambda **kwargs: payload)

    assert command(SimpleNamespace(json=False, **args)) == 0
    stdout = capsys.readouterr().out
    for expected in expected_texts:
        assert expected in stdout

    assert command(SimpleNamespace(json=True, **args)) == 0
    json_payload = json.loads(capsys.readouterr().out)
    assert json_payload["workflow_id"] == payload["workflow_id"]
    assert json_payload["template_name"] == payload["template_name"]


@pytest.mark.parametrize(
    ("factory_name", "command", "args", "payload"),
    [
        (
            "create_reaction_workflow",
            cli.cmd_workflow_create_reaction_ts_search,
            {
                "reactant_xyz": "/tmp/reactant.xyz",
                "product_xyz": "/tmp/product.xyz",
                "workflow_root": "/tmp/workflows",
                "crest_mode": "standard",
                "priority": 5,
                "max_cores": 8,
                "max_memory_gb": 16,
                "max_crest_candidates": 2,
                "max_xtb_stages": 2,
                "max_orca_stages": 2,
                "orca_route_line": "! Opt",
                "charge": 0,
                "multiplicity": 1,
            },
            _create_payload("reaction_ts_search"),
        ),
        (
            "create_conformer_screening_workflow",
            cli.cmd_workflow_create_conformer_screening,
            {
                "input_xyz": "/tmp/input.xyz",
                "workflow_root": "/tmp/workflows",
                "crest_mode": "standard",
                "priority": 5,
                "max_cores": 8,
                "max_memory_gb": 16,
                "max_orca_stages": 2,
                "orca_route_line": "! Opt",
                "charge": 0,
                "multiplicity": 1,
            },
            _create_payload("conformer_screening"),
        ),
        (
            "create_conformer_screening_workflow",
            cli.cmd_workflow_create_conformer_screening,
            {
                "input_xyz": "/tmp/input.xyz",
                "workflow_root": "/tmp/workflows",
                "crest_mode": "nci",
                "priority": 5,
                "max_cores": 8,
                "max_memory_gb": 16,
                "max_orca_stages": 2,
                "orca_route_line": "! Opt",
                "charge": 0,
                "multiplicity": 1,
            },
            _create_payload("conformer_screening"),
        ),
    ],
)
def test_workflow_create_commands_render_text_and_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    factory_name: str,
    command: Any,
    args: dict[str, Any],
    payload: dict[str, Any],
) -> None:
    monkeypatch.setattr(cli, factory_name, lambda **kwargs: payload)

    assert command(SimpleNamespace(json=False, **args)) == 0
    stdout = capsys.readouterr().out
    assert f"workflow_id: {payload['workflow_id']}" in stdout
    assert "workspace_dir: /tmp/workflows/wf_create" in stdout
    assert "stage_count: 2" in stdout

    assert command(SimpleNamespace(json=True, **args)) == 0
    json_payload = json.loads(capsys.readouterr().out)
    assert json_payload["workflow_id"] == payload["workflow_id"]


def test_cmd_run_dir_infers_reaction_workflow_from_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "reaction_job"
    workflow_dir.mkdir()
    (workflow_dir / "reactant.xyz").write_text("2\nreactant\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    (workflow_dir / "product.xyz").write_text("2\nproduct\nH 0 0 0\nH 0 0 0.80\n", encoding="utf-8")
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli, "_discover_workflow_root", lambda explicit: "/tmp/workflow_root")

    def fake_create_reaction_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _create_payload("reaction_ts_search")

    monkeypatch.setattr(cli, "create_reaction_workflow", fake_create_reaction_workflow)

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
    stdout = capsys.readouterr().out
    assert "workflow_id: wf_create_reaction_ts_search" in stdout
    assert captured == {
        "reactant_xyz": str((workflow_dir / "reactant.xyz").resolve()),
        "product_xyz": str((workflow_dir / "product.xyz").resolve()),
        "workflow_root": "/tmp/workflow_root",
        "crest_mode": "standard",
        "priority": 10,
        "max_cores": 8,
        "max_memory_gb": 32,
        "max_crest_candidates": 3,
        "max_xtb_stages": 3,
        "max_orca_stages": 3,
        "orca_route_line": "! r2scan-3c OptTS Freq TightSCF",
        "charge": 0,
        "multiplicity": 1,
    }


def test_cmd_run_dir_reads_manifest_for_conformer_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "conformer_job"
    workflow_dir.mkdir()
    (workflow_dir / "input.xyz").write_text("2\nmol\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    (workflow_dir / "chemstack.flow.yaml").write_text(
        "\n".join(
            [
                "workflow_type: conformer_screening",
                "workflow_root: /tmp/from_manifest",
                "crest_mode: nci",
                "priority: 7",
                "resources:",
                "  max_cores: 12",
                "  max_memory_gb: 48",
                "max_orca_stages: 5",
                'orca_route_line: "! test"',
                "charge: -1",
                "multiplicity: 2",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli, "_discover_workflow_root", lambda explicit: str(Path(str(explicit)).resolve()) if explicit else None)

    def fake_create_conformer_screening_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _create_payload("conformer_screening")

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
        json=True,
    )

    assert cli.cmd_run_dir(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["workflow_id"] == "wf_create_conformer_screening"
    assert captured == {
        "input_xyz": str((workflow_dir / "input.xyz").resolve()),
        "workflow_root": str(Path("/tmp/from_manifest").resolve()),
        "crest_mode": "nci",
        "priority": 7,
        "max_cores": 12,
        "max_memory_gb": 48,
        "max_orca_stages": 5,
        "orca_route_line": "! test",
        "charge": -1,
        "multiplicity": 2,
    }


def test_cmd_run_dir_reports_ambiguous_layout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "ambiguous_job"
    workflow_dir.mkdir()
    (workflow_dir / "reactant.xyz").write_text("x", encoding="utf-8")
    (workflow_dir / "product.xyz").write_text("x", encoding="utf-8")
    (workflow_dir / "input.xyz").write_text("x", encoding="utf-8")

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

    assert cli.cmd_run_dir(args) == 1
    assert "Ambiguous workflow_dir" in capsys.readouterr().out


def test_cmd_run_dir_for_reaction_uses_nested_engine_sections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    workflow_dir = tmp_path / "reaction_job_nested"
    workflow_dir.mkdir()
    (workflow_dir / "reactant.xyz").write_text("2\nreactant\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    (workflow_dir / "product.xyz").write_text("2\nproduct\nH 0 0 0\nH 0 0 0.80\n", encoding="utf-8")
    (workflow_dir / "path.inp").write_text("$path\nnrun=3\n$end\n", encoding="utf-8")
    (workflow_dir / "chemstack.flow.yaml").write_text(
        "\n".join(
            [
                "workflow_type: reaction_ts_search",
                "resources:",
                "  max_cores: 20",
                "  max_memory_gb: 64",
                "crest:",
                "  mode: nci",
                "  speed: squick",
                "xtb:",
                "  gfn: 1",
                "  namespace: rxn_case",
                "  xcontrol_file: path.inp",
                "orca:",
                '  route_line: "! custom ts"',
                "  charge: -2",
                "  multiplicity: 3",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(cli, "_discover_workflow_root", lambda explicit: "/tmp/workflow_root")

    def fake_create_reaction_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _create_payload("reaction_ts_search")

    monkeypatch.setattr(cli, "create_reaction_workflow", fake_create_reaction_workflow)

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
    assert "workflow_id: wf_create_reaction_ts_search" in capsys.readouterr().out
    assert captured["crest_mode"] == "nci"
    assert captured["orca_route_line"] == "! custom ts"
    assert captured["charge"] == -2
    assert captured["multiplicity"] == 3
    assert captured["max_cores"] == 20
    assert captured["max_memory_gb"] == 64
    assert captured["crest_job_manifest"] == {"mode": "nci", "speed": "squick"}
    assert captured["xtb_job_manifest"] == {
        "gfn": 1,
        "namespace": "rxn_case",
        "xcontrol_file": str((workflow_dir / "path.inp").resolve()),
    }


def test_build_parser_parses_additional_workflow_commands() -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "workflow",
                "create-precomplex-build",
                "--reactant-xyz",
                "reactant.xyz",
                "--product-xyz",
                "product.xyz",
                "--workflow-root",
                "/tmp/workflows",
            ]
        )

    run_dir_args = parser.parse_args(
        [
            "run-dir",
            "/tmp/workflow_dir",
            "--workflow-type",
            "reaction_ts_search",
            "--crest-mode",
            "nci",
            "--json",
        ]
    )
    assert run_dir_args.command == "run-dir"
    assert run_dir_args.workflow_dir == "/tmp/workflow_dir"
    assert run_dir_args.workflow_type == "reaction_ts_search"
    assert run_dir_args.crest_mode == "nci"
    assert run_dir_args.json is True
    assert run_dir_args.func is cli.cmd_run_dir
