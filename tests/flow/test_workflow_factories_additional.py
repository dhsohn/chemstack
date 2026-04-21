from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import orchestration


def _write_xyz(path: Path, atoms: list[tuple[str, float, float, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [str(len(atoms)), "comment"]
    for symbol, x, y, z in atoms:
        lines.append(f"{symbol} {x:.6f} {y:.6f} {z:.6f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_new_crest_stage_builds_expected_payload_and_metadata() -> None:
    stage = orchestration._new_crest_stage(
        workflow_id="wf_crest_01",
        template_name="reaction_ts_search",
        stage_id="crest_reactant_01",
        source_path="/tmp/reactant.xyz",
        input_role="reactant",
        mode="nci",
        priority=7,
        max_cores=4,
        max_memory_gb=12,
    )

    task = stage["task"]
    assert stage["stage_kind"] == "crest_stage"
    assert stage["status"] == "planned"
    assert stage["input_artifacts"] == [
        {
            "kind": "input_xyz",
            "path": "/tmp/reactant.xyz",
            "selected": True,
            "metadata": {"input_role": "reactant"},
        }
    ]
    assert task["engine"] == "crest"
    assert task["task_kind"] == "conformer_search"
    assert task["resource_request"] == {"max_cores": 4, "max_memory_gb": 12}
    assert task["payload"]["template_name"] == "reaction_ts_search"
    assert task["payload"]["input_role"] == "reactant"
    assert task["payload"]["mode"] == "nci"
    assert task["enqueue_payload"]["priority"] == 7
    assert task["enqueue_payload"]["command_argv"][:4] == [
        "python",
        "-m",
        "chemstack.crest.cli",
        "--config",
    ]
    assert task["enqueue_payload"]["command_argv"][4:7] == [
        "<crest_auto_config>",
        "run-dir",
        "<job_dir>",
    ]
    assert task["metadata"] == {"input_role": "reactant", "mode": "nci"}
    assert stage["metadata"] == {"input_role": "reactant", "mode": "nci"}


def test_create_reaction_ts_search_workflow_rejects_mismatched_atom_order(tmp_path: Path) -> None:
    reactant_xyz = tmp_path / "reactant_bad.xyz"
    product_xyz = tmp_path / "product_bad.xyz"
    _write_xyz(reactant_xyz, [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    _write_xyz(product_xyz, [("O", 0.0, 0.0, 0.96), ("H", 0.0, 0.0, 0.0)])

    with pytest.raises(ValueError, match="identical reactant/product atom order"):
        orchestration.create_reaction_ts_search_workflow(
            reactant_xyz=str(reactant_xyz),
            product_xyz=str(product_xyz),
            workflow_root=tmp_path,
        )


def test_create_reaction_ts_search_workflow_rejects_invalid_crest_mode(tmp_path: Path) -> None:
    reactant_xyz = tmp_path / "reactant.xyz"
    product_xyz = tmp_path / "product.xyz"
    _write_xyz(reactant_xyz, [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    _write_xyz(product_xyz, [("H", 0.1, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])

    with pytest.raises(ValueError, match="crest_mode 'standard' or 'nci'"):
        orchestration.create_reaction_ts_search_workflow(
            reactant_xyz=str(reactant_xyz),
            product_xyz=str(product_xyz),
            workflow_root=tmp_path,
            crest_mode="weird",
        )


@pytest.mark.parametrize("crest_mode", ["standard", "nci"])
def test_create_reaction_ts_search_workflow_materializes_two_crest_stages(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crest_mode: str,
) -> None:
    reactant_xyz = tmp_path / "reactant.xyz"
    product_xyz = tmp_path / "product.xyz"
    _write_xyz(reactant_xyz, [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    _write_xyz(product_xyz, [("H", 0.1, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    sync_calls: list[str] = []

    monkeypatch.setattr(orchestration, "_workflow_id", lambda prefix: "wf_reaction_extra")
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T16:10:00+00:00")
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, payload: sync_calls.append(payload["workflow_id"]))

    payload = orchestration.create_reaction_ts_search_workflow(
        reactant_xyz=str(reactant_xyz),
        product_xyz=str(product_xyz),
        workflow_root=tmp_path,
        crest_mode=crest_mode,
        max_crest_candidates=2,
        max_xtb_stages=4,
        max_xtb_handoff_retries=3,
        max_orca_stages=2,
        orca_route_line="! custom ts route",
    )

    workspace_dir = tmp_path / "workflows" / "wf_reaction_extra"
    request = payload["metadata"]["request"]
    assert payload["template_name"] == "reaction_ts_search"
    assert [stage["stage_id"] for stage in payload["stages"]] == ["crest_reactant_01", "crest_product_01"]
    assert [stage["metadata"]["mode"] for stage in payload["stages"]] == [crest_mode, crest_mode]
    assert request["parameters"]["crest_mode"] == crest_mode
    assert request["parameters"]["max_xtb_stages"] == 4
    assert request["parameters"]["max_xtb_handoff_retries"] == 3
    assert request["parameters"]["max_orca_stages"] == 2
    assert request["parameters"]["orca_route_line"] == "! custom ts route"
    assert (workspace_dir / "workflow.json").exists()
    assert sync_calls == ["wf_reaction_extra"]


@pytest.mark.parametrize(
    ("workflow_id", "crest_mode"),
    [
        ("wf_conformer_standard_extra", "standard"),
        ("wf_conformer_nci_extra", "nci"),
    ],
)
def test_single_input_crest_workflow_factories_materialize_expected_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    workflow_id: str,
    crest_mode: str,
) -> None:
    factory = "create_conformer_screening_workflow"
    stage_id = "crest_conformer_01"
    template_name = "conformer_screening"
    input_role = "molecule"
    artifact_kind = "input_xyz"
    input_xyz = tmp_path / f"{template_name}.xyz"
    _write_xyz(input_xyz, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])
    sync_calls: list[str] = []

    monkeypatch.setattr(orchestration, "_workflow_id", lambda prefix: workflow_id)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T16:20:00+00:00")
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, payload: sync_calls.append(payload["workflow_id"]))

    payload = getattr(orchestration, factory)(
        input_xyz=str(input_xyz),
        workflow_root=tmp_path,
        crest_mode=crest_mode,
        max_orca_stages=2,
        orca_route_line="! custom route",
        charge=1,
        multiplicity=2,
    )

    workspace_dir = tmp_path / "workflows" / workflow_id
    request = payload["metadata"]["request"]
    stage = payload["stages"][0]
    assert payload["workflow_id"] == workflow_id
    assert payload["template_name"] == template_name
    assert request["template_name"] == template_name
    assert request["parameters"]["max_orca_stages"] == 2
    assert request["parameters"]["orca_route_line"] == "! custom route"
    assert request["source_artifacts"][0]["kind"] == artifact_kind
    assert stage["stage_id"] == stage_id
    assert stage["metadata"]["input_role"] == input_role
    assert stage["task"]["payload"]["mode"] == crest_mode
    assert (workspace_dir / "workflow.json").exists()
    assert sync_calls == [workflow_id]


def test_create_conformer_screening_nci_workflow_writes_expected_request_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    input_xyz = tmp_path / "complex.xyz"
    _write_xyz(input_xyz, [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    sync_calls: list[str] = []

    monkeypatch.setattr(orchestration, "_workflow_id", lambda prefix: "wf_conf_nci_extra")
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T16:30:00+00:00")
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, payload: sync_calls.append(payload["workflow_id"]))

    payload = orchestration.create_conformer_screening_workflow(
        input_xyz=str(input_xyz),
        workflow_root=tmp_path,
        crest_mode="nci",
        priority=13,
        max_cores=10,
        max_memory_gb=40,
        max_orca_stages=4,
        orca_route_line="! nci route",
        charge=-1,
        multiplicity=3,
    )

    assert payload["workflow_id"] == "wf_conf_nci_extra"
    assert [stage["stage_id"] for stage in payload["stages"]] == ["crest_conformer_01"]
    assert payload["stages"][0]["metadata"]["mode"] == "nci"
    assert payload["metadata"]["request"]["template_name"] == "conformer_screening"
    assert payload["metadata"]["request"]["source_artifacts"] == [
        {
            "kind": "input_xyz",
            "path": str((tmp_path / "workflows" / "wf_conf_nci_extra" / "inputs" / input_xyz.name).resolve()),
            "selected": True,
            "metadata": {},
        }
    ]
    assert payload["metadata"]["request"]["parameters"] == {
        "priority": 13,
        "max_cores": 10,
        "max_memory_gb": 40,
        "max_orca_stages": 4,
        "orca_route_line": "! nci route",
        "charge": -1,
        "multiplicity": 3,
        "crest_mode": "nci",
    }
    assert sync_calls == ["wf_conf_nci_extra"]


def test_workflow_factories_preserve_engine_manifest_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reactant_xyz = tmp_path / "reactant.yaml.xyz"
    product_xyz = tmp_path / "product.yaml.xyz"
    input_xyz = tmp_path / "single.yaml.xyz"
    _write_xyz(reactant_xyz, [("H", 0.0, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    _write_xyz(product_xyz, [("H", 0.1, 0.0, 0.0), ("O", 0.0, 0.0, 0.96)])
    _write_xyz(input_xyz, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])

    monkeypatch.setattr(orchestration, "_workflow_id", lambda prefix: f"{prefix}_with_manifest")
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T17:00:00+00:00")
    monkeypatch.setattr(orchestration, "sync_workflow_registry", lambda workflow_root, workspace_dir, payload: None)

    reaction_payload = orchestration.create_reaction_ts_search_workflow(
        reactant_xyz=str(reactant_xyz),
        product_xyz=str(product_xyz),
        workflow_root=tmp_path,
        crest_job_manifest={"speed": "squick", "solvent": "water"},
        xtb_job_manifest={"gfn": 1, "namespace": "rxn_a"},
    )
    request_params = reaction_payload["metadata"]["request"]["parameters"]
    assert request_params["crest_job_manifest"] == {"speed": "squick", "solvent": "water"}
    assert request_params["xtb_job_manifest"] == {"gfn": 1, "namespace": "rxn_a"}
    assert reaction_payload["stages"][0]["task"]["payload"]["job_manifest_overrides"] == {
        "speed": "squick",
        "solvent": "water",
    }

    conformer_payload = orchestration.create_conformer_screening_workflow(
        input_xyz=str(input_xyz),
        workflow_root=tmp_path,
        crest_job_manifest={"speed": "mquick", "gfn": "ff"},
    )
    conformer_params = conformer_payload["metadata"]["request"]["parameters"]
    assert conformer_params["crest_job_manifest"] == {"speed": "mquick", "gfn": "ff"}
    assert conformer_payload["stages"][0]["task"]["payload"]["job_manifest_overrides"] == {
        "speed": "mquick",
        "gfn": "ff",
    }
