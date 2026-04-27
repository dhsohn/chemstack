from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import registry as workflow_registry
from chemstack.flow.contracts import (
    CrestArtifactContract,
    WorkflowStageInput,
    XtbArtifactContract,
    XtbCandidateArtifact,
)
from chemstack.flow.workflows import (
    conformer_screening,
    reaction_ts_search,
)


def _write_xyz(path: Path, atoms: list[tuple[str, float, float, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [str(len(atoms)), "comment"]
    for symbol, x, y, z in atoms:
        rows.append(f"{symbol} {x:.6f} {y:.6f} {z:.6f}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _stage_input(
    path: Path,
    *,
    source_job_id: str,
    source_job_type: str,
    reaction_key: str,
    kind: str,
    rank: int,
) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id=source_job_id,
        source_job_type=source_job_type,
        reaction_key=reaction_key,
        selected_input_xyz=str(path),
        rank=rank,
        kind=kind,
        artifact_path=str(path),
        selected=rank == 1,
        metadata={"kind": kind, "rank": rank},
    )


def test_build_conformer_screening_plan_materializes_orca_stages(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conformer_a = tmp_path / "inputs" / "conf_a.xyz"
    conformer_b = tmp_path / "inputs" / "conf_b.xyz"
    _write_xyz(conformer_a, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])
    _write_xyz(conformer_b, [("H", 0.0, 0.0, 0.0), ("H", 0.1, 0.0, 0.74)])

    contract = CrestArtifactContract(
        job_id="crest_job_1",
        mode="standard",
        status="completed",
        reason="retained",
        job_dir=str(tmp_path / "crest_job"),
        latest_known_path=str(tmp_path / "crest_job"),
        molecule_key="mol_1",
        selected_input_xyz=str(conformer_a),
        retained_conformer_count=2,
        retained_conformer_paths=(str(conformer_a), str(conformer_b)),
    )
    candidates = (
        _stage_input(conformer_a, source_job_id="crest_job_1", source_job_type="crest_standard", reaction_key="mol_1", kind="crest_conformer", rank=1),
        _stage_input(conformer_b, source_job_id="crest_job_1", source_job_type="crest_standard", reaction_key="mol_1", kind="crest_conformer", rank=2),
    )

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda contract: "wf_conformer_screening_1")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T03:00:00+00:00")
    monkeypatch.setattr(conformer_screening, "select_crest_downstream_inputs", lambda contract, policy: candidates)

    payload = conformer_screening.build_conformer_screening_plan(
        contract,
        workspace_root=tmp_path,
        max_orca_stages=2,
        charge=1,
        multiplicity=2,
        max_cores=4,
        max_memory_gb=8,
        orca_route_line="r2scan-3c Opt",
        priority=7,
    )

    workspace_dir = tmp_path / "wf_conformer_screening_1"
    assert payload["workflow_id"] == "wf_conformer_screening_1"
    assert payload["metadata"]["workspace_dir"] == str(workspace_dir)
    assert payload["metadata"]["request"]["parameters"] == {
        "max_orca_stages": 2,
        "charge": 1,
        "multiplicity": 2,
        "max_cores": 4,
        "max_memory_gb": 8,
        "orca_route_line": "r2scan-3c Opt",
        "priority": 7,
    }
    assert len(payload["stages"]) == 2
    assert payload["stages"][0]["stage_id"] == "orca_conformer_01"
    assert payload["stages"][0]["input_artifacts"][0]["kind"] == "crest_conformer"
    assert Path(payload["stages"][0]["task"]["payload"]["selected_inp"]).exists()
    assert Path(payload["stages"][0]["task"]["payload"]["selected_input_xyz"]).exists()
    assert (workspace_dir / "workflow.json").exists()
    assert "! r2scan-3c Opt" in Path(payload["stages"][0]["task"]["payload"]["selected_inp"]).read_text(encoding="utf-8")


def test_build_conformer_screening_plan_from_target_accepts_nci_contracts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conformer = tmp_path / "inputs" / "supramol.xyz"
    _write_xyz(conformer, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])

    contract = CrestArtifactContract(
        job_id="crest_job_2",
        mode="nci",
        status="completed",
        reason="retained",
        job_dir=str(tmp_path / "crest_job_2"),
        latest_known_path=str(tmp_path / "crest_job_2"),
        molecule_key="mol_2",
        selected_input_xyz=str(conformer),
        retained_conformer_count=1,
        retained_conformer_paths=(str(conformer),),
    )
    sync_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda contract: "wf_conformer_screening_nci")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T03:10:00+00:00")
    monkeypatch.setattr(
        conformer_screening,
        "load_crest_artifact_contract",
        lambda *, crest_index_root, target: contract,
    )
    monkeypatch.setattr(
        conformer_screening,
        "select_crest_downstream_inputs",
        lambda contract, policy: (
            _stage_input(conformer, source_job_id="crest_job_2", source_job_type="crest_nci", reaction_key="mol_2", kind="crest_conformer", rank=1),
        ),
    )
    monkeypatch.setattr(
        workflow_registry,
        "sync_workflow_registry",
        lambda workflow_root, workspace_dir, payload: sync_calls.append(
            {
                "workflow_root": Path(workflow_root).resolve(),
                "workspace_dir": Path(workspace_dir).resolve(),
                "workflow_id": payload["workflow_id"],
            }
        ),
    )

    payload = conformer_screening.build_conformer_screening_plan_from_target(
        crest_index_root="/tmp/crest_index",
        target="crest_job_2",
        workspace_root=tmp_path,
        max_orca_stages=1,
        priority=9,
    )

    workspace_dir = tmp_path / "wf_conformer_screening_nci"
    assert payload["workflow_id"] == "wf_conformer_screening_nci"
    assert payload["stages"][0]["stage_id"] == "orca_conformer_01"
    assert sync_calls == [
        {
            "workflow_root": tmp_path.resolve(),
            "workspace_dir": workspace_dir.resolve(),
            "workflow_id": "wf_conformer_screening_nci",
        }
    ]
    assert (workspace_dir / "workflow.json").exists()

def test_build_reaction_ts_search_plan_raises_clear_error_when_ts_guess_is_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    candidate_xyz = tmp_path / "xtb_candidate.xyz"
    _write_xyz(candidate_xyz, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])

    contract = XtbArtifactContract(
        job_id="xtb_job_1",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir=str(tmp_path / "xtb_job"),
        latest_known_path=str(tmp_path / "xtb_job"),
        reaction_key="rxn_xtb",
        selected_input_xyz=str(candidate_xyz),
        selected_candidate_paths=(str(candidate_xyz),),
        candidate_details=(
            XtbCandidateArtifact(rank=1, kind="optimized_geometry", path=str(candidate_xyz), selected=True),
        ),
    )

    monkeypatch.setattr(reaction_ts_search, "select_xtb_downstream_inputs", lambda contract, policy, require_geometry: ())

    with pytest.raises(ValueError, match="did not produce a ts_guess candidate"):
        reaction_ts_search.build_reaction_ts_search_plan(contract)


def test_build_reaction_ts_search_plan_materializes_orca_stage_and_syncs_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    ts_guess_xyz = tmp_path / "inputs" / "xtbpath_ts.xyz"
    _write_xyz(ts_guess_xyz, [("H", 0.0, 0.0, 0.0), ("H", 0.0, 0.0, 0.74)])
    contract = XtbArtifactContract(
        job_id="xtb_job_2",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir=str(tmp_path / "xtb_job_2"),
        latest_known_path=str(tmp_path / "xtb_job_2"),
        reaction_key="rxn_ts",
        selected_input_xyz=str(ts_guess_xyz),
        selected_candidate_paths=(str(ts_guess_xyz),),
        candidate_details=(
            XtbCandidateArtifact(rank=1, kind="ts_guess", path=str(ts_guess_xyz), selected=True),
        ),
    )
    candidate = WorkflowStageInput(
        source_job_id="xtb_job_2",
        source_job_type="path_search",
        reaction_key="rxn_ts",
        selected_input_xyz=str(ts_guess_xyz),
        rank=1,
        kind="ts_guess",
        artifact_path=str(ts_guess_xyz),
        selected=True,
        metadata={"origin": "xtb"},
    )
    sync_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(reaction_ts_search, "_workflow_id", lambda contract: "wf_reaction_ts_1")
    monkeypatch.setattr(reaction_ts_search, "now_utc_iso", lambda: "2026-04-19T03:40:00+00:00")
    monkeypatch.setattr(
        reaction_ts_search,
        "select_xtb_downstream_inputs",
        lambda contract, policy, require_geometry: (candidate,),
    )
    monkeypatch.setattr(
        reaction_ts_search,
        "sync_workflow_registry",
        lambda workflow_root, workspace_dir, payload: sync_calls.append(
            {
                "workflow_root": Path(workflow_root).resolve(),
                "workspace_dir": Path(workspace_dir).resolve(),
                "workflow_id": payload["workflow_id"],
            }
        ),
    )

    payload = reaction_ts_search.build_reaction_ts_search_plan(
        contract,
        workspace_root=tmp_path,
        max_orca_stages=1,
        charge=0,
        multiplicity=1,
        max_cores=4,
        max_memory_gb=8,
        orca_route_line="r2scan-3c OptTS Freq TightSCF",
        priority=15,
    )

    workspace_dir = tmp_path / "wf_reaction_ts_1"
    stage = payload["stages"][0]
    stage_dir = workspace_dir / "03_orca" / "01_ts_guess"
    assert payload["workflow_id"] == "wf_reaction_ts_1"
    assert payload["metadata"]["request"]["parameters"]["orca_route_line"] == "! r2scan-3c OptTS Freq TightSCF"
    assert len(payload["metadata"]["orca_stage_payloads"]) == 1
    assert len(payload["metadata"]["orca_stage_enqueue_payloads"]) == 1
    assert stage["stage_id"] == "orca_optts_freq_01"
    assert stage["task"]["payload"]["reaction_dir"]
    assert Path(stage["task"]["payload"]["selected_inp"]).exists()
    assert Path(stage["task"]["payload"]["selected_input_xyz"]).exists()
    assert (stage_dir / "enqueue_payload.json").exists()
    assert (workspace_dir / "workflow.json").exists()
    assert sync_calls == [
        {
            "workflow_root": tmp_path.resolve(),
            "workspace_dir": workspace_dir.resolve(),
            "workflow_id": "wf_reaction_ts_1",
        }
    ]


def test_build_reaction_ts_search_plan_from_target_loads_contract_and_forwards(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = XtbArtifactContract(
        job_id="xtb_job_3",
        job_type="path_search",
        status="completed",
        reason="ok",
        job_dir="/tmp/xtb_job_3",
        latest_known_path="/tmp/xtb_job_3",
        reaction_key="rxn_from_target",
    )
    captured: dict[str, Any] = {}

    def fake_load_xtb_artifact_contract(*, xtb_index_root: str, target: str) -> XtbArtifactContract:
        captured["load"] = {"xtb_index_root": xtb_index_root, "target": target}
        return contract

    monkeypatch.setattr(reaction_ts_search, "load_xtb_artifact_contract", fake_load_xtb_artifact_contract)

    def fake_build_reaction_ts_search_plan(contract_arg: XtbArtifactContract, **kwargs: Any) -> dict[str, Any]:
        captured["build"] = {"contract": contract_arg, **kwargs}
        return {"workflow_id": "wf_forward"}

    monkeypatch.setattr(reaction_ts_search, "build_reaction_ts_search_plan", fake_build_reaction_ts_search_plan)

    result = reaction_ts_search.build_reaction_ts_search_plan_from_target(
        xtb_index_root="/tmp/xtb_index",
        target="xtb_job_3",
        max_orca_stages=2,
        selected_only=False,
        workspace_root="/tmp/workspace_root",
        charge=-1,
        multiplicity=2,
        max_cores=16,
        max_memory_gb=32,
        orca_route_line="! custom route",
        priority=21,
    )

    assert captured["load"] == {"xtb_index_root": "/tmp/xtb_index", "target": "xtb_job_3"}
    assert captured["build"] == {
        "contract": contract,
        "max_orca_stages": 2,
        "selected_only": False,
        "workspace_root": "/tmp/workspace_root",
        "charge": -1,
        "multiplicity": 2,
        "max_cores": 16,
        "max_memory_gb": 32,
        "orca_route_line": "! custom route",
        "priority": 21,
    }
    assert result == {"workflow_id": "wf_forward"}
