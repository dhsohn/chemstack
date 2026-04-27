from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.contracts import CrestArtifactContract, WorkflowStageInput
from chemstack.flow.workflows import conformer_screening


def _contract(tmp_path: Path) -> CrestArtifactContract:
    input_xyz = tmp_path / "inputs" / "crest_input.xyz"
    input_xyz.parent.mkdir(parents=True, exist_ok=True)
    input_xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.7\n", encoding="utf-8")
    return CrestArtifactContract(
        job_id="crest_job_edge",
        mode="standard",
        status="completed",
        reason="retained",
        job_dir=str(tmp_path / "crest_job"),
        latest_known_path=str(tmp_path / "crest_job"),
        molecule_key="mol_edge",
        selected_input_xyz=str(input_xyz),
        retained_conformer_count=1,
        retained_conformer_paths=(str(input_xyz),),
    )


def _candidate(path: Path) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id="crest_job_edge",
        source_job_type="crest_standard",
        reaction_key="mol_edge",
        selected_input_xyz=str(path),
        rank=1,
        kind="crest_conformer",
        artifact_path=str(path),
        selected=True,
        metadata={"rank": 1},
    )


def _unexpected_materialization(build_calls: list[dict[str, Any]], **kwargs: Any) -> None:
    build_calls.append(kwargs)
    pytest.fail("materialization should not run without workspace")


def _record_path_call(atomic_calls: list[Path], path: Path, payload: object) -> None:
    atomic_calls.append(path)


def _record_payload_call(atomic_calls: list[dict[str, Any]], path: Path, payload: object) -> None:
    atomic_calls.append({"path": path, "payload": payload})


def _record_load_call(load_calls: list[dict[str, Any]], *, crest_index_root: object, target: object, contract: CrestArtifactContract) -> CrestArtifactContract:
    load_calls.append({"crest_index_root": crest_index_root, "target": target})
    return contract


def _record_build_call(
    build_calls: list[dict[str, Any]],
    current_contract: CrestArtifactContract,
    **kwargs: Any,
) -> dict[str, object]:
    build_calls.append({"contract": current_contract, **kwargs})
    return {"workflow_id": "wf_target_no_sync", "metadata": {"workspace_dir": ""}}


def test_workflow_id_delegates_to_timestamped_token(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    contract = _contract(tmp_path)
    monkeypatch.setattr(conformer_screening, "timestamped_token", lambda prefix: f"{prefix}_custom")
    assert conformer_screening._workflow_id(contract) == "wf_conformer_screening_custom"


def test_build_conformer_screening_plan_without_workspace_skips_materialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = _contract(tmp_path)
    conformer = Path(contract.selected_input_xyz)
    build_calls: list[dict[str, Any]] = []
    atomic_calls: list[Path] = []

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda current_contract: "wf_no_workspace")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T02:00:00+00:00")
    monkeypatch.setattr(conformer_screening, "select_crest_downstream_inputs", lambda current_contract, policy: (_candidate(conformer),))
    monkeypatch.setattr(
        conformer_screening,
        "build_materialized_orca_stage",
        lambda **kwargs: _unexpected_materialization(build_calls, **kwargs),
    )
    monkeypatch.setattr(
        conformer_screening,
        "atomic_write_json",
        lambda path, payload, ensure_ascii, indent: _record_path_call(atomic_calls, path, payload),
    )

    payload = conformer_screening.build_conformer_screening_plan(contract, workspace_root=None, max_orca_stages=1)

    assert payload["workflow_id"] == "wf_no_workspace"
    assert payload["stages"] == []
    assert payload["metadata"]["workspace_dir"] == ""
    assert payload["metadata"]["request"]["source_artifacts"] == []
    assert build_calls == []
    assert atomic_calls == []


def test_build_conformer_screening_plan_with_no_candidates_still_writes_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = _contract(tmp_path)
    atomic_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda current_contract: "wf_no_candidates")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T02:10:00+00:00")
    monkeypatch.setattr(conformer_screening, "select_crest_downstream_inputs", lambda current_contract, policy: ())
    monkeypatch.setattr(
        conformer_screening,
        "atomic_write_json",
        lambda path, payload, ensure_ascii, indent: _record_payload_call(atomic_calls, path, payload),
    )

    payload = conformer_screening.build_conformer_screening_plan(contract, workspace_root=tmp_path, max_orca_stages=0)

    expected_workspace = tmp_path / "wf_no_candidates"
    assert payload["workflow_id"] == "wf_no_candidates"
    assert payload["stages"] == []
    assert payload["metadata"]["workspace_dir"] == str(expected_workspace)
    assert payload["metadata"]["request"]["source_artifacts"] == []
    assert atomic_calls == [{"path": expected_workspace / "workflow.json", "payload": payload}]
    assert (expected_workspace / "02_orca").is_dir()


def test_build_conformer_screening_plan_from_target_forwards_args_without_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = _contract(tmp_path)
    load_calls: list[dict[str, Any]] = []
    build_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        conformer_screening,
        "load_crest_artifact_contract",
        lambda *, crest_index_root, target: _record_load_call(
            load_calls,
            crest_index_root=crest_index_root,
            target=target,
            contract=contract,
        ),
    )
    monkeypatch.setattr(
        conformer_screening,
        "build_conformer_screening_plan",
        lambda current_contract, **kwargs: _record_build_call(build_calls, current_contract, **kwargs),
    )

    payload = conformer_screening.build_conformer_screening_plan_from_target(
        crest_index_root="/tmp/crest_index",
        target="crest_job_edge",
        workspace_root=None,
        max_orca_stages=4,
        charge=-1,
        multiplicity=2,
        max_cores=6,
        max_memory_gb=12,
        orca_route_line="! Opt",
        priority=8,
    )

    assert payload == {"workflow_id": "wf_target_no_sync", "metadata": {"workspace_dir": ""}}
    assert load_calls == [{"crest_index_root": "/tmp/crest_index", "target": "crest_job_edge"}]
    assert build_calls == [
        {
            "contract": contract,
            "max_orca_stages": 4,
            "workspace_root": None,
            "charge": -1,
            "multiplicity": 2,
            "max_cores": 6,
            "max_memory_gb": 12,
            "orca_route_line": "! Opt",
            "priority": 8,
        }
    ]


def test_build_conformer_screening_plan_from_target_syncs_registry_when_workspace_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    contract = _contract(tmp_path)
    sync_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(conformer_screening, "load_crest_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(
        conformer_screening,
        "build_conformer_screening_plan",
        lambda current_contract, **kwargs: {
            "workflow_id": "wf_target_sync",
            "metadata": {"workspace_dir": str(tmp_path / "wf_target_sync")},
        },
    )
    import chemstack.flow.registry as workflow_registry

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
        target="crest_job_edge",
        workspace_root=tmp_path,
    )

    assert payload["workflow_id"] == "wf_target_sync"
    assert sync_calls == [
        {
            "workflow_root": tmp_path.resolve(),
            "workspace_dir": (tmp_path / "wf_target_sync").resolve(),
            "workflow_id": "wf_target_sync",
        }
    ]
