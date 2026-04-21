from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import registry as workflow_registry
from chemstack.flow.contracts import CrestArtifactContract, WorkflowStageInput
from chemstack.flow.workflows import conformer_screening


def _write_xyz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("2\ncomment\nH 0.0 0.0 0.0\nH 0.0 0.0 0.74\n", encoding="utf-8")


def _contract(tmp_path: Path, *, selected_input_xyz: str = "", retained_paths: tuple[str, ...] = ()) -> CrestArtifactContract:
    return CrestArtifactContract(
        job_id="crest_job_1",
        mode="standard",
        status="completed",
        reason="retained",
        job_dir=str(tmp_path / "crest_job"),
        latest_known_path=str(tmp_path / "crest_job"),
        molecule_key="mol_1",
        selected_input_xyz=selected_input_xyz,
        retained_conformer_count=len(retained_paths),
        retained_conformer_paths=retained_paths,
    )


def _candidate(path: Path, *, rank: int) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id="crest_job_1",
        source_job_type="crest_standard",
        reaction_key="mol_1",
        selected_input_xyz=str(path),
        rank=rank,
        kind="crest_conformer",
        artifact_path=str(path),
        selected=rank == 1,
        metadata={"rank": rank},
    )


def _record_load_call(
    load_calls: list[tuple[Path, str]],
    *,
    crest_index_root: str | Path,
    target: str,
    contract: CrestArtifactContract,
) -> CrestArtifactContract:
    load_calls.append((Path(crest_index_root), target))
    return contract


def _record_build_call(build_calls: list[dict[str, object]], current_contract: CrestArtifactContract, **kwargs: object) -> dict[str, object]:
    build_calls.append({"contract": current_contract, **kwargs})
    return {"workflow_id": "wf_target_no_sync", "metadata": {"workspace_dir": ""}}


def _record_atomic_call(atomic_calls: list[dict[str, Any]], path: Path, payload: object) -> None:
    atomic_calls.append({"path": path, "payload": payload})


def test_workflow_id_uses_conformer_screening_prefix(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    observed: list[str] = []

    def fake_timestamped_token(prefix: str) -> str:
        observed.append(prefix)
        return "wf_conformer_screening_token"

    monkeypatch.setattr(conformer_screening, "timestamped_token", fake_timestamped_token)

    workflow_id = conformer_screening._workflow_id(_contract(tmp_path))

    assert workflow_id == "wf_conformer_screening_token"
    assert observed == ["wf_conformer_screening"]


def test_build_conformer_screening_plan_without_workspace_root_skips_stage_materialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conformer_a = tmp_path / "inputs" / "conf_a.xyz"
    conformer_b = tmp_path / "inputs" / "conf_b.xyz"
    _write_xyz(conformer_a)
    _write_xyz(conformer_b)
    contract = _contract(
        tmp_path,
        selected_input_xyz=str(conformer_a),
        retained_paths=(str(conformer_a), str(conformer_b)),
    )
    candidates = (_candidate(conformer_a, rank=1), _candidate(conformer_b, rank=2))

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda contract: "wf_conformer_no_workspace")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T08:00:00+00:00")
    monkeypatch.setattr(conformer_screening, "select_crest_downstream_inputs", lambda contract, policy: candidates)
    monkeypatch.setattr(
        conformer_screening,
        "build_materialized_orca_stage",
        lambda **kwargs: pytest.fail("workspace_root=None should not materialize ORCA stages"),
    )
    monkeypatch.setattr(
        conformer_screening,
        "atomic_write_json",
        lambda *args, **kwargs: pytest.fail("workspace_root=None should not write workflow.json"),
    )

    payload = conformer_screening.build_conformer_screening_plan(
        contract,
        workspace_root=None,
        max_orca_stages=2,
    )

    assert payload["workflow_id"] == "wf_conformer_no_workspace"
    assert payload["stages"] == []
    assert payload["metadata"]["workspace_dir"] == ""
    assert payload["metadata"]["request"]["source_artifacts"] == []


def test_build_conformer_screening_plan_with_no_candidates_writes_empty_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conformer = tmp_path / "inputs" / "selected.xyz"
    _write_xyz(conformer)
    contract = _contract(tmp_path, selected_input_xyz=str(conformer), retained_paths=(str(conformer),))

    monkeypatch.setattr(conformer_screening, "_workflow_id", lambda contract: "wf_conformer_empty")
    monkeypatch.setattr(conformer_screening, "now_utc_iso", lambda: "2026-04-19T08:05:00+00:00")
    monkeypatch.setattr(conformer_screening, "select_crest_downstream_inputs", lambda contract, policy: ())

    payload = conformer_screening.build_conformer_screening_plan(
        contract,
        workspace_root=tmp_path,
        max_orca_stages=4,
    )

    workspace_dir = tmp_path / "workflows" / "wf_conformer_empty"
    workflow_json = workspace_dir / "workflow.json"

    assert payload["stages"] == []
    assert payload["metadata"]["request"]["source_artifacts"] == []
    assert payload["metadata"]["workspace_dir"] == str(workspace_dir)
    assert (workspace_dir / "stage_02_orca").is_dir()
    assert workflow_json.exists()
    assert json.loads(workflow_json.read_text(encoding="utf-8")) == payload


def test_build_conformer_screening_plan_from_target_without_workspace_root_forwards_arguments(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    crest_index_root = tmp_path / "crest_index"
    contract = _contract(tmp_path)
    load_calls: list[tuple[Path, str]] = []
    build_calls: list[dict[str, object]] = []
    sync_calls: list[tuple[Path, Path, dict[str, object]]] = []

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

    def fake_build(
        received_contract: CrestArtifactContract,
        *,
        max_orca_stages: int,
        workspace_root: str | Path | None,
        charge: int,
        multiplicity: int,
        max_cores: int,
        max_memory_gb: int,
        orca_route_line: str,
        priority: int,
    ) -> dict[str, object]:
        build_calls.append(
            {
                "contract": received_contract,
                "max_orca_stages": max_orca_stages,
                "workspace_root": workspace_root,
                "charge": charge,
                "multiplicity": multiplicity,
                "max_cores": max_cores,
                "max_memory_gb": max_memory_gb,
                "orca_route_line": orca_route_line,
                "priority": priority,
            }
        )
        return {"workflow_id": "wf_conformer_target", "metadata": {"workspace_dir": ""}}

    monkeypatch.setattr(conformer_screening, "build_conformer_screening_plan", fake_build)
    monkeypatch.setattr(
        workflow_registry,
        "sync_workflow_registry",
        lambda workflow_root, workspace_dir, payload: sync_calls.append(
            (Path(workflow_root), Path(workspace_dir), payload)
        ),
    )

    payload = conformer_screening.build_conformer_screening_plan_from_target(
        crest_index_root=crest_index_root,
        target="crest_job_1",
        workspace_root=None,
        max_orca_stages=5,
        charge=-1,
        multiplicity=2,
        max_cores=12,
        max_memory_gb=48,
        orca_route_line="! PBEh-3c Opt",
        priority=17,
    )

    assert payload == {"workflow_id": "wf_conformer_target", "metadata": {"workspace_dir": ""}}
    assert load_calls == [(crest_index_root, "crest_job_1")]
    assert build_calls == [
        {
            "contract": contract,
            "max_orca_stages": 5,
            "workspace_root": None,
            "charge": -1,
            "multiplicity": 2,
            "max_cores": 12,
            "max_memory_gb": 48,
            "orca_route_line": "! PBEh-3c Opt",
            "priority": 17,
        }
    ]
    assert sync_calls == []


def test_build_conformer_screening_plan_from_target_syncs_registry_when_workspace_root_present(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_root = tmp_path / "workspace_root"
    workspace_dir = workspace_root / "workflows" / "wf_conformer_sync"
    contract = _contract(tmp_path)
    build_calls: list[dict[str, object]] = []
    sync_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        conformer_screening,
        "load_crest_artifact_contract",
        lambda *, crest_index_root, target: contract,
    )

    def fake_build(
        received_contract: CrestArtifactContract,
        *,
        max_orca_stages: int,
        workspace_root: str | Path | None,
        charge: int,
        multiplicity: int,
        max_cores: int,
        max_memory_gb: int,
        orca_route_line: str,
        priority: int,
    ) -> dict[str, object]:
        build_calls.append(
            {
                "contract": received_contract,
                "max_orca_stages": max_orca_stages,
                "workspace_root": workspace_root,
                "charge": charge,
                "multiplicity": multiplicity,
                "max_cores": max_cores,
                "max_memory_gb": max_memory_gb,
                "orca_route_line": orca_route_line,
                "priority": priority,
            }
        )
        return {"workflow_id": "wf_conformer_sync", "metadata": {"workspace_dir": str(workspace_dir)}}

    monkeypatch.setattr(conformer_screening, "build_conformer_screening_plan", fake_build)
    monkeypatch.setattr(
        workflow_registry,
        "sync_workflow_registry",
        lambda workflow_root, resolved_workspace_dir, payload: sync_calls.append(
            {
                "workflow_root": Path(workflow_root),
                "workspace_dir": Path(resolved_workspace_dir),
                "payload": payload,
            }
        ),
    )

    payload = conformer_screening.build_conformer_screening_plan_from_target(
        crest_index_root=tmp_path / "crest_index",
        target="crest_job_sync",
        workspace_root=workspace_root,
        max_orca_stages=3,
        charge=1,
        multiplicity=1,
        max_cores=6,
        max_memory_gb=24,
        orca_route_line="r2scan-3c Opt TightSCF",
        priority=9,
    )

    assert payload == {"workflow_id": "wf_conformer_sync", "metadata": {"workspace_dir": str(workspace_dir)}}
    assert build_calls == [
        {
            "contract": contract,
            "max_orca_stages": 3,
            "workspace_root": workspace_root,
            "charge": 1,
            "multiplicity": 1,
            "max_cores": 6,
            "max_memory_gb": 24,
            "orca_route_line": "r2scan-3c Opt TightSCF",
            "priority": 9,
        }
    ]
    assert sync_calls == [
        {
            "workflow_root": workspace_root.resolve(),
            "workspace_dir": workspace_dir.resolve(),
            "payload": payload,
        }
    ]
