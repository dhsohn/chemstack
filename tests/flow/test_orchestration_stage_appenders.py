from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import orchestration
from chemstack.flow.contracts import WorkflowStageInput


def _candidate(
    path: str,
    *,
    source_job_id: str,
    source_job_type: str,
    reaction_key: str,
    rank: int,
    kind: str,
    selected_input_xyz: str | None = None,
    selected: bool = True,
    score: float = 0.0,
    metadata: dict[str, Any] | None = None,
) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id=source_job_id,
        source_job_type=source_job_type,
        reaction_key=reaction_key,
        selected_input_xyz=selected_input_xyz or path,
        rank=rank,
        kind=kind,
        artifact_path=path,
        selected=selected,
        score=score,
        metadata=metadata or {},
    )


def _orca_stage_result(**kwargs: Any) -> SimpleNamespace:
    candidate = kwargs["candidate"]
    stage = {
        "stage_id": kwargs["stage_id"],
        "status": "planned",
        "metadata": {},
        "input_artifacts": [
            {
                "kind": kwargs["input_artifact_kind"],
                "path": candidate.artifact_path,
                "selected": candidate.selected,
            }
        ],
        "task": {
            "engine": "orca",
            "task_kind": kwargs["task_kind"],
            "status": "planned",
            "payload": {"reaction_dir": ""},
            "metadata": {"source_candidate_path": candidate.artifact_path},
        },
    }
    return SimpleNamespace(to_dict=lambda: stage)


def test_append_reaction_xtb_stages_creates_full_cartesian_product(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_01",
        "stages": [
            {"stage_id": "crest_reactant", "status": "completed", "metadata": {"input_role": "reactant"}, "task": {"engine": "crest"}},
            {"stage_id": "crest_product", "status": "completed", "metadata": {"input_role": "product"}, "task": {"engine": "crest"}},
        ],
        "metadata": {
            "request": {
                "parameters": {
                    "max_crest_candidates": 2,
                    "max_xtb_stages": 3,
                    "max_xtb_handoff_retries": 4,
                }
            }
        },
    }
    reactant_inputs = [
        _candidate("/tmp/reactant_a.xyz", source_job_id="crest_r", source_job_type="crest", reaction_key="rxn_r_a", rank=1, kind="conformer"),
        _candidate("/tmp/reactant_b.xyz", source_job_id="crest_r", source_job_type="crest", reaction_key="rxn_r_b", rank=2, kind="conformer"),
    ]
    product_inputs = [
        _candidate("/tmp/product_a.xyz", source_job_id="crest_p", source_job_type="crest", reaction_key="rxn_p_a", rank=1, kind="conformer"),
        _candidate("/tmp/product_b.xyz", source_job_id="crest_p", source_job_type="crest", reaction_key="rxn_p_b", rank=2, kind="conformer"),
    ]

    monkeypatch.setattr(
        orchestration,
        "_completed_crest_stage",
        lambda stage, **kwargs: "reactant_contract" if stage["metadata"]["input_role"] == "reactant" else "product_contract",
    )
    monkeypatch.setattr(
        orchestration,
        "select_crest_downstream_inputs",
        lambda contract, policy: reactant_inputs if contract == "reactant_contract" else product_inputs,
    )

    created = orchestration._append_reaction_xtb_stages(
        payload,
        workspace_dir=tmp_path,
        crest_auto_config="/tmp/crest.yaml",
    )

    xtb_stages = [stage for stage in payload["stages"] if stage.get("task", {}).get("engine") == "xtb"]
    assert created is True
    assert [stage["stage_id"] for stage in xtb_stages] == [
        "xtb_path_search_01",
        "xtb_path_search_02",
        "xtb_path_search_03",
        "xtb_path_search_04",
    ]
    assert all(stage["task"]["payload"]["max_handoff_retries"] == 4 for stage in xtb_stages)


def test_append_reaction_orca_stages_sets_xtb_handoff_workflow_error_when_no_candidate_survives(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_02",
        "metadata": {"request": {"parameters": {"max_orca_stages": 2}}},
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "metadata": {},
                "task": {
                    "engine": "xtb",
                    "payload": {"job_dir": "/tmp/xtb_job_01"},
                },
            }
        ],
    }
    contract = SimpleNamespace(job_id="xtb_job_01", job_type="path_search", candidate_details=())

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / ("xtb" if "xtb" in str(path) else "orca"))
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda *args, **kwargs: ())
    monkeypatch.setattr(
        orchestration,
        "_reaction_ts_guess_error",
        lambda current_contract: {
            "reason": "xtb_ts_guess_missing",
            "message": "missing ts guess",
        },
    )

    created = orchestration._append_reaction_orca_stages(
        payload,
        workspace_dir=tmp_path,
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    xtb_stage = payload["stages"][0]
    assert created is False
    assert xtb_stage["metadata"]["reaction_handoff_status"] == "failed"
    assert xtb_stage["metadata"]["reaction_handoff_reason"] == "xtb_ts_guess_missing"
    assert payload["metadata"]["workflow_error"] == {
        "status": "failed",
        "scope": "reaction_ts_search_xtb_handoff",
        "stage_id": "xtb_path_search_01",
        "job_id": "xtb_job_01",
        "reason": "xtb_ts_guess_missing",
        "message": "missing ts guess",
    }


def test_append_reaction_orca_stages_appends_unattempted_candidate_without_mutating_failed_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_candidate = _candidate(
        "/tmp/candidate_01.xyz",
        source_job_id="xtb_job_02",
        source_job_type="path_search",
        reaction_key="rxn_02",
        rank=1,
        kind="ts_guess",
        score=-10.0,
    )
    second_candidate = _candidate(
        "/tmp/candidate_02.xyz",
        source_job_id="xtb_job_02",
        source_job_type="path_search",
        reaction_key="rxn_02",
        rank=2,
        kind="ts_guess",
        score=-9.0,
    )
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_03",
        "metadata": {
            "workflow_error": {"scope": "reaction_ts_search_orca_candidate_exhausted"},
            "request": {
                "parameters": {
                    "max_orca_stages": 3,
                    "orca_route_line": "! custom route",
                }
            },
        },
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "metadata": {},
                "task": {
                    "engine": "xtb",
                    "payload": {"job_dir": "/tmp/xtb_job_02"},
                },
            },
            {
                "stage_id": "orca_optts_freq_01",
                "status": "failed",
                "metadata": {"analyzer_status": "ts_not_found"},
                "task": {
                    "engine": "orca",
                    "metadata": {"source_candidate_path": first_candidate.artifact_path},
                },
            },
        ],
    }
    contract = SimpleNamespace(job_id="xtb_job_02", job_type="path_search")

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / ("xtb" if "xtb" in str(path) else "orca"))
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda *args, **kwargs: (first_candidate, second_candidate))
    monkeypatch.setattr(orchestration, "build_materialized_orca_stage", _orca_stage_result)
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T15:00:00+00:00")

    created = orchestration._append_reaction_orca_stages(
        payload,
        workspace_dir=tmp_path,
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    latest_existing = payload["stages"][1]
    appended = payload["stages"][2]
    assert created is True
    assert latest_existing["metadata"] == {"analyzer_status": "ts_not_found"}
    assert "workflow_error" not in payload["metadata"]
    assert appended["stage_id"] == "orca_optts_freq_02"
    assert appended["metadata"]["reaction_candidate_attempt_index"] == 2
    assert appended["metadata"]["reaction_candidate_pool_size"] == 2
    assert appended["metadata"]["reaction_remaining_candidates_after_this"] == 0


def test_append_reaction_orca_stages_materializes_under_workflow_internal_orca_runs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _candidate(
        "/tmp/candidate_local.xyz",
        source_job_id="xtb_job_local",
        source_job_type="path_search",
        reaction_key="rxn_local",
        rank=1,
        kind="ts_guess",
    )
    payload: dict[str, Any] = {
        "workflow_id": "wf_reaction_local",
        "metadata": {
            "request": {"parameters": {"max_orca_stages": 1}},
            "workspace_dir": str((tmp_path / "wf_reaction_local").resolve()),
        },
        "stages": [
            {
                "stage_id": "xtb_path_search_01",
                "status": "completed",
                "metadata": {},
                "task": {
                    "engine": "xtb",
                    "payload": {"job_dir": "/tmp/xtb_job_local"},
                },
            }
        ],
    }
    contract = SimpleNamespace(job_id="xtb_job_local", job_type="path_search", candidate_details=())
    build_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / "orca_allowed")
    monkeypatch.setattr(orchestration, "load_xtb_artifact_contract", lambda **kwargs: contract)
    monkeypatch.setattr(orchestration, "select_xtb_downstream_inputs", lambda *args, **kwargs: (candidate,))
    monkeypatch.setattr(
        orchestration,
        "build_materialized_orca_stage",
        lambda **kwargs: build_calls.append(kwargs) or _orca_stage_result(**kwargs),
    )

    created = orchestration._append_reaction_orca_stages(
        payload,
        workspace_dir=tmp_path / "wf_reaction_local",
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    assert created is True
    assert build_calls[0]["workspace_dir"] == (tmp_path / "wf_reaction_local" / "internal" / "orca" / "runs").resolve()
    assert build_calls[0]["stage_root_name"] == "stage_03_orca"


def test_append_crest_orca_stages_materializes_orca_stages_from_completed_crest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crest_candidate = _candidate(
        "/tmp/crest_conformer.xyz",
        source_job_id="crest_job_01",
        source_job_type="conformer_search",
        reaction_key="rxn_crest",
        rank=1,
        kind="conformer",
    )
    payload: dict[str, Any] = {
        "workflow_id": "wf_conf_01",
        "metadata": {"request": {"parameters": {"max_orca_stages": 1}}},
        "stages": [
            {
                "stage_id": "crest_stage_01",
                "status": "completed",
                "task": {"engine": "crest"},
            }
        ],
    }

    monkeypatch.setattr(orchestration, "_completed_crest_stage", lambda stage, **kwargs: "crest_contract")
    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / "orca_allowed")
    monkeypatch.setattr(orchestration, "select_crest_downstream_inputs", lambda contract, policy: (crest_candidate,))
    monkeypatch.setattr(orchestration, "build_materialized_orca_stage", _orca_stage_result)

    created = orchestration._append_crest_orca_stages(
        payload,
        template_name="conformer_screening",
        crest_auto_config="/tmp/crest.yaml",
        orca_auto_config="/tmp/orca.yaml",
        stage_id_prefix="orca_conformer",
        xyz_filename="conformer_guess.xyz",
        inp_filename="conformer_opt.inp",
    )

    assert created is True
    assert payload["stages"][-1]["stage_id"] == "orca_conformer_01"
    assert payload["stages"][-1]["task"]["engine"] == "orca"


def test_append_crest_orca_stages_materializes_twenty_orca_children(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    crest_candidates = tuple(
        _candidate(
            f"/tmp/crest_conformer_{index:02d}.xyz",
            source_job_id="crest_job_20",
            source_job_type="conformer_search",
            reaction_key="mol_20",
            rank=index,
            kind="conformer",
        )
        for index in range(1, 21)
    )
    payload: dict[str, Any] = {
        "workflow_id": "wf_conf_20",
        "metadata": {"request": {"parameters": {"max_orca_stages": 20}}},
        "stages": [
            {
                "stage_id": "crest_stage_01",
                "status": "completed",
                "task": {"engine": "crest"},
            }
        ],
    }

    monkeypatch.setattr(orchestration, "_completed_crest_stage", lambda stage, **kwargs: "crest_contract")
    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / "orca_allowed")
    monkeypatch.setattr(orchestration, "select_crest_downstream_inputs", lambda contract, policy: crest_candidates)
    monkeypatch.setattr(orchestration, "build_materialized_orca_stage", _orca_stage_result)

    created = orchestration._append_crest_orca_stages(
        payload,
        template_name="conformer_screening",
        crest_auto_config="/tmp/crest.yaml",
        orca_auto_config="/tmp/orca.yaml",
        stage_id_prefix="orca_conformer",
        xyz_filename="conformer_guess.xyz",
        inp_filename="conformer_opt.inp",
    )

    orca_stages = [stage for stage in payload["stages"] if stage.get("task", {}).get("engine") == "orca"]
    assert created is True
    assert len(orca_stages) == 20
    assert orca_stages[0]["stage_id"] == "orca_conformer_01"
    assert orca_stages[-1]["stage_id"] == "orca_conformer_20"
