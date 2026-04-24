from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
from chemstack.flow.adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
from chemstack.flow.contracts.crest import CrestDownstreamPolicy
from chemstack.flow.contracts.xtb import XtbArtifactContract, XtbDownstreamPolicy


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_xyz(path: Path, *, comment: str = "comment") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "2",
                comment,
                "H 0.0 0.0 0.0",
                "H 0.0 0.0 0.74",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _write_xyz_ensemble(path: Path, comments: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for comment in comments:
        lines.extend(
            [
                "2",
                comment,
                "H 0.0 0.0 0.0",
                "H 0.0 0.0 0.74",
            ]
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_load_xtb_artifact_contract_parses_candidate_details_from_direct_path_target(tmp_path: Path) -> None:
    job_dir = tmp_path / "xtb_direct"
    selected_input_xyz = job_dir / "input.xyz"
    ts_guess = job_dir / "ts_guess.xyz"
    optimized = job_dir / "optimized.xyz"

    _write_xyz(selected_input_xyz)
    _write_xyz(ts_guess, comment="energy: -0.5")
    _write_xyz(optimized, comment="energy: -1.2")
    _write_json(
        job_dir / "job_report.json",
        {
            "job_id": "xtb_direct_1",
            "job_type": "path",
            "status": "completed",
            "reason": "ok",
            "reaction_key": "rxn-1",
            "selected_input_xyz": str(selected_input_xyz),
            "analysis_summary": {"best_score": -0.5},
            "resource_request": {"max_cores": "4"},
            "candidate_details": [
                {"rank": 2, "kind": "optimized_geometry", "path": str(optimized), "selected": False, "score": "-1.2"},
                {
                    "rank": "1",
                    "kind": "ts_guess",
                    "path": str(ts_guess),
                    "selected": "yes",
                    "score": "-0.5",
                    "source": "scan",
                },
                {"rank": 3, "kind": "candidate", "path": "  ", "selected": True},
                "skip-me",
            ],
        },
    )

    contract = load_xtb_artifact_contract(xtb_index_root=tmp_path, target=str(job_dir))

    assert contract.job_id == "xtb_direct_1"
    assert contract.job_dir == str(job_dir.resolve())
    assert contract.latest_known_path == str(job_dir.resolve())
    assert contract.selected_candidate_paths == (str(ts_guess),)
    assert contract.analysis_summary == {"best_score": -0.5}
    assert contract.resource_request == {"max_cores": 4}
    assert contract.resource_actual == {"max_cores": 4}
    assert len(contract.candidate_details) == 2

    details_by_kind = {detail.kind: detail for detail in contract.candidate_details}
    assert details_by_kind["ts_guess"].selected is True
    assert details_by_kind["ts_guess"].score == pytest.approx(-0.5)
    assert details_by_kind["ts_guess"].metadata == {"source": "scan"}
    assert details_by_kind["optimized_geometry"].selected is False

    stage_inputs = select_xtb_downstream_inputs(contract, require_geometry=True)

    assert len(stage_inputs) == 1
    assert stage_inputs[0].artifact_path == str(ts_guess)
    assert stage_inputs[0].kind == "ts_guess"
    assert stage_inputs[0].selected is True
    assert stage_inputs[0].metadata == {"source": "scan"}


def test_load_xtb_artifact_contract_falls_back_to_selected_candidate_paths(tmp_path: Path) -> None:
    index_root = tmp_path / "xtb_index"
    job_dir = tmp_path / "xtb_job_fallback"
    selected_input_xyz = job_dir / "input.xyz"
    candidate_one = job_dir / "candidate_1.xyz"
    candidate_two = job_dir / "candidate_2.xyz"

    _write_xyz(selected_input_xyz)
    _write_xyz(candidate_one)
    _write_xyz(candidate_two)
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "xtb_job_fallback",
                "app_name": "xtb_auto",
                "job_type": "xtb_ts",
                "status": "completed",
                "original_run_dir": str(job_dir),
                "molecule_key": "rxn-2",
                "selected_input_xyz": str(selected_input_xyz),
                "organized_output_dir": str(job_dir),
                "latest_known_path": str(job_dir),
                "resource_request": {"max_cores": "8"},
            }
        ],
    )
    _write_json(
        job_dir / "job_report.json",
        {
            "job_type": "",
            "selected_candidate_paths": [" ", str(candidate_one), str(candidate_two)],
        },
    )

    contract = load_xtb_artifact_contract(xtb_index_root=index_root, target="xtb_job_fallback")

    assert contract.job_id == "xtb_job_fallback"
    assert contract.job_type == "ts"
    assert contract.status == "completed"
    assert contract.reaction_key == "rxn-2"
    assert contract.selected_input_xyz == str(selected_input_xyz)
    assert contract.selected_candidate_paths == (str(candidate_one), str(candidate_two))
    assert contract.resource_request == {"max_cores": 8}
    assert contract.resource_actual == {"max_cores": 8}
    assert [detail.path for detail in contract.candidate_details] == [str(candidate_one), str(candidate_two)]
    assert all(detail.kind == "candidate" and detail.selected for detail in contract.candidate_details)


def test_select_xtb_downstream_inputs_uses_selected_path_fallback_when_details_are_empty(tmp_path: Path) -> None:
    invalid_candidate = tmp_path / "candidate.txt"
    valid_candidate = tmp_path / "candidate.xyz"

    invalid_candidate.write_text("not xyz", encoding="utf-8")
    _write_xyz(valid_candidate)

    contract = XtbArtifactContract(
        job_id="xtb_no_details",
        job_type="scan",
        status="completed",
        reason="",
        job_dir=str(tmp_path),
        latest_known_path=str(tmp_path),
        reaction_key="rxn-3",
        selected_input_xyz=str(valid_candidate),
        selected_candidate_paths=(str(invalid_candidate), str(valid_candidate)),
        candidate_details=(),
    )

    stage_inputs = select_xtb_downstream_inputs(
        contract,
        policy=XtbDownstreamPolicy.build(max_candidates=2),
        require_geometry=True,
    )

    assert len(stage_inputs) == 1
    assert stage_inputs[0].artifact_path == str(valid_candidate)
    assert stage_inputs[0].rank == 1
    assert stage_inputs[0].kind == "candidate"
    assert stage_inputs[0].selected is True


def test_load_xtb_artifact_contract_rejects_non_xtb_index_records(tmp_path: Path) -> None:
    index_root = tmp_path / "xtb_index"
    job_dir = tmp_path / "xtb_wrong_app"

    job_dir.mkdir(parents=True)
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "xtb_bad_app",
                "app_name": "crest_auto",
                "job_type": "xtb_path",
                "status": "completed",
                "original_run_dir": str(job_dir),
                "latest_known_path": str(job_dir),
            }
        ],
    )
    _write_json(job_dir / "job_state.json", {"job_id": "xtb_bad_app", "status": "completed"})

    with pytest.raises(ValueError, match="Expected xtb_auto index record"):
        load_xtb_artifact_contract(xtb_index_root=index_root, target="xtb_bad_app")


def test_load_crest_artifact_contract_and_select_retained_conformers(tmp_path: Path) -> None:
    job_dir = tmp_path / "crest_direct"
    selected_input_xyz = job_dir / "input.xyz"
    conformer_one = job_dir / "conf_1.xyz"
    conformer_two = job_dir / "conf_2.xyz"

    _write_xyz(selected_input_xyz)
    _write_xyz(conformer_one, comment="energy: -2.0")
    _write_xyz(conformer_two, comment="energy: -1.5")
    _write_json(
        job_dir / "job_report.json",
        {
            "job_id": "crest_direct_1",
            "mode": "nci",
            "status": "completed",
            "reason": "retained",
            "molecule_key": "mol-1",
            "selected_input_xyz": str(selected_input_xyz),
            "retained_conformer_paths": [" ", str(conformer_one), str(conformer_two)],
            "resource_request": {"max_cores": "2"},
        },
    )

    contract = load_crest_artifact_contract(crest_index_root=tmp_path, target=str(job_dir))

    assert contract.job_id == "crest_direct_1"
    assert contract.mode == "nci"
    assert contract.job_dir == str(job_dir.resolve())
    assert contract.latest_known_path == str(job_dir.resolve())
    assert contract.retained_conformer_count == 2
    assert contract.retained_conformer_paths == (str(conformer_one), str(conformer_two))
    assert contract.resource_request == {"max_cores": 2}
    assert contract.resource_actual == {"max_cores": 2}

    stage_inputs = select_crest_downstream_inputs(contract, policy=CrestDownstreamPolicy.build(max_candidates=2))

    assert len(stage_inputs) == 2
    assert stage_inputs[0].artifact_path == str(conformer_one)
    assert stage_inputs[0].source_job_type == "crest_nci"
    assert stage_inputs[0].kind == "crest_conformer"
    assert stage_inputs[0].selected is True
    assert stage_inputs[0].metadata == {"mode": "nci"}
    assert stage_inputs[1].artifact_path == str(conformer_two)
    assert stage_inputs[1].selected is False


def test_select_crest_downstream_inputs_splits_multiframe_retained_ensemble(tmp_path: Path) -> None:
    job_dir = tmp_path / "crest_multiframe"
    selected_input_xyz = job_dir / "input.xyz"
    retained_ensemble = job_dir / "crest_conformers.xyz"

    _write_xyz(selected_input_xyz)
    _write_xyz_ensemble(
        retained_ensemble,
        (
            "energy: -2.0",
            "energy: -1.7",
            "energy: -1.4",
        ),
    )
    _write_json(
        job_dir / "job_report.json",
        {
            "job_id": "crest_multiframe_1",
            "mode": "standard",
            "status": "completed",
            "reason": "retained",
            "molecule_key": "mol-frames",
            "selected_input_xyz": str(selected_input_xyz),
            "retained_conformer_paths": [str(retained_ensemble)],
        },
    )

    contract = load_crest_artifact_contract(crest_index_root=tmp_path, target=str(job_dir))
    stage_inputs = select_crest_downstream_inputs(contract, policy=CrestDownstreamPolicy.build(max_candidates=2))

    assert len(stage_inputs) == 2
    assert [item.rank for item in stage_inputs] == [1, 2]
    assert all(item.artifact_path == str(retained_ensemble.resolve()) for item in stage_inputs)
    assert stage_inputs[0].selected is True
    assert stage_inputs[0].metadata == {
        "mode": "standard",
        "source_artifact_path": str(retained_ensemble.resolve()),
        "source_frame_index": 1,
        "source_frame_count": 3,
        "source_frame_energy": -2.0,
    }
    assert stage_inputs[1].selected is False
    assert stage_inputs[1].metadata["source_frame_index"] == 2
