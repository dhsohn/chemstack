from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.contracts import OrcaArtifactContract
from chemstack.flow.orchestration import _sync_orca_stage


def test_sync_orca_stage_applies_contract_state_metadata_and_artifacts() -> None:
    stage: dict[str, object] = {
        "stage_id": "orca_opt_01",
        "stage_kind": "orca_stage",
        "status": "submitted",
        "metadata": {"queue_id": "q_123"},
        "task": {
            "engine": "orca",
            "task_kind": "geometry_opt",
            "status": "submitted",
            "payload": {"reaction_dir": "/tmp/rxn_pending", "selected_inp": ""},
            "enqueue_payload": {"reaction_dir": "/tmp/rxn_pending", "priority": 10},
        },
    }
    contract = OrcaArtifactContract(
        run_id="run_123",
        status="completed",
        reason="normal_termination",
        state_status="completed",
        reaction_dir="/tmp/rxn_done",
        latest_known_path="/tmp/rxn_done",
        organized_output_dir="/tmp/orca_outputs/opt/H2/run_123",
        optimized_xyz_path="/tmp/orca_outputs/opt/H2/run_123/rxn.xyz",
        queue_id="q_123",
        queue_status="completed",
        cancel_requested=False,
        selected_inp="/tmp/rxn_done/rxn.inp",
        selected_input_xyz="/tmp/rxn_done/rxn.xyz",
        analyzer_status="completed",
        completed_at="2026-04-19T00:00:00+00:00",
        last_out_path="/tmp/rxn_done/rxn.out",
        run_state_path="/tmp/rxn_done/run_state.json",
        report_json_path="/tmp/rxn_done/run_report.json",
        report_md_path="/tmp/rxn_done/run_report.md",
        attempt_count=2,
        max_retries=3,
        attempts=(
            {
                "index": 2,
                "attempt_number": 1,
                "inp_path": "/tmp/rxn_done/rxn.retry01.inp",
                "out_path": "/tmp/rxn_done/rxn.retry01.out",
                "return_code": 0,
                "analyzer_status": "completed",
                "analyzer_reason": "normal_termination",
                "markers": [],
                "patch_actions": [],
                "started_at": "2026-04-19T00:00:00+00:00",
                "ended_at": "2026-04-19T00:10:00+00:00",
            },
        ),
        final_result={
            "status": "completed",
            "analyzer_status": "completed",
            "reason": "normal_termination",
            "completed_at": "2026-04-19T00:10:00+00:00",
        },
        resource_request={"max_cores": 8, "max_memory_gb": 16},
        resource_actual={"max_cores": 8, "max_memory_gb": 16},
    )

    with patch("chemstack.flow.orchestration.load_orca_artifact_contract", return_value=contract) as mock_load:
        _sync_orca_stage(
            stage,
            orca_auto_config=None,
            orca_auto_executable="orca_auto",
            orca_auto_repo_root=None,
            submit_ready=False,
        )

    assert isinstance(stage["task"], dict)
    assert isinstance(stage["metadata"], dict)
    task = stage["task"]
    metadata = stage["metadata"]
    assert isinstance(task.get("payload"), dict)
    payload = task["payload"]

    assert stage["status"] == "completed"
    assert task["status"] == "completed"
    assert payload["selected_inp"] == contract.selected_inp
    assert payload["selected_input_xyz"] == contract.selected_input_xyz
    assert payload["last_out_path"] == contract.last_out_path
    assert payload["optimized_xyz_path"] == contract.optimized_xyz_path
    assert payload["orca_latest_attempt_inp"] == "/tmp/rxn_done/rxn.retry01.inp"
    assert payload["orca_latest_attempt_out"] == "/tmp/rxn_done/rxn.retry01.out"

    assert metadata["queue_id"] == "q_123"
    assert metadata["run_id"] == "run_123"
    assert metadata["queue_status"] == "completed"
    assert metadata["latest_known_path"] == contract.latest_known_path
    assert metadata["organized_output_dir"] == contract.organized_output_dir
    assert metadata["optimized_xyz_path"] == contract.optimized_xyz_path
    assert metadata["attempt_count"] == 2
    assert metadata["max_retries"] == 3
    assert metadata["orca_latest_attempt_number"] == 1
    assert metadata["orca_latest_attempt_status"] == "completed"
    assert metadata["orca_final_result"]["reason"] == "normal_termination"

    assert isinstance(stage.get("output_artifacts"), list)
    output_artifacts = stage["output_artifacts"]
    assert isinstance(output_artifacts, list)
    artifact_dicts = [artifact for artifact in output_artifacts if isinstance(artifact, dict)]
    artifact_kinds = {artifact["kind"] for artifact in artifact_dicts if "kind" in artifact}
    assert artifact_kinds == {
        "orca_selected_inp",
        "orca_selected_input_xyz",
        "orca_optimized_xyz",
        "orca_last_out",
        "orca_run_state",
        "orca_report_json",
        "orca_report_md",
        "orca_output_dir",
        "orca_organized_output_dir",
    }
    mock_load.assert_called_once()
