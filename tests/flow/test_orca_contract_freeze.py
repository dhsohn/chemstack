from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.operations import (
    advance_materialized_workflow,
    get_workflow,
    get_workflow_artifacts,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_orca_config(path: Path, *, allowed_root: Path, organized_root: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "runtime:",
                f"  allowed_root: {json.dumps(str(allowed_root.resolve()))}",
                f"  organized_root: {json.dumps(str(organized_root.resolve()))}",
                "",
                "paths:",
                f"  orca_executable: {json.dumps('/opt/orca/orca')}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def test_orca_contract_freeze_completed_result_survives_public_workflow_sync(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    workflow_workspace = workflow_root / "workflows" / "wf_contract_freeze"
    orca_allowed_root = tmp_path / "orca_runs"
    orca_organized_root = tmp_path / "orca_outputs"
    original_dir = orca_allowed_root / "rxn_original"
    organized_dir = orca_organized_root / "opt" / "H2" / "run_hist_1"
    config_path = tmp_path / "chemstack.yaml"

    workflow_workspace.mkdir(parents=True)
    original_dir.mkdir(parents=True)
    organized_dir.mkdir(parents=True)

    inp = organized_dir / "rxn.inp"
    xyz = organized_dir / "rxn.xyz"
    out = organized_dir / "rxn.out"
    report_md = organized_dir / "run_report.md"
    inp.write_text("! Opt\n* xyzfile 0 1 rxn.xyz\n", encoding="utf-8")
    xyz.write_text("2\ncomment\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    out.write_text("****ORCA TERMINATED NORMALLY****\n", encoding="utf-8")
    report_md.write_text("# ORCA Run Report\n", encoding="utf-8")

    _write_json(
        organized_dir / "run_state.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "max_retries": 3,
            "status": "completed",
            "attempts": [
                {
                    "index": 2,
                    "inp_path": str(inp),
                    "out_path": str(out),
                    "return_code": 0,
                    "analyzer_status": "completed",
                    "analyzer_reason": "normal_termination",
                    "markers": [],
                    "patch_actions": [],
                    "started_at": "2026-04-19T00:00:00+00:00",
                    "ended_at": "2026-04-19T00:10:00+00:00",
                }
            ],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:10:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        organized_dir / "run_report.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "reaction_dir": str(organized_dir),
            "selected_inp": str(inp),
            "status": "completed",
            "attempt_count": 1,
            "max_retries": 3,
            "attempts": [
                {
                    "index": 2,
                    "inp_path": str(inp),
                    "out_path": str(out),
                    "return_code": 0,
                    "analyzer_status": "completed",
                    "analyzer_reason": "normal_termination",
                    "markers": [],
                    "patch_actions": [],
                    "started_at": "2026-04-19T00:00:00+00:00",
                    "ended_at": "2026-04-19T00:10:00+00:00",
                }
            ],
            "final_result": {
                "status": "completed",
                "analyzer_status": "completed",
                "reason": "normal_termination",
                "completed_at": "2026-04-19T00:10:00+00:00",
                "last_out_path": str(out),
            },
        },
    )
    _write_json(
        original_dir / "organized_ref.json",
        {
            "job_id": "job_hist_1",
            "run_id": "run_hist_1",
            "original_run_dir": str(original_dir),
            "organized_output_dir": str(organized_dir),
            "selected_inp": str(inp),
            "selected_input_xyz": str(xyz),
            "status": "completed",
            "job_type": "opt",
            "molecule_key": "H2",
            "resource_request": {"max_cores": 8, "max_memory_gb": 16},
            "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
        },
    )
    _write_json(
        orca_allowed_root / "queue.json",
        [
            {
                "queue_id": "q_hist_1",
                "task_id": "job_hist_1",
                "run_id": "run_hist_1",
                "reaction_dir": str(original_dir),
                "status": "completed",
                "cancel_requested": False,
                "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
            }
        ],
    )
    _write_json(
        orca_allowed_root / "job_locations.json",
        [
            {
                "job_id": "job_hist_1",
                "app_name": "orca_auto",
                "job_type": "orca_opt",
                "status": "completed",
                "original_run_dir": str(original_dir),
                "molecule_key": "H2",
                "selected_input_xyz": str(inp),
                "organized_output_dir": str(organized_dir),
                "latest_known_path": str(organized_dir),
                "resource_request": {"max_cores": 8, "max_memory_gb": 16},
                "resource_actual": {"max_cores": 8, "max_memory_gb": 16},
            }
        ],
    )
    _write_orca_config(
        config_path,
        allowed_root=orca_allowed_root,
        organized_root=orca_organized_root,
    )
    _write_json(
        workflow_workspace / "workflow.json",
        {
            "workflow_id": "wf_contract_freeze",
            "template_name": "reaction_ts_search",
            "status": "completed",
            "source_job_id": "wf_contract_freeze",
            "source_job_type": "reaction_ts_search",
            "reaction_key": "R1",
            "requested_at": "2026-04-19T00:00:00+00:00",
            "metadata": {},
            "stages": [
                {
                    "stage_id": "orca_opt_01",
                    "stage_kind": "orca_stage",
                    "status": "submitted",
                    "metadata": {"queue_id": "q_hist_1"},
                    "task": {
                        "engine": "orca",
                        "task_kind": "geometry_opt",
                        "status": "submitted",
                        "payload": {"reaction_dir": str(original_dir), "selected_inp": ""},
                        "enqueue_payload": {"reaction_dir": str(original_dir), "priority": 10},
                    },
                }
            ],
        },
    )

    payload = advance_materialized_workflow(
        target="wf_contract_freeze",
        workflow_root=workflow_root,
        orca_auto_config=str(config_path),
        submit_ready=False,
    )
    workflow = get_workflow(
        target="wf_contract_freeze",
        workflow_root=workflow_root,
        sync_registry=True,
    )
    artifacts = get_workflow_artifacts(
        target="wf_contract_freeze",
        workflow_root=workflow_root,
        sync_registry=True,
    )

    stage = payload["stages"][0]
    stage_metadata = stage["metadata"]
    task = stage["task"]
    task_payload = task["payload"]
    stage_summary = workflow["summary"]["stage_summaries"][0]
    artifact_kinds = {item["kind"] for item in artifacts["artifacts"]}

    assert payload["status"] == "completed"
    assert stage["status"] == "completed"
    assert task["status"] == "completed"
    assert stage_metadata["queue_id"] == "q_hist_1"
    assert stage_metadata["run_id"] == "run_hist_1"
    assert stage_metadata["latest_known_path"] == str(organized_dir.resolve())
    assert stage_metadata["organized_output_dir"] == str(organized_dir.resolve())
    assert stage_metadata["optimized_xyz_path"] == str(xyz.resolve())
    assert stage_metadata["analyzer_status"] == "completed"
    assert stage_metadata["reason"] == "normal_termination"
    assert stage_metadata["attempt_count"] == 1
    assert stage_metadata["max_retries"] == 3
    assert task_payload["selected_inp"] == str(inp.resolve())
    assert task_payload["selected_input_xyz"] == str(xyz.resolve())
    assert task_payload["last_out_path"] == str(out.resolve())

    assert stage_summary["queue_id"] == "q_hist_1"
    assert stage_summary["run_id"] == "run_hist_1"
    assert stage_summary["latest_known_path"] == str(organized_dir.resolve())
    assert stage_summary["organized_output_dir"] == str(organized_dir.resolve())
    assert stage_summary["optimized_xyz_path"] == str(xyz.resolve())
    assert stage_summary["last_out_path"] == str(out.resolve())
    assert stage_summary["analyzer_status"] == "completed"
    assert stage_summary["reason"] == "normal_termination"
    assert stage_summary["orca_attempt_count"] == 1
    assert stage_summary["orca_max_retries"] == 3

    assert artifacts["artifact_count"] >= 6
    assert {
        "orca_selected_inp",
        "orca_selected_input_xyz",
        "orca_optimized_xyz",
        "orca_last_out",
        "orca_run_state",
        "orca_report_json",
        "orca_report_md",
        "orca_organized_output_dir",
    }.issubset(artifact_kinds)
