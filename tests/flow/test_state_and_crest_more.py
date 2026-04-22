from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.adapters import crest as crest_adapter
from chemstack.flow.adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
from chemstack.flow.contracts.crest import CrestDownstreamPolicy
from chemstack.flow.state import (
    WORKFLOW_FILE_NAME,
    WORKFLOW_LOCK_NAME,
    iter_workflow_workspaces,
    list_workflow_summaries,
    load_workflow_payload,
    resolve_workflow_workspace,
    workflow_artifacts,
    workflow_file_path,
    workflow_has_active_downstream,
    workflow_lock_path,
    workflow_root_dir,
    workflow_summary,
    write_workflow_payload,
)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


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


def test_workflow_file_helpers_and_resolution_support_direct_and_root_targets(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    workspace = workflow_root_dir(workflow_root) / "wf_003"
    payload = {"workflow_id": "wf_003", "status": "queued", "metadata": {"tag": "sample"}}

    written_path = write_workflow_payload(workspace, payload)

    assert workflow_root_dir(workflow_root) == workflow_root.resolve()
    assert written_path == workspace.resolve() / WORKFLOW_FILE_NAME
    assert workflow_file_path(workspace) == workspace.resolve() / WORKFLOW_FILE_NAME
    assert workflow_lock_path(workspace) == workspace.resolve() / WORKFLOW_LOCK_NAME
    assert load_workflow_payload(workspace) == payload
    assert resolve_workflow_workspace(target=str(workspace / WORKFLOW_FILE_NAME)) == workspace.resolve()
    assert resolve_workflow_workspace(
        target=f"{workspace.name}/{WORKFLOW_FILE_NAME}",
        workflow_root=workflow_root,
    ) == workspace.resolve()

    with pytest.raises(ValueError, match="workflow target is required"):
        resolve_workflow_workspace(target="   ", workflow_root=workflow_root)

    with pytest.raises(FileNotFoundError, match="workflow not found: missing-workflow"):
        resolve_workflow_workspace(target="missing-workflow", workflow_root=workflow_root)


def test_iter_workflow_workspaces_and_list_summaries_skip_invalid_payloads(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    workflows_dir = workflow_root_dir(workflow_root)
    valid_workspace = workflows_dir / "wf_003"
    non_dict_workspace = workflows_dir / "wf_002"
    invalid_json_workspace = workflows_dir / "wf_001"

    write_workflow_payload(valid_workspace, {"workflow_id": "wf_003", "status": "completed", "metadata": {}})
    _write_json(non_dict_workspace / WORKFLOW_FILE_NAME, ["not", "a", "mapping"])
    _write_text(invalid_json_workspace / WORKFLOW_FILE_NAME, "{broken json")
    (workflows_dir / "ignored_dir").mkdir(parents=True, exist_ok=True)

    assert iter_workflow_workspaces(workflow_root) == [
        valid_workspace.resolve(),
        non_dict_workspace.resolve(),
        invalid_json_workspace.resolve(),
    ]

    summaries = list_workflow_summaries(workflow_root)

    assert len(summaries) == 1
    assert summaries[0]["workflow_id"] == "wf_003"
    assert summaries[0]["workflow_file"] == str(valid_workspace.resolve() / WORKFLOW_FILE_NAME)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"metadata": {"downstream_reaction_workflow": {"status": "queued"}}}, True),
        ({"metadata": {"downstream_reaction_workflow": {"final_child_sync_pending": "yes"}}}, True),
        ({"metadata": {"downstream_reaction_workflow": {"latest_stage": {"status": "running"}}}}, True),
        ({"metadata": {"downstream_reaction_workflow": {"latest_stage": {"task_status": "submitted"}}}}, True),
        ({"metadata": {"downstream_reaction_workflow": {"status": "completed", "latest_stage": {"task_status": "done"}}}}, False),
        ({"metadata": {"downstream_reaction_workflow": "skip-me"}}, False),
    ],
)
def test_workflow_has_active_downstream_covers_status_and_latest_stage_branches(
    payload: dict[str, object],
    expected: bool,
) -> None:
    assert workflow_has_active_downstream(payload) is expected


def test_workflow_summary_and_artifacts_cover_enqueue_precomplex_and_downstream_branches(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    selected_xyz = workspace / "inputs" / "selected.xyz"
    selected_inp = workspace / "inputs" / "reaction.inp"
    source_xyz = workspace / "inputs" / "source.xyz"
    reactant_xyz = workspace / "inputs" / "reactant.xyz"
    product_xyz = workspace / "inputs" / "product.xyz"
    reaction_dir = workspace / "jobs" / "reaction_dir"
    latest_known_path = workspace / "runs" / "latest"
    organized_output_dir = workspace / "runs" / "organized"
    report_json = workspace / "outputs" / "report.json"
    last_out = workspace / "outputs" / "final.out"
    optimized_xyz = workspace / "outputs" / "optimized.xyz"
    downstream_workspace = workspace / "downstream_child"
    downstream_latest = downstream_workspace / "latest_stage"
    downstream_organized = downstream_workspace / "organized_stage"

    for xyz_path in (selected_xyz, source_xyz, reactant_xyz, product_xyz, optimized_xyz):
        _write_xyz(xyz_path)
    _write_text(selected_inp, "! opt\n",)
    _write_text(report_json, '{"status": "ok"}\n')
    _write_text(last_out, "normal termination\n")
    reaction_dir.mkdir(parents=True, exist_ok=True)
    latest_known_path.mkdir(parents=True, exist_ok=True)
    organized_output_dir.mkdir(parents=True, exist_ok=True)
    downstream_latest.mkdir(parents=True, exist_ok=True)
    downstream_organized.mkdir(parents=True, exist_ok=True)
    write_workflow_payload(downstream_workspace, {"workflow_id": "child_wf", "status": "running", "metadata": {}})

    payload = {
        "workflow_id": "wf_parent",
        "template_name": "parent_template",
        "status": "running",
        "source_job_id": "job_parent",
        "source_job_type": "orca",
        "reaction_key": "rxn-parent",
        "requested_at": "2026-04-19T12:00:00Z",
        "metadata": {
            "submission_summary": {"submitted": 1},
            "request": {
                "source_artifacts": [
                    {"kind": "source_xyz", "path": "inputs/source.xyz", "selected": 1, "metadata": {"role": "seed"}},
                    {"kind": "source_xyz", "path": "inputs/source.xyz", "selected": 0, "metadata": {"role": "duplicate"}},
                ]
            },
            "precomplex_handoff": {
                "reactant_xyz": "inputs/reactant.xyz",
                "product_xyz": "inputs/product.xyz",
            },
            "parent_workflow": {"workflow_id": "wf_grandparent"},
            "final_child_sync_pending": "yes",
            "downstream_reaction_workflow": {
                "workflow_id": "child_wf",
                "workspace_dir": "downstream_child",
                "status": "completed",
                "latest_stage": {
                    "stage_id": "child_stage_01",
                    "latest_known_path": "downstream_child/latest_stage",
                    "organized_output_dir": "downstream_child/organized_stage",
                    "status": "completed",
                    "task_status": "failed",
                },
            },
        },
        "stages": [
            {
                "stage_id": "stage_orca_01",
                "stage_kind": "orca",
                "status": "completed",
                "task": {
                    "status": "running",
                    "engine": "orca",
                    "task_kind": "optimization",
                    "payload": {
                        "selected_input_xyz": "inputs/selected.xyz",
                        "selected_inp": "",
                        "reaction_dir": "",
                        "optimized_xyz_path": "",
                        "last_out_path": "outputs/final.out",
                    },
                    "enqueue_payload": {
                        "selected_inp": "inputs/reaction.inp",
                        "reaction_dir": "jobs/reaction_dir",
                    },
                    "submission_result": {"status": "submitted"},
                },
                "metadata": {
                    "queue_id": "queue_01",
                    "run_id": "run_01",
                    "latest_known_path": "runs/latest",
                    "organized_output_dir": "runs/organized",
                    "optimized_xyz_path": "outputs/optimized.xyz",
                    "analyzer_status": "completed",
                    "reason": "normal_termination",
                    "reaction_handoff_status": "queued",
                    "reaction_handoff_reason": "waiting_for_child",
                    "xtb_handoff_retries_used": 2,
                    "xtb_handoff_retry_limit": 5,
                    "attempt_count": 1,
                    "max_retries": 3,
                    "completed_at": "2026-04-19T12:05:00Z",
                },
                "input_artifacts": [
                    {"kind": "candidate_xyz", "path": "inputs/selected.xyz", "selected": True, "metadata": {"rank": 1}},
                    {"kind": "candidate_xyz", "path": "inputs/selected.xyz", "selected": False, "metadata": {"rank": 9}},
                ],
                "output_artifacts": [
                    {"kind": "report", "path": "outputs/report.json", "selected": False},
                    {"kind": "report", "path": "outputs/report.json", "selected": True},
                ],
            },
            "skip-me",
        ],
    }

    summary = workflow_summary(workspace, payload)
    artifacts = workflow_artifacts(workspace, payload)

    assert summary["workspace_dir"] == str(workspace.resolve())
    assert summary["stage_count"] == 2
    assert summary["stage_status_counts"] == {"completed": 1, "unknown": 1}
    assert summary["task_status_counts"] == {"running": 1, "unknown": 1}
    assert summary["submission_summary"] == {"submitted": 1}
    assert summary["final_child_sync_pending"] is True
    assert summary["downstream_reaction_workflow"]["workflow_id"] == "child_wf"
    assert summary["precomplex_handoff"]["reactant_xyz"] == "inputs/reactant.xyz"
    assert summary["parent_workflow"] == {"workflow_id": "wf_grandparent"}
    assert summary["stage_summaries"][0]["queue_id"] == "queue_01"
    assert summary["stage_summaries"][0]["reaction_dir"] == "jobs/reaction_dir"
    assert summary["stage_summaries"][0]["selected_inp"] == "inputs/reaction.inp"
    assert summary["stage_summaries"][0]["optimized_xyz_path"] == "outputs/optimized.xyz"
    assert summary["stage_summaries"][0]["output_artifact_count"] == 2
    assert summary["stage_summaries"][1]["status"] == "unknown"

    source_rows = [row for row in artifacts if row["kind"] == "source_xyz"]
    report_rows = [row for row in artifacts if row["kind"] == "report"]
    precomplex_rows = [row for row in artifacts if row["kind"] == "precomplex_handoff_xyz"]
    downstream_file_rows = [row for row in artifacts if row["kind"] == "downstream_workflow_file"]
    selected_inp_rows = [row for row in artifacts if row["kind"] == "selected_inp"]
    optimized_rows = [row for row in artifacts if row["kind"] == "optimized_xyz_path"]

    assert len(source_rows) == 1
    assert source_rows[0]["selected"] is True
    assert source_rows[0]["metadata"] == {"role": "seed"}
    assert source_rows[0]["resolved_path"] == str(source_xyz.resolve())

    assert len(report_rows) == 1
    assert report_rows[0]["exists"] is True
    assert report_rows[0]["resolved_path"] == str(report_json.resolve())

    assert len(precomplex_rows) == 2
    assert {row["metadata"]["role"] for row in precomplex_rows} == {"reactant", "product"}
    assert all(row["selected"] is True for row in precomplex_rows)

    assert len(selected_inp_rows) == 1
    assert selected_inp_rows[0]["path"] == "inputs/reaction.inp"
    assert selected_inp_rows[0]["source"] == "task.payload"
    assert selected_inp_rows[0]["resolved_path"] == str(selected_inp.resolve())

    assert len(optimized_rows) == 1
    assert optimized_rows[0]["path"] == "outputs/optimized.xyz"
    assert optimized_rows[0]["source"] == "task.payload"
    assert optimized_rows[0]["resolved_path"] == str(optimized_xyz.resolve())

    assert len(downstream_file_rows) == 1
    assert downstream_file_rows[0]["exists"] is True
    assert downstream_file_rows[0]["resolved_path"] == str((downstream_workspace / WORKFLOW_FILE_NAME).resolve())

    downstream_latest_rows = [row for row in artifacts if row["kind"] == "downstream_latest_known_path"]
    assert len(downstream_latest_rows) == 1
    assert downstream_latest_rows[0]["metadata"] == {"stage_id": "child_stage_01"}
    assert downstream_latest_rows[0]["is_dir"] is True


def test_load_crest_artifact_contract_uses_state_and_index_fallbacks_for_resources_and_selection(
    tmp_path: Path,
) -> None:
    index_root = tmp_path / "crest_index"
    job_dir = tmp_path / "crest_job"
    missing_latest = tmp_path / "missing_latest"
    missing_original = tmp_path / "missing_original"
    selected_input_xyz = job_dir / "selected.xyz"
    conformer_one = job_dir / "conf_1.xyz"
    conformer_two = job_dir / "conf_2.xyz"
    organized_output_dir = job_dir / "organized_outputs"

    _write_xyz(selected_input_xyz)
    _write_xyz(conformer_one, comment="energy: -1.1")
    _write_xyz(conformer_two, comment="energy: -2.2")
    organized_output_dir.mkdir(parents=True, exist_ok=True)
    _write_text(job_dir / "job_report.json", "{invalid json")
    _write_json(
        job_dir / "job_state.json",
        {
            "reason": "recovered_from_state",
            "retained_conformer_paths": [" ", str(conformer_two), str(conformer_one)],
            "retained_conformer_count": 0,
            "resource_request": {" ": 5},
        },
    )
    _write_json(job_dir / "organized_ref.json", {"organized_output_dir": str(organized_output_dir)})
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "crest_job_01",
                "app_name": "crest_auto",
                "job_type": "screen",
                "status": "completed",
                "original_run_dir": str(missing_original),
                "molecule_key": "mol-screen",
                "selected_input_xyz": str(selected_input_xyz),
                "organized_output_dir": str(job_dir),
                "latest_known_path": str(missing_latest),
                "resource_request": {"max_cores": "8", "memory_gb": "32"},
                "resource_actual": {},
            }
        ],
    )

    contract = load_crest_artifact_contract(crest_index_root=index_root, target="crest_job_01")
    stage_inputs = select_crest_downstream_inputs(contract, policy=CrestDownstreamPolicy.build(max_candidates=0))

    assert contract.job_id == "crest_job_01"
    assert contract.mode == "screen"
    assert contract.status == "completed"
    assert contract.reason == "recovered_from_state"
    assert contract.job_dir == str(job_dir.resolve())
    assert contract.latest_known_path == str(missing_latest.resolve())
    assert contract.organized_output_dir == str(organized_output_dir.resolve())
    assert contract.molecule_key == "mol-screen"
    assert contract.selected_input_xyz == str(selected_input_xyz)
    assert contract.retained_conformer_count == 2
    assert contract.retained_conformer_paths == (str(conformer_two), str(conformer_one))
    assert contract.resource_request == {"max_cores": 8, "memory_gb": 32}
    assert contract.resource_actual == {"max_cores": 8, "memory_gb": 32}

    assert len(stage_inputs) == 1
    assert stage_inputs[0].artifact_path == str(conformer_two)
    assert stage_inputs[0].source_job_id == "crest_job_01"
    assert stage_inputs[0].source_job_type == "crest_screen"
    assert stage_inputs[0].reaction_key == "mol-screen"
    assert stage_inputs[0].selected_input_xyz == str(selected_input_xyz)
    assert stage_inputs[0].selected is True
    assert stage_inputs[0].metadata == {"mode": "screen"}


def test_load_crest_artifact_contract_rejects_non_crest_index_records(tmp_path: Path) -> None:
    index_root = tmp_path / "crest_index"
    job_dir = tmp_path / "crest_wrong_app"

    job_dir.mkdir(parents=True)
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "crest_wrong_app",
                "app_name": "xtb_auto",
                "job_type": "screen",
                "status": "completed",
                "original_run_dir": str(job_dir),
                "latest_known_path": str(job_dir),
            }
        ],
    )
    _write_json(job_dir / "job_state.json", {"job_id": "crest_wrong_app", "status": "completed"})

    with pytest.raises(ValueError, match="Expected crest_auto index record"):
        load_crest_artifact_contract(crest_index_root=index_root, target="crest_wrong_app")


def test_load_crest_artifact_contract_remaps_stale_paths_to_organized_outputs(tmp_path: Path) -> None:
    index_root = tmp_path / "crest_index"
    original_dir = tmp_path / "crest_runs" / "crest_job_organized"
    organized_dir = tmp_path / "crest_outputs" / "standard" / "mol-organized" / "crest_job_organized"
    stale_selected_input = original_dir / "input.xyz"
    stale_conformer_one = original_dir / "crest_conformers.xyz"
    stale_conformer_two = original_dir / "crest_best.xyz"
    organized_selected_input = organized_dir / "input.xyz"
    organized_conformer_one = organized_dir / "crest_conformers.xyz"
    organized_conformer_two = organized_dir / "crest_best.xyz"

    original_dir.mkdir(parents=True, exist_ok=True)
    organized_dir.mkdir(parents=True, exist_ok=True)
    _write_json(original_dir / "organized_ref.json", {"organized_output_dir": str(organized_dir)})
    _write_xyz(organized_selected_input)
    _write_xyz(organized_conformer_one, comment="energy: -2.0")
    _write_xyz(organized_conformer_two, comment="energy: -1.5")
    _write_json(
        organized_dir / "job_report.json",
        {
            "job_id": "crest_job_organized",
            "mode": "standard",
            "status": "completed",
            "reason": "retained",
            "molecule_key": "mol-organized",
            "selected_input_xyz": str(stale_selected_input),
            "organized_output_dir": str(organized_dir),
            "retained_conformer_paths": [str(stale_conformer_one), str(stale_conformer_two)],
        },
    )
    _write_json(
        index_root / "job_locations.json",
        [
            {
                "job_id": "crest_job_organized",
                "app_name": "crest_auto",
                "job_type": "standard",
                "status": "completed",
                "original_run_dir": str(original_dir),
                "molecule_key": "mol-organized",
                "selected_input_xyz": str(stale_selected_input),
                "organized_output_dir": str(organized_dir),
                "latest_known_path": str(organized_dir),
            }
        ],
    )

    contract = load_crest_artifact_contract(crest_index_root=index_root, target="crest_job_organized")
    stage_inputs = select_crest_downstream_inputs(contract, policy=CrestDownstreamPolicy.build(max_candidates=2))

    assert contract.job_dir == str(organized_dir.resolve())
    assert contract.selected_input_xyz == str(organized_selected_input.resolve())
    assert contract.retained_conformer_paths == (
        str(organized_conformer_one.resolve()),
        str(organized_conformer_two.resolve()),
    )
    assert len(stage_inputs) == 2
    assert stage_inputs[0].artifact_path == str(organized_conformer_one.resolve())
    assert stage_inputs[0].selected_input_xyz == str(organized_selected_input.resolve())
    assert stage_inputs[1].artifact_path == str(organized_conformer_two.resolve())


def test_crest_artifact_helpers_cover_empty_missing_and_oserror_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_dir = tmp_path / "crest_job"
    job_dir.mkdir()

    class FakePath:
        def __init__(self, raw: str) -> None:
            self.raw = raw

        def expanduser(self) -> Path:
            if self.raw == "bad-path":
                raise OSError("boom")
            return Path(self.raw)

    monkeypatch.setattr(crest_adapter, "Path", FakePath)

    roots = crest_adapter._artifact_roots(job_dir, "bad-path")

    assert roots == (job_dir.resolve(),)
    assert crest_adapter._resolve_artifact_path("", roots=roots) == ""
    assert crest_adapter._resolve_artifact_path("bad-path", roots=roots) == "bad-path"
    assert crest_adapter._resolve_artifact_path(str(tmp_path / "missing.xyz"), roots=roots) == str(
        (tmp_path / "missing.xyz").resolve()
    )
