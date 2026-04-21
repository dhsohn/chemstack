from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from chemstack.core.indexing import get_job_location
from chemstack.core.queue import list_queue
from chemstack.flow.adapters.xtb import load_xtb_artifact_contract
from chemstack.flow.operations import (
    advance_materialized_workflow,
    create_conformer_screening_workflow,
    get_workflow,
)
from chemstack.flow.submitters import xtb_auto as xtb_submitter
from chemstack.flow.workflows.reaction_ts_search import build_reaction_ts_search_plan_from_target


def _write_xyz(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "2",
                "workflow input",
                "H 0.0 0.0 0.0",
                "H 0.0 0.0 0.74",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def _write_orca_config(path: Path, *, allowed_root: Path, organized_root: Path) -> None:
    payload: dict[str, Any] = {}
    if path.exists():
        loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            payload = dict(loaded)
    payload["orca"] = {
        "runtime": {
            "allowed_root": str(allowed_root.resolve()),
            "organized_root": str(organized_root.resolve()),
        },
        "paths": {
            "orca_executable": "/opt/orca/orca",
        },
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _engine_stages(payload: dict[str, Any], engine: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        task = stage.get("task")
        if not isinstance(task, dict):
            continue
        if str(task.get("engine", "")).strip() == engine:
            rows.append(stage)
    return rows


def _queue_status(entry: Any) -> str:
    return str(getattr(getattr(entry, "status", None), "value", "")).strip()


def test_conformer_screening_workflow_handoff_smoke(
    smoke_workspace: Any,
    app_runner: Any,
) -> None:
    workflow_root = smoke_workspace.root / "workflow_root"
    workflow_root.mkdir(parents=True, exist_ok=True)

    orca_allowed_root = smoke_workspace.root / "orca_runs"
    orca_organized_root = smoke_workspace.root / "orca_outputs"
    orca_allowed_root.mkdir(parents=True, exist_ok=True)
    orca_organized_root.mkdir(parents=True, exist_ok=True)

    orca_config_path = smoke_workspace.config_path
    _write_orca_config(
        orca_config_path,
        allowed_root=orca_allowed_root,
        organized_root=orca_organized_root,
    )

    input_xyz = smoke_workspace.root / "workflow_inputs" / "input.xyz"
    _write_xyz(input_xyz)

    created = create_conformer_screening_workflow(
        input_xyz=str(input_xyz),
        workflow_root=workflow_root,
        priority=5,
        max_cores=2,
        max_memory_gb=2,
        max_orca_stages=2,
    )
    workflow_id = str(created["workflow_id"])

    initial_payload = get_workflow(target=workflow_id, workflow_root=workflow_root)["workflow"]
    initial_crest_stages = _engine_stages(initial_payload, "crest")
    assert len(initial_crest_stages) == 1
    assert initial_crest_stages[0]["status"] == "planned"
    assert initial_crest_stages[0]["task"]["status"] == "planned"
    assert _engine_stages(initial_payload, "orca") == []

    submitted_payload = advance_materialized_workflow(
        target=workflow_id,
        workflow_root=workflow_root,
        crest_auto_config=str(smoke_workspace.crest_config_path),
        crest_auto_repo_root=str(smoke_workspace.repo_root),
        submit_ready=True,
    )

    submitted_crest_stage = _engine_stages(submitted_payload, "crest")[0]
    submitted_metadata = dict(submitted_crest_stage.get("metadata") or {})
    submitted_task = dict(submitted_crest_stage.get("task") or {})
    assert submitted_task["submission_result"]["status"] == "submitted"
    assert submitted_metadata["queue_id"]
    assert submitted_metadata["child_job_id"]
    assert _engine_stages(submitted_payload, "orca") == []

    record = get_job_location(smoke_workspace.crest_allowed_root, submitted_metadata["child_job_id"])
    assert record is not None
    assert record.app_name == "crest_auto"
    assert record.status in {"queued", "pending"}

    queue_entries = list_queue(smoke_workspace.crest_allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == submitted_metadata["child_job_id"]
    assert queue_entries[0].queue_id == submitted_metadata["queue_id"]
    assert _queue_status(queue_entries[0]) == "pending"

    worker = app_runner(
        smoke_workspace.repo_root,
        "chemstack.crest._internal_cli",
        "--config",
        str(smoke_workspace.crest_config_path),
        "queue",
        "worker",
        "--once",
        "--auto-organize",
    )
    assert worker.returncode == 0, worker.stderr or worker.stdout
    assert f"queue_id: {submitted_metadata['queue_id']}" in worker.stdout
    assert f"job_id: {submitted_metadata['child_job_id']}" in worker.stdout
    assert "status: completed" in worker.stdout

    handed_off_payload = advance_materialized_workflow(
        target=workflow_id,
        workflow_root=workflow_root,
        crest_auto_config=str(smoke_workspace.crest_config_path),
        orca_auto_config=str(orca_config_path),
        submit_ready=False,
    )

    crest_stage = _engine_stages(handed_off_payload, "crest")[0]
    crest_metadata = dict(crest_stage.get("metadata") or {})
    assert crest_stage["status"] == "completed"
    assert crest_stage["task"]["status"] == "completed"
    assert crest_metadata["queue_id"] == submitted_metadata["queue_id"]
    assert crest_metadata["child_job_id"] == submitted_metadata["child_job_id"]
    assert crest_metadata["organized_output_dir"]
    assert Path(crest_metadata["organized_output_dir"]).exists()
    assert Path(crest_metadata["organized_output_dir"], "crest_conformers.xyz").exists()

    orca_stages = _engine_stages(handed_off_payload, "orca")
    assert len(orca_stages) == 2
    for index, stage in enumerate(orca_stages, start=1):
        task = dict(stage.get("task") or {})
        payload = dict(task.get("payload") or {})
        reaction_dir = Path(payload["reaction_dir"])
        selected_inp = Path(payload["selected_inp"])
        selected_input_xyz = Path(payload["selected_input_xyz"])

        assert stage["stage_id"] == f"orca_conformer_{index:02d}"
        assert stage["status"] == "planned"
        assert task["status"] == "planned"
        assert reaction_dir.exists()
        assert selected_inp.exists()
        assert selected_input_xyz.exists()
        assert (reaction_dir / selected_inp.name).exists()
        assert (reaction_dir.parent / "source_candidate.json").exists()
        assert (reaction_dir.parent / "enqueue_payload.json").exists()
        assert "r2scan-3c Opt TightSCF" in selected_inp.read_text(encoding="utf-8")

    persisted = get_workflow(target=workflow_id, workflow_root=workflow_root)
    assert persisted["summary"]["workflow_id"] == workflow_id
    assert persisted["registry_record"]["stage_count"] == 3
    assert len(_engine_stages(persisted["workflow"], "orca")) == 2


def test_xtb_reaction_ts_search_handoff_smoke(
    smoke_workspace: Any,
    app_runner: Any,
    xtb_path_search_job: Path,
) -> None:
    submission = xtb_submitter.submit_job_dir(
        job_dir=str(xtb_path_search_job),
        priority=5,
        config_path=str(smoke_workspace.xtb_config_path),
        repo_root=str(smoke_workspace.repo_root),
    )

    assert submission["status"] == "submitted"
    assert submission["parsed_stdout"]["status"] == "queued"
    assert submission["job_id"]
    assert submission["queue_id"]

    queue_entries = list_queue(smoke_workspace.xtb_allowed_root)
    assert len(queue_entries) == 1
    assert queue_entries[0].task_id == submission["job_id"]
    assert queue_entries[0].queue_id == submission["queue_id"]
    assert _queue_status(queue_entries[0]) == "pending"

    worker = app_runner(
        smoke_workspace.repo_root,
        "chemstack.xtb._internal_cli",
        "--config",
        str(smoke_workspace.xtb_config_path),
        "queue",
        "worker",
        "--once",
        "--auto-organize",
    )

    assert worker.returncode == 0, worker.stderr or worker.stdout
    assert f"queue_id: {submission['queue_id']}" in worker.stdout
    assert f"job_id: {submission['job_id']}" in worker.stdout
    assert "status: completed" in worker.stdout

    record = get_job_location(smoke_workspace.xtb_allowed_root, submission["job_id"])
    assert record is not None
    assert record.app_name == "xtb_auto"
    assert record.status == "completed"
    assert record.organized_output_dir

    organized_dir = Path(record.organized_output_dir)
    assert organized_dir.exists()
    assert (organized_dir / "xtbpath_ts.xyz").exists()
    assert (organized_dir / "xtbpath_0.xyz").exists()
    assert not (organized_dir / "xtbpath_1.xyz").exists()
    assert not (organized_dir / "xtbpath.xyz").exists()
    assert (xtb_path_search_job / "organized_ref.json").exists()

    contract = load_xtb_artifact_contract(
        xtb_index_root=smoke_workspace.xtb_allowed_root,
        target=submission["job_id"],
    )
    assert contract.status == "completed"
    assert contract.job_type == "path_search"
    assert contract.organized_output_dir == str(organized_dir)
    assert Path(contract.selected_candidate_paths[0]).name == "xtbpath_ts.xyz"
    assert any(detail.kind == "ts_guess" for detail in contract.candidate_details)
    assert any(detail.kind == "selected_path" for detail in contract.candidate_details)

    workflow_root = smoke_workspace.root / "workflow_root_xtb"
    payload = build_reaction_ts_search_plan_from_target(
        xtb_index_root=smoke_workspace.xtb_allowed_root,
        target=submission["job_id"],
        workspace_root=workflow_root,
        max_orca_stages=1,
        max_cores=2,
        max_memory_gb=2,
        priority=5,
    )

    workflow_id = str(payload["workflow_id"])
    workspace_dir = workflow_root / "workflows" / workflow_id
    assert payload["template_name"] == "reaction_ts_search"
    assert payload["source_job_id"] == submission["job_id"]
    assert payload["metadata"]["source_contract"]["job_id"] == submission["job_id"]
    assert payload["metadata"]["source_contract"]["organized_output_dir"] == str(organized_dir)
    assert len(payload["stages"]) == 1

    stage = payload["stages"][0]
    task = dict(stage.get("task") or {})
    task_payload = dict(task.get("payload") or {})
    reaction_dir = Path(task_payload["reaction_dir"])
    selected_inp = Path(task_payload["selected_inp"])
    selected_input_xyz = Path(task_payload["selected_input_xyz"])
    stage_dir = reaction_dir.parent

    assert stage["stage_id"] == "orca_optts_freq_01"
    assert stage["status"] == "planned"
    assert task["status"] == "planned"
    assert stage["metadata"]["candidate_kind"] == "ts_guess"
    assert Path(stage["input_artifacts"][0]["path"]).name == "xtbpath_ts.xyz"
    assert reaction_dir.exists()
    assert selected_inp.exists()
    assert selected_input_xyz.exists()
    assert selected_input_xyz.name == "xtbpath_ts.xyz"
    assert (reaction_dir / "ts_guess.xyz").exists()
    assert (stage_dir / "source_candidate.json").exists()
    assert (stage_dir / "enqueue_payload.json").exists()
    assert (workspace_dir / "workflow.json").exists()
    assert "OptTS Freq TightSCF" in selected_inp.read_text(encoding="utf-8")

    persisted = get_workflow(target=workflow_id, workflow_root=workflow_root)
    assert persisted["summary"]["workflow_id"] == workflow_id
    assert persisted["registry_record"]["stage_count"] == 1
    assert len(_engine_stages(persisted["workflow"], "orca")) == 1
