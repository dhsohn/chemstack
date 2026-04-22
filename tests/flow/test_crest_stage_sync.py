from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import orchestration


def test_ensure_crest_job_dir_copies_input_and_populates_manifest(tmp_path: Path) -> None:
    source_xyz = tmp_path / "inputs" / "complex.xyz"
    source_xyz.parent.mkdir(parents=True, exist_ok=True)
    source_xyz.write_text("2\ncomplex\nH 0 0 0\nH 0 0 0.74\n", encoding="utf-8")
    stage: dict[str, Any] = {
        "stage_id": "crest_nci_01",
        "task": {
            "resource_request": {"max_cores": 10, "max_memory_gb": 48},
            "payload": {
                "source_input_xyz": str(source_xyz),
                "job_dir": "",
                "selected_input_xyz": "",
                "mode": "nci",
            },
            "enqueue_payload": {"job_dir": ""},
        },
    }

    job_dir = orchestration._ensure_crest_job_dir(
        stage,
        crest_allowed_root=tmp_path / "crest_allowed",
        workflow_id="wf_ensure_crest",
    )

    job_path = Path(job_dir)
    manifest = (job_path / "crest_job.yaml").read_text(encoding="utf-8")
    assert job_path == tmp_path / "crest_allowed" / "crest_nci_01"
    assert (job_path / "input.xyz").exists()
    assert "mode: nci" in manifest
    assert "max_cores: 10" in manifest
    assert "max_memory_gb: 48" in manifest
    assert "input_xyz: input.xyz" in manifest
    assert stage["task"]["payload"]["job_dir"] == str(job_path)
    assert stage["task"]["payload"]["selected_input_xyz"] == str(job_path / "input.xyz")
    assert stage["task"]["enqueue_payload"]["job_dir"] == str(job_path)

    assert orchestration._ensure_crest_job_dir(
        stage,
        crest_allowed_root=tmp_path / "crest_allowed",
        workflow_id="wf_ensure_crest",
    ) == str(job_path)


def test_ensure_xtb_job_dir_returns_existing_or_delegates_to_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    existing_stage = {
        "task": {
            "payload": {"job_dir": "/tmp/already_there"},
        }
    }
    assert orchestration._ensure_xtb_job_dir(
        existing_stage,
        xtb_allowed_root=tmp_path / "xtb_allowed",
        workflow_id="wf_existing",
    ) == "/tmp/already_there"

    delegated_stage = {
        "task": {
            "payload": {"job_dir": ""},
        }
    }
    calls: list[tuple[str, int]] = []
    def fake_write_xtb_path_job(stage: dict[str, Any], *, xtb_allowed_root: Path, workflow_id: str, attempt_number: int) -> str:
        calls.append((workflow_id, attempt_number))
        return "/tmp/generated_xtb_job"

    monkeypatch.setattr(orchestration, "_write_xtb_path_job", fake_write_xtb_path_job)

    assert orchestration._ensure_xtb_job_dir(
        delegated_stage,
        xtb_allowed_root=tmp_path / "xtb_allowed",
        workflow_id="wf_generated",
    ) == "/tmp/generated_xtb_job"
    assert calls == [("wf_generated", 0)]


def test_sync_crest_stage_ignores_non_dict_task_and_non_crest_engine(tmp_path: Path) -> None:
    stage_without_task = {"task": "bad"}
    stage_xtb = {"task": {"engine": "xtb", "status": "planned"}}

    orchestration._sync_crest_stage(
        stage_without_task,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root="/tmp/crest_repo",
        submit_ready=True,
        workflow_id="wf_01",
        workspace_dir=tmp_path / "workspace" / "wf_01",
    )
    orchestration._sync_crest_stage(
        stage_xtb,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root="/tmp/crest_repo",
        submit_ready=True,
        workflow_id="wf_01",
        workspace_dir=tmp_path / "workspace" / "wf_01",
    )

    assert stage_without_task == {"task": "bad"}
    assert stage_xtb == {"task": {"engine": "xtb", "status": "planned"}}


def test_sync_crest_stage_submits_and_materializes_retained_conformers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = SimpleNamespace(
        status="completed",
        job_id="crest_job_01",
        latest_known_path="/tmp/crest_done",
        organized_output_dir="/tmp/crest_outputs/run_01",
        selected_input_xyz="/tmp/crest_done/input.xyz",
        retained_conformer_paths=(
            "/tmp/crest_done/conf_01.xyz",
            "/tmp/crest_done/conf_02.xyz",
        ),
        mode="nci",
    )
    stage: dict[str, Any] = {
        "stage_id": "crest_nci_01",
        "status": "planned",
        "metadata": {},
        "task": {
            "engine": "crest",
            "status": "planned",
            "payload": {"job_dir": "", "selected_input_xyz": ""},
            "enqueue_payload": {"priority": 8},
        },
    }

    monkeypatch.setattr(orchestration, "sibling_allowed_root", lambda path: tmp_path / "crest_allowed")
    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / "crest_allowed")
    monkeypatch.setattr(orchestration, "_ensure_crest_job_dir", lambda stage, **kwargs: str(tmp_path / "crest_allowed" / "wf_01" / "job_01"))
    monkeypatch.setattr(
        orchestration,
        "submit_crest_job_dir",
        lambda **kwargs: {"status": "submitted", "queue_id": "q_crest_01", "job_id": "crest_job_01"},
    )
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T16:20:00+00:00")
    monkeypatch.setattr(orchestration, "load_crest_artifact_contract", lambda **kwargs: contract)

    orchestration._sync_crest_stage(
        stage,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root="/tmp/crest_repo",
        submit_ready=True,
        workflow_id="wf_01",
        workspace_dir=tmp_path / "workspace" / "wf_01",
    )

    task = cast(dict[str, Any], stage["task"])
    metadata = cast(dict[str, Any], stage["metadata"])
    assert stage["status"] == "completed"
    assert task["status"] == "completed"
    assert task["submission_result"]["queue_id"] == "q_crest_01"
    assert task["submission_result"]["submitted_at"] == "2026-04-19T16:20:00+00:00"
    assert task["payload"]["selected_input_xyz"] == "/tmp/crest_done/input.xyz"
    assert metadata["queue_id"] == "q_crest_01"
    assert metadata["child_job_id"] == "crest_job_01"
    assert metadata["latest_known_path"] == "/tmp/crest_done"
    assert metadata["organized_output_dir"] == "/tmp/crest_outputs/run_01"
    assert stage["output_artifacts"] == [
        {
            "kind": "crest_conformer",
            "path": "/tmp/crest_done/conf_01.xyz",
            "selected": True,
            "metadata": {"rank": 1, "mode": "nci"},
        },
        {
            "kind": "crest_conformer",
            "path": "/tmp/crest_done/conf_02.xyz",
            "selected": False,
            "metadata": {"rank": 2, "mode": "nci"},
        },
    ]


def test_sync_crest_stage_returns_without_target_when_not_submitted_and_no_queue_id(tmp_path: Path) -> None:
    stage: dict[str, Any] = {
        "stage_id": "crest_nci_02",
        "status": "planned",
        "task": {
            "engine": "crest",
            "status": "planned",
            "payload": {"job_dir": "", "selected_input_xyz": ""},
            "enqueue_payload": {"priority": 5},
        },
    }

    orchestration._sync_crest_stage(
        stage,
        crest_auto_config=None,
        crest_auto_executable="crest_auto",
        crest_auto_repo_root=None,
        submit_ready=False,
        workflow_id="wf_02",
        workspace_dir=tmp_path / "workspace" / "wf_02",
    )

    assert stage["status"] == "planned"
    assert stage["task"]["status"] == "planned"
    assert "output_artifacts" not in stage


def test_sync_crest_stage_returns_cleanly_when_contract_lookup_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage: dict[str, Any] = {
        "stage_id": "crest_nci_03",
        "status": "submitted",
        "metadata": {"queue_id": "q_existing"},
        "task": {
            "engine": "crest",
            "status": "submitted",
            "payload": {"job_dir": str(tmp_path / "job_dir"), "selected_input_xyz": ""},
            "enqueue_payload": {"priority": 5},
        },
    }

    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: tmp_path / "crest_allowed")
    monkeypatch.setattr(
        orchestration,
        "load_crest_artifact_contract",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    orchestration._sync_crest_stage(
        stage,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root="/tmp/crest_repo",
        submit_ready=False,
        workflow_id="wf_03",
        workspace_dir=tmp_path / "workspace" / "wf_03",
    )

    assert stage["status"] == "submitted"
    assert stage["task"]["status"] == "submitted"
    assert stage["metadata"]["queue_id"] == "q_existing"
    assert "output_artifacts" not in stage
