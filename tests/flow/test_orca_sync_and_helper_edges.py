from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.contracts import OrcaArtifactContract
from chemstack.flow.orchestration import (
    _clear_reaction_xtb_handoff_error_if_recovering,
    _reaction_orca_source_candidate_path,
    _sync_orca_stage,
)
from chemstack.flow import orchestration


def test_reaction_orca_source_candidate_path_uses_metadata_then_artifacts() -> None:
    assert (
        _reaction_orca_source_candidate_path(
            {"task": {"metadata": {"source_candidate_path": "/tmp/from-task.xyz"}}}
        )
        == "/tmp/from-task.xyz"
    )
    assert (
        _reaction_orca_source_candidate_path(
            {
                "input_artifacts": [
                    {"kind": "other", "path": "/tmp/skip.xyz"},
                    {"kind": "xtb_candidate", "path": "/tmp/from-artifact.xyz"},
                ]
            }
        )
        == "/tmp/from-artifact.xyz"
    )
    assert _reaction_orca_source_candidate_path({"input_artifacts": ["skip", {"kind": "xtb_candidate", "path": ""}]}) == ""


def test_clear_reaction_xtb_handoff_error_only_clears_recovering_xtb_cases() -> None:
    payload: dict[str, Any] = {
        "metadata": {"workflow_error": {"scope": "different_scope"}},
        "stages": [{"status": "completed", "task": {"engine": "xtb"}, "metadata": {"reaction_handoff_status": "failed"}}],
    }
    _clear_reaction_xtb_handoff_error_if_recovering(payload)
    assert payload["metadata"]["workflow_error"] == {"scope": "different_scope"}

    recovering_payload: dict[str, Any] = {
        "metadata": {"workflow_error": {"scope": "reaction_ts_search_xtb_handoff"}},
        "stages": [{"status": "completed", "task": {"engine": "orca"}, "metadata": {"reaction_handoff_status": "retrying"}}],
    }
    _clear_reaction_xtb_handoff_error_if_recovering(recovering_payload)
    assert recovering_payload["metadata"]["workflow_error"] == {"scope": "reaction_ts_search_xtb_handoff"}

    _clear_reaction_xtb_handoff_error_if_recovering({"metadata": None, "stages": []})


def test_sync_orca_stage_returns_early_for_non_orca_missing_enqueue_and_missing_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_calls: list[dict[str, Any]] = []

    def fake_load_orca_artifact_contract(**kwargs: Any) -> None:
        load_calls.append(kwargs)
        return None

    monkeypatch.setattr(orchestration, "load_orca_artifact_contract", fake_load_orca_artifact_contract)

    _sync_orca_stage(
        {"task": {"engine": "xtb"}},
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
        submit_ready=False,
    )
    _sync_orca_stage(
        {"task": {"engine": "orca"}},
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
        submit_ready=False,
    )
    _sync_orca_stage(
        {"task": {"engine": "orca", "enqueue_payload": {}, "payload": {}}, "metadata": {}},
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
        submit_ready=False,
    )

    assert load_calls == []


def test_sync_orca_stage_submit_path_preserves_submitted_state_for_unknown_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stage: dict[str, object] = {
        "status": "planned",
        "metadata": {},
        "task": {
            "engine": "orca",
            "status": "planned",
            "payload": {"reaction_dir": "/tmp/rxn_from_payload"},
            "enqueue_payload": {"reaction_dir": "/tmp/rxn_from_enqueue", "priority": 7},
        },
    }
    submit_calls: list[dict[str, object]] = []
    load_calls: list[dict[str, object]] = []

    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T01:23:45+00:00")

    def fake_submit_reaction_dir(**kwargs: Any) -> dict[str, str]:
        submit_calls.append(kwargs)
        return {"status": "submitted", "queue_id": "q_submitted"}

    monkeypatch.setattr(orchestration, "submit_reaction_dir", fake_submit_reaction_dir)
    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: Path("/tmp/orca_allowed"))
    monkeypatch.setattr(orchestration, "_load_config_organized_root", lambda path: Path("/tmp/orca_organized"))
    def fake_load_orca_artifact_contract(**kwargs: Any) -> OrcaArtifactContract:
        load_calls.append(kwargs)
        return OrcaArtifactContract(
            run_id="",
            status="unknown",
            reason="",
            state_status="running",
            reaction_dir="/tmp/rxn_from_payload",
            latest_known_path="/tmp/rxn_from_payload",
            queue_id="",
            queue_status="running",
            attempt_count=3,
            max_retries=5,
            attempts=(),
            final_result={},
        )

    monkeypatch.setattr(orchestration, "load_orca_artifact_contract", fake_load_orca_artifact_contract)

    _sync_orca_stage(
        stage,
        orca_auto_config="/tmp/orca.yaml",
        orca_auto_executable="orca_auto",
        orca_auto_repo_root="/tmp/orca_repo",
        submit_ready=True,
    )

    assert submit_calls == [
        {
            "reaction_dir": "/tmp/rxn_from_enqueue",
            "priority": 7,
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto",
            "repo_root": "/tmp/orca_repo",
        }
    ]
    assert load_calls == [
        {
            "target": "/tmp/rxn_from_payload",
            "orca_allowed_root": Path("/tmp/orca_allowed"),
            "orca_organized_root": Path("/tmp/orca_organized"),
            "queue_id": "q_submitted",
            "run_id": "",
            "reaction_dir": "/tmp/rxn_from_payload",
        }
    ]

    task = stage["task"]
    metadata = stage["metadata"]
    assert isinstance(task, dict)
    assert isinstance(metadata, dict)
    assert task["status"] == "submitted"
    assert stage["status"] == "queued"
    assert task["submission_result"] == {
        "status": "submitted",
        "queue_id": "q_submitted",
        "submitted_at": "2026-04-19T01:23:45+00:00",
    }
    assert metadata["submission_status"] == "submitted"
    assert metadata["submitted_at"] == "2026-04-19T01:23:45+00:00"
    assert metadata["queue_id"] == "q_submitted"
    assert metadata["queue_status"] == "running"
    assert metadata["orca_current_attempt_number"] == 3
    assert "orca_latest_attempt_number" not in metadata
    assert "orca_latest_attempt_status" not in metadata
    assert stage["output_artifacts"] == [
        {
            "kind": "orca_output_dir",
            "path": "/tmp/rxn_from_payload",
            "selected": False,
            "metadata": {"organized": False},
        }
    ]


def test_sync_orca_stage_prefers_workflow_local_organized_root_for_internal_orca_runs(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    reaction_dir = (
        tmp_path
        / "wf_local"
        / "internal"
        / "orca"
        / "runs"
        / "stage_02_orca"
        / "job_01"
        / "reaction_dir"
    )
    stage: dict[str, object] = {
        "status": "planned",
        "metadata": {},
        "task": {
            "engine": "orca",
            "status": "planned",
            "payload": {"reaction_dir": str(reaction_dir)},
            "enqueue_payload": {"reaction_dir": str(reaction_dir), "priority": 7},
        },
    }
    load_calls: list[dict[str, object]] = []

    monkeypatch.setattr(orchestration, "submit_reaction_dir", lambda **kwargs: {"status": "submitted", "queue_id": "q_local"})
    monkeypatch.setattr(orchestration, "now_utc_iso", lambda: "2026-04-19T01:23:45+00:00")
    monkeypatch.setattr(orchestration, "_load_config_root", lambda path: Path("/tmp/orca_allowed"))
    monkeypatch.setattr(orchestration, "_load_config_organized_root", lambda path: Path("/tmp/orca_organized"))
    monkeypatch.setattr(
        orchestration,
        "load_orca_artifact_contract",
        lambda **kwargs: load_calls.append(kwargs)
        or OrcaArtifactContract(
            run_id="",
            status="unknown",
            reason="",
            state_status="running",
            reaction_dir=str(reaction_dir),
            latest_known_path=str(reaction_dir),
            queue_id="",
            queue_status="running",
            attempt_count=0,
            max_retries=0,
            attempts=(),
            final_result={},
        ),
    )

    _sync_orca_stage(
        stage,
        orca_auto_config="/tmp/orca.yaml",
        orca_auto_executable="orca_auto",
        orca_auto_repo_root="/tmp/orca_repo",
        submit_ready=True,
    )

    assert load_calls[0]["orca_allowed_root"] == Path("/tmp/orca_allowed")
    assert load_calls[0]["orca_organized_root"] == (
        tmp_path / "wf_local" / "internal" / "orca" / "outputs"
    ).resolve()
