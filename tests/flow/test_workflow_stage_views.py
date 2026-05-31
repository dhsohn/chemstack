from __future__ import annotations

from typing import Any

from chemstack.flow.orchestration.stage_views import WorkflowStageView, WorkflowTaskView


def test_task_view_mapping_fields_create_mutable_dicts() -> None:
    task: dict[str, Any] = {
        "payload": None,
        "metadata": "bad",
        "enqueue_payload": [],
        "submission_result": "",
    }
    view = WorkflowTaskView(task)

    view.update_payload({"selected_input_xyz": "/tmp/a.xyz"})
    view.update_metadata({"input_role": "reactant"})
    view.enqueue_payload()["priority"] = 7
    view.set_submission_result({"status": "submitted", "queue_id": "q1"})

    assert task["payload"] == {"selected_input_xyz": "/tmp/a.xyz"}
    assert task["metadata"] == {"input_role": "reactant"}
    assert task["enqueue_payload"] == {"priority": 7}
    assert task["submission_result"] == {"status": "submitted", "queue_id": "q1"}


def test_task_view_field_helpers_update_and_clear_nested_fields() -> None:
    task: dict[str, Any] = {
        "payload": {"stale": "x"},
        "enqueue_payload": {"job_dir": "/old", "priority": 1},
        "metadata": {"keep": True, "stale": "x"},
    }
    view = WorkflowTaskView(task)

    view.set_payload_field("selected_input_xyz", "/tmp/a.xyz")
    view.clear_payload_keys("stale", "missing")
    view.update_enqueue_payload({"priority": 9})
    view.clear_enqueue_payload_keys("job_dir")
    view.set_metadata_field("attempt", 2)
    view.clear_metadata_keys("stale")

    assert task["payload"] == {"selected_input_xyz": "/tmp/a.xyz"}
    assert task["enqueue_payload"] == {"priority": 9}
    assert task["metadata"] == {"keep": True, "attempt": 2}


def test_stage_view_status_pair_updates_stage_and_existing_task() -> None:
    stage: dict[str, Any] = {"status": "planned", "task": {"status": "planned"}}

    WorkflowStageView(stage).set_status_pair(stage_status="queued", task_status="submitted")

    assert stage["status"] == "queued"
    assert stage["task"]["status"] == "submitted"


def test_stage_view_status_pair_ignores_missing_task() -> None:
    stage = {"status": "planned", "task": None}

    WorkflowStageView(stage).set_status_pair(stage_status="cancelled", task_status="cancelled")

    assert stage["status"] == "cancelled"
    assert stage["task"] is None


def test_stage_view_ensure_task_creates_mutable_task_mapping() -> None:
    stage: dict[str, Any] = {"task": None}

    task = WorkflowStageView(stage).ensure_task()
    task.set_status("planned")

    assert stage["task"] == {"status": "planned"}


def test_stage_view_output_artifacts_replaces_existing_value() -> None:
    stage = {"output_artifacts": "bad"}
    artifacts = [{"kind": "orca_last_out", "path": "/tmp/out"}]

    WorkflowStageView(stage).set_output_artifacts(artifacts)

    assert stage["output_artifacts"] == artifacts


def test_stage_view_metadata_helpers_update_and_clear_fields() -> None:
    stage = {"metadata": {"keep": True, "stale": "x"}}
    view = WorkflowStageView(stage)

    view.set_metadata_field("queue_id", "q1")
    view.clear_metadata_keys("stale", "missing")

    assert stage["metadata"] == {"keep": True, "queue_id": "q1"}


def test_stage_view_xtb_attempt_helpers_filter_sort_and_find_rows() -> None:
    stage: dict[str, Any] = {
        "metadata": {
            "xtb_attempts": [
                {"attempt_number": "2", "status": "failed"},
                "skip",
                {"attempt_number": 0, "status": "completed"},
            ]
        }
    }
    view = WorkflowStageView(stage)

    attempt = view.xtb_attempt_record(1)

    assert attempt == {"attempt_number": 1}
    assert stage["metadata"]["xtb_attempts"] == [
        {"attempt_number": 0, "status": "completed"},
        {"attempt_number": 1},
        {"attempt_number": "2", "status": "failed"},
    ]
    assert view.xtb_current_attempt_number() == 2


def test_stage_view_reaction_handoff_sets_and_clears_optional_fields() -> None:
    stage: dict[str, Any] = {
        "metadata": {
            "reaction_handoff_reason": "old",
            "reaction_handoff_message": "old",
            "reaction_handoff_artifact_path": "/old",
        }
    }
    view = WorkflowStageView(stage)

    view.set_reaction_handoff(
        {
            "status": "ready",
            "reason": "",
            "message": "",
            "artifact_path": "/tmp/ts_guess.xyz",
        }
    )

    assert stage["metadata"] == {
        "reaction_handoff_status": "ready",
        "reaction_handoff_artifact_path": "/tmp/ts_guess.xyz",
    }
