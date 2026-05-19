from __future__ import annotations

import json
from types import SimpleNamespace

from chemstack.flow import cli_workflow_output as output


def test_emit_workflow_get_text_includes_downstream_submission_and_stage_details(capsys) -> None:
    response = {
        "summary": {
            "workflow_id": "wf_get",
            "template_name": "reaction_ts_search",
            "status": "running",
            "source_job_id": "src_1",
            "reaction_key": "rxn_1",
            "workspace_dir": "/tmp/wf_get",
            "stage_count": 2,
            "downstream_reaction_workflow": {"workflow_id": "wf_child", "status": "planned"},
            "submission_summary": {
                "submitted_count": 1,
                "skipped_count": 2,
                "failed_count": 3,
            },
            "stage_summaries": [
                {
                    "stage_id": "orca_01",
                    "engine": "orca",
                    "task_kind": "opt",
                    "status": "submitted",
                    "task_status": "running",
                    "queue_id": "q_1",
                    "selected_input_xyz": "/tmp/input.xyz",
                    "selected_inp": "/tmp/input.inp",
                }
            ],
        }
    }

    assert output.emit_workflow_get(response, json_mode=False) == 0

    stdout = capsys.readouterr().out
    assert "workflow_id: wf_get" in stdout
    assert "downstream_reaction: wf_child status=planned" in stdout
    assert "submission_summary: submitted=1 skipped=2 failed=3" in stdout
    assert "- orca_01 orca/opt stage_status=submitted task_status=running" in stdout
    assert "queue_id=q_1" in stdout
    assert "selected_input_xyz=/tmp/input.xyz" in stdout
    assert "selected_inp=/tmp/input.inp" in stdout


def test_emit_workflow_cancel_text_includes_all_result_groups(capsys) -> None:
    payload = {
        "workflow_id": "wf_cancel",
        "workspace_dir": "/tmp/wf_cancel",
        "status": "cancel_requested",
        "cancelled": [{"stage_id": "orca_done", "queue_id": "q_done"}],
        "requested": [{"stage_id": "orca_wait", "queue_id": "q_wait"}],
        "skipped": [{"stage_id": "crest_done", "reason": "terminal"}],
        "failed": [{"stage_id": "xtb_fail", "reason": "queue_missing"}],
    }

    assert output.emit_workflow_cancel(payload, json_mode=False) == 0

    stdout = capsys.readouterr().out
    assert "cancelled_count: 1" in stdout
    assert "- cancelled orca_done queue_id=q_done" in stdout
    assert "requested_count: 1" in stdout
    assert "- cancel_requested orca_wait queue_id=q_wait" in stdout
    assert "skipped_count: 1" in stdout
    assert "- skipped crest_done reason=terminal" in stdout
    assert "failed_count: 1" in stdout
    assert "- failed xtb_fail reason=queue_missing" in stdout


def test_emit_worker_payload_json_pretty_only_for_single_cycle(capsys) -> None:
    payload = {"worker_session_id": "worker_1", "workflow_results": [{"workflow_id": "wf"}]}

    output.emit_worker_payload(payload, json_mode=True, single_cycle=True)
    pretty = capsys.readouterr().out
    assert json.loads(pretty)["worker_session_id"] == "worker_1"
    assert "\n  " in pretty

    output.emit_worker_payload(payload, json_mode=True, single_cycle=False)
    compact = capsys.readouterr().out.strip()
    assert json.loads(compact)["workflow_results"][0]["workflow_id"] == "wf"
    assert "\n" not in compact


def test_emit_json_uses_ascii_for_non_ascii_payload(capsys) -> None:
    output.emit_json({"message": "반응"}, pretty=False)

    stdout = capsys.readouterr().out
    assert "\\ubc18\\uc751" in stdout
    assert "반응" not in stdout


def test_emit_workflow_reindex_uses_record_objects_in_text(capsys) -> None:
    records = [
        SimpleNamespace(
            workflow_id="wf_one",
            status="running",
            template_name="reaction_ts_search",
        )
    ]

    assert output.emit_workflow_reindex({"count": 1}, records=records, json_mode=False) == 0

    assert "- wf_one status=running template=reaction_ts_search" in capsys.readouterr().out
