from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.submitters import orca_auto


def _install_workflow_io(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: dict[str, Any],
    workspace_dir: Path,
    saved_payloads: list[dict[str, Any]],
    sync_calls: list[dict[str, Any]],
) -> None:
    monkeypatch.setattr(orca_auto, "resolve_workflow_workspace", lambda target, workflow_root: workspace_dir)
    monkeypatch.setattr(orca_auto, "load_workflow_payload", lambda current_workspace_dir: payload)
    monkeypatch.setattr(
        orca_auto,
        "write_workflow_payload",
        lambda current_workspace_dir, current_payload: saved_payloads.append(
            {"workspace_dir": current_workspace_dir, "payload": deepcopy(current_payload)}
        ),
    )
    monkeypatch.setattr(
        orca_auto,
        "sync_workflow_registry",
        lambda workflow_root, current_workspace_dir, current_payload: sync_calls.append(
            {
                "workflow_root": workflow_root,
                "workspace_dir": current_workspace_dir,
                "payload": deepcopy(current_payload),
            }
        ),
    )


def _install_timestamps(monkeypatch: pytest.MonkeyPatch, *timestamps: str) -> None:
    values = iter(timestamps)
    monkeypatch.setattr(orca_auto, "now_utc_iso", lambda: next(values))


def _append_and_return(
    calls: list[dict[str, Any]],
    result: dict[str, Any],
    **kwargs: Any,
) -> dict[str, Any]:
    calls.append(kwargs)
    return result


def test_submit_reaction_ts_search_workflow_covers_continue_and_failed_only_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    payload: dict[str, Any] = {
        "workflow_id": "wf_submit_edges",
        "status": "planned",
        "metadata": {},
        "stages": [
            "skip_non_dict",
            {"stage_id": "no_task", "task": "bad"},
            {"stage_id": "no_enqueue", "task": {"status": "planned", "enqueue_payload": "bad"}},
            {
                "stage_id": "skip_other_submitter",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {"reaction_dir": "/tmp/skip", "priority": 2, "submitter": "external_cli"},
                },
            },
            {
                "stage_id": "fail_submit",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {"reaction_dir": "/tmp/fail", "priority": "6", "submitter": "chemstack_orca_cli"},
                },
            },
        ],
    }
    saved_payloads: list[dict[str, Any]] = []
    sync_calls: list[dict[str, Any]] = []
    submit_calls: list[dict[str, Any]] = []

    _install_workflow_io(
        monkeypatch,
        payload=payload,
        workspace_dir=workspace_dir,
        saved_payloads=saved_payloads,
        sync_calls=sync_calls,
    )
    _install_timestamps(
        monkeypatch,
        "2026-04-19T01:00:00+00:00",
        "2026-04-19T01:01:00+00:00",
    )
    monkeypatch.setattr(
        orca_auto,
        "submit_reaction_dir",
        lambda **kwargs: _append_and_return(
            submit_calls,
            {
                "status": "failed",
                "returncode": 9,
                "stdout": "queue_id: q_fail\n",
                "stderr": "submit boom",
                "parsed_stdout": {"queue_id": "q_fail"},
                "queue_id": "q_fail",
                "reaction_dir": "/tmp/fail",
                "priority": 6,
            },
            **kwargs,
        ),
    )

    result = orca_auto.submit_reaction_ts_search_workflow(
        workflow_target="wf_submit_edges",
        workflow_root=None,
        orca_auto_config=" /tmp/orca.yaml ",
        orca_auto_executable=" orca_auto_bin ",
        orca_auto_repo_root=" /tmp/orca_repo ",
        skip_submitted=False,
    )

    assert submit_calls == [
        {
            "reaction_dir": "/tmp/fail",
            "priority": 6,
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        }
    ]
    assert result == {
        "workflow_id": "wf_submit_edges",
        "workspace_dir": str(workspace_dir),
        "status": "submission_failed",
        "submitted": [],
        "skipped": [],
        "failed": [
            {
                "stage_id": "fail_submit",
                "returncode": 9,
                "stderr": "submit boom",
                "stdout": "queue_id: q_fail",
            }
        ],
    }
    assert len(saved_payloads) == 1
    assert sync_calls == []

    saved_payload = saved_payloads[0]["payload"]
    assert saved_payload["status"] == "submission_failed"
    fail_stage = saved_payload["stages"][-1]
    assert fail_stage["status"] == "submission_failed"
    assert fail_stage["task"]["status"] == "submission_failed"
    assert fail_stage["metadata"] == {
        "submission_status": "submission_failed",
        "submitted_at": "2026-04-19T01:00:00+00:00",
    }
    assert fail_stage["task"]["submission_result"]["submitted_at"] == "2026-04-19T01:00:00+00:00"
    assert saved_payload["metadata"]["submission_summary"] == {
        "submitted_count": 0,
        "skipped_count": 0,
        "failed_count": 1,
        "stage_results": [
            {
                "stage_id": "fail_submit",
                "status": "submission_failed",
                "queue_id": "q_fail",
                "returncode": 9,
            }
        ],
        "updated_at": "2026-04-19T01:01:00+00:00",
    }


def test_submit_reaction_ts_search_workflow_sets_queued_when_only_submitted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    workflow_root = tmp_path / "workflow_root"
    payload: dict[str, Any] = {
        "workflow_id": "wf_submit_only_success",
        "status": "planned",
        "metadata": {},
        "stages": [
            {
                "stage_id": "submit_stage",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {"reaction_dir": "/tmp/success", "priority": 4, "submitter": "chemstack_orca_cli"},
                },
            }
        ],
    }
    saved_payloads: list[dict[str, Any]] = []
    sync_calls: list[dict[str, Any]] = []

    _install_workflow_io(
        monkeypatch,
        payload=payload,
        workspace_dir=workspace_dir,
        saved_payloads=saved_payloads,
        sync_calls=sync_calls,
    )
    _install_timestamps(monkeypatch, "2026-04-19T01:10:00+00:00", "2026-04-19T01:11:00+00:00")
    monkeypatch.setattr(
        orca_auto,
        "submit_reaction_dir",
        lambda **kwargs: {
            "status": "submitted",
            "returncode": 0,
            "stdout": "status: queued\nqueue_id: q_ok\n",
            "stderr": "",
            "parsed_stdout": {"status": "queued", "queue_id": "q_ok"},
            "queue_id": "q_ok",
            "reaction_dir": "/tmp/success",
            "priority": 4,
        },
    )

    result = orca_auto.submit_reaction_ts_search_workflow(
        workflow_target="wf_submit_only_success",
        workflow_root=workflow_root,
        orca_auto_config="/tmp/orca.yaml",
    )

    assert result["status"] == "queued"
    assert len(result["submitted"]) == 1
    assert len(saved_payloads) == 1
    assert len(sync_calls) == 1
    assert saved_payloads[0]["payload"]["status"] == "queued"


def test_cancel_reaction_ts_search_workflow_covers_terminal_missing_target_and_failed_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_edges",
        "status": "running",
        "metadata": {},
        "stages": [
            "skip_non_dict",
            {"stage_id": "no_task", "task": "bad"},
            {
                "stage_id": "terminal_stage",
                "status": "completed",
                "task": {"status": "submitted", "enqueue_payload": {"submitter": "chemstack_orca_cli"}},
            },
            {
                "stage_id": "missing_target_stage",
                "status": "queued",
                "task": {"status": "planned", "enqueue_payload": "bad"},
            },
            {
                "stage_id": "skip_other_submitter",
                "status": "queued",
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/skip"},
                    "enqueue_payload": {"reaction_dir": "/tmp/skip", "submitter": "external_cli"},
                },
            },
            {
                "stage_id": "cancel_fail_stage",
                "status": "running",
                "metadata": {"queue_id": "q_fail"},
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/cancel_fail"},
                    "enqueue_payload": {"reaction_dir": "/tmp/cancel_fail", "submitter": "chemstack_orca_cli"},
                },
            },
        ],
    }
    saved_payloads: list[dict[str, Any]] = []
    sync_calls: list[dict[str, Any]] = []
    cancel_calls: list[dict[str, Any]] = []

    _install_workflow_io(
        monkeypatch,
        payload=payload,
        workspace_dir=workspace_dir,
        saved_payloads=saved_payloads,
        sync_calls=sync_calls,
    )
    _install_timestamps(
        monkeypatch,
        "2026-04-19T01:20:00+00:00",
        "2026-04-19T01:21:00+00:00",
        "2026-04-19T01:22:00+00:00",
    )
    monkeypatch.setattr(
        orca_auto,
        "cancel_target",
        lambda **kwargs: _append_and_return(
            cancel_calls,
            {
                "status": "failed",
                "returncode": 5,
                "stdout": "cancel failed",
                "stderr": "boom",
                "command_argv": ["orca_auto", "queue", "cancel", "q_fail"],
            },
            **kwargs,
        ),
    )

    result = orca_auto.cancel_reaction_ts_search_workflow(
        workflow_target="wf_cancel_edges",
        workflow_root=None,
        orca_auto_config="/tmp/orca.yaml",
        orca_auto_executable="orca_auto_bin",
        orca_auto_repo_root="/tmp/orca_repo",
    )

    assert cancel_calls == [
        {
            "target": "q_fail",
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        }
    ]
    assert result == {
        "workflow_id": "wf_cancel_edges",
        "workspace_dir": str(workspace_dir),
        "status": "cancel_failed",
        "cancelled": [],
        "requested": [],
        "skipped": [{"stage_id": "terminal_stage", "reason": "already_terminal"}],
        "failed": [
            {"stage_id": "missing_target_stage", "reason": "missing_cancel_target"},
            {
                "stage_id": "cancel_fail_stage",
                "queue_id": "q_fail",
                "reaction_dir": "/tmp/cancel_fail",
                "returncode": 5,
            },
        ],
    }
    assert len(saved_payloads) == 1
    assert sync_calls == []

    saved_payload = saved_payloads[0]["payload"]
    assert saved_payload["status"] == "cancel_failed"
    missing_target_stage = saved_payload["stages"][3]
    cancel_fail_stage = saved_payload["stages"][5]
    assert missing_target_stage["task"]["cancel_result"] == {
        "status": "failed",
        "reason": "missing_cancel_target",
        "cancelled_at": "2026-04-19T01:20:00+00:00",
    }
    assert cancel_fail_stage["task"]["cancel_result"] == {
        "status": "failed",
        "returncode": 5,
        "stdout": "cancel failed",
        "stderr": "boom",
        "command_argv": ["orca_auto", "queue", "cancel", "q_fail"],
        "cancelled_at": "2026-04-19T01:21:00+00:00",
        "target": "q_fail",
    }
    assert saved_payload["metadata"]["cancellation_summary"] == {
        "cancelled_count": 0,
        "requested_count": 0,
        "skipped_count": 1,
        "failed_count": 2,
        "stage_results": [
            {"stage_id": "terminal_stage", "status": "skipped", "reason": "already_terminal"},
            {"stage_id": "missing_target_stage", "status": "cancel_failed", "reason": "missing_cancel_target"},
            {"stage_id": "cancel_fail_stage", "status": "cancel_failed", "returncode": 5},
        ],
        "updated_at": "2026-04-19T01:22:00+00:00",
    }
