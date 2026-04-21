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
    monkeypatch.setattr(
        orca_auto,
        "resolve_workflow_workspace",
        lambda target, workflow_root: workspace_dir,
    )
    monkeypatch.setattr(
        orca_auto,
        "load_workflow_payload",
        lambda current_workspace_dir: payload,
    )

    def fake_write_workflow_payload(current_workspace_dir: Path, current_payload: dict[str, Any]) -> None:
        saved_payloads.append(
            {
                "workspace_dir": current_workspace_dir,
                "payload": deepcopy(current_payload),
            }
        )

    def fake_sync_workflow_registry(
        workflow_root: Path,
        current_workspace_dir: Path,
        current_payload: dict[str, Any],
    ) -> None:
        sync_calls.append(
            {
                "workflow_root": workflow_root,
                "workspace_dir": current_workspace_dir,
                "payload": deepcopy(current_payload),
            }
        )

    monkeypatch.setattr(orca_auto, "write_workflow_payload", fake_write_workflow_payload)
    monkeypatch.setattr(orca_auto, "sync_workflow_registry", fake_sync_workflow_registry)


def _install_timestamps(monkeypatch: pytest.MonkeyPatch, *timestamps: str) -> None:
    values = iter(timestamps)
    monkeypatch.setattr(orca_auto, "now_utc_iso", lambda: next(values))


def test_submit_reaction_ts_search_workflow_ignores_invalid_stages_and_sets_submitted_only_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workspace"
    payload: dict[str, Any] = {
        "workflow_id": "wf_submit_only",
        "status": "planned",
        "metadata": {},
        "stages": [
            "not-a-stage",
            {"stage_id": "bad_task", "task": []},
            {"stage_id": "bad_enqueue", "task": {"enqueue_payload": []}},
            {
                "stage_id": "foreign_submitter",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/foreign",
                        "priority": 1,
                        "submitter": "other_submitter",
                    },
                },
            },
            {
                "stage_id": "submit_stage",
                "status": "planned",
                "metadata": "not-a-dict",
                "task": {
                    "status": "planned",
                    "metadata": "not-a-dict",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_submit",
                        "priority": "7",
                        "submitter": "",
                    },
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

    def fake_submit_reaction_dir(**kwargs: Any) -> dict[str, Any]:
        submit_calls.append(kwargs)
        return {
            "status": "submitted",
            "returncode": 0,
            "stdout": "status: queued\n",
            "stderr": "",
            "parsed_stdout": "not-a-dict",
        }

    monkeypatch.setattr(orca_auto, "submit_reaction_dir", fake_submit_reaction_dir)

    result = orca_auto.submit_reaction_ts_search_workflow(
        workflow_target="wf_submit_only",
        workflow_root=None,
        orca_auto_config=" /tmp/orca.yaml ",
        orca_auto_executable=" orca_auto_bin ",
        orca_auto_repo_root=" /tmp/orca_repo ",
    )

    assert submit_calls == [
        {
            "reaction_dir": "/tmp/rxn_submit",
            "priority": 7,
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        }
    ]
    assert sync_calls == []
    assert result == {
        "workflow_id": "wf_submit_only",
        "workspace_dir": str(workspace_dir),
        "status": "queued",
        "submitted": [
            {
                "stage_id": "submit_stage",
                "queue_id": "",
                "reaction_dir": "/tmp/rxn_submit",
            }
        ],
        "skipped": [],
        "failed": [],
    }

    saved_payload = saved_payloads[0]["payload"]
    foreign_stage = saved_payload["stages"][3]
    submit_stage = saved_payload["stages"][4]

    assert foreign_stage["status"] == "planned"
    assert foreign_stage["task"]["status"] == "planned"
    assert "submission_result" not in foreign_stage["task"]

    assert submit_stage["status"] == "queued"
    assert submit_stage["metadata"] == {
        "queue_id": "",
        "submission_status": "submitted",
        "submitted_at": "2026-04-19T01:00:00+00:00",
    }
    assert submit_stage["task"]["status"] == "submitted"
    assert submit_stage["task"]["metadata"] == {}
    assert submit_stage["task"]["submission_result"] == {
        "status": "submitted",
        "returncode": 0,
        "stdout": "status: queued\n",
        "stderr": "",
        "parsed_stdout": "not-a-dict",
        "submitted_at": "2026-04-19T01:00:00+00:00",
    }

    assert saved_payload["status"] == "queued"
    assert saved_payload["metadata"]["submission_summary"] == {
        "submitted_count": 1,
        "skipped_count": 0,
        "failed_count": 0,
        "stage_results": [
            {
                "stage_id": "submit_stage",
                "status": "submitted",
                "queue_id": "",
                "returncode": 0,
            }
        ],
        "updated_at": "2026-04-19T01:01:00+00:00",
    }


def test_submit_reaction_ts_search_workflow_records_failed_only_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workspace"
    payload: dict[str, Any] = {
        "workflow_id": "wf_submit_failed",
        "status": "planned",
        "metadata": {},
        "stages": [
            {
                "stage_id": "failed_stage",
                "status": "planned",
                "metadata": {},
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_failed",
                        "priority": 11,
                        "submitter": "chemstack_orca_cli",
                    },
                },
            }
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
        "2026-04-19T01:10:00+00:00",
        "2026-04-19T01:11:00+00:00",
    )

    def fake_submit_reaction_dir(**kwargs: Any) -> dict[str, Any]:
        submit_calls.append(kwargs)
        return {
            "status": "failed",
            "returncode": 5,
            "stdout": "queue_id: q_failed\n",
            "stderr": "  submit exploded  \n",
            "parsed_stdout": {"queue_id": "q_failed"},
        }

    monkeypatch.setattr(orca_auto, "submit_reaction_dir", fake_submit_reaction_dir)

    result = orca_auto.submit_reaction_ts_search_workflow(
        workflow_target="wf_submit_failed",
        workflow_root=tmp_path / "workflow_root",
        orca_auto_config="/tmp/orca.yaml",
    )

    assert submit_calls == [
        {
            "reaction_dir": "/tmp/rxn_failed",
            "priority": 11,
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto",
            "repo_root": None,
        }
    ]
    assert len(sync_calls) == 1
    assert result == {
        "workflow_id": "wf_submit_failed",
        "workspace_dir": str(workspace_dir),
        "status": "submission_failed",
        "submitted": [],
        "skipped": [],
        "failed": [
            {
                "stage_id": "failed_stage",
                "returncode": 5,
                "stderr": "submit exploded",
                "stdout": "queue_id: q_failed",
            }
        ],
    }

    saved_payload = saved_payloads[0]["payload"]
    failed_stage = saved_payload["stages"][0]

    assert failed_stage["status"] == "submission_failed"
    assert failed_stage["metadata"] == {
        "submission_status": "submission_failed",
        "submitted_at": "2026-04-19T01:10:00+00:00",
    }
    assert failed_stage["task"]["status"] == "submission_failed"
    assert failed_stage["task"]["submission_result"] == {
        "status": "failed",
        "returncode": 5,
        "stdout": "queue_id: q_failed\n",
        "stderr": "  submit exploded  \n",
        "parsed_stdout": {"queue_id": "q_failed"},
        "submitted_at": "2026-04-19T01:10:00+00:00",
    }

    assert saved_payload["status"] == "submission_failed"
    assert saved_payload["metadata"]["submission_summary"] == {
        "submitted_count": 0,
        "skipped_count": 0,
        "failed_count": 1,
        "stage_results": [
            {
                "stage_id": "failed_stage",
                "status": "submission_failed",
                "queue_id": "q_failed",
                "returncode": 5,
            }
        ],
        "updated_at": "2026-04-19T01:11:00+00:00",
    }


def test_cancel_reaction_ts_search_workflow_records_failed_only_summary_for_edge_cases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workspace"
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_failed",
        "status": "running",
        "metadata": {},
        "stages": [
            "not-a-stage",
            {"stage_id": "bad_task", "task": []},
            {
                "stage_id": "terminal_stage",
                "status": "completed",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/terminal",
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
            {
                "stage_id": "missing_target_stage",
                "status": "queued",
                "metadata": "not-a-dict",
                "task": {
                    "status": "planned",
                    "payload": "not-a-dict",
                    "enqueue_payload": [],
                },
            },
            {
                "stage_id": "foreign_submitter_stage",
                "status": "running",
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/foreign"},
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/foreign",
                        "submitter": "manual_submitter",
                    },
                },
            },
            {
                "stage_id": "failed_cancel_stage",
                "status": "queued",
                "metadata": {"queue_id": "q_fail"},
                "task": {
                    "status": "submitted",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/failed_cancel",
                        "submitter": "",
                    },
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

    def fake_cancel_target(**kwargs: Any) -> dict[str, Any]:
        cancel_calls.append(kwargs)
        return {
            "status": "failed",
            "returncode": 9,
            "stdout": "denied\n",
            "stderr": "permission denied",
            "command_argv": ["orca_auto", "queue", "cancel", "q_fail"],
        }

    monkeypatch.setattr(orca_auto, "cancel_target", fake_cancel_target)

    result = orca_auto.cancel_reaction_ts_search_workflow(
        workflow_target="wf_cancel_failed",
        workflow_root=None,
        orca_auto_config=" /tmp/orca.yaml ",
    )

    assert cancel_calls == [
        {
            "target": "q_fail",
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto",
            "repo_root": None,
        }
    ]
    assert sync_calls == []
    assert result == {
        "workflow_id": "wf_cancel_failed",
        "workspace_dir": str(workspace_dir),
        "status": "cancel_failed",
        "cancelled": [],
        "requested": [],
        "skipped": [{"stage_id": "terminal_stage", "reason": "already_terminal"}],
        "failed": [
            {"stage_id": "missing_target_stage", "reason": "missing_cancel_target"},
            {
                "stage_id": "failed_cancel_stage",
                "queue_id": "q_fail",
                "reaction_dir": "/tmp/failed_cancel",
                "returncode": 9,
            },
        ],
    }

    saved_payload = saved_payloads[0]["payload"]
    terminal_stage = saved_payload["stages"][2]
    missing_target_stage = saved_payload["stages"][3]
    foreign_stage = saved_payload["stages"][4]
    failed_cancel_stage = saved_payload["stages"][5]

    assert terminal_stage["status"] == "completed"
    assert terminal_stage["task"]["status"] == "planned"

    assert missing_target_stage["status"] == "queued"
    assert missing_target_stage["metadata"] == {}
    assert missing_target_stage["task"]["status"] == "planned"
    assert missing_target_stage["task"]["cancel_result"] == {
        "status": "failed",
        "reason": "missing_cancel_target",
        "cancelled_at": "2026-04-19T01:20:00+00:00",
    }

    assert foreign_stage["status"] == "running"
    assert foreign_stage["task"]["status"] == "submitted"
    assert "cancel_result" not in foreign_stage["task"]

    assert failed_cancel_stage["status"] == "queued"
    assert failed_cancel_stage["task"]["status"] == "submitted"
    assert failed_cancel_stage["task"]["cancel_result"] == {
        "status": "failed",
        "returncode": 9,
        "stdout": "denied\n",
        "stderr": "permission denied",
        "command_argv": ["orca_auto", "queue", "cancel", "q_fail"],
        "cancelled_at": "2026-04-19T01:21:00+00:00",
        "target": "q_fail",
    }

    assert saved_payload["status"] == "cancel_failed"
    assert saved_payload["metadata"]["cancellation_summary"] == {
        "cancelled_count": 0,
        "requested_count": 0,
        "skipped_count": 1,
        "failed_count": 2,
        "stage_results": [
            {
                "stage_id": "terminal_stage",
                "status": "skipped",
                "reason": "already_terminal",
            },
            {
                "stage_id": "missing_target_stage",
                "status": "cancel_failed",
                "reason": "missing_cancel_target",
            },
            {
                "stage_id": "failed_cancel_stage",
                "status": "cancel_failed",
                "returncode": 9,
            },
        ],
        "updated_at": "2026-04-19T01:22:00+00:00",
    }
