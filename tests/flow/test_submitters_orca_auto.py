from __future__ import annotations

import sys
from copy import deepcopy
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow.submitters import orca_auto


def _completed_process(
    *,
    returncode: int,
    stdout: str,
    stderr: str = "",
    args: Any,
) -> SimpleNamespace:
    return SimpleNamespace(
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        args=args,
    )


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


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected_status", "expected_reaction_dir"),
    [
        (
            0,
            "status: queued\nqueue_id: q_123\njob_dir: /tmp/rxn_stdout\n",
            "submitted",
            "/tmp/rxn_stdout",
        ),
        (
            0,
            "status: running\nqueue_id: q_123\n",
            "failed",
            "/tmp/rxn_input",
        ),
        (
            3,
            "status: queued\nqueue_id: q_123\n",
            "failed",
            "/tmp/rxn_input",
        ),
    ],
)
def test_submit_reaction_dir_maps_queue_status(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: str,
    expected_status: str,
    expected_reaction_dir: str,
) -> None:
    sibling_calls: list[dict[str, Any]] = []

    def fake_run_sibling_app(**kwargs: Any) -> SimpleNamespace:
        sibling_calls.append(kwargs)
        return _completed_process(
            returncode=returncode,
            stdout=stdout,
            stderr="stderr text",
            args=(
                "python",
                "-m",
                "chemstack.cli",
                "--config",
                "/tmp/orca.yaml",
                "run-dir",
                "/tmp/rxn_input",
            ),
        )

    monkeypatch.setattr(orca_auto, "run_sibling_app", fake_run_sibling_app)

    result = orca_auto.submit_reaction_dir(
        reaction_dir="/tmp/rxn_input",
        priority=12,
        config_path=" /tmp/orca.yaml ",
        executable=" orca_auto_bin ",
        repo_root=" /tmp/orca_repo ",
    )

    assert sibling_calls == [
        {
            "executable": "orca_auto_bin",
            "config_path": "/tmp/orca.yaml",
            "repo_root": "/tmp/orca_repo",
            "module_name": "chemstack.cli",
            "tail_argv": ["run-dir", "/tmp/rxn_input", "--priority", "12"],
        }
    ]
    assert result["status"] == expected_status
    assert result["queue_id"] == "q_123"
    assert result["reaction_dir"] == expected_reaction_dir
    assert result["priority"] == 12
    assert result["parsed_stdout"]["status"] == stdout.splitlines()[0].split(": ", 1)[1]
    assert result["command_argv"] == [
        "python",
        "-m",
        "chemstack.cli",
        "--config",
        "/tmp/orca.yaml",
        "run-dir",
        "/tmp/rxn_input",
    ]
    assert result["stderr"] == "stderr text"


def test_submit_reaction_dir_passes_resource_override_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sibling_calls: list[dict[str, Any]] = []

    def fake_run_sibling_app(**kwargs: Any) -> SimpleNamespace:
        sibling_calls.append(kwargs)
        return _completed_process(
            returncode=0,
            stdout="status: queued\nqueue_id: q_456\n",
            stderr="",
            args=(
                "python",
                "-m",
                "chemstack.cli",
                "--config",
                "/tmp/orca.yaml",
                "run-dir",
                "/tmp/rxn_input",
            ),
        )

    monkeypatch.setattr(orca_auto, "run_sibling_app", fake_run_sibling_app)

    result = orca_auto.submit_reaction_dir(
        reaction_dir="/tmp/rxn_input",
        priority=4,
        config_path="/tmp/orca.yaml",
        max_cores=16,
        max_memory_gb=64,
        executable="orca_auto_bin",
    )

    assert sibling_calls == [
        {
            "executable": "orca_auto_bin",
            "config_path": "/tmp/orca.yaml",
            "repo_root": None,
            "module_name": "chemstack.cli",
            "tail_argv": [
                "run-dir",
                "/tmp/rxn_input",
                "--priority",
                "4",
                "--max-cores",
                "16",
                "--max-memory-gb",
                "64",
            ],
        }
    ]
    assert result["status"] == "submitted"


def test_submit_reaction_dir_passes_force_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sibling_calls: list[dict[str, Any]] = []

    def fake_run_sibling_app(**kwargs: Any) -> SimpleNamespace:
        sibling_calls.append(kwargs)
        return _completed_process(
            returncode=0,
            stdout="status: queued\nqueue_id: q_force\n",
            stderr="",
            args=("python", "-m", "chemstack.cli", "run-dir", "/tmp/rxn_input", "--force"),
        )

    monkeypatch.setattr(orca_auto, "run_sibling_app", fake_run_sibling_app)

    result = orca_auto.submit_reaction_dir(
        reaction_dir="/tmp/rxn_input",
        priority=4,
        config_path="/tmp/orca.yaml",
        force=True,
    )

    assert sibling_calls[0]["tail_argv"] == [
        "run-dir",
        "/tmp/rxn_input",
        "--priority",
        "4",
        "--force",
    ]
    assert result["force"] is True


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected_status"),
    [
        (0, "Cancelled: q_123\n", "cancelled"),
        (0, "Cancel requested for q_123\n", "cancel_requested"),
        (0, "Request accepted\n", "cancelled"),
        (2, "Cancelled: q_123\n", "failed"),
    ],
)
def test_cancel_target_maps_cli_cancel_status(
    monkeypatch: pytest.MonkeyPatch,
    returncode: int,
    stdout: str,
    expected_status: str,
) -> None:
    sibling_calls: list[dict[str, Any]] = []

    def fake_run_sibling_app(**kwargs: Any) -> SimpleNamespace:
        sibling_calls.append(kwargs)
        return _completed_process(
            returncode=returncode,
            stdout=stdout,
            stderr="cancel stderr",
            args="python -m chemstack.orca._internal_cli --config /tmp/orca.yaml queue cancel q_123",
        )

    monkeypatch.setattr(orca_auto, "run_sibling_app", fake_run_sibling_app)

    result = orca_auto.cancel_target(
        target="q_123",
        config_path=" /tmp/orca.yaml ",
        executable=" orca_auto_bin ",
        repo_root=" /tmp/orca_repo ",
    )

    assert sibling_calls == [
        {
            "executable": "orca_auto_bin",
            "config_path": "/tmp/orca.yaml",
            "repo_root": "/tmp/orca_repo",
            "module_name": "chemstack.orca._internal_cli",
            "tail_argv": ["queue", "cancel", "q_123"],
            "timeout_seconds": 5.0,
        }
    ]
    assert result["status"] == expected_status
    assert result["returncode"] == returncode
    assert result["stdout"] == stdout
    assert result["stderr"] == "cancel stderr"
    assert result["command_argv"] == ["python -m chemstack.orca._internal_cli --config /tmp/orca.yaml queue cancel q_123"]


def test_cancel_target_reports_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run_sibling_app(**kwargs: Any) -> SimpleNamespace:
        raise subprocess.TimeoutExpired(cmd=["python", "-m", "chemstack.orca._internal_cli", "queue", "cancel", "q_123"], timeout=5.0, output="slow", stderr="timeout")

    monkeypatch.setattr(orca_auto, "run_sibling_app", fake_run_sibling_app)

    result = orca_auto.cancel_target(
        target="q_123",
        config_path="/tmp/orca.yaml",
        executable="orca_auto_bin",
        repo_root="/tmp/orca_repo",
    )

    assert result["status"] == "failed"
    assert result["reason"] == "cancel_command_timeout"
    assert result["returncode"] == 124
    assert result["stdout"] == "slow"
    assert result["stderr"] == "timeout"


def test_submit_reaction_ts_search_workflow_updates_skip_failure_and_submit_branches(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    workflow_root = tmp_path / "workflow_root"
    payload: dict[str, Any] = {
        "workflow_id": "wf_submit",
        "status": "planned",
        "metadata": {},
        "stages": [
            {
                "stage_id": "skip_stage",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_skip",
                        "priority": 3,
                        "submitter": "chemstack_orca_cli",
                    },
                    "submission_result": {"status": "submitted"},
                },
            },
            {
                "stage_id": "missing_stage",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "priority": 4,
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
            {
                "stage_id": "submit_stage",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_submit",
                        "priority": "8",
                        "submitter": "chemstack_orca_cli",
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
        "2026-04-19T00:00:00+00:00",
        "2026-04-19T00:01:00+00:00",
        "2026-04-19T00:02:00+00:00",
    )

    def fake_submit_reaction_dir(**kwargs: Any) -> dict[str, Any]:
        submit_calls.append(kwargs)
        return {
            "status": "submitted",
            "returncode": 0,
            "stdout": "status: queued\nqueue_id: q_submit\njob_dir: /tmp/rxn_stdout\n",
            "stderr": "",
            "parsed_stdout": {
                "status": "queued",
                "queue_id": "q_submit",
                "job_dir": "/tmp/rxn_stdout",
            },
            "queue_id": "q_submit",
            "reaction_dir": "/tmp/rxn_stdout",
            "priority": 8,
        }

    monkeypatch.setattr(orca_auto, "submit_reaction_dir", fake_submit_reaction_dir)

    result = orca_auto.submit_reaction_ts_search_workflow(
        workflow_target="wf_submit",
        workflow_root=workflow_root,
        orca_auto_config=" /tmp/orca.yaml ",
        orca_auto_executable=" orca_auto_bin ",
        orca_auto_repo_root=" /tmp/orca_repo ",
    )

    assert submit_calls == [
        {
            "reaction_dir": "/tmp/rxn_submit",
            "priority": 8,
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        }
    ]
    assert result == {
        "workflow_id": "wf_submit",
        "workspace_dir": str(workspace_dir),
        "status": "queued",
        "submitted": [
            {
                "stage_id": "submit_stage",
                "queue_id": "q_submit",
                "reaction_dir": "/tmp/rxn_stdout",
            }
        ],
        "skipped": [{"stage_id": "skip_stage", "reason": "already_submitted"}],
        "failed": [{"stage_id": "missing_stage", "reason": "missing_reaction_dir"}],
    }
    assert len(saved_payloads) == 1
    assert len(sync_calls) == 1
    assert sync_calls[0]["workflow_root"] == workflow_root
    assert sync_calls[0]["workspace_dir"] == workspace_dir

    saved_payload = saved_payloads[0]["payload"]
    skip_stage, missing_stage, submit_stage = saved_payload["stages"]

    assert missing_stage["status"] == "submission_failed"
    assert missing_stage["metadata"] == {
        "submission_status": "submission_failed",
        "submitted_at": "2026-04-19T00:00:00+00:00",
    }
    assert missing_stage["task"]["status"] == "submission_failed"
    assert missing_stage["task"]["submission_result"] == {
        "status": "failed",
        "reason": "missing_reaction_dir",
        "submitted_at": "2026-04-19T00:00:00+00:00",
    }

    assert submit_stage["status"] == "queued"
    assert submit_stage["metadata"] == {
        "queue_id": "q_submit",
        "submission_status": "submitted",
        "submitted_at": "2026-04-19T00:01:00+00:00",
    }
    assert submit_stage["task"]["status"] == "submitted"
    assert submit_stage["task"]["submission_result"]["status"] == "submitted"
    assert submit_stage["task"]["submission_result"]["submitted_at"] == "2026-04-19T00:01:00+00:00"

    assert skip_stage["task"]["submission_result"] == {"status": "submitted"}

    assert saved_payload["status"] == "queued"
    assert saved_payload["metadata"]["submission_summary"] == {
        "status": "partially_submitted",
        "submitted_count": 1,
        "skipped_count": 1,
        "failed_count": 1,
        "stage_results": [
            {
                "stage_id": "skip_stage",
                "status": "skipped",
                "reason": "already_submitted",
            },
            {
                "stage_id": "missing_stage",
                "status": "submission_failed",
                "reason": "missing_reaction_dir",
            },
            {
                "stage_id": "submit_stage",
                "status": "submitted",
                "queue_id": "q_submit",
                "returncode": 0,
            },
        ],
        "updated_at": "2026-04-19T00:02:00+00:00",
    }


def test_cancel_reaction_ts_search_workflow_handles_local_cancel_and_config_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    workflow_root = tmp_path / "workflow_root"
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_local",
        "status": "queued",
        "metadata": {},
        "stages": [
            {
                "stage_id": "local_stage",
                "status": "planned",
                "task": {
                    "status": "planned",
                    "enqueue_payload": {
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
            {
                "stage_id": "needs_config_stage",
                "status": "queued",
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/rxn_needs_config"},
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_needs_config",
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
            {
                "stage_id": "skip_cancelled_stage",
                "status": "cancelled",
                "task": {
                    "status": "cancelled",
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_cancelled",
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
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
    _install_timestamps(
        monkeypatch,
        "2026-04-19T00:10:00+00:00",
        "2026-04-19T00:11:00+00:00",
        "2026-04-19T00:12:00+00:00",
    )
    monkeypatch.setattr(
        orca_auto,
        "cancel_target",
        lambda **kwargs: pytest.fail("cancel_target should not run without config"),
    )

    result = orca_auto.cancel_reaction_ts_search_workflow(
        workflow_target="wf_cancel_local",
        workflow_root=workflow_root,
        orca_auto_config=None,
    )

    assert result == {
        "workflow_id": "wf_cancel_local",
        "workspace_dir": str(workspace_dir),
        "status": "cancelled",
        "cancelled": [{"stage_id": "local_stage", "mode": "local"}],
        "requested": [],
        "skipped": [{"stage_id": "skip_cancelled_stage", "reason": "already_cancelled"}],
        "failed": [{"stage_id": "needs_config_stage", "reason": "orca_auto_config_required"}],
    }
    assert len(saved_payloads) == 1
    assert len(sync_calls) == 1

    saved_payload = saved_payloads[0]["payload"]
    local_stage, needs_config_stage, skip_cancelled_stage = saved_payload["stages"]

    assert local_stage["status"] == "cancelled"
    assert local_stage["metadata"] == {
        "cancel_status": "cancelled",
        "cancelled_at": "2026-04-19T00:10:00+00:00",
    }
    assert local_stage["task"]["status"] == "cancelled"
    assert local_stage["task"]["cancel_result"] == {
        "status": "cancelled",
        "cancelled_at": "2026-04-19T00:10:00+00:00",
        "mode": "local",
    }

    assert needs_config_stage["status"] == "queued"
    assert needs_config_stage["metadata"] == {}
    assert needs_config_stage["task"]["status"] == "submitted"
    assert needs_config_stage["task"]["cancel_result"] == {
        "status": "failed",
        "reason": "orca_auto_config_required",
        "cancelled_at": "2026-04-19T00:11:00+00:00",
    }

    assert skip_cancelled_stage["status"] == "cancelled"
    assert skip_cancelled_stage["task"]["status"] == "cancelled"

    assert saved_payload["status"] == "cancelled"
    assert saved_payload["metadata"]["cancellation_summary"] == {
        "cancelled_count": 1,
        "requested_count": 0,
        "skipped_count": 1,
        "failed_count": 1,
        "stage_results": [
            {"stage_id": "local_stage", "status": "cancelled", "mode": "local"},
            {
                "stage_id": "needs_config_stage",
                "status": "cancel_failed",
                "reason": "orca_auto_config_required",
            },
            {
                "stage_id": "skip_cancelled_stage",
                "status": "skipped",
                "reason": "already_cancelled",
            },
        ],
        "updated_at": "2026-04-19T00:12:00+00:00",
    }


def test_cancel_reaction_ts_search_workflow_records_requested_and_cancelled_statuses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace_dir = tmp_path / "workflow_workspace"
    workflow_root = tmp_path / "workflow_root"
    payload: dict[str, Any] = {
        "workflow_id": "wf_cancel_remote",
        "status": "running",
        "metadata": {},
        "stages": [
            {
                "stage_id": "request_stage",
                "status": "running",
                "metadata": {"queue_id": "q_request"},
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/rxn_request"},
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_request",
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
            {
                "stage_id": "cancel_stage",
                "status": "queued",
                "task": {
                    "status": "submitted",
                    "payload": {"reaction_dir": "/tmp/rxn_cancel"},
                    "enqueue_payload": {
                        "reaction_dir": "/tmp/rxn_cancel",
                        "submitter": "chemstack_orca_cli",
                    },
                },
            },
        ],
    }
    saved_payloads: list[dict[str, Any]] = []
    sync_calls: list[dict[str, Any]] = []
    cancel_calls: list[dict[str, Any]] = []
    cancel_responses = iter(
        [
            {
                "status": "cancel_requested",
                "returncode": 0,
                "stdout": "Cancel requested for q_request\n",
                "stderr": "",
                "command_argv": ["orca_auto_bin", "queue", "cancel", "q_request"],
            },
            {
                "status": "cancelled",
                "returncode": 0,
                "stdout": "Cancelled: /tmp/rxn_cancel\n",
                "stderr": "",
                "command_argv": ["orca_auto_bin", "queue", "cancel", "/tmp/rxn_cancel"],
            },
        ]
    )

    _install_workflow_io(
        monkeypatch,
        payload=payload,
        workspace_dir=workspace_dir,
        saved_payloads=saved_payloads,
        sync_calls=sync_calls,
    )
    _install_timestamps(
        monkeypatch,
        "2026-04-19T00:20:00+00:00",
        "2026-04-19T00:21:00+00:00",
        "2026-04-19T00:22:00+00:00",
    )

    def fake_cancel_target(**kwargs: Any) -> dict[str, Any]:
        cancel_calls.append(kwargs)
        return dict(next(cancel_responses))

    monkeypatch.setattr(orca_auto, "cancel_target", fake_cancel_target)

    result = orca_auto.cancel_reaction_ts_search_workflow(
        workflow_target="wf_cancel_remote",
        workflow_root=workflow_root,
        orca_auto_config=" /tmp/orca.yaml ",
        orca_auto_executable=" orca_auto_bin ",
        orca_auto_repo_root=" /tmp/orca_repo ",
    )

    assert cancel_calls == [
        {
            "target": "q_request",
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        },
        {
            "target": "/tmp/rxn_cancel",
            "config_path": "/tmp/orca.yaml",
            "executable": "orca_auto_bin",
            "repo_root": "/tmp/orca_repo",
        },
    ]
    assert result == {
        "workflow_id": "wf_cancel_remote",
        "workspace_dir": str(workspace_dir),
        "status": "cancel_requested",
        "cancelled": [
            {
                "stage_id": "cancel_stage",
                "queue_id": "",
                "reaction_dir": "/tmp/rxn_cancel",
            }
        ],
        "requested": [
            {
                "stage_id": "request_stage",
                "queue_id": "q_request",
                "reaction_dir": "/tmp/rxn_request",
            }
        ],
        "skipped": [],
        "failed": [],
    }
    assert len(saved_payloads) == 1
    assert len(sync_calls) == 1

    saved_payload = saved_payloads[0]["payload"]
    request_stage, cancel_stage = saved_payload["stages"]

    assert request_stage["status"] == "cancel_requested"
    assert request_stage["metadata"] == {
        "queue_id": "q_request",
        "cancel_status": "cancel_requested",
        "cancelled_at": "2026-04-19T00:20:00+00:00",
    }
    assert request_stage["task"]["status"] == "cancel_requested"
    assert request_stage["task"]["cancel_result"] == {
        "status": "cancel_requested",
        "returncode": 0,
        "stdout": "Cancel requested for q_request\n",
        "stderr": "",
        "command_argv": ["orca_auto_bin", "queue", "cancel", "q_request"],
        "cancelled_at": "2026-04-19T00:20:00+00:00",
        "target": "q_request",
    }

    assert cancel_stage["status"] == "cancelled"
    assert cancel_stage["metadata"] == {
        "cancel_status": "cancelled",
        "cancelled_at": "2026-04-19T00:21:00+00:00",
    }
    assert cancel_stage["task"]["status"] == "cancelled"
    assert cancel_stage["task"]["cancel_result"] == {
        "status": "cancelled",
        "returncode": 0,
        "stdout": "Cancelled: /tmp/rxn_cancel\n",
        "stderr": "",
        "command_argv": ["orca_auto_bin", "queue", "cancel", "/tmp/rxn_cancel"],
        "cancelled_at": "2026-04-19T00:21:00+00:00",
        "target": "/tmp/rxn_cancel",
    }

    assert saved_payload["status"] == "cancel_requested"
    assert saved_payload["metadata"]["cancellation_summary"] == {
        "cancelled_count": 1,
        "requested_count": 1,
        "skipped_count": 0,
        "failed_count": 0,
        "stage_results": [
            {"stage_id": "request_stage", "status": "cancel_requested"},
            {"stage_id": "cancel_stage", "status": "cancelled"},
        ],
        "updated_at": "2026-04-19T00:22:00+00:00",
    }
