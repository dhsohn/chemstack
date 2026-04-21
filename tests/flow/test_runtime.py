from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import runtime


def _registry_record(
    *,
    workflow_id: str,
    status: str,
    template_name: str = "reaction_ts_search",
    workspace_dir: str = "/tmp/workflow_workspace",
    stage_count: int = 1,
) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_id=workflow_id,
        status=status,
        template_name=template_name,
        workspace_dir=workspace_dir,
        stage_count=stage_count,
    )


def _capture_worker_side_effects(
    monkeypatch: pytest.MonkeyPatch,
    *,
    records: list[SimpleNamespace],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    state_calls: list[dict[str, Any]] = []
    journal_calls: list[dict[str, Any]] = []
    registry_calls = {"list": 0, "reindex": 0}
    timestamps = iter(
        [
            "2026-04-19T00:00:00+00:00",
            "2026-04-19T00:01:00+00:00",
        ]
    )

    monkeypatch.setattr(runtime, "now_utc_iso", lambda: next(timestamps))

    def fake_write_workflow_worker_state(root: Path, **kwargs: Any) -> None:
        state_calls.append({"root": root, **kwargs})

    def fake_append_workflow_journal_event(root: Path, **kwargs: Any) -> None:
        journal_calls.append({"root": root, **kwargs})

    def fake_list_workflow_registry(root: Path) -> list[SimpleNamespace]:
        registry_calls["list"] += 1
        return list(records)

    def fake_reindex_workflow_registry(root: Path) -> list[SimpleNamespace]:
        registry_calls["reindex"] += 1
        return list(records)

    monkeypatch.setattr(runtime, "write_workflow_worker_state", fake_write_workflow_worker_state)
    monkeypatch.setattr(runtime, "append_workflow_journal_event", fake_append_workflow_journal_event)
    monkeypatch.setattr(runtime, "list_workflow_registry", fake_list_workflow_registry)
    monkeypatch.setattr(runtime, "reindex_workflow_registry", fake_reindex_workflow_registry)
    return state_calls, journal_calls, registry_calls


def _always_false_after_append(sync_checks: list[str], workspace_dir: object) -> bool:
    sync_checks.append(str(workspace_dir))
    return False


def test_workflow_worker_lock_path_expands_home_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    lock_path = runtime.workflow_worker_lock_path("~/chem_root")

    assert lock_path == (tmp_path / "chem_root").resolve() / runtime.WORKFLOW_WORKER_LOCK_NAME


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError("workflow missing"),
        ValueError("workflow invalid"),
    ],
)
def test_workflow_needs_terminal_sync_returns_false_for_unreadable_payload(
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    def fake_load_workflow_payload(workspace_dir: str | Path) -> dict[str, Any]:
        raise error

    monkeypatch.setattr(runtime, "load_workflow_payload", fake_load_workflow_payload)
    monkeypatch.setattr(
        runtime,
        "workflow_has_active_downstream",
        lambda payload: pytest.fail("downstream activity should not be consulted"),
    )

    assert runtime._workflow_needs_terminal_sync("/tmp/workflow_workspace") is False


def test_workflow_needs_terminal_sync_short_circuits_for_final_child_sync_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "metadata": {"final_child_sync_pending": True},
        "stages": [],
    }

    monkeypatch.setattr(runtime, "load_workflow_payload", lambda workspace_dir: payload)
    monkeypatch.setattr(
        runtime,
        "workflow_has_active_downstream",
        lambda payload: pytest.fail("downstream activity should not be consulted"),
    )

    assert runtime._workflow_needs_terminal_sync("/tmp/workflow_workspace") is True


@pytest.mark.parametrize(
    "stage",
    [
        {"status": " running ", "task": {"status": "completed"}},
        {"status": "completed", "task": {"status": " Submitted "}},
    ],
)
def test_workflow_needs_terminal_sync_detects_active_stage_or_task_status(
    monkeypatch: pytest.MonkeyPatch,
    stage: dict[str, Any],
) -> None:
    monkeypatch.setattr(
        runtime,
        "load_workflow_payload",
        lambda workspace_dir: {"metadata": {}, "stages": [stage]},
    )
    monkeypatch.setattr(
        runtime,
        "workflow_has_active_downstream",
        lambda payload: pytest.fail("downstream activity should not be consulted"),
    )

    assert runtime._workflow_needs_terminal_sync("/tmp/workflow_workspace") is True


@pytest.mark.parametrize(("downstream_active", "expected"), [(True, True), (False, False)])
def test_workflow_needs_terminal_sync_falls_back_to_downstream_activity(
    monkeypatch: pytest.MonkeyPatch,
    downstream_active: bool,
    expected: bool,
) -> None:
    payload = {
        "metadata": {},
        "stages": [{"status": "completed", "task": {"status": "completed"}}],
    }
    downstream_checks: list[dict[str, Any]] = []

    monkeypatch.setattr(runtime, "load_workflow_payload", lambda workspace_dir: payload)

    def fake_workflow_has_active_downstream(current_payload: dict[str, Any]) -> bool:
        downstream_checks.append(current_payload)
        return downstream_active

    monkeypatch.setattr(runtime, "workflow_has_active_downstream", fake_workflow_has_active_downstream)

    assert runtime._workflow_needs_terminal_sync("/tmp/workflow_workspace") is expected
    assert downstream_checks == [payload]


def test_advance_workflow_registry_once_skips_terminal_workflow_without_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = _registry_record(
        workflow_id="wf_terminal_skip",
        status="completed",
        workspace_dir="/tmp/wf_terminal_skip",
        stage_count=2,
    )
    state_calls, journal_calls, registry_calls = _capture_worker_side_effects(
        monkeypatch,
        records=[record],
    )
    sync_checks: list[str] = []

    monkeypatch.setattr(
        runtime,
        "_workflow_needs_terminal_sync",
        lambda workspace_dir: _always_false_after_append(sync_checks, workspace_dir),
    )
    monkeypatch.setattr(
        runtime,
        "advance_workflow",
        lambda **kwargs: pytest.fail("advance_workflow should not run for skipped terminal workflows"),
    )

    result = runtime.advance_workflow_registry_once(
        workflow_root=tmp_path / "workflow_root",
        worker_session_id="session-1",
        lease_seconds=0,
    )

    assert result["workflow_root"] == str((tmp_path / "workflow_root").resolve())
    assert result["discovered_count"] == 1
    assert result["advanced_count"] == 0
    assert result["skipped_count"] == 1
    assert result["failed_count"] == 0
    assert result["workflow_results"] == [
        {
            "workflow_id": "wf_terminal_skip",
            "template_name": "reaction_ts_search",
            "previous_status": "completed",
            "status": "completed",
            "advanced": False,
            "reason": "terminal_status",
            "stage_count": 2,
        }
    ]
    assert sync_checks == ["/tmp/wf_terminal_skip"]
    assert registry_calls == {"list": 1, "reindex": 0}
    assert [call["status"] for call in state_calls] == ["running", "idle"]
    assert state_calls[-1]["metadata"] == {
        "discovered_count": 1,
        "advanced_count": 0,
        "skipped_count": 1,
        "failed_count": 0,
    }
    assert [call["event_type"] for call in journal_calls] == [
        "worker_cycle_started",
        "worker_cycle_finished",
    ]


def test_advance_workflow_registry_once_runs_terminal_child_sync_when_needed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = _registry_record(
        workflow_id="wf_terminal_sync",
        status="failed",
        workspace_dir="/tmp/wf_terminal_sync",
        stage_count=1,
    )
    state_calls, journal_calls, registry_calls = _capture_worker_side_effects(
        monkeypatch,
        records=[record],
    )
    advance_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(runtime, "_workflow_needs_terminal_sync", lambda workspace_dir: True)

    def fake_advance_workflow(**kwargs: Any) -> dict[str, Any]:
        advance_calls.append(kwargs)
        return {
            "workflow_id": "wf_terminal_sync",
            "template_name": "reaction_ts_search",
            "status": "completed",
            "stages": [{"stage_id": "s1"}, {"stage_id": "s2"}],
        }

    monkeypatch.setattr(runtime, "advance_workflow", fake_advance_workflow)

    result = runtime.advance_workflow_registry_once(
        workflow_root=tmp_path / "workflow_root",
        submit_ready=True,
        worker_session_id="session-1",
        lease_seconds=0,
    )

    assert result["advanced_count"] == 1
    assert result["skipped_count"] == 0
    assert result["failed_count"] == 0
    assert registry_calls == {"list": 1, "reindex": 0}
    assert advance_calls[0]["target"] == "wf_terminal_sync"
    assert advance_calls[0]["submit_ready"] is False
    assert result["workflow_results"] == [
        {
            "workflow_id": "wf_terminal_sync",
            "template_name": "reaction_ts_search",
            "previous_status": "failed",
            "status": "completed",
            "advanced": True,
            "changed": True,
            "reason": "terminal_child_sync",
            "stage_count": 2,
        }
    ]
    assert state_calls[-1]["metadata"] == {
        "discovered_count": 1,
        "advanced_count": 1,
        "skipped_count": 0,
        "failed_count": 0,
    }
    assert [call["event_type"] for call in journal_calls] == [
        "worker_cycle_started",
        "workflow_status_changed",
        "worker_cycle_finished",
    ]
    assert journal_calls[1]["reason"] == "terminal_child_sync"


def test_advance_workflow_registry_once_advances_non_terminal_workflow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = _registry_record(
        workflow_id="wf_running",
        status="queued",
        workspace_dir="/tmp/wf_running",
        stage_count=1,
    )
    _, journal_calls, registry_calls = _capture_worker_side_effects(
        monkeypatch,
        records=[record],
    )
    advance_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        runtime,
        "_workflow_needs_terminal_sync",
        lambda workspace_dir: pytest.fail("terminal sync checks should not run for active workflows"),
    )

    def fake_advance_workflow(**kwargs: Any) -> dict[str, Any]:
        advance_calls.append(kwargs)
        return {
            "workflow_id": "wf_running",
            "template_name": "reaction_ts_search",
            "status": "running",
            "stages": [{"stage_id": "s1"}, {"stage_id": "s2"}, {"stage_id": "s3"}],
        }

    monkeypatch.setattr(runtime, "advance_workflow", fake_advance_workflow)

    result = runtime.advance_workflow_registry_once(
        workflow_root=tmp_path / "workflow_root",
        refresh_registry=True,
        submit_ready=False,
        worker_session_id="session-1",
        lease_seconds=0,
    )

    assert result["advanced_count"] == 1
    assert result["skipped_count"] == 0
    assert result["failed_count"] == 0
    assert registry_calls == {"list": 0, "reindex": 1}
    assert advance_calls[0]["submit_ready"] is False
    assert result["workflow_results"] == [
        {
            "workflow_id": "wf_running",
            "template_name": "reaction_ts_search",
            "previous_status": "queued",
            "status": "running",
            "advanced": True,
            "changed": True,
            "stage_count": 3,
        }
    ]
    assert [call["event_type"] for call in journal_calls] == [
        "worker_cycle_started",
        "workflow_status_changed",
        "worker_cycle_finished",
    ]


def test_advance_workflow_registry_once_records_non_terminal_advance_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = _registry_record(
        workflow_id="wf_failure",
        status="running",
        workspace_dir="/tmp/wf_failure",
        stage_count=4,
    )
    state_calls, journal_calls, _ = _capture_worker_side_effects(
        monkeypatch,
        records=[record],
    )

    monkeypatch.setattr(
        runtime,
        "_workflow_needs_terminal_sync",
        lambda workspace_dir: pytest.fail("terminal sync checks should not run for active workflows"),
    )

    def fake_advance_workflow(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime, "advance_workflow", fake_advance_workflow)

    result = runtime.advance_workflow_registry_once(
        workflow_root=tmp_path / "workflow_root",
        worker_session_id="session-1",
        lease_seconds=0,
    )

    assert result["advanced_count"] == 0
    assert result["skipped_count"] == 0
    assert result["failed_count"] == 1
    assert result["workflow_results"] == [
        {
            "workflow_id": "wf_failure",
            "template_name": "reaction_ts_search",
            "previous_status": "running",
            "status": "advance_failed",
            "advanced": False,
            "reason": "boom",
            "stage_count": 4,
        }
    ]
    assert state_calls[-1]["metadata"] == {
        "discovered_count": 1,
        "advanced_count": 0,
        "skipped_count": 0,
        "failed_count": 1,
    }
    assert [call["event_type"] for call in journal_calls] == [
        "worker_cycle_started",
        "workflow_advance_failed",
        "worker_cycle_finished",
    ]
    assert journal_calls[1]["reason"] == "boom"


def test_advance_workflow_registry_once_records_terminal_child_sync_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    record = _registry_record(
        workflow_id="wf_terminal_failure",
        status="cancelled",
        workspace_dir="/tmp/wf_terminal_failure",
        stage_count=3,
    )
    _, journal_calls, _ = _capture_worker_side_effects(
        monkeypatch,
        records=[record],
    )

    monkeypatch.setattr(runtime, "_workflow_needs_terminal_sync", lambda workspace_dir: True)

    def fake_advance_workflow(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("sync broke")

    monkeypatch.setattr(runtime, "advance_workflow", fake_advance_workflow)

    result = runtime.advance_workflow_registry_once(
        workflow_root=tmp_path / "workflow_root",
        worker_session_id="session-1",
        lease_seconds=0,
    )

    assert result["advanced_count"] == 0
    assert result["skipped_count"] == 0
    assert result["failed_count"] == 1
    assert result["workflow_results"] == [
        {
            "workflow_id": "wf_terminal_failure",
            "template_name": "reaction_ts_search",
            "previous_status": "cancelled",
            "status": "advance_failed",
            "advanced": False,
            "reason": "terminal_child_sync_failed: sync broke",
            "stage_count": 3,
        }
    ]
    assert [call["event_type"] for call in journal_calls] == [
        "worker_cycle_started",
        "workflow_advance_failed",
        "worker_cycle_finished",
    ]
    assert journal_calls[1]["reason"] == "terminal_child_sync_failed: sync broke"
