from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import operations


def _record(
    workflow_id: str,
    *,
    template_name: str = "reaction_ts_search",
    status: str = "completed",
    stage_count: int = 1,
    stage_status_counts: dict[str, int] | None = None,
    task_status_counts: dict[str, int] | None = None,
    submission_summary: dict[str, int] | None = None,
    metadata: dict[str, Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_id=workflow_id,
        template_name=template_name,
        status=status,
        source_job_id=f"job-{workflow_id}",
        source_job_type="reaction_ts_search",
        reaction_key=f"rxn-{workflow_id}",
        requested_at="2026-04-19T00:00:00+00:00",
        workspace_dir=f"/tmp/{workflow_id}",
        workflow_file=f"/tmp/{workflow_id}/workflow.json",
        stage_count=stage_count,
        updated_at="2026-04-19T01:00:00+00:00",
        stage_status_counts=dict(stage_status_counts or {status: stage_count}),
        task_status_counts=dict(task_status_counts or {status: stage_count}),
        submission_summary=dict(submission_summary or {"submitted": stage_count}),
        metadata=dict(metadata or {"tag": workflow_id}),
    )


def test_list_workflows_without_refresh_uses_registry_listing_and_limit(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    records = [
        _record("wf-1", stage_count=2),
        _record("wf-2", status="running", template_name="conformer_screening"),
    ]
    captured: dict[str, Any] = {}

    def fake_list_workflow_registry(root: str | Path) -> list[SimpleNamespace]:
        captured["listed_root"] = root
        return records

    def fake_reindex_workflow_registry(root: str | Path) -> list[SimpleNamespace]:
        raise AssertionError(f"unexpected refresh for {root}")

    def fake_worker_state_path(root: Path) -> Path:
        captured["worker_state_root"] = root
        return root / "worker_state.json"

    def fake_journal_path(root: Path) -> Path:
        captured["journal_root"] = root
        return root / "journal.jsonl"

    def fake_load_workflow_worker_state(root: Path) -> dict[str, Any]:
        captured["worker_state_loaded_from"] = root
        return {"active_workflows": 1}

    monkeypatch.setattr(operations, "list_workflow_registry", fake_list_workflow_registry)
    monkeypatch.setattr(operations, "reindex_workflow_registry", fake_reindex_workflow_registry)
    monkeypatch.setattr(operations, "workflow_worker_state_path", fake_worker_state_path)
    monkeypatch.setattr(operations, "workflow_journal_path", fake_journal_path)
    monkeypatch.setattr(operations, "load_workflow_worker_state", fake_load_workflow_worker_state)

    result = operations.list_workflows(workflow_root=workflow_root, limit=1, refresh=False)
    resolved_root = workflow_root.expanduser().resolve()

    assert captured == {
        "listed_root": workflow_root,
        "worker_state_root": resolved_root,
        "journal_root": resolved_root,
        "worker_state_loaded_from": resolved_root,
    }
    assert result == {
        "workflow_root": str(resolved_root),
        "worker_state_file": str(resolved_root / "worker_state.json"),
        "journal_file": str(resolved_root / "journal.jsonl"),
        "worker_state": {"active_workflows": 1},
        "count": 1,
        "workflows": [
            {
                "workflow_id": "wf-1",
                "template_name": "reaction_ts_search",
                "status": "completed",
                "source_job_id": "job-wf-1",
                "source_job_type": "reaction_ts_search",
                "reaction_key": "rxn-wf-1",
                "requested_at": "2026-04-19T00:00:00+00:00",
                "workspace_dir": "/tmp/wf-1",
                "workflow_file": "/tmp/wf-1/workflow.json",
                "stage_count": 2,
                "updated_at": "2026-04-19T01:00:00+00:00",
                "stage_status_counts": {"completed": 2},
                "task_status_counts": {"completed": 2},
                "submission_summary": {"submitted": 2},
                "metadata": {"tag": "wf-1"},
            }
        ],
    }


def test_list_workflows_refresh_reindexes_registry(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    records = [_record("wf-refresh", template_name="conformer_screening", status="running")]
    captured: dict[str, Any] = {}

    def fake_list_workflow_registry(root: str | Path) -> list[SimpleNamespace]:
        raise AssertionError(f"unexpected non-refresh listing for {root}")

    def fake_reindex_workflow_registry(root: str | Path) -> list[SimpleNamespace]:
        captured["reindexed_root"] = root
        return records

    monkeypatch.setattr(operations, "list_workflow_registry", fake_list_workflow_registry)
    monkeypatch.setattr(operations, "reindex_workflow_registry", fake_reindex_workflow_registry)
    monkeypatch.setattr(operations, "workflow_worker_state_path", lambda root: root / "worker_state.json")
    monkeypatch.setattr(operations, "workflow_journal_path", lambda root: root / "journal.jsonl")
    monkeypatch.setattr(operations, "load_workflow_worker_state", lambda root: {"active_workflows": 2})

    result = operations.list_workflows(workflow_root=workflow_root, refresh=True)

    assert captured == {"reindexed_root": workflow_root}
    assert result["count"] == 1
    assert result["workflows"][0]["workflow_id"] == "wf-refresh"
    assert result["workflows"][0]["template_name"] == "conformer_screening"
    assert result["workflows"][0]["status"] == "running"


def test_get_workflow_syncs_registry_when_requested(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    workspace_dir = workflow_root / "workflows" / "wf-1"
    payload = {"workflow_id": "wf-1", "status": "running"}
    summary = {"workflow_id": "wf-1", "workspace_dir": str(workspace_dir), "status": "running"}
    synced_record = _record(
        "wf-1",
        status="running",
        stage_count=3,
        stage_status_counts={"running": 2, "completed": 1},
        task_status_counts={"submitted": 1, "running": 2},
        submission_summary={"submitted": 3},
        metadata={"synced": True},
    )
    captured: dict[str, Any] = {}

    def fake_resolve_workflow_workspace(*, target: str, workflow_root: str | Path | None) -> Path:
        captured["resolve_args"] = {"target": target, "workflow_root": workflow_root}
        return workspace_dir

    def fake_load_workflow_payload(path: Path) -> dict[str, Any]:
        captured["payload_path"] = path
        return payload

    def fake_workflow_summary(path: Path, payload_arg: dict[str, Any]) -> dict[str, Any]:
        captured["summary_args"] = {"path": path, "payload": payload_arg}
        return summary

    def fake_sync_workflow_registry(root: str | Path, path: Path, payload_arg: dict[str, Any]) -> SimpleNamespace:
        captured["sync_args"] = {"workflow_root": root, "workspace_dir": path, "payload": payload_arg}
        return synced_record

    def fake_resolve_workflow_registry_record(root: str | Path, target: str) -> SimpleNamespace | None:
        raise AssertionError(f"unexpected registry lookup for {root} / {target}")

    monkeypatch.setattr(operations, "resolve_workflow_workspace", fake_resolve_workflow_workspace)
    monkeypatch.setattr(operations, "load_workflow_payload", fake_load_workflow_payload)
    monkeypatch.setattr(operations, "workflow_summary", fake_workflow_summary)
    monkeypatch.setattr(operations, "sync_workflow_registry", fake_sync_workflow_registry)
    monkeypatch.setattr(operations, "resolve_workflow_registry_record", fake_resolve_workflow_registry_record)
    monkeypatch.setattr(operations, "load_workflow_worker_state", lambda root: {"active_workflows": 1})

    result = operations.get_workflow(target="wf-1", workflow_root=workflow_root, sync_registry=True)

    assert captured == {
        "resolve_args": {"target": "wf-1", "workflow_root": workflow_root},
        "payload_path": workspace_dir,
        "summary_args": {"path": workspace_dir, "payload": payload},
        "sync_args": {"workflow_root": workflow_root, "workspace_dir": workspace_dir, "payload": payload},
    }
    assert result == {
        "summary": summary,
        "registry_record": {
            "workflow_id": "wf-1",
            "template_name": "reaction_ts_search",
            "status": "running",
            "workspace_dir": "/tmp/wf-1",
            "workflow_file": "/tmp/wf-1/workflow.json",
            "stage_count": 3,
            "updated_at": "2026-04-19T01:00:00+00:00",
            "stage_status_counts": {"running": 2, "completed": 1},
            "task_status_counts": {"submitted": 1, "running": 2},
            "submission_summary": {"submitted": 3},
            "metadata": {"synced": True},
        },
        "worker_state": {"active_workflows": 1},
        "workflow": payload,
    }


def test_get_workflow_without_sync_uses_registry_lookup(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    workspace_dir = workflow_root / "workflows" / "wf-2"
    payload = {"workflow_id": "wf-2", "status": "completed"}
    summary = {"workflow_id": "wf-2", "workspace_dir": str(workspace_dir), "status": "completed"}
    resolved_record = _record(
        "wf-2",
        template_name="conformer_screening",
        metadata={"synced": False},
    )
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        operations,
        "resolve_workflow_workspace",
        lambda *, target, workflow_root: workspace_dir,
    )
    monkeypatch.setattr(operations, "load_workflow_payload", lambda path: payload)
    monkeypatch.setattr(operations, "workflow_summary", lambda path, payload_arg: summary)

    def fake_sync_workflow_registry(root: str | Path, path: Path, payload_arg: dict[str, Any]) -> SimpleNamespace:
        raise AssertionError(f"unexpected sync for {root} / {path}")

    def fake_resolve_workflow_registry_record(root: str | Path, target: str) -> SimpleNamespace | None:
        captured["registry_lookup"] = {"workflow_root": root, "target": target}
        return resolved_record

    monkeypatch.setattr(operations, "sync_workflow_registry", fake_sync_workflow_registry)
    monkeypatch.setattr(operations, "resolve_workflow_registry_record", fake_resolve_workflow_registry_record)
    monkeypatch.setattr(operations, "load_workflow_worker_state", lambda root: {"active_workflows": 0})

    result = operations.get_workflow(target="wf-2", workflow_root=workflow_root, sync_registry=False)

    assert captured == {"registry_lookup": {"workflow_root": workflow_root, "target": "wf-2"}}
    assert result["summary"] == summary
    assert result["registry_record"] == {
        "workflow_id": "wf-2",
        "template_name": "conformer_screening",
        "status": "completed",
        "workspace_dir": "/tmp/wf-2",
        "workflow_file": "/tmp/wf-2/workflow.json",
        "stage_count": 1,
        "updated_at": "2026-04-19T01:00:00+00:00",
        "stage_status_counts": {"completed": 1},
        "task_status_counts": {"completed": 1},
        "submission_summary": {"submitted": 1},
        "metadata": {"synced": False},
    }
    assert result["worker_state"] == {"active_workflows": 0}
    assert result["workflow"] == payload


def test_get_workflow_artifacts_syncs_registry_and_counts_artifacts(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    workspace_dir = workflow_root / "workflows" / "wf-artifacts"
    payload = {"workflow_id": "wf-artifacts", "status": "completed"}
    summary = {"workflow_id": "wf-artifacts", "workspace_dir": str(workspace_dir)}
    artifacts = [
        {"kind": "report", "path": str(workspace_dir / "report.md")},
        {"kind": "structure", "path": str(workspace_dir / "final.xyz")},
    ]
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        operations,
        "resolve_workflow_workspace",
        lambda *, target, workflow_root: workspace_dir,
    )
    monkeypatch.setattr(operations, "load_workflow_payload", lambda path: payload)
    monkeypatch.setattr(operations, "workflow_summary", lambda path, payload_arg: summary)
    monkeypatch.setattr(operations, "workflow_artifacts", lambda path, payload_arg: artifacts)

    def fake_sync_workflow_registry(root: str | Path, path: Path, payload_arg: dict[str, Any]) -> SimpleNamespace:
        captured["sync_args"] = {"workflow_root": root, "workspace_dir": path, "payload": payload_arg}
        return _record("wf-artifacts")

    monkeypatch.setattr(operations, "sync_workflow_registry", fake_sync_workflow_registry)

    result = operations.get_workflow_artifacts(
        target="wf-artifacts",
        workflow_root=workflow_root,
        sync_registry=True,
    )

    assert captured == {
        "sync_args": {
            "workflow_root": workflow_root,
            "workspace_dir": workspace_dir,
            "payload": payload,
        }
    }
    assert result == {
        "workflow_id": "wf-artifacts",
        "workspace_dir": str(workspace_dir),
        "artifact_count": 2,
        "artifacts": artifacts,
    }


def test_get_workflow_runtime_status_returns_resolved_paths_and_state(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    captured: dict[str, Any] = {}

    def fake_worker_state_path(root: Path) -> Path:
        captured["worker_state_root"] = root
        return root / "worker_state.json"

    def fake_journal_path(root: Path) -> Path:
        captured["journal_root"] = root
        return root / "journal.jsonl"

    def fake_load_workflow_worker_state(root: Path) -> dict[str, Any]:
        captured["state_loaded_from"] = root
        return {"running": ["wf-1"]}

    monkeypatch.setattr(operations, "workflow_worker_state_path", fake_worker_state_path)
    monkeypatch.setattr(operations, "workflow_journal_path", fake_journal_path)
    monkeypatch.setattr(operations, "load_workflow_worker_state", fake_load_workflow_worker_state)

    result = operations.get_workflow_runtime_status(workflow_root=workflow_root)
    resolved_root = workflow_root.expanduser().resolve()

    assert captured == {
        "worker_state_root": resolved_root,
        "journal_root": resolved_root,
        "state_loaded_from": resolved_root,
    }
    assert result == {
        "workflow_root": str(resolved_root),
        "worker_state_file": str(resolved_root / "worker_state.json"),
        "journal_file": str(resolved_root / "journal.jsonl"),
        "worker_state": {"running": ["wf-1"]},
    }


def test_get_workflow_journal_returns_events_and_count(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    events = [{"event_type": "workflow_started"}, {"event_type": "workflow_finished"}]
    captured: dict[str, Any] = {}

    def fake_list_workflow_journal(root: Path, limit: int) -> list[dict[str, Any]]:
        captured["journal_args"] = {"workflow_root": root, "limit": limit}
        return events

    monkeypatch.setattr(operations, "list_workflow_journal", fake_list_workflow_journal)
    monkeypatch.setattr(operations, "workflow_journal_path", lambda root: root / "journal.jsonl")

    result = operations.get_workflow_journal(workflow_root=workflow_root, limit=7)
    resolved_root = workflow_root.expanduser().resolve()

    assert captured == {"journal_args": {"workflow_root": resolved_root, "limit": 7}}
    assert result == {
        "workflow_root": str(resolved_root),
        "journal_file": str(resolved_root / "journal.jsonl"),
        "count": 2,
        "events": events,
    }


def test_get_workflow_telemetry_aggregates_registry_and_journal_data(monkeypatch, tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow-root"
    records = [
        _record("wf-1", status="completed", template_name="reaction_ts_search"),
        _record("wf-2", status="running", template_name="reaction_ts_search"),
        _record("wf-3", status="running", template_name="conformer_screening"),
    ]
    events = [
        {"event_type": "workflow_status_changed", "id": "s1"},
        {"event_type": "workflow_advance_failed", "id": "f1"},
        {"event_type": "workflow_advance_failed", "id": "f2"},
        {"event_type": "workflow_status_changed", "id": "s2"},
        {"event_type": "workflow_advance_failed", "id": "f3"},
        {"event_type": " workflow_status_changed ", "id": "s3"},
        {"event_type": "workflow_advance_failed", "id": "f4"},
        {"event_type": "workflow_advance_failed", "id": "f5"},
        {"event_type": "workflow_advance_failed", "id": "f6"},
        {"event_type": "workflow_status_changed", "id": "s4"},
        {"event_type": "workflow_status_changed", "id": "s5"},
        {"event_type": "workflow_status_changed", "id": "s6"},
        {"event_type": "", "id": "blank"},
    ]

    monkeypatch.setattr(operations, "list_workflow_registry", lambda root: records)
    monkeypatch.setattr(operations, "load_workflow_worker_state", lambda root: {"active_workflows": 2})
    monkeypatch.setattr(operations, "list_workflow_journal", lambda root, limit: events)
    monkeypatch.setattr(operations, "workflow_journal_path", lambda root: root / "journal.jsonl")
    monkeypatch.setattr(operations, "workflow_worker_state_path", lambda root: root / "worker_state.json")

    result = operations.get_workflow_telemetry(workflow_root=workflow_root, limit=99)
    resolved_root = workflow_root.expanduser().resolve()

    assert result == {
        "workflow_root": str(resolved_root),
        "registry_count": 3,
        "worker_state": {"active_workflows": 2},
        "workflow_status_counts": {"completed": 1, "running": 2},
        "template_counts": {"reaction_ts_search": 2, "conformer_screening": 1},
        "journal_event_count": 13,
        "journal_event_type_counts": {
            "workflow_status_changed": 6,
            "workflow_advance_failed": 6,
        },
        "recent_failures": [events[1], events[2], events[4], events[6], events[7]],
        "recent_status_changes": [events[0], events[3], events[5], events[9], events[10]],
        "journal_file": str(resolved_root / "journal.jsonl"),
        "worker_state_file": str(resolved_root / "worker_state.json"),
    }


@pytest.mark.parametrize(
    ("wrapper_name", "impl_name", "call_kwargs", "expected_kwargs", "expected_result"),
    [
        (
            "create_reaction_workflow",
            "create_reaction_ts_search_workflow",
            {
                "reactant_xyz": "reactant.xyz",
                "product_xyz": "product.xyz",
                "workflow_root": "/tmp/workflows",
                "priority": "high",
            },
            {
                "reactant_xyz": "reactant.xyz",
                "product_xyz": "product.xyz",
                "workflow_root": "/tmp/workflows",
                "priority": "high",
            },
            {"workflow_id": "wf-create"},
        ),
        (
            "cancel_workflow",
            "cancel_materialized_workflow",
            {
                "target": "wf-cancel",
                "workflow_root": None,
                "crest_auto_config": "crest.yaml",
                "xtb_auto_executable": "xtb-custom",
                "orca_auto_repo_root": "/opt/orca_auto",
            },
            {
                "target": "wf-cancel",
                "workflow_root": "",
                "crest_auto_config": "crest.yaml",
                "crest_auto_executable": "crest_auto",
                "crest_auto_repo_root": None,
                "xtb_auto_config": None,
                "xtb_auto_executable": "xtb-custom",
                "xtb_auto_repo_root": None,
                "orca_auto_config": None,
                "orca_auto_executable": "orca_auto",
                "orca_auto_repo_root": "/opt/orca_auto",
            },
            {"status": "cancel_requested"},
        ),
        (
            "advance_materialized_workflow",
            "advance_workflow",
            {
                "target": "wf-advance",
                "workflow_root": "/tmp/workflows",
                "submit_ready": False,
                "dry_run": True,
            },
            {
                "target": "wf-advance",
                "workflow_root": "/tmp/workflows",
                "submit_ready": False,
                "dry_run": True,
            },
            {"status": "advanced"},
        ),
    ],
)
def test_wrapper_functions_forward_arguments(
    monkeypatch,
    wrapper_name: str,
    impl_name: str,
    call_kwargs: dict[str, Any],
    expected_kwargs: dict[str, Any],
    expected_result: dict[str, Any],
) -> None:
    captured: dict[str, Any] = {}

    def fake_impl(**kwargs: Any) -> dict[str, Any]:
        captured["kwargs"] = kwargs
        return expected_result

    monkeypatch.setattr(operations, impl_name, fake_impl)

    result = getattr(operations, wrapper_name)(**call_kwargs)

    assert captured == {"kwargs": expected_kwargs}
    assert result == expected_result
