from __future__ import annotations

from contextlib import contextmanager
import json
import sys
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from chemstack.flow import registry


@contextmanager
def _no_lock(*args: Any, **kwargs: Any):
    yield


def test_record_from_summary_coerces_counts_and_nested_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(registry, "now_utc_iso", lambda: "2026-04-19T00:00:00+00:00")

    record = registry._record_from_summary(
        {
            "workflow_id": "wf_1",
            "template_name": "reaction_ts_search",
            "status": "planned",
            "source_job_id": "job_1",
            "source_job_type": "xtb_path",
            "reaction_key": "rxn_1",
            "requested_at": "2026-04-19T00:00:00+00:00",
            "workspace_dir": "/tmp/workspace_1",
            "stage_count": "2",
            "stage_status_counts": {"planned": "2", "bad": "nan"},
            "task_status_counts": {"submitted": 1},
            "submission_summary": {"updated_at": "2026-04-19T00:30:00+00:00", "submitted_count": 1},
            "downstream_reaction_workflow": {"workflow_id": "child_1"},
            "precomplex_handoff": {"reactant_xyz": "/tmp/reactant.xyz"},
            "parent_workflow": {"workflow_id": "parent_1"},
            "final_child_sync_pending": 1,
        }
    )

    assert record.workflow_id == "wf_1"
    assert record.workflow_file == str(Path("/tmp/workspace_1").resolve() / "workflow.json")
    assert record.stage_count == 2
    assert record.updated_at == "2026-04-19T00:30:00+00:00"
    assert record.stage_status_counts == {"planned": 2}
    assert record.task_status_counts == {"submitted": 1}
    assert record.metadata == {
        "downstream_reaction_workflow": {"workflow_id": "child_1"},
        "precomplex_handoff": {"reactant_xyz": "/tmp/reactant.xyz"},
        "parent_workflow": {"workflow_id": "parent_1"},
        "final_child_sync_pending": True,
    }


@pytest.mark.parametrize(
    ("event", "expected_lines"),
    [
        (
            {
                "event_type": "workflow_status_changed",
                "workflow_id": "wf_1",
                "template_name": "reaction_ts_search",
                "status": "running",
                "previous_status": "planned",
                "worker_session_id": "session-1",
            },
            ["workflow=wf_1", "template=reaction_ts_search", "status=planned -> running"],
        ),
        (
            {
                "event_type": "workflow_advance_failed",
                "workflow_id": "wf_2",
                "template_name": "reaction_ts_search",
                "reason": "boom",
                "worker_session_id": "session-2",
            },
            ["workflow=wf_2", "advance_failed=boom", "worker_session=session-2"],
        ),
        (
            {
                "event_type": "workflow_stage_submitted",
                "workflow_id": "wf_stage",
                "template_name": "reaction_ts_search",
                "stage_id": "xtb_path_search_01",
                "engine": "xtb",
                "task_kind": "path_search",
                "status": "queued",
                "previous_status": "planned",
                "stage_status": "queued",
                "previous_stage_status": "planned",
                "worker_session_id": "session-stage",
            },
            [
                "workflow=wf_stage",
                "event=workflow_stage_submitted",
                "stage=xtb_path_search_01",
                "task=xtb/path_search",
                "stage_status=planned -> queued",
            ],
        ),
        (
            {
                "event_type": "workflow_stage_handoff_ready",
                "workflow_id": "wf_stage",
                "template_name": "reaction_ts_search",
                "stage_id": "xtb_path_search_01",
                "engine": "xtb",
                "task_kind": "path_search",
                "stage_status": "completed",
                "reaction_handoff_status": "ready",
                "previous_reaction_handoff_status": "queued",
                "reason": "xtb_ts_guess_ready",
                "worker_session_id": "session-handoff",
            },
            [
                "workflow=wf_stage",
                "event=workflow_stage_handoff_ready",
                "stage=xtb_path_search_01",
                "task=xtb/path_search",
                "stage_status=completed",
                "reaction_handoff_status=queued -> ready",
                "reason=xtb_ts_guess_ready",
            ],
        ),
        (
            {
                "event_type": "worker_started",
                "reason": "started",
                "worker_session_id": "session-1",
            },
            ["event=worker_started", "workflow_root=/tmp/root_3", "reason=started"],
        ),
        (
            {
                "event_type": "workflow_phase_finished",
                "workflow_id": "wf_phase",
                "template_name": "reaction_ts_search",
                "status": "mixed",
                "worker_session_id": "session-phase",
                "metadata": {
                    "phase": "xtb",
                    "phase_label": "xTB",
                    "phase_outcome": "mixed",
                    "stage_count": 2,
                    "stage_status_counts": {"completed": 2},
                    "reaction_handoff_status_counts": {"ready": 1, "failed": 1},
                    "failure_reasons": ["xtb_ts_guess_missing"],
                },
            },
            [
                "workflow=wf_phase",
                "event=workflow_phase_finished",
                "phase=xTB",
                "phase_outcome=mixed",
                "stage_status_counts=completed:2",
                "reaction_handoff_status_counts=failed:1,ready:1",
                "failure_reasons=xtb_ts_guess_missing",
            ],
        ),
        (
            {
                "event_type": "custom_event",
                "workflow_id": "wf_4",
                "status": "queued",
                "previous_status": "planned",
                "reason": "started",
                "worker_session_id": "session-1",
            },
            ["event=custom_event", "workflow=wf_4", "status=queued"],
        ),
    ],
)
def test_journal_event_message_formats_supported_event_types(
    event: dict[str, Any],
    expected_lines: list[str],
) -> None:
    message = registry._journal_event_message(event, "/tmp/root_3")

    assert message.startswith("[chem_flow]\n")
    for line in expected_lines:
        assert line in message


def test_notification_configuration_helpers_cover_default_override_and_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("CHEM_FLOW_NOTIFY_EVENT_TYPES", raising=False)
    monkeypatch.delenv("CHEM_FLOW_NOTIFY_DISABLED", raising=False)
    monkeypatch.delenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("CHEM_FLOW_TELEGRAM_CHAT_ID", raising=False)

    assert registry._notification_event_types_from_env() == set(registry.DEFAULT_NOTIFICATION_EVENT_TYPES)
    assert registry._journal_notification_enabled("workflow_status_changed") is True
    assert registry._journal_notification_enabled("workflow_stage_submitted") is True
    assert registry._journal_notification_enabled("workflow_stage_handoff_ready") is True
    assert registry._journal_notification_enabled("workflow_phase_finished") is True
    assert registry._telegram_transport_from_env() is None

    monkeypatch.setenv(
        "CHEM_FLOW_NOTIFY_EVENT_TYPES",
        "custom_event, workflow_status_changed, workflow_stage_submitted",
    )
    monkeypatch.setenv("CHEM_FLOW_NOTIFY_DISABLED", "true")
    assert registry._notification_event_types_from_env() == {
        "custom_event",
        "workflow_stage_submitted",
        "workflow_status_changed",
    }
    assert registry._journal_notification_enabled("custom_event") is False

    monkeypatch.setenv("CHEM_FLOW_NOTIFY_DISABLED", "0")
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("CHEM_FLOW_TELEGRAM_CHAT_ID", "chat-id")

    captured: dict[str, Any] = {}

    def fake_build_telegram_transport(config: Any) -> str:
        captured["bot_token"] = config.bot_token
        captured["chat_id"] = config.chat_id
        return "transport"

    monkeypatch.setattr(registry, "build_telegram_transport", fake_build_telegram_transport)

    assert registry._journal_notification_enabled("custom_event") is True
    assert registry._telegram_transport_from_env() == "transport"
    assert captured == {"bot_token": "bot-token", "chat_id": "chat-id"}


def test_maybe_notify_journal_event_sends_message_and_swallows_transport_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    sent_messages: list[str] = []

    class FakeTransport:
        def __init__(self, *, fail: bool) -> None:
            self.fail = fail

        def send_text(self, message: str) -> None:
            if self.fail:
                raise RuntimeError("transport failed")
            sent_messages.append(message)

    event = {
        "event_type": "workflow_status_changed",
        "workflow_id": "wf_notify",
        "template_name": "reaction_ts_search",
        "status": "running",
        "previous_status": "planned",
        "worker_session_id": "session-notify",
    }

    monkeypatch.setattr(registry, "_journal_notification_enabled", lambda event_type: True)
    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: FakeTransport(fail=False))
    registry._maybe_notify_journal_event(event, tmp_path)
    registry._maybe_notify_journal_event(
        {
            "event_type": "workflow_stage_submitted",
            "workflow_id": "wf_notify",
            "template_name": "reaction_ts_search",
            "stage_id": "xtb_path_search_01",
            "engine": "xtb",
            "task_kind": "path_search",
            "metadata": {"engine": "xtb"},
        },
        tmp_path,
    )
    registry._maybe_notify_journal_event(
        {
            "event_type": "workflow_phase_finished",
            "workflow_id": "wf_notify",
            "template_name": "reaction_ts_search",
            "worker_session_id": "session-notify",
            "metadata": {
                "phase": "xtb",
                "phase_label": "xTB",
                "phase_outcome": "completed",
                "stage_count": 2,
                "stage_status_counts": {"completed": 2},
            },
        },
        tmp_path,
    )

    assert len(sent_messages) == 2
    assert "workflow=wf_notify" in sent_messages[0]
    assert "phase=xTB" in sent_messages[1]

    monkeypatch.setattr(registry, "_telegram_transport_from_env", lambda: FakeTransport(fail=True))
    registry._maybe_notify_journal_event(event, tmp_path)
    assert len(sent_messages) == 2


def test_clear_terminal_workflow_registry_removes_only_terminal_rows(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)

    records = [
        registry.WorkflowRegistryRecord(
            workflow_id="wf-completed",
            template_name="reaction_ts_search",
            status="completed",
            source_job_id="job-1",
            source_job_type="reaction_ts_search",
            reaction_key="rxn-1",
            requested_at="2026-04-19T00:00:00+00:00",
            workspace_dir="/tmp/wf-completed",
            workflow_file="/tmp/wf-completed/workflow.json",
        ),
        registry.WorkflowRegistryRecord(
            workflow_id="wf-running",
            template_name="reaction_ts_search",
            status="running",
            source_job_id="job-2",
            source_job_type="reaction_ts_search",
            reaction_key="rxn-2",
            requested_at="2026-04-19T00:01:00+00:00",
            workspace_dir="/tmp/wf-running",
            workflow_file="/tmp/wf-running/workflow.json",
        ),
        registry.WorkflowRegistryRecord(
            workflow_id="wf-cancelled",
            template_name="reaction_ts_search",
            status="cancelled",
            source_job_id="job-3",
            source_job_type="reaction_ts_search",
            reaction_key="rxn-3",
            requested_at="2026-04-19T00:02:00+00:00",
            workspace_dir="/tmp/wf-cancelled",
            workflow_file="/tmp/wf-cancelled/workflow.json",
        ),
    ]
    registry._save_records(tmp_path, records)

    assert registry.clear_terminal_workflow_registry(tmp_path) == 2
    remaining = registry.list_workflow_registry(tmp_path, reindex_if_missing=False)
    assert [record.workflow_id for record in remaining] == ["wf-running"]
    assert registry.clear_terminal_workflow_registry(tmp_path) == 0


def test_clear_terminal_workflow_registry_prevents_reindex_resurrection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    monkeypatch.setattr(registry, "now_utc_iso", lambda: "2026-04-19T00:20:00+00:00")

    completed_workspace = tmp_path / "wf-completed"
    running_workspace = tmp_path / "wf-running"
    completed_workspace.mkdir()
    running_workspace.mkdir()
    completed_payload = {
        "workflow_id": "wf-completed",
        "template_name": "reaction_ts_search",
        "status": "completed",
        "source_job_id": "job-1",
        "source_job_type": "reaction_ts_search",
        "reaction_key": "rxn-1",
        "requested_at": "2026-04-19T00:00:00+00:00",
        "stages": [],
        "metadata": {},
    }
    (completed_workspace / "workflow.json").write_text(json.dumps(completed_payload), encoding="utf-8")
    (running_workspace / "workflow.json").write_text(
        json.dumps(
            {
                "workflow_id": "wf-running",
                "template_name": "reaction_ts_search",
                "status": "running",
                "source_job_id": "job-2",
                "source_job_type": "reaction_ts_search",
                "reaction_key": "rxn-2",
                "requested_at": "2026-04-19T00:01:00+00:00",
                "stages": [],
                "metadata": {},
            }
        ),
        encoding="utf-8",
    )

    assert registry.clear_terminal_workflow_registry(tmp_path) == 1
    assert [record.workflow_id for record in registry.list_workflow_registry(tmp_path, reindex_if_missing=False)] == [
        "wf-running"
    ]

    reindexed = registry.reindex_workflow_registry(tmp_path)
    assert [record.workflow_id for record in reindexed] == ["wf-running"]

    completed_payload["status"] = "running"
    (completed_workspace / "workflow.json").write_text(json.dumps(completed_payload), encoding="utf-8")
    assert {record.workflow_id for record in registry.reindex_workflow_registry(tmp_path)} == {
        "wf-completed",
        "wf-running",
    }

    completed_payload["status"] = "completed"
    (completed_workspace / "workflow.json").write_text(json.dumps(completed_payload), encoding="utf-8")
    assert {record.workflow_id for record in registry.reindex_workflow_registry(tmp_path)} == {
        "wf-completed",
        "wf-running",
    }


def test_sync_skips_cleared_terminal_workflow_until_it_becomes_active(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    monkeypatch.setattr(registry, "now_utc_iso", lambda: "2026-04-19T00:20:00+00:00")

    workspace = tmp_path / "wf-completed"
    workspace.mkdir()
    terminal_payload = {
        "workflow_id": "wf-completed",
        "template_name": "reaction_ts_search",
        "status": "completed",
        "source_job_id": "job-1",
        "source_job_type": "reaction_ts_search",
        "reaction_key": "rxn-1",
        "requested_at": "2026-04-19T00:00:00+00:00",
        "stages": [],
        "metadata": {},
    }
    (workspace / "workflow.json").write_text(json.dumps(terminal_payload), encoding="utf-8")

    assert registry.clear_terminal_workflow_registry(tmp_path) == 1
    registry.sync_workflow_registry(tmp_path, workspace, terminal_payload)
    assert registry.list_workflow_registry(tmp_path, reindex_if_missing=False) == []

    active_payload = dict(terminal_payload)
    active_payload["status"] = "running"
    registry.sync_workflow_registry(tmp_path, workspace, active_payload)
    records = registry.list_workflow_registry(tmp_path, reindex_if_missing=False)
    assert [(record.workflow_id, record.status) for record in records] == [("wf-completed", "running")]


def test_list_workflow_registry_does_not_reindex_valid_empty_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    registry._save_records(tmp_path, [])

    def fake_reindex_workflow_registry(root: str | Path) -> list[registry.WorkflowRegistryRecord]:
        raise AssertionError(f"unexpected reindex for {root}")

    monkeypatch.setattr(registry, "reindex_workflow_registry", fake_reindex_workflow_registry)

    assert registry.list_workflow_registry(tmp_path) == []


def test_list_workflow_registry_reindexes_invalid_existing_registry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    registry._registry_path(tmp_path).write_text("{invalid", encoding="utf-8")

    list_result = [
        registry.WorkflowRegistryRecord(
            workflow_id="wf_reindexed",
            template_name="reaction_ts_search",
            status="planned",
            source_job_id="job_reindexed",
            source_job_type="xtb_path",
            reaction_key="rxn_reindexed",
            requested_at="2026-04-19T00:00:00+00:00",
            workspace_dir=str(tmp_path / "wf_reindexed"),
            workflow_file=str(tmp_path / "wf_reindexed" / "workflow.json"),
        )
    ]
    reindex_calls: list[Path] = []

    def fake_reindex_workflow_registry(root: str | Path) -> list[registry.WorkflowRegistryRecord]:
        reindex_calls.append(Path(root).resolve())
        return list_result

    monkeypatch.setattr(registry, "reindex_workflow_registry", fake_reindex_workflow_registry)

    assert registry.list_workflow_registry(tmp_path) == list_result
    assert reindex_calls == [tmp_path.resolve()]


def test_append_workflow_journal_event_writes_jsonl_and_returns_event(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    notifications: list[dict[str, Any]] = []
    token_values = iter(["wf_evt_1", "wf_evt_2", "wf_evt_3"])
    time_values = iter(
        [
            "2026-04-19T01:00:00+00:00",
            "2026-04-19T01:05:00+00:00",
            "2026-04-19T01:10:00+00:00",
        ]
    )

    monkeypatch.setattr(registry, "file_lock", _no_lock)
    monkeypatch.setattr(registry, "timestamped_token", lambda prefix: next(token_values))
    monkeypatch.setattr(registry, "now_utc_iso", lambda: next(time_values))
    monkeypatch.setattr(
        registry,
        "_maybe_notify_journal_event",
        lambda event, workflow_root: notifications.append(
            {"event": dict(event), "workflow_root": str(Path(workflow_root).resolve())}
        ),
    )

    first = registry.append_workflow_journal_event(
        tmp_path,
        event_type="workflow_status_changed",
        workflow_id="wf_1",
        template_name="reaction_ts_search",
        status="running",
        previous_status="planned",
        reason="advanced",
        worker_session_id="session-1",
        metadata={"attempt": 1},
    )
    second = registry.append_workflow_journal_event(
        tmp_path,
        event_type="workflow_stage_submitted",
        workflow_id="wf_2",
        template_name="reaction_ts_search",
        stage_id="xtb_path_search_01",
        engine="xtb",
        task_kind="path_search",
        stage_status="queued",
        previous_stage_status="planned",
        worker_session_id="session-stage",
    )
    third = registry.append_workflow_journal_event(
        tmp_path,
        event_type="workflow_stage_handoff_ready",
        workflow_id="wf_2",
        template_name="reaction_ts_search",
        stage_id="xtb_path_search_01",
        engine="xtb",
        task_kind="path_search",
        stage_status="completed",
        reaction_handoff_status="ready",
        previous_reaction_handoff_status="queued",
        reason="xtb_ts_guess_ready",
    )

    journal_path = registry.workflow_journal_path(tmp_path)
    lines = journal_path.read_text(encoding="utf-8").splitlines()
    second_raw = json.loads(lines[1])
    third_raw = json.loads(lines[2])

    assert first["event_id"] == "wf_evt_1"
    assert first["occurred_at"] == "2026-04-19T01:00:00+00:00"
    assert first["metadata"] == {"attempt": 1}
    assert second["event_id"] == "wf_evt_2"
    assert third["event_id"] == "wf_evt_3"
    assert len(lines) == 3
    assert json.loads(lines[0])["workflow_id"] == "wf_1"
    assert second_raw["workflow_id"] == "wf_2"
    assert second_raw["stage_id"] == "xtb_path_search_01"
    assert second_raw["engine"] == "xtb"
    assert second_raw["task_kind"] == "path_search"
    assert second_raw["previous_stage_status"] == "planned"
    assert second_raw["stage_status"] == "queued"
    assert third_raw["reaction_handoff_status"] == "ready"
    assert third_raw["previous_reaction_handoff_status"] == "queued"
    assert notifications[0]["workflow_root"] == str(tmp_path.resolve())
    assert notifications[1]["event"]["stage_status"] == "queued"
    assert notifications[2]["event"]["reason"] == "xtb_ts_guess_ready"


def test_list_workflow_journal_sorts_descending_and_applies_limit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    journal_path = registry.workflow_journal_path(tmp_path)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    journal_path.write_text(
        "\n".join(
            [
                json.dumps({"event_id": "evt_1", "occurred_at": "2026-04-19T00:01:00+00:00", "event_type": "a"}),
                "not-json",
                "",
                json.dumps({"event_id": "evt_3", "occurred_at": "2026-04-19T00:03:00+00:00", "event_type": "c"}),
                json.dumps({"event_id": "evt_2", "occurred_at": "2026-04-19T00:02:00+00:00", "event_type": "b"}),
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(registry, "file_lock", _no_lock)

    result = registry.list_workflow_journal(tmp_path, limit=2)

    assert [item["event_id"] for item in result] == ["evt_3", "evt_2"]


def test_write_and_load_workflow_worker_state_round_trip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    monkeypatch.setattr(registry.os, "getpid", lambda: 4242)
    monkeypatch.setattr(registry.socket, "gethostname", lambda: "host-1")
    monkeypatch.setattr(registry, "now_utc_iso", lambda: "2026-04-19T02:00:00+00:00")

    written = registry.write_workflow_worker_state(
        tmp_path,
        worker_session_id="session-42",
        status="running",
        workflow_root_path=tmp_path / "custom_root",
        last_cycle_started_at="2026-04-19T01:00:00+00:00",
        interval_seconds=30.0,
        submit_ready=True,
        metadata={"cycle": 1},
    )
    loaded = registry.load_workflow_worker_state(tmp_path)

    assert written == {
        "worker_session_id": "session-42",
        "status": "running",
        "workflow_root": str((tmp_path / "custom_root").resolve()),
        "pid": 4242,
        "hostname": "host-1",
        "last_heartbeat_at": "2026-04-19T02:00:00+00:00",
        "last_cycle_started_at": "2026-04-19T01:00:00+00:00",
        "last_cycle_finished_at": "",
        "lease_expires_at": "",
        "interval_seconds": 30.0,
        "submit_ready": True,
        "metadata": {"cycle": 1},
    }
    assert loaded == written


def test_upsert_list_get_and_resolve_workflow_registry_record(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    record_older = registry.WorkflowRegistryRecord(
        workflow_id="wf_a",
        template_name="reaction_ts_search",
        status="planned",
        source_job_id="job_a",
        source_job_type="xtb_path",
        reaction_key="rxn_a",
        requested_at="2026-04-19T00:00:00+00:00",
        workspace_dir=str(tmp_path / "wf_a"),
        workflow_file=str(tmp_path / "wf_a" / "workflow.json"),
    )
    record_newer = registry.WorkflowRegistryRecord(
        workflow_id="wf_b",
        template_name="conformer_screening",
        status="running",
        source_job_id="job_b",
        source_job_type="raw_xyz",
        reaction_key="rxn_b",
        requested_at="2026-04-19T00:05:00+00:00",
        workspace_dir=str(tmp_path / "wf_b"),
        workflow_file=str(tmp_path / "wf_b" / "workflow.json"),
    )
    record_updated = registry.WorkflowRegistryRecord(
        workflow_id="wf_a",
        template_name="reaction_ts_search",
        status="completed",
        source_job_id="job_a",
        source_job_type="xtb_path",
        reaction_key="rxn_a",
        requested_at="2026-04-19T00:10:00+00:00",
        workspace_dir=str(tmp_path / "wf_a"),
        workflow_file=str(tmp_path / "wf_a" / "workflow.json"),
    )

    registry.upsert_workflow_registry_record(tmp_path, record_older)
    registry.upsert_workflow_registry_record(tmp_path, record_newer)
    registry.upsert_workflow_registry_record(tmp_path, record_updated)

    records = registry.list_workflow_registry(tmp_path, reindex_if_missing=False)

    assert [(item.workflow_id, item.status) for item in records] == [
        ("wf_a", "completed"),
        ("wf_b", "running"),
    ]
    assert registry.get_workflow_registry_record(tmp_path, "wf_b") == record_newer
    assert registry.resolve_workflow_registry_record(tmp_path, "wf_a") == record_updated
    assert registry.resolve_workflow_registry_record(tmp_path, str(tmp_path / "wf_b")) == record_newer
    assert registry.resolve_workflow_registry_record(tmp_path, str(tmp_path / "wf_a" / "workflow.json")) == record_updated
    assert registry.resolve_workflow_registry_record(tmp_path, "") is None


def test_list_workflow_registry_reindexes_when_missing_and_reindex_skips_bad_workspaces(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(registry, "file_lock", _no_lock)
    original_reindex = registry.reindex_workflow_registry

    list_result = [registry.WorkflowRegistryRecord(
        workflow_id="wf_reindexed",
        template_name="reaction_ts_search",
        status="planned",
        source_job_id="job_reindexed",
        source_job_type="xtb_path",
        reaction_key="rxn_reindexed",
        requested_at="2026-04-19T00:00:00+00:00",
        workspace_dir=str(tmp_path / "wf_reindexed"),
        workflow_file=str(tmp_path / "wf_reindexed" / "workflow.json"),
    )]
    reindex_calls: list[Path] = []

    def fake_reindex_workflow_registry(root: str | Path) -> list[registry.WorkflowRegistryRecord]:
        reindex_calls.append(Path(root).resolve())
        return list_result

    monkeypatch.setattr(registry, "reindex_workflow_registry", fake_reindex_workflow_registry)
    assert registry.list_workflow_registry(tmp_path) == list_result
    assert reindex_calls == [tmp_path.resolve()]
    monkeypatch.setattr(registry, "reindex_workflow_registry", original_reindex)

    good_workspace = tmp_path / "wf_good"
    bad_workspace = tmp_path / "wf_bad"
    summaries = {
        good_workspace: {
            "workflow_id": "wf_good",
            "template_name": "conformer_screening",
            "status": "planned",
            "source_job_id": "job_good",
            "source_job_type": "raw_xyz",
            "reaction_key": "rxn_good",
            "requested_at": "2026-04-19T00:15:00+00:00",
            "workspace_dir": str(good_workspace),
            "stage_count": 1,
            "stage_status_counts": {"planned": 1},
            "task_status_counts": {"planned": 1},
            "submission_summary": {},
        }
    }

    def fake_iter_workflow_workspaces(root: Path) -> list[Path]:
        return [good_workspace, bad_workspace]

    def fake_load_workflow_payload(workspace_dir: Path) -> dict[str, Any]:
        if workspace_dir == bad_workspace:
            raise FileNotFoundError("missing workflow")
        return {"workflow_id": "wf_good"}

    def fake_workflow_summary(workspace_dir: Path, payload: dict[str, Any]) -> dict[str, Any]:
        return summaries[workspace_dir]

    monkeypatch.setattr(registry, "iter_workflow_workspaces", fake_iter_workflow_workspaces)
    monkeypatch.setattr(registry, "load_workflow_payload", fake_load_workflow_payload)
    monkeypatch.setattr(registry, "workflow_summary", fake_workflow_summary)
    monkeypatch.setattr(registry, "now_utc_iso", lambda: "2026-04-19T00:20:00+00:00")

    records = registry.reindex_workflow_registry(tmp_path)

    assert len(records) == 1
    assert records[0].workflow_id == "wf_good"
    assert registry.sync_workflow_registry(tmp_path, good_workspace, {"workflow_id": "wf_good"}).workflow_id == "wf_good"
