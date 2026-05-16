from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack import cli as unified_cli


@pytest.fixture(autouse=True)
def _isolate_shared_config_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    def _explicit_shared_config_path(explicit: str | None) -> str | None:
        if not explicit:
            return None
        return str(Path(explicit).expanduser().resolve())

    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", _explicit_shared_config_path)
    monkeypatch.setattr(unified_cli, "shared_workflow_root_from_config", lambda config_path: None)


def test_queue_table_lines_align_wide_headers_and_icons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc)
    )

    lines = unified_cli._queue_table_lines(
        [
            (
                0,
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "workflow",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:30:00+00:00",
                    "updated_at": "2026-04-26T02:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "request_parameters": {"crest_mode": "nci"},
                    },
                },
            ),
            (
                1,
                {
                    "activity_id": "orca-q-very-long-child-id",
                    "kind": "job",
                    "engine": "orca",
                    "status": "retrying",
                    "label": "standalone-ts",
                    "source": "chemstack_orca",
                    "submitted_at": "2026-04-26T02:00:00+00:00",
                    "updated_at": "2026-04-26T02:20:00+00:00",
                    "metadata": {
                        "task_kind": "optts_freq",
                    },
                },
            ),
            (
                0,
                {
                    "activity_id": "custom-q-1",
                    "kind": "job",
                    "engine": "custom",
                    "status": "failed",
                    "label": "very-long-detail-label-for-width-checking-and-truncation",
                    "source": "custom",
                    "submitted_at": "2026-04-26T02:10:00+00:00",
                    "updated_at": "2026-04-26T02:40:00+00:00",
                },
            ),
        ]
    )

    widths = [unified_cli._queue_display_width(line) for line in lines]
    assert len(set(widths)) == 1
    assert "..." in lines[-1]


def test_queue_elapsed_prefers_attempt_anchor_metadata() -> None:
    now = datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc)

    assert (
        unified_cli._queue_elapsed_text(
            {
                "status": "running",
                "submitted_at": "2026-04-26T01:00:00+00:00",
                "updated_at": "2026-04-26T02:00:00+00:00",
                "metadata": {"elapsed_started_at": "2026-04-26T02:45:00+00:00"},
            },
            now=now,
        )
        == "00:15:00"
    )
    assert (
        unified_cli._queue_elapsed_text(
            {
                "status": "completed",
                "submitted_at": "2026-04-26T01:00:00+00:00",
                "updated_at": "2026-04-26T02:20:00+00:00",
                "metadata": {"last_restarted_at": "2026-04-26T02:00:00+00:00"},
            },
            now=now,
        )
        == "00:20:00"
    )


def test_cmd_queue_list_filters_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc)
    )
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 3,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "workflow",
                    "status": "running",
                    "label": "wf-1",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                },
                {
                    "activity_id": "xtb-q-1",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "xtb_auto",
                    "submitted_at": "2026-04-26T02:00:00+00:00",
                    "updated_at": "2026-04-26T02:30:00+00:00",
                    "metadata": {"task_kind": "path_search"},
                },
                {
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "label": "mol-a",
                    "source": "crest_auto",
                    "submitted_at": "2026-04-26T02:15:00+00:00",
                    "updated_at": "2026-04-26T02:15:00+00:00",
                },
            ],
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=0,
            refresh=False,
            engine=["xtb"],
            status=["running"],
            kind=["job"],
            json=False,
        )
    )

    assert result == 0
    stdout = capsys.readouterr().out
    assert "active_simulations: 1" in stdout
    assert (
        "Status" in stdout
        and "Name" in stdout
        and "Detail" in stdout
        and "ID" in stdout
        and "Elapsed" in stdout
    )
    assert "▶" in stdout
    assert "xtb-q-1" in stdout
    assert "TS path" in stdout
    assert "01:00:00" in stdout
    assert "crest-q-1" not in stdout
    assert "wf-1" not in stdout


def test_cmd_queue_list_hides_non_orca_workflow_children_in_default_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc)
    )
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 5,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "workflow",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:30:00+00:00",
                    "updated_at": "2026-04-26T02:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "current_engine": "orca",
                        "request_parameters": {"crest_mode": "nci"},
                    },
                },
                {
                    "activity_id": "xtb-q-1",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "path-search",
                    "source": "xtb_auto",
                    "submitted_at": "2026-04-26T02:00:00+00:00",
                    "updated_at": "2026-04-26T02:15:00+00:00",
                    "metadata": {
                        "task_kind": "path_search",
                        "workflow_id": "wf-1",
                        "job_dir": "/tmp/workflows/wf-1/02_xtb/xtb_path_search_01",
                    },
                },
                {
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "label": "conformer-search",
                    "source": "crest_auto",
                    "submitted_at": "2026-04-26T02:10:00+00:00",
                    "updated_at": "2026-04-26T02:10:00+00:00",
                    "metadata": {
                        "task_kind": "conformer_search",
                        "workflow_id": "wf-1",
                        "job_dir": "/tmp/workflows/wf-1/01_crest/crest_reactant_01",
                    },
                },
                {
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "ts-opt",
                    "source": "chemstack_orca",
                    "submitted_at": "2026-04-26T02:00:00+00:00",
                    "updated_at": "2026-04-26T02:20:00+00:00",
                    "metadata": {
                        "task_kind": "optts_freq",
                        "workflow_id": "wf-1",
                        "reaction_dir": "/tmp/workflows/wf-1/03_orca/case_001",
                    },
                },
                {
                    "activity_id": "orca-q-standalone",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "standalone-ts",
                    "source": "chemstack_orca",
                    "submitted_at": "2026-04-26T00:30:00+00:00",
                    "updated_at": "2026-04-26T01:30:00+00:00",
                    "metadata": {
                        "job_type": "neb",
                        "reaction_dir": "/tmp/orca/standalone/case_002",
                    },
                },
            ],
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=0,
            refresh=False,
            engine=None,
            status=None,
            kind=None,
            json=False,
        )
    )

    assert result == 0
    stdout = capsys.readouterr().out
    assert "active_simulations: 3" in stdout
    assert "▶" in stdout
    assert "wf-1" in stdout
    assert "ts_search(nci)" in stdout
    assert "xtb-q-1" not in stdout
    assert "crest-q-1" not in stdout
    assert "orca-q-1" in stdout
    assert "OptTS+Freq" in stdout
    assert "orca-q-standalone" in stdout
    assert "NEB" in stdout


def test_cmd_queue_list_shows_all_workflow_child_jobs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc)
    )
    child_rows = [
        {
            "activity_id": f"orca-q-{index}",
            "kind": "job",
            "engine": "orca",
            "status": "running",
            "label": f"ts-{index}",
            "source": "chemstack_orca",
            "submitted_at": "2026-04-26T02:00:00+00:00",
            "updated_at": "2026-04-26T02:00:00+00:00",
            "metadata": {
                "task_kind": "optts_freq",
                "reaction_dir": f"/tmp/orca/workflow_jobs/wf-1/03_orca/case_{index:03d}",
            },
        }
        for index in range(1, 10)
    ]
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 10,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "workflow",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                    "metadata": {
                        "template_name": "reaction_ts_search",
                        "current_engine": "orca",
                    },
                },
                *child_rows,
            ],
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=0,
            refresh=False,
            engine=None,
            status=None,
            kind=None,
            json=False,
        )
    )

    assert result == 0
    stdout = capsys.readouterr().out
    assert "active_simulations: 9" in stdout
    assert stdout.count("▶") >= 1
    assert stdout.count("orca-q-") == 9
    assert "wf-1" in stdout
    assert "ts_search" in stdout


def test_cmd_queue_list_reports_empty_filtered_results(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 1,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "workflow",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
                    "submitted_at": "2026-04-26T01:00:00+00:00",
                    "updated_at": "2026-04-26T01:00:00+00:00",
                    "metadata": {"template_name": "reaction_ts_search"},
                }
            ],
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=0,
            refresh=False,
            engine=["orca"],
            status=["failed"],
            kind=["job"],
            json=False,
        )
    )

    assert result == 0
    stdout = capsys.readouterr().out
    assert "active_simulations: 0" in stdout
    assert "No matching activities." in stdout
    assert "Status" not in stdout


def test_cmd_queue_list_json_filters_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 2,
            "activities": [
                {
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "ts-1",
                    "source": "chemstack_orca",
                },
                {
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "xtb",
                    "status": "queued",
                    "label": "wf-1",
                    "source": "chem_flow",
                },
            ],
            "sources": {"orca_auto_config": "/tmp/chemstack.yaml"},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=0,
            refresh=False,
            engine=["orca"],
            status=None,
            kind=None,
            json=True,
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["active_simulations"] == 1
    assert payload["activities"][0]["activity_id"] == "orca-q-1"
    assert payload["sources"]["orca_auto_config"] == "/tmp/chemstack.yaml"


def test_cmd_queue_list_uses_global_active_simulation_count_from_full_payload(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 3,
            "activities": [
                {
                    "activity_id": "xtb-q-1",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "xtb_auto",
                },
                {
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "ts-a",
                    "source": "chemstack_orca",
                },
                {
                    "activity_id": "xtb-q-2",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-b",
                    "source": "xtb_auto",
                },
            ],
            "sources": {"orca_auto_config": "/tmp/chemstack.yaml"},
        },
    )

    def _fake_count(items: list[dict[str, Any]], *, config_path: str | None = None) -> int:
        captured["items"] = items
        captured["config_path"] = config_path
        return 7

    monkeypatch.setattr(unified_cli, "count_global_active_simulations", _fake_count)

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=1,
            refresh=False,
            engine=["xtb"],
            status=["running"],
            kind=["job"],
            json=True,
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["active_simulations"] == 7
    assert payload["activities"][0]["activity_id"] == "xtb-q-1"
    assert len(captured["items"]) == 3
    assert captured["config_path"] == "/tmp/chemstack.yaml"


def test_cmd_queue_list_applies_limit_after_filters(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "list_activities",
        lambda **kwargs: {
            "count": 4,
            "activities": [
                {
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "label": "mol-a",
                    "source": "crest_auto",
                },
                {
                    "activity_id": "xtb-q-1",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "xtb_auto",
                },
                {
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "ts-a",
                    "source": "chemstack_orca",
                },
                {
                    "activity_id": "xtb-q-2",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-b",
                    "source": "xtb_auto",
                },
            ],
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            limit=1,
            refresh=False,
            engine=["xtb"],
            status=["running"],
            kind=["job"],
            json=True,
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["count"] == 1
    assert payload["active_simulations"] == 3
    assert payload["activities"][0]["activity_id"] == "xtb-q-1"


def test_cmd_queue_list_clear_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "clear_activities",
        lambda **kwargs: {
            "total_cleared": 5,
            "cleared": {
                "workflows": 1,
                "xtb_queue_entries": 2,
                "crest_queue_entries": 0,
                "orca_queue_entries": 1,
                "orca_run_states": 1,
            },
            "sources": {},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            action="clear",
            workflow_root=None,
            chemstack_config="/tmp/chemstack.yaml",
            limit=0,
            refresh=False,
            engine=None,
            status=None,
            kind=None,
            json=False,
        )
    )

    assert result == 0
    stdout = capsys.readouterr().out
    assert "Cleared 5 completed/failed/cancelled entries." in stdout
    assert "workflows: 1" in stdout
    assert "xTB queue entries: 2" in stdout
    assert "ORCA queue entries: 1" in stdout
    assert "ORCA run states: 1" in stdout


def test_cmd_queue_list_clear_json_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "clear_activities",
        lambda **kwargs: {
            "total_cleared": 0,
            "cleared": {
                "workflows": 0,
                "xtb_queue_entries": 0,
                "crest_queue_entries": 0,
                "orca_queue_entries": 0,
                "orca_run_states": 0,
            },
            "sources": {"workflow_root": "/tmp/workflows"},
        },
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            action="clear",
            workflow_root=None,
            chemstack_config="/tmp/chemstack.yaml",
            limit=0,
            refresh=False,
            engine=None,
            status=None,
            kind=None,
            json=True,
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["total_cleared"] == 0
    assert payload["sources"]["workflow_root"] == "/tmp/workflows"


def test_cmd_queue_list_clear_rejects_filters(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "clear_activities",
        lambda **kwargs: pytest.fail("clear_activities should not run"),
    )

    result = unified_cli.cmd_queue_list(
        SimpleNamespace(
            action="clear",
            workflow_root=None,
            chemstack_config="/tmp/chemstack.yaml",
            limit=0,
            refresh=False,
            engine=["orca"],
            status=None,
            kind=None,
            json=False,
        )
    )

    assert result == 1
    assert (
        capsys.readouterr().out
        == "error: `chemstack queue list clear` does not support --engine/--status/--kind/--limit filters.\n"
    )


def test_cmd_queue_cancel_reports_lookup_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_cancel_activity(**kwargs: Any) -> dict[str, Any]:
        raise LookupError("Activity target not found: missing")

    monkeypatch.setattr(unified_cli, "cancel_activity", fake_cancel_activity)

    result = unified_cli.cmd_queue_cancel(
        SimpleNamespace(
            target="missing",
            workflow_root=None,
            chemstack_config=None,
            json=False,
        )
    )

    assert result == 1
    assert capsys.readouterr().out == "error: Activity target not found: missing\n"


def test_cmd_queue_cancel_reports_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fake_cancel_activity(**kwargs: Any) -> dict[str, Any]:
        raise TimeoutError(
            "Workflow is busy and could not be locked for cancellation within 5s: /tmp/wf_busy"
        )

    monkeypatch.setattr(unified_cli, "cancel_activity", fake_cancel_activity)

    result = unified_cli.cmd_queue_cancel(
        SimpleNamespace(
            target="wf_busy",
            workflow_root=None,
            chemstack_config=None,
            json=False,
        )
    )

    assert result == 1
    assert (
        capsys.readouterr().out
        == "error: Workflow is busy and could not be locked for cancellation within 5s: /tmp/wf_busy\n"
    )


def test_cmd_queue_cancel_json_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        unified_cli,
        "cancel_activity",
        lambda **kwargs: {
            "activity_id": "crest-q-1",
            "kind": "job",
            "engine": "crest",
            "source": "crest_auto",
            "label": "mol-a",
            "status": "cancel_requested",
            "cancel_target": "crest-q-1",
        },
    )

    result = unified_cli.cmd_queue_cancel(
        SimpleNamespace(
            target="crest-q-1",
            workflow_root=None,
            chemstack_config="/tmp/chemstack.yaml",
            json=True,
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "cancel_requested"
    assert payload["engine"] == "crest"
