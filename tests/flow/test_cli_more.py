# ruff: noqa: E402

from __future__ import annotations

from contextlib import contextmanager
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow import cli


def test_emit_worker_payload_formats_text_and_json(capsys) -> None:
    payload = {
        "cycle_started_at": "2026-04-19T17:00:00+00:00",
        "worker_session_id": "worker_1",
        "discovered_count": 3,
        "advanced_count": 2,
        "skipped_count": 1,
        "failed_count": 0,
        "workflow_results": [
            {
                "workflow_id": "wf_1",
                "template_name": "reaction_ts_search",
                "previous_status": "planned",
                "status": "running",
                "advanced": True,
                "reason": "submitted",
            }
        ],
    }

    cli._emit_worker_payload(payload, json_mode=False, single_cycle=False)
    stdout = capsys.readouterr().out
    assert "cycle_started_at: 2026-04-19T17:00:00+00:00 worker_session_id=worker_1" in stdout
    assert "- wf_1 template=reaction_ts_search previous=planned status=running advanced=yes" in stdout
    assert "reason=submitted" in stdout

    cli._emit_worker_payload(payload, json_mode=True, single_cycle=True)
    pretty = json.loads(capsys.readouterr().out)
    assert pretty["worker_session_id"] == "worker_1"

    cli._emit_worker_payload(payload, json_mode=True, single_cycle=False)
    compact = capsys.readouterr().out.strip()
    assert '"workflow_results"' in compact
    assert "\n" not in compact


def test_cmd_workflow_runtime_status_journal_telemetry_text_and_json(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "get_workflow_runtime_status",
        lambda **kwargs: {
            "worker_state": {
                "worker_session_id": "worker_2",
                "status": "running",
                "pid": 123,
                "hostname": "host",
                "last_heartbeat_at": "hb",
                "lease_expires_at": "lease",
                "last_cycle_started_at": "start",
                "last_cycle_finished_at": "finish",
            }
        },
    )
    monkeypatch.setattr(
        cli,
        "get_workflow_journal",
        lambda **kwargs: {
            "events": [
                {
                    "occurred_at": "2026-04-19T17:10:00+00:00",
                    "event_type": "worker_started",
                    "workflow_id": "wf_journal",
                    "status": "running",
                    "reason": "startup",
                }
            ]
        },
    )
    monkeypatch.setattr(
        cli,
        "get_workflow_telemetry",
        lambda **kwargs: {
            "workflow_root": "/tmp/wf",
            "worker_state": {"status": "running", "worker_session_id": "worker_2"},
            "registry_count": 4,
            "journal_event_count": 5,
            "workflow_status_counts": {"running": 2},
            "template_counts": {"reaction_ts_search": 1},
            "journal_event_type_counts": {"worker_started": 1},
            "recent_failures": [{"occurred_at": "t1", "workflow_id": "wf_fail", "reason": "boom"}],
            "recent_status_changes": [{"occurred_at": "t2", "workflow_id": "wf_change", "previous_status": "planned", "status": "running"}],
        },
    )

    assert cli.cmd_workflow_runtime_status(SimpleNamespace(workflow_root="/tmp/wf", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "worker_session_id: worker_2" in stdout
    assert "last_cycle_finished_at: finish" in stdout

    assert cli.cmd_workflow_journal(SimpleNamespace(workflow_root="/tmp/wf", limit=5, json=False)) == 0
    stdout = capsys.readouterr().out
    assert "event_count: 1" in stdout
    assert "- 2026-04-19T17:10:00+00:00 worker_started workflow_id=wf_journal status=running" in stdout
    assert "reason=startup" in stdout

    assert cli.cmd_workflow_telemetry(SimpleNamespace(workflow_root="/tmp/wf", limit=10, json=False)) == 0
    stdout = capsys.readouterr().out
    assert "workflow_root: /tmp/wf" in stdout
    assert "recent_failures:" in stdout
    assert "workflow=wf_change planned->running" in stdout

    assert cli.cmd_workflow_telemetry(SimpleNamespace(workflow_root="/tmp/wf", limit=10, json=True)) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["registry_count"] == 4


def test_cmd_workflow_list_get_and_artifacts_text_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "list_workflows",
        lambda **kwargs: {
            "count": 1,
            "workflows": [
                {
                    "workflow_id": "wf_list",
                    "template_name": "reaction_ts_search",
                    "status": "running",
                    "stage_count": 3,
                    "submission_summary": {"submitted_count": 2, "failed_count": 1},
                }
            ],
        },
    )
    monkeypatch.setattr(
        cli,
        "get_workflow",
        lambda **kwargs: {
            "summary": {
                "workflow_id": "wf_get",
                "template_name": "conformer_screening",
                "status": "completed",
                "source_job_id": "source_1",
                "reaction_key": "rxn_1",
                "workspace_dir": "/tmp/wf_get",
                "stage_count": 1,
                "downstream_reaction_workflow": {"workflow_id": "wf_child", "status": "running"},
                "submission_summary": {"submitted_count": 1, "skipped_count": 0, "failed_count": 0},
                "stage_summaries": [
                    {
                        "stage_id": "orca_01",
                        "engine": "orca",
                        "task_kind": "opt",
                        "status": "completed",
                        "task_status": "completed",
                        "queue_id": "q_1",
                        "selected_input_xyz": "/tmp/input.xyz",
                        "selected_inp": "/tmp/input.inp",
                    }
                ],
            }
        },
    )
    monkeypatch.setattr(
        cli,
        "get_workflow_artifacts",
        lambda **kwargs: {
            "workflow_id": "wf_art",
            "workspace_dir": "/tmp/wf_art",
            "artifact_count": 1,
            "artifacts": [
                {
                    "kind": "orca_optimized_xyz",
                    "stage_id": "orca_01",
                    "exists": True,
                    "selected": True,
                    "path": "/tmp/final.xyz",
                }
            ],
        },
    )

    assert cli.cmd_workflow_list(SimpleNamespace(workflow_root="/tmp/wf", limit=0, refresh=False, json=False)) == 0
    assert "- wf_list template=reaction_ts_search status=running stages=3 submitted=2 failed=1" in capsys.readouterr().out

    assert cli.cmd_workflow_get(SimpleNamespace(target="wf_get", workflow_root="/tmp/wf", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "downstream_reaction: wf_child status=running" in stdout
    assert "submission_summary: submitted=1 skipped=0 failed=0" in stdout
    assert "- orca_01 orca/opt stage_status=completed task_status=completed" in stdout
    assert "queue_id=q_1" in stdout
    assert "selected_inp=/tmp/input.inp" in stdout

    assert cli.cmd_workflow_artifacts(SimpleNamespace(target="wf_art", workflow_root="/tmp/wf", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "artifact_count: 1" in stdout
    assert "- orca_optimized_xyz stage=orca_01 exists=yes selected=yes" in stdout
    assert "path=/tmp/final.xyz" in stdout


def test_cmd_workflow_cancel_reindex_submit_and_advance_output_paths(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "cancel_workflow",
        lambda **kwargs: {
            "workflow_id": "wf_cancel",
            "workspace_dir": "/tmp/wf_cancel",
            "status": "cancelled",
            "cancelled": [{"stage_id": "orca_01", "queue_id": "q_cancel"}],
            "requested": [{"stage_id": "xtb_01", "queue_id": "q_request"}],
            "skipped": [{"stage_id": "crest_01", "reason": "already_completed"}],
            "failed": [{"stage_id": "orca_02", "reason": "cancel_failed"}],
        },
    )
    monkeypatch.setattr(
        cli,
        "reindex_workflow_registry",
        lambda workflow_root: [
            SimpleNamespace(workflow_id="wf_one", status="running", template_name="reaction_ts_search"),
            SimpleNamespace(workflow_id="wf_two", status="completed", template_name="conformer_screening"),
        ],
    )
    monkeypatch.setattr(
        cli,
        "submit_reaction_ts_search_workflow",
        lambda **kwargs: {
            "workflow_id": "wf_submit",
            "workspace_dir": "/tmp/wf_submit",
            "status": "running",
            "submitted": [{"stage_id": "orca_01", "queue_id": "q_submit"}],
            "skipped": [{"stage_id": "orca_02", "reason": "already_submitted"}],
            "failed": [{"stage_id": "orca_03", "returncode": 1}],
        },
    )
    monkeypatch.setattr(
        cli,
        "advance_materialized_workflow",
        lambda **kwargs: {"workflow_id": "wf_advance", "status": "running", "stages": [{}, {}]},
    )

    assert cli.cmd_workflow_cancel(SimpleNamespace(target="wf_cancel", workflow_root="/tmp/wf", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "cancelled_count: 1" in stdout
    assert "- cancelled orca_01 queue_id=q_cancel" in stdout
    assert "- cancel_requested xtb_01 queue_id=q_request" in stdout
    assert "- skipped crest_01 reason=already_completed" in stdout
    assert "- failed orca_02 reason=cancel_failed" in stdout

    assert cli.cmd_workflow_reindex(SimpleNamespace(workflow_root="/tmp/wf", json=False)) == 0
    stdout = capsys.readouterr().out
    assert "workflow_count: 2" in stdout
    assert "- wf_one status=running template=reaction_ts_search" in stdout

    assert cli.cmd_workflow_submit_reaction_ts_search(
        SimpleNamespace(
            target="wf_submit",
            workflow_root="/tmp/wf",
            orca_auto_config="/tmp/orca.yaml",
            orca_auto_executable="orca_auto",
            orca_auto_repo_root=None,
            resubmit=False,
            json=False,
        )
    ) == 0
    stdout = capsys.readouterr().out
    assert "submitted_count: 1" in stdout
    assert "- submitted orca_01 queue_id=q_submit" in stdout
    assert "skipped_count: 1" in stdout
    assert "failed_count: 1" in stdout

    assert cli.cmd_workflow_advance(
        SimpleNamespace(
            target="wf_advance",
            workflow_root="/tmp/wf",
            crest_auto_config=None,
            crest_auto_executable="crest_auto",
            crest_auto_repo_root=None,
            xtb_auto_config=None,
            xtb_auto_executable="xtb_auto",
            xtb_auto_repo_root=None,
            orca_auto_config=None,
            orca_auto_executable="orca_auto",
            orca_auto_repo_root=None,
            no_submit=True,
            json=False,
        )
    ) == 0
    stdout = capsys.readouterr().out
    assert "workflow_id: wf_advance" in stdout
    assert "status: running" in stdout
    assert "stage_count: 2" in stdout


def test_cmd_workflow_worker_handles_negative_cycles_and_lock_timeout(monkeypatch, capsys) -> None:
    args = SimpleNamespace(
        once=False,
        max_cycles=-1,
        interval_seconds=1.0,
        lock_timeout_seconds=5.0,
        refresh_registry=False,
        refresh_each_cycle=False,
        service_mode=False,
        json=False,
        workflow_root="/tmp/wf",
        worker_session_id="",
        lease_seconds=60.0,
        no_submit=False,
        crest_auto_config=None,
        crest_auto_executable="crest_auto",
        crest_auto_repo_root=None,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
    )
    assert cli.cmd_workflow_worker(args) == 1
    assert "--max-cycles must be >= 0" in capsys.readouterr().out

    writes: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    @contextmanager
    def raising_lock(*args: Any, **kwargs: Any):
        raise TimeoutError("already running")
        yield

    monkeypatch.setattr(cli, "file_lock", raising_lock)
    monkeypatch.setattr(cli, "workflow_worker_lock_path", lambda workflow_root: Path("/tmp/worker.lock"))
    monkeypatch.setattr(cli, "now_utc_iso", lambda: "2026-04-19T17:20:00+00:00")
    monkeypatch.setattr(cli, "timestamped_token", lambda prefix: "wf_worker_01")
    monkeypatch.setattr(cli, "write_workflow_worker_state", lambda workflow_root, **kwargs: writes.append(kwargs))
    monkeypatch.setattr(cli, "append_workflow_journal_event", lambda workflow_root, **kwargs: events.append(kwargs))

    args.max_cycles = 0
    result = cli.cmd_workflow_worker(args)

    assert result == 1
    assert "worker_lock_error: already running" in capsys.readouterr().out
    assert writes[-1]["status"] == "lock_error"
    assert events[-1]["event_type"] == "worker_lock_error"


def test_cmd_workflow_worker_single_cycle_and_keyboard_interrupt(monkeypatch, capsys) -> None:
    writes: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    @contextmanager
    def fake_lock(*args: Any, **kwargs: Any):
        yield

    monkeypatch.setattr(cli, "file_lock", fake_lock)
    monkeypatch.setattr(cli, "workflow_worker_lock_path", lambda workflow_root: Path("/tmp/worker.lock"))
    monkeypatch.setattr(cli, "now_utc_iso", lambda: "2026-04-19T17:30:00+00:00")
    monkeypatch.setattr(cli, "timestamped_token", lambda prefix: "wf_worker_02")
    monkeypatch.setattr(cli, "write_workflow_worker_state", lambda workflow_root, **kwargs: writes.append(kwargs))
    monkeypatch.setattr(cli, "append_workflow_journal_event", lambda workflow_root, **kwargs: events.append(kwargs))
    monkeypatch.setattr(
        cli,
        "advance_workflow_registry_once",
        lambda **kwargs: {
            "cycle_started_at": "2026-04-19T17:30:00+00:00",
            "worker_session_id": kwargs["worker_session_id"],
            "discovered_count": 1,
            "advanced_count": 1,
            "skipped_count": 0,
            "failed_count": 0,
            "workflow_results": [],
        },
    )
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)

    args = SimpleNamespace(
        once=True,
        max_cycles=0,
        interval_seconds=1.0,
        lock_timeout_seconds=5.0,
        refresh_registry=True,
        refresh_each_cycle=False,
        service_mode=False,
        json=False,
        workflow_root="/tmp/wf",
        worker_session_id="",
        lease_seconds=60.0,
        no_submit=True,
        crest_auto_config=None,
        crest_auto_executable="crest_auto",
        crest_auto_repo_root=None,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
    )

    assert cli.cmd_workflow_worker(args) == 0
    stdout = capsys.readouterr().out
    assert "worker_session_id=wf_worker_02" in stdout
    assert writes[-1]["status"] == "stopped"
    assert writes[-1]["metadata"]["stop_reason"] == "max_cycles_reached"
    assert events[0]["event_type"] == "worker_started"
    assert events[-1]["event_type"] == "worker_stopped"

    def raise_keyboard_interrupt(**kwargs: Any) -> dict[str, Any]:
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "advance_workflow_registry_once", raise_keyboard_interrupt)
    writes.clear()
    events.clear()
    args.once = False
    args.max_cycles = 0
    assert cli.cmd_workflow_worker(args) == 130
    assert writes[-1]["status"] == "interrupted"
    assert events[-1]["event_type"] == "worker_interrupted"


def test_build_parser_and_main_cover_worker_and_submit_commands(monkeypatch) -> None:
    parser = cli.build_parser()
    worker_args = parser.parse_args(
        [
            "workflow",
            "worker",
            "--workflow-root",
            "/tmp/wf",
            "--once",
            "--no-submit",
            "--json",
        ]
    )
    assert worker_args.workflow_command == "worker"
    assert worker_args.once is True
    assert worker_args.no_submit is True
    assert worker_args.json is True
    assert worker_args.func is cli.cmd_workflow_worker

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "workflow",
                "worker",
                "--workflow-root",
                "/tmp/wf",
                "--crest-executable",
                "crest-bin",
            ]
        )

    submit_args = parser.parse_args(
        [
            "workflow",
            "submit-reaction-ts-search",
            "wf_submit",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--resubmit",
        ]
    )
    assert submit_args.workflow_command == "submit-reaction-ts-search"
    assert submit_args.target == "wf_submit"
    assert submit_args.chemstack_config == "/tmp/chemstack.yaml"
    assert submit_args.resubmit is True
    assert submit_args.func is cli.cmd_workflow_submit_reaction_ts_search

    captured: dict[str, Any] = {}

    def fake_cmd_workflow_list(args: Any) -> int:
        captured["workflow_root"] = args.workflow_root
        captured["json"] = args.json
        return 23

    monkeypatch.setattr(cli, "cmd_workflow_list", fake_cmd_workflow_list)
    result = cli.main(["workflow", "list", "--workflow-root", "/tmp/wf", "--json"])
    assert result == 23
    assert captured == {"workflow_root": "/tmp/wf", "json": True}


def test_build_parser_rejects_removed_workflow_alias_flags() -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["workflow", "worker", "--workflow-root", "/tmp/wf", "--orca-auto-repo-root", "/tmp/repo"])
