# ruff: noqa: E402

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.core.queue.types import QueueEntry, QueueStatus

from chemstack.flow import activity, cli, operations


def test_list_activities_merges_workflows_and_standalone_sources(monkeypatch) -> None:
    workflow_record = SimpleNamespace(
        workflow_id="wf-2",
        template_name="reaction_ts_search",
        status="running",
        source_job_id="",
        source_job_type="",
        reaction_key="rxn-2",
        requested_at="2026-04-20T10:00:00+00:00",
        workspace_dir="/tmp/wf/workflows/wf-2",
        workflow_file="/tmp/wf/workflows/wf-2/workflow.json",
        stage_count=2,
        updated_at="2026-04-20T10:06:00+00:00",
    )

    monkeypatch.setattr(activity, "list_workflow_registry", lambda workflow_root: [workflow_record])
    monkeypatch.setattr(
        activity,
        "list_workflow_summaries",
        lambda workflow_root: [
            {
                "workflow_id": "wf-2",
                "stage_summaries": [
                    {
                        "stage_id": "crest.reactant",
                        "status": "completed",
                        "task_status": "completed",
                        "engine": "crest",
                        "reaction_dir": "",
                    },
                    {
                        "stage_id": "xtb.path",
                        "status": "running",
                        "task_status": "running",
                        "engine": "xtb",
                        "reaction_dir": "/tmp/xtb_jobs/rxn-2",
                    },
                ],
            }
        ],
    )
    monkeypatch.setattr(
        activity,
        "sibling_runtime_paths",
        lambda config_path: {
            "allowed_root": Path("/tmp/crest_root" if "crest" in config_path else "/tmp/xtb_root"),
        },
    )

    crest_entry = QueueEntry(
        queue_id="crest-q-1",
        app_name="crest_auto",
        task_id="crest-job-1",
        task_kind="crest_run_dir",
        engine="crest",
        status=QueueStatus.PENDING,
        priority=10,
        enqueued_at="2026-04-20T10:01:00+00:00",
        metadata={"job_dir": "/tmp/crest_root/jobs/mol-a"},
    )
    xtb_entry = QueueEntry(
        queue_id="xtb-q-1",
        app_name="xtb_auto",
        task_id="xtb-job-1",
        task_kind="xtb_run_dir",
        engine="xtb",
        status=QueueStatus.RUNNING,
        priority=5,
        enqueued_at="2026-04-20T10:02:00+00:00",
        started_at="2026-04-20T10:03:00+00:00",
        metadata={"job_dir": "/tmp/xtb_root/jobs/rxn-a", "reaction_key": "rxn-a"},
    )
    monkeypatch.setattr(
        activity,
        "list_queue",
        lambda root: [crest_entry] if str(root).endswith("crest_root") else [xtb_entry],
    )
    monkeypatch.setattr(
        activity,
        "_orca_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="orca-q-1",
                kind="job",
                engine="orca",
                status="running",
                label="ts-run-1",
                source="chemstack_orca",
                submitted_at="2026-04-20T10:04:00+00:00",
                updated_at="2026-04-20T10:07:00+00:00",
                cancel_target="orca-q-1",
                aliases=("orca-q-1",),
                metadata={},
            )
        ],
    )

    payload = activity.list_activities(
        workflow_root="/tmp/wf",
        crest_auto_config="/tmp/crest.yaml",
        xtb_auto_config="/tmp/xtb.yaml",
        orca_auto_config="/tmp/orca.yaml",
    )

    assert payload["count"] == 4
    assert [item["activity_id"] for item in payload["activities"]] == [
        "orca-q-1",
        "wf-2",
        "xtb-q-1",
        "crest-q-1",
    ]
    workflow_item = next(item for item in payload["activities"] if item["activity_id"] == "wf-2")
    assert workflow_item["engine"] == "xtb"
    assert workflow_item["label"] == "/tmp/xtb_jobs/rxn-2"


def test_cancel_activity_routes_workflow_targets(monkeypatch) -> None:
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="wf-9",
                kind="workflow",
                engine="xtb",
                status="running",
                label="wf-9",
                source="chem_flow",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:00:00+00:00",
                cancel_target="wf-9",
                aliases=("wf-9", "/tmp/wf/workflows/wf-9"),
                metadata={},
            )
        ],
    )
    monkeypatch.setattr(
        operations,
        "cancel_workflow",
        lambda **kwargs: {"workflow_id": "wf-9", "status": "cancelled", "cancelled": []},
    )

    payload = activity.cancel_activity(target="wf-9", workflow_root="/tmp/wf")

    assert payload["activity_id"] == "wf-9"
    assert payload["status"] == "cancelled"
    assert payload["source"] == "chem_flow"


def test_cancel_activity_routes_xtb_targets(monkeypatch) -> None:
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="xtb-q-1",
                kind="job",
                engine="xtb",
                status="running",
                label="rxn-a",
                source="xtb_auto",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:01:00+00:00",
                cancel_target="xtb-q-1",
                aliases=("xtb-q-1", "xtb-job-1"),
                metadata={},
            )
        ],
    )
    monkeypatch.setattr(
        activity,
        "cancel_xtb_target",
        lambda **kwargs: {"status": "cancel_requested", "queue_id": kwargs["target"]},
    )

    payload = activity.cancel_activity(target="xtb-job-1", xtb_auto_config="/tmp/xtb.yaml")

    assert payload["activity_id"] == "xtb-q-1"
    assert payload["status"] == "cancel_requested"
    assert payload["source"] == "xtb_auto"


def test_cmd_activity_list_text_output(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "list_activities",
        lambda **kwargs: {
            "count": 2,
            "activities": [
                {
                    "activity_id": "wf-1",
                    "engine": "xtb",
                    "status": "running",
                    "label": "rxn-a",
                    "source": "chem_flow",
                },
                {
                    "activity_id": "xtb-q-1",
                    "engine": "xtb",
                    "status": "pending",
                    "label": "rxn-b",
                    "source": "xtb_auto",
                },
            ],
        },
    )

    assert cli.cmd_activity_list(
        SimpleNamespace(
            workflow_root="/tmp/wf",
            limit=0,
            refresh=False,
            crest_auto_config=None,
            xtb_auto_config="/tmp/xtb.yaml",
            orca_auto_config=None,
            orca_auto_repo_root=None,
            json=False,
        )
    ) == 0

    stdout = capsys.readouterr().out
    assert "activity_count: 2" in stdout
    assert "- wf-1 engine=xtb status=running label=rxn-a source=chem_flow" in stdout
    assert "- xtb-q-1 engine=xtb status=pending label=rxn-b source=xtb_auto" in stdout


def test_cmd_activity_cancel_json_and_error_paths(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        cli,
        "cancel_activity",
        lambda **kwargs: {
            "activity_id": "crest-q-1",
            "engine": "crest",
            "source": "crest_auto",
            "label": "mol-a",
            "status": "cancel_requested",
            "cancel_target": "crest-q-1",
        },
    )
    args = SimpleNamespace(
        target="crest-q-1",
        workflow_root=None,
        crest_auto_config="/tmp/crest.yaml",
        crest_auto_executable="crest_auto",
        crest_auto_repo_root=None,
        xtb_auto_config=None,
        xtb_auto_executable="xtb_auto",
        xtb_auto_repo_root=None,
        orca_auto_config=None,
        orca_auto_executable="orca_auto",
        orca_auto_repo_root=None,
        json=True,
    )
    assert cli.cmd_activity_cancel(args) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "cancel_requested"

    def fake_cancel_activity(**kwargs: Any) -> dict[str, Any]:
        raise LookupError("Activity target not found: missing")

    monkeypatch.setattr(cli, "cancel_activity", fake_cancel_activity)
    args.json = False
    args.target = "missing"
    assert cli.cmd_activity_cancel(args) == 1
    assert "error: Activity target not found: missing" in capsys.readouterr().out


def test_build_parser_parses_top_level_activity_commands() -> None:
    parser = cli.build_parser()

    list_args = parser.parse_args(
        [
            "list",
            "--workflow-root",
            "/tmp/wf",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--json",
        ]
    )
    assert list_args.command == "list"
    assert list_args.workflow_root == "/tmp/wf"
    assert list_args.chemstack_config == "/tmp/chemstack.yaml"
    assert list_args.func is cli.cmd_activity_list

    cancel_args = parser.parse_args(
        [
            "cancel",
            "xtb-q-1",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
        ]
    )
    assert cancel_args.command == "cancel"
    assert cancel_args.target == "xtb-q-1"
    assert cancel_args.chemstack_config == "/tmp/chemstack.yaml"
    assert cancel_args.func is cli.cmd_activity_cancel

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "cancel",
                "xtb-q-1",
                "--crest-executable",
                "crest-bin",
                "--xtb-executable",
                "xtb-bin",
                "--orca-auto-executable",
                "orca-auto-bin",
            ]
        )


def test_build_parser_rejects_removed_activity_alias_flags() -> None:
    parser = cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["cancel", "xtb-q-1", "--chemstack-executable", "orca_auto"])


def test_list_activities_autodiscovers_defaults_when_no_args(monkeypatch) -> None:
    monkeypatch.setattr(activity, "_discover_workflow_root", lambda workflow_root: "/tmp/workflow_root")
    monkeypatch.setattr(
        activity,
        "_discover_sibling_config",
        lambda explicit, *, app_name: "/tmp/chemstack.yaml",
    )
    captured: dict[str, Any] = {}

    def fake_collect(**kwargs: Any) -> list[activity.ActivityRecord]:
        captured.update(kwargs)
        return []

    monkeypatch.setattr(activity, "_collect_activity_records", fake_collect)

    payload = activity.list_activities()

    assert payload["count"] == 0
    assert payload["sources"] == {
        "workflow_root": str(Path("/tmp/workflow_root").resolve()),
        "crest_auto_config": "/tmp/chemstack.yaml",
        "xtb_auto_config": "/tmp/chemstack.yaml",
        "orca_auto_config": "/tmp/chemstack.yaml",
    }
    assert captured["workflow_root"] == "/tmp/workflow_root"
    assert captured["crest_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["xtb_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["orca_auto_config"] == "/tmp/chemstack.yaml"


def test_cancel_activity_autodiscovers_defaults(monkeypatch) -> None:
    monkeypatch.setattr(activity, "_discover_workflow_root", lambda workflow_root: "/tmp/workflow_root")
    monkeypatch.setattr(
        activity,
        "_discover_sibling_config",
        lambda explicit, *, app_name: "/tmp/chemstack.yaml",
    )
    monkeypatch.setattr(
        activity,
        "_collect_activity_records",
        lambda **kwargs: [
            activity.ActivityRecord(
                activity_id="wf-77",
                kind="workflow",
                engine="workflow",
                status="running",
                label="wf-77",
                source="chem_flow",
                submitted_at="2026-04-20T10:00:00+00:00",
                updated_at="2026-04-20T10:00:00+00:00",
                cancel_target="wf-77",
                aliases=("wf-77",),
                metadata={},
            )
        ],
    )
    captured: dict[str, Any] = {}

    def fake_cancel_workflow(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {"workflow_id": "wf-77", "status": "cancelled"}

    monkeypatch.setattr(operations, "cancel_workflow", fake_cancel_workflow)

    payload = activity.cancel_activity(target="wf-77")

    assert payload["status"] == "cancelled"
    assert captured["workflow_root"] == "/tmp/workflow_root"
    assert captured["crest_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["xtb_auto_config"] == "/tmp/chemstack.yaml"
    assert captured["orca_auto_config"] == "/tmp/chemstack.yaml"


def test_build_parser_accepts_one_line_activity_commands() -> None:
    parser = cli.build_parser()

    list_args = parser.parse_args(["list"])
    assert list_args.command == "list"
    assert list_args.workflow_root is None
    assert list_args.func is cli.cmd_activity_list

    cancel_args = parser.parse_args(["cancel", "wf-1"])
    assert cancel_args.command == "cancel"
    assert cancel_args.target == "wf-1"
    assert cancel_args.func is cli.cmd_activity_cancel
