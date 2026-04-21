from __future__ import annotations

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


def test_build_parser_parses_unified_queue_commands() -> None:
    parser = unified_cli.build_parser()

    list_args = parser.parse_args(
        [
            "queue",
            "list",
            "--engine",
            "xtb",
            "--status",
            "running",
            "--kind",
            "job",
        ]
    )
    assert list_args.command == "queue"
    assert list_args.queue_command == "list"
    assert list_args.engine == ["xtb"]
    assert list_args.status == ["running"]
    assert list_args.kind == ["job"]
    assert list_args.func is unified_cli.cmd_queue_list

    cancel_args = parser.parse_args(["queue", "cancel", "xtb-q-1"])
    assert cancel_args.queue_command == "cancel"
    assert cancel_args.target == "xtb-q-1"
    assert cancel_args.func is unified_cli.cmd_queue_cancel

    worker_args = parser.parse_args(["queue", "worker", "--app", "orca", "--app", "workflow"])
    assert worker_args.queue_command == "worker"
    assert worker_args.app == ["orca", "workflow"]
    assert worker_args.func is unified_cli.cmd_queue_worker


def test_build_parser_parses_unified_run_dir_commands() -> None:
    parser = unified_cli.build_parser()

    orca_args = parser.parse_args(
        [
            "run-dir",
            "orca",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--verbose",
            "--log-file",
            "/tmp/orca.log",
            "/tmp/rxn",
            "--priority",
            "4",
            "--force",
            "--max-cores",
            "12",
            "--max-memory-gb",
            "48",
        ]
    )
    xtb_args = parser.parse_args(["run-dir", "xtb", "--chemstack-config", "/tmp/chemstack.yaml", "/tmp/xtb-job"])
    crest_args = parser.parse_args(
        ["run-dir", "crest", "--chemstack-config", "/tmp/chemstack.yaml", "/tmp/crest-job", "--priority", "2"]
    )
    workflow_args = parser.parse_args(
        ["run-dir", "workflow", "/tmp/workflow-inputs", "--workflow-root", "/tmp/workflows", "--priority", "6"]
    )

    assert orca_args.command == "run-dir"
    assert orca_args.run_dir_app == "orca"
    assert orca_args.path == "/tmp/rxn"
    assert orca_args.priority == 4
    assert orca_args.force is True
    assert orca_args.max_cores == 12
    assert orca_args.max_memory_gb == 48
    assert orca_args.func is unified_cli.cmd_orca_run_dir

    assert xtb_args.run_dir_app == "xtb"
    assert xtb_args.path == "/tmp/xtb-job"
    assert xtb_args.func is unified_cli.cmd_xtb_run_dir

    assert crest_args.run_dir_app == "crest"
    assert crest_args.path == "/tmp/crest-job"
    assert crest_args.priority == 2
    assert crest_args.func is unified_cli.cmd_crest_run_dir

    assert workflow_args.run_dir_app == "workflow"
    assert workflow_args.workflow_dir == "/tmp/workflow-inputs"
    assert workflow_args.workflow_root == "/tmp/workflows"
    assert workflow_args.priority == 6
    assert workflow_args.func is unified_cli.cmd_workflow_run_dir


def test_build_parser_parses_unified_init_organize_and_summary_commands() -> None:
    parser = unified_cli.build_parser()

    init_args = parser.parse_args(["init", "xtb", "--chemstack-config", "/tmp/chemstack.yaml", "--root", "/tmp/job"])
    organize_args = parser.parse_args(
        ["organize", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--reaction-dir", "/tmp/rxn", "--apply"]
    )
    summary_args = parser.parse_args(["summary", "crest", "--chemstack-config", "/tmp/chemstack.yaml", "job-123", "--json"])

    assert init_args.command == "init"
    assert init_args.init_app == "xtb"
    assert init_args.root == "/tmp/job"
    assert init_args.func is unified_cli.cmd_xtb_init

    assert organize_args.command == "organize"
    assert organize_args.organize_app == "orca"
    assert organize_args.reaction_dir == "/tmp/rxn"
    assert organize_args.apply is True
    assert organize_args.func is unified_cli.cmd_orca_organize

    assert summary_args.command == "summary"
    assert summary_args.summary_app == "crest"
    assert summary_args.target == "job-123"
    assert summary_args.json is True
    assert summary_args.func is unified_cli.cmd_crest_summary


def test_main_dispatches_unified_queue_list(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[SimpleNamespace] = []

    def fake_cmd(args: SimpleNamespace) -> int:
        seen.append(args)
        return 17

    monkeypatch.setattr(unified_cli, "cmd_queue_list", fake_cmd)

    result = unified_cli.main(["queue", "list", "--engine", "xtb", "--status", "running"])

    assert result == 17
    assert len(seen) == 1
    assert seen[0].queue_command == "list"
    assert seen[0].engine == ["xtb"]
    assert seen[0].status == ["running"]


def test_main_dispatches_unified_queue_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[SimpleNamespace] = []

    def fake_cmd(args: SimpleNamespace) -> int:
        seen.append(args)
        return 18

    monkeypatch.setattr(unified_cli, "cmd_queue_cancel", fake_cmd)

    result = unified_cli.main(["queue", "cancel", "crest-q-1", "--json"])

    assert result == 18
    assert len(seen) == 1
    assert seen[0].queue_command == "cancel"
    assert seen[0].target == "crest-q-1"
    assert seen[0].json is True


def test_main_dispatches_unified_queue_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[SimpleNamespace] = []

    def fake_cmd(args: SimpleNamespace) -> int:
        seen.append(args)
        return 19

    monkeypatch.setattr(unified_cli, "cmd_queue_worker", fake_cmd)

    result = unified_cli.main(
        ["queue", "worker", "--app", "orca", "--app", "workflow", "--workflow-root", "/tmp/workflows"]
    )

    assert result == 19
    assert len(seen) == 1
    assert seen[0].queue_command == "worker"
    assert seen[0].app == ["orca", "workflow"]
    assert seen[0].workflow_root == "/tmp/workflows"


@pytest.mark.parametrize(
    ("argv", "attr_name", "expected_attrs", "expected_result"),
    [
        (
            ["run-dir", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "/tmp/rxn", "--priority", "3"],
            "cmd_orca_run_dir",
            {"command": "run-dir", "run_dir_app": "orca", "path": "/tmp/rxn", "priority": 3},
            21,
        ),
        (
            ["init", "xtb", "--chemstack-config", "/tmp/chemstack.yaml", "--root", "/tmp/job", "--job-type", "opt"],
            "cmd_xtb_init",
            {"command": "init", "init_app": "xtb", "root": "/tmp/job", "job_type": "opt"},
            22,
        ),
        (
            ["organize", "crest", "--chemstack-config", "/tmp/chemstack.yaml", "--root", "/tmp/jobs", "--apply"],
            "cmd_crest_organize",
            {"command": "organize", "organize_app": "crest", "root": "/tmp/jobs", "apply": True},
            23,
        ),
        (
            ["summary", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"],
            "cmd_orca_summary",
            {"command": "summary", "summary_app": "orca", "no_send": True},
            24,
        ),
    ],
)
def test_main_dispatches_unified_engine_commands(
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
    attr_name: str,
    expected_attrs: dict[str, Any],
    expected_result: int,
) -> None:
    seen: list[SimpleNamespace] = []

    def fake_cmd(args: SimpleNamespace) -> int:
        seen.append(args)
        return expected_result

    monkeypatch.setattr(unified_cli, attr_name, fake_cmd)

    result = unified_cli.main(argv)

    assert result == expected_result
    assert len(seen) == 1
    for key, expected_value in expected_attrs.items():
        assert getattr(seen[0], key) == expected_value


def test_cmd_queue_list_filters_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
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
                    "activity_id": "crest-q-1",
                    "kind": "job",
                    "engine": "crest",
                    "status": "pending",
                    "label": "mol-a",
                    "source": "crest_auto",
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
    assert "activity_count: 1" in stdout
    assert "- xtb-q-1 kind=job engine=xtb status=running label=rxn-a source=xtb_auto" in stdout
    assert "crest-q-1" not in stdout
    assert "wf-1" not in stdout


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
    assert payload["activities"][0]["activity_id"] == "orca-q-1"
    assert payload["sources"]["orca_auto_config"] == "/tmp/chemstack.yaml"


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
    assert payload["activities"][0]["activity_id"] == "xtb-q-1"


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


def test_build_worker_specs_defaults_to_engine_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")

    def fake_sibling_app_command(
        *,
        executable: str,
        config_path: str,
        repo_root: str | None,
        module_name: str,
        tail_argv: list[str],
    ) -> tuple[list[str], str | None, dict[str, str] | None]:
        del executable, repo_root
        return (["python", "-m", module_name, "--config", config_path, *tail_argv], None, {})

    monkeypatch.setattr(unified_cli, "sibling_app_command", fake_sibling_app_command)

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=None, workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["orca", "xtb", "crest"]
    assert specs[0].argv[-2:] == ("queue", "worker")
    assert str(specs[1].argv[2]) == "chemstack.xtb.cli"
    assert str(specs[2].argv[2]) == "chemstack.crest.cli"
    assert specs[0].env is not None
    assert specs[0].env[unified_cli._DIRECT_ENGINE_WORKER_ENV_VAR] == "1"


def test_build_worker_specs_defaults_to_all_workers_when_workflow_root_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")
    monkeypatch.setattr(unified_cli, "shared_workflow_root_from_config", lambda config_path: "/tmp/workflows")

    def fake_sibling_app_command(
        *,
        executable: str,
        config_path: str,
        repo_root: str | None,
        module_name: str,
        tail_argv: list[str],
    ) -> tuple[list[str], str | None, dict[str, str] | None]:
        del executable, repo_root
        return (["python", "-m", module_name, "--config", config_path, *tail_argv], None, {})

    monkeypatch.setattr(unified_cli, "sibling_app_command", fake_sibling_app_command)

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=None, workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["orca", "xtb", "crest", "workflow"]
    assert specs[-1].argv[1:5] == (
        "-m",
        "chemstack.flow.cli",
        "workflow",
        "worker",
    )
    assert "--workflow-root" in specs[-1].argv
    assert "/tmp/workflows" in specs[-1].argv


def test_build_worker_specs_requires_workflow_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")
    with pytest.raises(ValueError, match="workflow worker requires workflow.root in chemstack.yaml"):
        unified_cli._build_worker_specs(
            SimpleNamespace(app=["workflow"], workflow_root=None, chemstack_config=None)
        )


def test_build_worker_specs_explicit_workflow_app_uses_configured_workflow_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")
    monkeypatch.setattr(unified_cli, "shared_workflow_root_from_config", lambda config_path: "/tmp/workflows")

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=["workflow"], workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["workflow"]
    assert "--workflow-root" in specs[0].argv
    assert "/tmp/workflows" in specs[0].argv


def test_workflow_root_for_args_uses_shared_config(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str | None] = []

    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")
    monkeypatch.setattr(
        unified_cli,
        "shared_workflow_root_from_config",
        lambda config_path: seen.append(config_path) or "/tmp/from-config-workflows",
    )

    discovered = unified_cli._workflow_root_for_args(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            config=None,
            global_config=None,
        )
    )

    assert discovered == "/tmp/from-config-workflows"
    assert seen == ["/tmp/chemstack.yaml"]


def test_cmd_queue_worker_delegates_to_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = [unified_cli.WorkerSpec(app="orca", argv=("python", "-m", "chemstack.orca.cli", "queue", "worker"))]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)
    monkeypatch.setattr(unified_cli, "_run_worker_supervisor", lambda built_specs: 0 if built_specs == specs else 1)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(app=["orca"], workflow_root=None, chemstack_config=None, json=False)
    )

    assert result == 0


def test_cmd_queue_worker_json_outputs_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    specs = [
        unified_cli.WorkerSpec(
            app="workflow",
            argv=("python", "-m", "chemstack.flow.cli", "workflow", "worker", "--workflow-root", "/tmp/workflows"),
        )
    ]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(app=["workflow"], workflow_root="/tmp/workflows", chemstack_config=None, json=True)
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["workers"][0]["app"] == "workflow"
    assert payload["workers"][0]["argv"][2] == "chemstack.flow.cli"


def test_worker_spec_to_dict_redacts_unrelated_environment_keys() -> None:
    spec = unified_cli.WorkerSpec(
        app="orca",
        argv=("python", "-m", "chemstack.orca.cli", "queue", "worker"),
        cwd="/tmp/chemstack",
        env={
            "PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack",
            "SECRET_TOKEN": "do-not-print",
        },
    )

    payload = spec.to_dict()

    assert payload["env"] == {"PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack"}
