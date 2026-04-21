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
            "/tmp/rxn",
            "--config",
            "/tmp/chemstack.yaml",
            "--verbose",
            "--log-file",
            "/tmp/orca.log",
            "--priority",
            "4",
            "--force",
            "--max-cores",
            "12",
            "--max-memory-gb",
            "48",
        ]
    )
    workflow_args = parser.parse_args(
        ["run-dir", "/tmp/workflow-inputs", "--priority", "6", "--json"]
    )

    assert orca_args.command == "run-dir"
    assert orca_args.path == "/tmp/rxn"
    assert orca_args.config == "/tmp/chemstack.yaml"
    assert orca_args.priority == 4
    assert orca_args.force is True
    assert orca_args.max_cores == 12
    assert orca_args.max_memory_gb == 48
    assert orca_args.func is unified_cli.cmd_run_dir

    assert workflow_args.path == "/tmp/workflow-inputs"
    assert workflow_args.priority == 6
    assert workflow_args.json is True
    assert workflow_args.func is unified_cli.cmd_run_dir


def test_build_parser_parses_unified_init_bot_scaffold_organize_and_summary_commands() -> None:
    parser = unified_cli.build_parser()

    init_args = parser.parse_args(["init", "--chemstack-config", "/tmp/chemstack.yaml", "--force"])
    bot_args = parser.parse_args(["bot", "--chemstack-config", "/tmp/chemstack.yaml"])
    ts_scaffold_args = parser.parse_args(["scaffold", "ts_search", "/tmp/workflow-inputs"])
    shortcut_scaffold_args = parser.parse_args(
        ["scaffold", "conformer_search", "/tmp/conformer-inputs"]
    )
    organize_args = parser.parse_args(
        ["organize", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--reaction-dir", "/tmp/rxn", "--apply"]
    )
    summary_args = parser.parse_args(
        ["summary", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"]
    )

    assert init_args.command == "init"
    assert init_args.force is True
    assert init_args.func is unified_cli.cmd_init

    assert bot_args.command == "bot"
    assert bot_args.config == "/tmp/chemstack.yaml"
    assert bot_args.func is unified_cli.cmd_bot

    assert ts_scaffold_args.command == "scaffold"
    assert ts_scaffold_args.scaffold_app == "ts_search"
    assert ts_scaffold_args.root == "/tmp/workflow-inputs"
    assert ts_scaffold_args.workflow_type == "reaction_ts_search"
    assert getattr(ts_scaffold_args, "crest_mode", None) is None
    assert ts_scaffold_args.func is unified_cli.cmd_workflow_scaffold

    assert shortcut_scaffold_args.command == "scaffold"
    assert shortcut_scaffold_args.scaffold_app == "conformer_search"
    assert shortcut_scaffold_args.root == "/tmp/conformer-inputs"
    assert shortcut_scaffold_args.workflow_type == "conformer_screening"
    assert getattr(shortcut_scaffold_args, "crest_mode", None) is None
    assert shortcut_scaffold_args.func is unified_cli.cmd_workflow_scaffold

    assert organize_args.command == "organize"
    assert organize_args.organize_app == "orca"
    assert organize_args.reaction_dir == "/tmp/rxn"
    assert organize_args.apply is True
    assert organize_args.func is unified_cli.cmd_orca_organize

    assert summary_args.command == "summary"
    assert summary_args.summary_app == "orca"
    assert summary_args.no_send is True
    assert summary_args.func is unified_cli.cmd_orca_summary


def test_build_parser_rejects_removed_engine_specific_init_subcommands() -> None:
    parser = unified_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["init", "orca"])

    with pytest.raises(SystemExit):
        parser.parse_args(["init", "xtb"])

    with pytest.raises(SystemExit):
        parser.parse_args(["init", "crest"])


@pytest.mark.parametrize(
    "argv",
    [
        ["run-dir", "orca", "/tmp/orca-job"],
        ["run-dir", "workflow", "/tmp/workflow-job"],
        ["run-dir", "crest", "/tmp/crest-job"],
        ["run-dir", "xtb", "/tmp/xtb-job"],
        ["organize", "crest", "--root", "/tmp/crest-jobs"],
        ["organize", "xtb", "--root", "/tmp/xtb-jobs"],
        ["summary", "crest", "job-123"],
        ["summary", "xtb", "job-123"],
        ["queue", "worker", "--app", "crest"],
        ["queue", "worker", "--app", "xtb"],
    ],
)
def test_build_parser_rejects_removed_internal_engine_public_commands(argv: list[str]) -> None:
    parser = unified_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(argv)


@pytest.mark.parametrize(
    "argv",
    [
        ["run-dir", "/tmp/workflow-inputs", "--workflow-type", "reaction_ts_search"],
        ["run-dir", "/tmp/workflow-inputs", "--workflow-root", "/tmp/workflows"],
        ["run-dir", "/tmp/workflow-inputs", "--reactant-xyz", "/tmp/reactant.xyz"],
        ["run-dir", "/tmp/workflow-inputs", "--product-xyz", "/tmp/product.xyz"],
        ["run-dir", "/tmp/workflow-inputs", "--input-xyz", "/tmp/input.xyz"],
        ["run-dir", "/tmp/workflow-inputs", "--crest-mode", "nci"],
        ["run-dir", "/tmp/workflow-inputs", "--max-crest-candidates", "3"],
        ["run-dir", "/tmp/workflow-inputs", "--max-xtb-stages", "3"],
        ["run-dir", "/tmp/workflow-inputs", "--max-orca-stages", "3"],
        ["run-dir", "/tmp/workflow-inputs", "--orca-route-line", "! Opt"],
        ["run-dir", "/tmp/workflow-inputs", "--charge", "0"],
        ["run-dir", "/tmp/workflow-inputs", "--multiplicity", "1"],
    ],
)
def test_build_parser_rejects_removed_workflow_run_dir_override_flags(argv: list[str]) -> None:
    parser = unified_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(argv)


@pytest.mark.parametrize(
    "argv",
    [
        ["scaffold", "ts_search_std", "/tmp/workflow-inputs"],
        ["scaffold", "ts_search_nci", "/tmp/workflow-inputs"],
        ["scaffold", "conformer_search_std", "/tmp/workflow-inputs"],
        ["scaffold", "conformer_search_nci", "/tmp/workflow-inputs"],
        [
            "scaffold",
            "workflow",
            "--root",
            "/tmp/workflow-inputs",
            "--workflow-type",
            "reaction_ts_search",
            "--crest-mode",
            "nci",
        ],
    ],
)
def test_build_parser_rejects_removed_workflow_scaffold_forms(argv: list[str]) -> None:
    parser = unified_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(argv)


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


def test_cmd_bot_uses_flow_telegram_bot(monkeypatch: pytest.MonkeyPatch) -> None:
    import chemstack.flow.telegram_bot as flow_bot

    sentinel = object()
    captured: dict[str, object | None] = {}

    def fake_settings_from_config(config_path: str | None = None) -> object:
        captured["config_path"] = config_path
        return sentinel

    def fake_run_bot(settings: object | None = None) -> int:
        captured["settings"] = settings
        return 26

    monkeypatch.setattr(flow_bot, "settings_from_config", fake_settings_from_config)
    monkeypatch.setattr(flow_bot, "run_bot", fake_run_bot)

    result = unified_cli.cmd_bot(SimpleNamespace(config="/tmp/chemstack.yaml"))

    assert result == 26
    assert captured == {
        "config_path": str(Path("/tmp/chemstack.yaml").resolve()),
        "settings": sentinel,
    }


@pytest.mark.parametrize(
    ("argv", "attr_name", "expected_attrs", "expected_result"),
    [
        (
            ["run-dir", "/tmp/rxn", "--chemstack-config", "/tmp/chemstack.yaml", "--priority", "3"],
            "cmd_run_dir",
            {"command": "run-dir", "path": "/tmp/rxn", "priority": 3},
            21,
        ),
        (
            ["init", "--chemstack-config", "/tmp/chemstack.yaml", "--force"],
            "cmd_init",
            {"command": "init", "force": True},
            22,
        ),
        (
            ["bot", "--chemstack-config", "/tmp/chemstack.yaml"],
            "cmd_bot",
            {"command": "bot", "config": "/tmp/chemstack.yaml"},
            23,
        ),
        (
            ["scaffold", "conformer_search", "/tmp/workflow-job"],
            "cmd_workflow_scaffold",
            {
                "command": "scaffold",
                "scaffold_app": "conformer_search",
                "root": "/tmp/workflow-job",
                "workflow_type": "conformer_screening",
            },
            24,
        ),
        (
            ["organize", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--root", "/tmp/jobs", "--apply"],
            "cmd_orca_organize",
            {"command": "organize", "organize_app": "orca", "root": "/tmp/jobs", "apply": True},
            25,
        ),
        (
            ["summary", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"],
            "cmd_orca_summary",
            {"command": "summary", "summary_app": "orca", "no_send": True},
            27,
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


def test_cmd_run_dir_dispatches_to_orca_for_inp_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "orca_job"
    target.mkdir()
    (target / "job.inp").write_text("! Opt\n", encoding="utf-8")
    calls: list[tuple[str, str]] = []

    monkeypatch.setattr(unified_cli, "cmd_orca_run_dir", lambda args: calls.append(("orca", args.path)) or 41)
    monkeypatch.setattr(unified_cli, "cmd_workflow_run_dir", lambda args: calls.append(("workflow", args.path)) or 42)

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 41
    assert calls == [("orca", str(target))]


def test_cmd_run_dir_dispatches_to_workflow_for_manifest_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "workflow_job"
    target.mkdir()
    (target / "flow.yaml").write_text("workflow_type: conformer_screening\n", encoding="utf-8")
    (target / "path.inp").write_text("$path\n$end\n", encoding="utf-8")
    calls: list[tuple[str, str, str | None]] = []

    monkeypatch.setattr(unified_cli, "cmd_orca_run_dir", lambda args: calls.append(("orca", args.path, None)) or 41)
    monkeypatch.setattr(
        unified_cli,
        "cmd_workflow_run_dir",
        lambda args: calls.append(("workflow", args.path, getattr(args, "workflow_dir", None))) or 42,
    )

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 42
    assert calls == [("workflow", str(target), str(target))]


def test_cmd_run_dir_reports_unknown_directory_layout(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "unknown_job"
    target.mkdir()

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 1
    assert "Could not infer run-dir target type from directory" in capsys.readouterr().out


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
    assert "active_simulations: 1" in stdout
    assert "- xtb-q-1 kind=job engine=xtb status=running label=rxn-a source=xtb_auto" in stdout
    assert "crest-q-1" not in stdout
    assert "wf-1" not in stdout


def test_cmd_queue_list_groups_workflow_children_in_text_output(
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
                    "activity_id": "wf-1",
                    "kind": "workflow",
                    "engine": "orca",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
                },
                {
                    "activity_id": "xtb-q-1",
                    "kind": "job",
                    "engine": "xtb",
                    "status": "running",
                    "label": "path-search",
                    "source": "xtb_auto",
                    "metadata": {
                        "job_dir": "/tmp/xtb/workflow_jobs/wf-1/stage_01_xtb",
                    },
                },
                {
                    "activity_id": "orca-q-1",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "ts-opt",
                    "source": "chemstack_orca",
                    "metadata": {
                        "reaction_dir": "/tmp/orca/workflow_jobs/wf-1/stage_03_orca/case_001",
                    },
                },
                {
                    "activity_id": "orca-q-standalone",
                    "kind": "job",
                    "engine": "orca",
                    "status": "running",
                    "label": "standalone-ts",
                    "source": "chemstack_orca",
                    "metadata": {
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
    assert "- wf-1 kind=workflow engine=orca status=running label=reaction-case source=chem_flow" in stdout
    assert "  - xtb-q-1 kind=job engine=xtb status=running label=path-search source=xtb_auto" in stdout
    assert "  - orca-q-1 kind=job engine=orca status=running label=ts-opt source=chemstack_orca" in stdout
    assert "- orca-q-standalone kind=job engine=orca status=running label=standalone-ts source=chemstack_orca" in stdout


def test_cmd_queue_list_shows_all_workflow_child_jobs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    child_rows = [
        {
            "activity_id": f"orca-q-{index}",
            "kind": "job",
            "engine": "orca",
            "status": "running",
            "label": f"ts-{index}",
            "source": "chemstack_orca",
            "metadata": {
                "reaction_dir": f"/tmp/orca/workflow_jobs/wf-1/stage_03_orca/case_{index:03d}",
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
                    "engine": "orca",
                    "status": "running",
                    "label": "reaction-case",
                    "source": "chem_flow",
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
    assert stdout.count("  - orca-q-") == 9
    assert "- wf-1 kind=workflow engine=orca status=running label=reaction-case source=chem_flow" in stdout


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
    assert payload["active_simulations"] == 1
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

    assert [spec.app for spec in specs] == ["orca"]
    assert specs[0].argv[-2:] == ("queue", "worker")
    assert str(specs[0].argv[2]) == "chemstack.orca.cli"
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

    assert [spec.app for spec in specs] == ["orca", "crest", "xtb", "workflow"]
    assert str(specs[1].argv[2]) == "chemstack.crest._internal_cli"
    assert str(specs[2].argv[2]) == "chemstack.xtb._internal_cli"
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

    assert [spec.app for spec in specs] == ["crest", "xtb", "workflow"]
    assert str(specs[0].argv[2]) == "chemstack.crest._internal_cli"
    assert str(specs[1].argv[2]) == "chemstack.xtb._internal_cli"
    assert "--workflow-root" in specs[2].argv
    assert "/tmp/workflows" in specs[2].argv


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
