from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack import cli_common
from chemstack import cli_queue
from chemstack import cli_run_dir
from chemstack import cli_summary
from chemstack import cli_workers
from chemstack import cli as unified_cli


@pytest.fixture(autouse=True)
def _isolate_shared_config_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    def _explicit_shared_config_path(explicit: str | None) -> str | None:
        if not explicit:
            return None
        return str(Path(explicit).expanduser().resolve())

    monkeypatch.setattr(cli_common, "_discover_shared_config_path", _explicit_shared_config_path)
    monkeypatch.setattr(cli_common, "shared_workflow_root_from_config", lambda config_path: None)


class _FakeWorkerProcess:
    def __init__(self, poll_values: list[int | None]) -> None:
        self._poll_values = list(poll_values)
        self._terminal_returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0

    def poll(self) -> int | None:
        if self._terminal_returncode is not None:
            return self._terminal_returncode
        if self._poll_values:
            value = self._poll_values.pop(0)
            if value is not None:
                self._terminal_returncode = value
            return value
        return None

    def terminate(self) -> None:
        self.terminate_calls += 1
        self._poll_values.clear()
        self._terminal_returncode = -15

    def kill(self) -> None:
        self.kill_calls += 1
        self._poll_values.clear()
        self._terminal_returncode = -9


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
    assert list_args.func is cli_queue.cmd_queue_list

    clear_args = parser.parse_args(["queue", "list", "clear", "--json"])
    assert clear_args.command == "queue"
    assert clear_args.queue_command == "list"
    assert clear_args.action == "clear"
    assert clear_args.json is True
    assert clear_args.func is cli_queue.cmd_queue_list

    cancel_args = parser.parse_args(["queue", "cancel", "xtb-q-1"])
    assert cancel_args.queue_command == "cancel"
    assert cancel_args.target == "xtb-q-1"
    assert cancel_args.func is cli_queue.cmd_queue_cancel


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
    assert orca_args.func is cli_run_dir.cmd_run_dir

    assert workflow_args.path == "/tmp/workflow-inputs"
    assert workflow_args.priority == 6
    assert workflow_args.json is True
    assert workflow_args.func is cli_run_dir.cmd_run_dir


def test_build_parser_parses_unified_init_scaffold_organize_and_summary_commands() -> None:
    parser = unified_cli.build_parser()

    init_args = parser.parse_args(["init", "--chemstack-config", "/tmp/chemstack.yaml", "--force"])
    ts_scaffold_args = parser.parse_args(["scaffold", "ts_search", "/tmp/workflow-inputs"])
    shortcut_scaffold_args = parser.parse_args(
        ["scaffold", "conformer_search", "/tmp/conformer-inputs"]
    )
    organize_args = parser.parse_args(
        [
            "organize",
            "orca",
            "--chemstack-config",
            "/tmp/chemstack.yaml",
            "--reaction-dir",
            "/tmp/rxn",
            "--apply",
        ]
    )
    summary_args = parser.parse_args(
        ["summary", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"]
    )
    combined_summary_args = parser.parse_args(
        ["summary", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"]
    )

    assert init_args.command == "init"
    assert init_args.force is True
    assert init_args.func is cli_run_dir.cmd_init

    assert ts_scaffold_args.command == "scaffold"
    assert ts_scaffold_args.scaffold_app == "ts_search"
    assert ts_scaffold_args.root == "/tmp/workflow-inputs"
    assert ts_scaffold_args.workflow_type == "reaction_ts_search"
    assert getattr(ts_scaffold_args, "crest_mode", None) is None
    assert ts_scaffold_args.func is cli_run_dir.cmd_workflow_scaffold

    assert shortcut_scaffold_args.command == "scaffold"
    assert shortcut_scaffold_args.scaffold_app == "conformer_search"
    assert shortcut_scaffold_args.root == "/tmp/conformer-inputs"
    assert shortcut_scaffold_args.workflow_type == "conformer_screening"
    assert getattr(shortcut_scaffold_args, "crest_mode", None) is None
    assert shortcut_scaffold_args.func is cli_run_dir.cmd_workflow_scaffold

    assert organize_args.command == "organize"
    assert organize_args.organize_app == "orca"
    assert organize_args.reaction_dir == "/tmp/rxn"
    assert organize_args.apply is True
    assert organize_args.func is cli_run_dir.cmd_orca_organize

    assert summary_args.command == "summary"
    assert summary_args.summary_app == "orca"
    assert summary_args.no_send is True
    assert summary_args.func is cli_summary.cmd_summary

    assert combined_summary_args.command == "summary"
    assert combined_summary_args.summary_app == "combined"
    assert combined_summary_args.no_send is True
    assert combined_summary_args.func is cli_summary.cmd_summary


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


@pytest.mark.parametrize(
    "argv",
    [
        ["bot"],
        ["runtime"],
        ["queue", "worker"],
    ],
)
def test_build_parser_rejects_removed_service_commands(argv: list[str]) -> None:
    parser = unified_cli.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(argv)


def test_main_dispatches_unified_queue_list(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[SimpleNamespace] = []

    def fake_cmd(args: SimpleNamespace) -> int:
        seen.append(args)
        return 17

    monkeypatch.setattr(cli_queue, "cmd_queue_list", fake_cmd)

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

    monkeypatch.setattr(cli_queue, "cmd_queue_cancel", fake_cmd)

    result = unified_cli.main(["queue", "cancel", "crest-q-1", "--json"])

    assert result == 18
    assert len(seen) == 1
    assert seen[0].queue_command == "cancel"
    assert seen[0].target == "crest-q-1"
    assert seen[0].json is True


@pytest.mark.parametrize(
    ("command_argv", "expected"),
    [
        (
            (
                "/home/user/chemstack/.venv/bin/python",
                "-m",
                "chemstack.orca._internal_cli",
                "--config",
                "/tmp/chemstack.yaml",
                "queue",
                "worker",
            ),
            "chemstack",
        ),
        (("/home/user/.venv/bin/chemstack", "queue", "worker"), "chemstack"),
        (("/usr/bin/python", "-m", "something_else"), "unknown"),
    ],
)
def test_classify_existing_orca_worker_distinguishes_chemstack_and_unknown(
    command_argv: tuple[str, ...],
    expected: str,
) -> None:
    assert cli_workers._classify_existing_orca_worker(command_argv) == expected


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
            [
                "organize",
                "orca",
                "--chemstack-config",
                "/tmp/chemstack.yaml",
                "--root",
                "/tmp/jobs",
                "--apply",
            ],
            "cmd_orca_organize",
            {"command": "organize", "organize_app": "orca", "root": "/tmp/jobs", "apply": True},
            25,
        ),
        (
            ["summary", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"],
            "cmd_summary",
            {"command": "summary", "summary_app": "orca", "no_send": True},
            27,
        ),
        (
            ["summary", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"],
            "cmd_summary",
            {"command": "summary", "summary_app": "combined", "no_send": True},
            28,
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

    target_module = cli_summary if attr_name == "cmd_summary" else cli_run_dir
    monkeypatch.setattr(target_module, attr_name, fake_cmd)

    result = unified_cli.main(argv)

    assert result == expected_result
    assert len(seen) == 1
    for key, expected_value in expected_attrs.items():
        assert getattr(seen[0], key) == expected_value
