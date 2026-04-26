from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import signal
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
    assert list_args.func is unified_cli.cmd_queue_list

    clear_args = parser.parse_args(["queue", "list", "clear", "--json"])
    assert clear_args.command == "queue"
    assert clear_args.queue_command == "list"
    assert clear_args.action == "clear"
    assert clear_args.json is True
    assert clear_args.func is unified_cli.cmd_queue_list

    cancel_args = parser.parse_args(["queue", "cancel", "xtb-q-1"])
    assert cancel_args.queue_command == "cancel"
    assert cancel_args.target == "xtb-q-1"
    assert cancel_args.func is unified_cli.cmd_queue_cancel


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


def test_build_parser_parses_unified_init_scaffold_organize_and_summary_commands() -> None:
    parser = unified_cli.build_parser()

    init_args = parser.parse_args(["init", "--chemstack-config", "/tmp/chemstack.yaml", "--force"])
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
    combined_summary_args = parser.parse_args(
        ["summary", "--chemstack-config", "/tmp/chemstack.yaml", "--no-send"]
    )

    assert init_args.command == "init"
    assert init_args.force is True
    assert init_args.func is unified_cli.cmd_init

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
    assert summary_args.func is unified_cli.cmd_summary

    assert combined_summary_args.command == "summary"
    assert combined_summary_args.summary_app == "combined"
    assert combined_summary_args.no_send is True
    assert combined_summary_args.func is unified_cli.cmd_summary


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
    assert unified_cli._classify_existing_orca_worker(command_argv) == expected


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
            ["organize", "orca", "--chemstack-config", "/tmp/chemstack.yaml", "--root", "/tmp/jobs", "--apply"],
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

    monkeypatch.setattr(unified_cli, attr_name, fake_cmd)

    result = unified_cli.main(argv)

    assert result == expected_result
    assert len(seen) == 1
    for key, expected_value in expected_attrs.items():
        assert getattr(seen[0], key) == expected_value


def test_cmd_summary_dispatches_combined_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[argparse.Namespace] = []

    def _fake_combined_summary(args: argparse.Namespace) -> int:
        seen.append(args)
        return 29

    monkeypatch.setattr(unified_cli, "_configure_orca_logging", lambda args: None)
    monkeypatch.setattr(unified_cli, "_engine_config_for_command", lambda args: "/tmp/chemstack.yaml")
    monkeypatch.setattr("chemstack.summary.cmd_summary", _fake_combined_summary)

    args = argparse.Namespace(
        command="summary",
        summary_app="combined",
        chemstack_config="/tmp/chemstack.yaml",
        config=None,
        no_send=True,
        verbose=False,
        log_file=None,
    )

    result = unified_cli.cmd_summary(args)

    assert result == 29
    assert args.config == "/tmp/chemstack.yaml"
    assert seen == [args]


def test_cmd_summary_dispatches_orca_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[argparse.Namespace] = []

    def _fake_orca_summary(args: argparse.Namespace) -> int:
        seen.append(args)
        return 30

    monkeypatch.setattr(unified_cli, "cmd_orca_summary", _fake_orca_summary)

    args = argparse.Namespace(
        command="summary",
        summary_app="orca",
        chemstack_config="/tmp/chemstack.yaml",
        config=None,
        no_send=True,
        verbose=False,
        log_file=None,
    )

    result = unified_cli.cmd_summary(args)

    assert result == 30
    assert args.config is None
    assert seen == [args]


def test_cmd_run_dir_dispatches_to_orca_for_inp_directories(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "orca_job"
    target.mkdir()
    (target / "job.inp").write_text("! Opt\n", encoding="utf-8")
    calls: list[tuple[str, str]] = []

    def _fake_orca_run_dir(args: Any) -> int:
        calls.append(("orca", args.path))
        return 41

    def _fake_workflow_run_dir(args: Any) -> int:
        calls.append(("workflow", args.path))
        return 42

    monkeypatch.setattr(unified_cli, "cmd_orca_run_dir", _fake_orca_run_dir)
    monkeypatch.setattr(unified_cli, "cmd_workflow_run_dir", _fake_workflow_run_dir)

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

    def _fake_orca_run_dir(args: Any) -> int:
        calls.append(("orca", args.path, None))
        return 41

    def _fake_workflow_run_dir(args: Any) -> int:
        calls.append(("workflow", args.path, getattr(args, "workflow_dir", None)))
        return 42

    monkeypatch.setattr(unified_cli, "cmd_orca_run_dir", _fake_orca_run_dir)
    monkeypatch.setattr(unified_cli, "cmd_workflow_run_dir", _fake_workflow_run_dir)

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 42
    assert calls == [("workflow", str(target), str(target))]


def test_cmd_run_dir_prefers_orca_for_mixed_input_xyz_and_inp_without_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "mixed_job"
    target.mkdir()
    (target / "input.xyz").write_text("3\nmixed\nH 0 0 0\nH 0 0 0.7\nH 0 0 1.4\n", encoding="utf-8")
    (target / "tsopt.inp").write_text("! OptTS\n", encoding="utf-8")
    calls: list[tuple[str, str]] = []

    def _fake_orca_run_dir(args: Any) -> int:
        calls.append(("orca", args.path))
        return 41

    def _fake_workflow_run_dir(args: Any) -> int:
        calls.append(("workflow", args.path))
        return 42

    monkeypatch.setattr(unified_cli, "cmd_orca_run_dir", _fake_orca_run_dir)
    monkeypatch.setattr(unified_cli, "cmd_workflow_run_dir", _fake_workflow_run_dir)

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 41
    assert calls == [("orca", str(target))]


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


def test_cmd_run_dir_requires_manifest_for_workflow_scaffold_directories(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    target = tmp_path / "workflow_scaffold"
    target.mkdir()
    (target / "input.xyz").write_text("3\nmol\nH 0 0 0\nH 0 0 0.7\nH 0 0 1.4\n", encoding="utf-8")

    result = unified_cli.cmd_run_dir(
        SimpleNamespace(
            path=str(target),
        )
    )

    assert result == 1
    assert "Could not infer run-dir target type from directory" in capsys.readouterr().out


def test_queue_table_lines_align_wide_headers_and_icons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc))

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


def test_cmd_queue_list_filters_text_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc))
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
    assert "Status" in stdout and "Name" in stdout and "Detail" in stdout and "ID" in stdout and "Elapsed" in stdout
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
    monkeypatch.setattr(unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc))
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
                        "job_dir": "/tmp/workflows/wf-1/internal/xtb/runs/stage_01_xtb",
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
                        "job_dir": "/tmp/workflows/wf-1/internal/crest/runs/stage_00_crest",
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
                        "reaction_dir": "/tmp/workflows/wf-1/stage_03_orca/case_001/reaction_dir",
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
    monkeypatch.setattr(unified_cli, "_queue_table_now", lambda: datetime(2026, 4, 26, 3, 0, 0, tzinfo=timezone.utc))
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
    monkeypatch.setattr(unified_cli, "clear_activities", lambda **kwargs: pytest.fail("clear_activities should not run"))

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
        raise TimeoutError("Workflow is busy and could not be locked for cancellation within 5s: /tmp/wf_busy")

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
    assert str(specs[0].argv[2]) == "chemstack.orca._internal_cli"
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

    def _shared_workflow_root(config_path: str | None) -> str:
        seen.append(config_path)
        return "/tmp/from-config-workflows"

    monkeypatch.setattr(
        unified_cli,
        "shared_workflow_root_from_config",
        _shared_workflow_root,
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


def test_engine_config_for_command_uses_discovered_shared_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")

    discovered = unified_cli._engine_config_for_command(
        argparse.Namespace(
            chemstack_config=None,
            config=None,
            global_config=None,
        )
    )

    assert discovered == str(Path("/tmp/chemstack.yaml").resolve())


def test_cmd_orca_run_dir_uses_discovered_shared_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "orca_job"
    target.mkdir()
    (target / "job.inp").write_text("! Opt\n", encoding="utf-8")
    captured: list[tuple[str | None, str]] = []

    monkeypatch.setattr(unified_cli, "_configure_orca_logging", lambda args: None)
    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml")

    import chemstack.orca.commands.run_inp as run_inp_cmd

    def _fake_cmd_run_inp(args: argparse.Namespace) -> int:
        captured.append((getattr(args, "config", None), getattr(args, "path", "")))
        return 31

    monkeypatch.setattr(
        run_inp_cmd,
        "cmd_run_inp",
        _fake_cmd_run_inp,
    )

    result = unified_cli.cmd_orca_run_dir(
        argparse.Namespace(
            path=str(target),
            chemstack_config=None,
            config=None,
            global_config=None,
            verbose=False,
            log_file=None,
        )
    )

    assert result == 31
    assert captured == [(str(Path("/tmp/chemstack.yaml").resolve()), str(target))]


def test_cmd_queue_worker_delegates_to_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = [unified_cli.WorkerSpec(app="orca", argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker"))]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)
    monkeypatch.setattr(unified_cli, "_run_worker_supervisor", lambda built_specs: 0 if built_specs == specs else 1)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(app=["orca"], workflow_root=None, chemstack_config=None, json=False)
    )

    assert result == 0


def test_cmd_queue_worker_reports_existing_chemstack_orca_worker_conflict(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    specs = [unified_cli.WorkerSpec(app="orca", argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker"))]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)
    monkeypatch.setattr(
        unified_cli,
        "_detect_existing_orca_worker_conflict",
        lambda built_specs, args: unified_cli._ExistingWorkerConflict(
            app="orca",
            pid=3589996,
            allowed_root="/home/user/orca_runs",
            source="chemstack",
            command="/home/user/chemstack/.venv/bin/python -m chemstack.orca.cli --config /tmp/chemstack.yaml queue worker",
        ),
    )
    monkeypatch.setattr(unified_cli, "_run_worker_supervisor", lambda built_specs: 99)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(app=["orca"], workflow_root=None, chemstack_config="/tmp/chemstack.yaml", json=False)
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "existing ORCA queue worker detected" in out
    assert "source: chemstack queue worker" in out
    assert "legacy orca_auto queue worker" not in out
    assert "Stop the existing queue-worker service before starting another worker." in out


def test_run_worker_supervisor_keeps_siblings_running_after_clean_exit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([0]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([None]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            installed_handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 0
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 1
    assert popen_calls == 3
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 0" in out
    assert "restarting worker[workflow]: workflow worker" in out


def test_run_worker_supervisor_restarts_workers_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([2]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([None]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            installed_handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 0
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 1
    assert popen_calls == 3
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 2" in out
    assert "restarting worker[workflow]: workflow worker" in out


def test_run_worker_supervisor_stops_after_repeated_startup_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([2]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([2]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 2
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 0
    assert popen_calls == 3
    assert sleep_calls == 1
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 2" in out
    assert "worker[workflow] failed repeatedly during startup; stopping supervisor to avoid a restart loop." in out
    assert "restarting worker[workflow]: workflow worker" in out


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
        argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker"),
        cwd="/tmp/chemstack",
        env={
            "PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack",
            "SECRET_TOKEN": "do-not-print",
        },
    )

    payload = spec.to_dict()

    assert payload["env"] == {"PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack"}
