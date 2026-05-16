from __future__ import annotations

import argparse
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


def test_cmd_summary_dispatches_combined_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[argparse.Namespace] = []

    def _fake_combined_summary(args: argparse.Namespace) -> int:
        seen.append(args)
        return 29

    monkeypatch.setattr(unified_cli, "_configure_orca_logging", lambda args: None)
    monkeypatch.setattr(
        unified_cli, "_engine_config_for_command", lambda args: "/tmp/chemstack.yaml"
    )
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
