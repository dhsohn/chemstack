from __future__ import annotations

import subprocess
from argparse import Namespace
from pathlib import Path
from typing import Any

from chemstack import cli_systemd


def _make_repo(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "chemstack"
    python_path = repo / ".venv" / "bin" / "python"
    config_path = repo / "config" / "chemstack.yaml"
    python_path.parent.mkdir(parents=True)
    config_path.parent.mkdir(parents=True)
    python_path.write_text("#!/usr/bin/env python\n", encoding="utf-8")
    config_path.write_text(
        "\n".join(
            [
                "telegram:",
                "  bot_token: token",
                "  chat_id: chat",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return repo, config_path


def test_build_systemd_install_plan_renders_repo_and_config_paths(tmp_path: Path) -> None:
    repo, config_path = _make_repo(tmp_path)
    unit_dir = tmp_path / "units"

    plan = cli_systemd.build_systemd_install_plan(
        target_user="alice",
        repo=repo,
        config=config_path,
        unit_dir=unit_dir,
        is_root=lambda: True,
    )

    assert plan.enabled_unit == "chemstack-runtime@alice.target"
    assert plan.use_sudo is False
    assert plan.warnings == ()
    assert plan.commands == (
        ("systemctl", "daemon-reload"),
        ("systemctl", "enable", "--now", "chemstack-runtime@alice.target"),
    )

    unit_by_name = {unit.name: unit for unit in plan.units}
    worker_content = unit_by_name["chemstack-queue-worker@.service"].content
    assert f"WorkingDirectory={repo.resolve(strict=False)}" in worker_content
    assert f"Environment=CHEMSTACK_CONFIG={config_path.resolve(strict=False)}" in worker_content
    assert f"ExecStart={repo.resolve(strict=False)}/.venv/bin/python" in worker_content
    assert unit_by_name["chemstack-runtime@.target"].destination == (
        unit_dir.resolve(strict=False) / "chemstack-runtime@.target"
    )


def test_build_systemd_install_plan_worker_only_enables_worker_service(tmp_path: Path) -> None:
    repo, config_path = _make_repo(tmp_path)

    plan = cli_systemd.build_systemd_install_plan(
        target_user="alice",
        repo=repo,
        config=config_path,
        unit_dir=tmp_path / "units",
        worker_only=True,
        no_start=True,
        is_root=lambda: True,
    )

    assert plan.enabled_unit == "chemstack-queue-worker@alice.service"
    assert plan.commands == (
        ("systemctl", "daemon-reload"),
        ("systemctl", "enable", "chemstack-queue-worker@alice.service"),
    )


def test_cmd_systemd_install_writes_units_and_runs_commands(
    tmp_path: Path,
    capsys: Any,
) -> None:
    repo, config_path = _make_repo(tmp_path)
    unit_dir = tmp_path / "units"
    commands: list[tuple[str, ...]] = []

    def _fake_run(argv: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
        del check
        commands.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0)

    args = Namespace(
        target_user="alice",
        repo=str(repo),
        config=str(config_path),
        unit_dir=str(unit_dir),
        worker_only=False,
        no_enable=False,
        no_start=False,
        dry_run=False,
        no_sudo=True,
    )

    result = cli_systemd.cmd_systemd_install(
        args,
        deps=Namespace(run=_fake_run, is_root=lambda: True),
    )

    assert result == 0
    assert commands == [
        ("systemctl", "daemon-reload"),
        ("systemctl", "enable", "--now", "chemstack-runtime@alice.target"),
    ]
    assert (unit_dir / "chemstack-queue-worker@.service").exists()
    assert (unit_dir / "chemstack-runtime@.target").exists()
    captured = capsys.readouterr().out
    assert "installed:" in captured
    assert "enabled: chemstack-runtime@alice.target" in captured


def test_cmd_systemd_install_dry_run_does_not_write_units(
    tmp_path: Path,
    capsys: Any,
) -> None:
    repo, config_path = _make_repo(tmp_path)
    unit_dir = tmp_path / "units"

    args = Namespace(
        target_user="alice",
        repo=str(repo),
        config=str(config_path),
        unit_dir=str(unit_dir),
        worker_only=True,
        no_enable=False,
        no_start=False,
        dry_run=True,
        no_sudo=True,
    )

    result = cli_systemd.cmd_systemd_install(
        args,
        deps=Namespace(is_root=lambda: True),
    )

    assert result == 0
    assert not unit_dir.exists()
    captured = capsys.readouterr().out
    assert "systemd install plan:" in captured
    assert "enable: chemstack-queue-worker@alice.service" in captured
    assert "systemctl enable --now chemstack-queue-worker@alice.service" in captured


def test_full_runtime_warns_when_telegram_is_not_configured(tmp_path: Path) -> None:
    repo, config_path = _make_repo(tmp_path)
    config_path.write_text("telegram:\n  bot_token: ''\n  chat_id: ''\n", encoding="utf-8")

    plan = cli_systemd.build_systemd_install_plan(
        target_user="alice",
        repo=repo,
        config=config_path,
        unit_dir=tmp_path / "units",
        is_root=lambda: True,
    )

    assert plan.enabled_unit == "chemstack-queue-worker@alice.service"
    assert any("Telegram is not fully configured" in warning for warning in plan.warnings)
