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


def test_cmd_service_status_prints_compact_systemd_state(capsys: Any) -> None:
    states = {
        ("is-active", "chemstack-runtime@alice.target"): "active",
        ("is-enabled", "chemstack-runtime@alice.target"): "enabled",
        ("is-active", "chemstack-queue-worker@alice.service"): "active",
        ("is-enabled", "chemstack-queue-worker@alice.service"): "enabled",
        ("is-active", "chemstack-bot@alice.service"): "inactive",
        ("is-enabled", "chemstack-bot@alice.service"): "disabled",
        ("is-active", "chemstack-summary@alice.timer"): "active",
        ("is-enabled", "chemstack-summary@alice.timer"): "enabled",
    }

    def _fake_run(
        argv: list[str],
        check: bool = False,
        stdout: Any = None,
        stderr: Any = None,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, stdout, stderr, text
        value = states[(argv[1], argv[2])]
        return subprocess.CompletedProcess(argv, 0, stdout=f"{value}\n", stderr="")

    result = cli_systemd.cmd_service_status(
        Namespace(target_user=None),
        deps=Namespace(
            _default_service_user=lambda: "alice",
            run=_fake_run,
            which=lambda name: "/bin/systemctl" if name == "systemctl" else None,
        ),
    )

    assert result == 0
    output = capsys.readouterr().out
    assert "ChemStack service status for alice:" in output
    assert "worker" in output
    assert "chemstack-queue-worker@alice.service" in output
    assert "inactive" in output


def test_cmd_service_status_fails_when_systemctl_is_missing(capsys: Any) -> None:
    result = cli_systemd.cmd_service_status(
        Namespace(target_user=None),
        deps=Namespace(which=lambda name: None),
    )

    assert result == 1
    assert "systemctl is not available" in capsys.readouterr().out


def test_cmd_service_restart_prefers_runtime_when_enabled(capsys: Any) -> None:
    commands: list[tuple[str, ...]] = []

    def _fake_run(
        argv: list[str],
        check: bool = False,
        stdout: Any = None,
        stderr: Any = None,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, stdout, stderr, text
        commands.append(tuple(argv))
        if argv[1] == "is-active":
            return subprocess.CompletedProcess(argv, 3, stdout="inactive\n", stderr="")
        if argv[1] == "is-enabled":
            return subprocess.CompletedProcess(argv, 0, stdout="enabled\n", stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    result = cli_systemd.cmd_service_restart(
        Namespace(target_user=None),
        deps=Namespace(
            _default_service_user=lambda: "alice",
            is_root=lambda: True,
            run=_fake_run,
            which=lambda name: "/bin/systemctl" if name == "systemctl" else None,
        ),
    )

    assert result == 0
    assert commands[-1] == ("systemctl", "restart", "chemstack-runtime@alice.target")
    assert "Restarting chemstack-runtime@alice.target" in capsys.readouterr().out


def test_cmd_service_restart_falls_back_to_worker_when_runtime_is_disabled() -> None:
    commands: list[tuple[str, ...]] = []

    def _fake_run(
        argv: list[str],
        check: bool = False,
        stdout: Any = None,
        stderr: Any = None,
        text: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        del check, stdout, stderr, text
        commands.append(tuple(argv))
        if argv[1] == "is-active":
            return subprocess.CompletedProcess(argv, 3, stdout="inactive\n", stderr="")
        if argv[1] == "is-enabled":
            return subprocess.CompletedProcess(argv, 1, stdout="disabled\n", stderr="")
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    result = cli_systemd.cmd_service_restart(
        Namespace(target_user=None),
        deps=Namespace(
            _default_service_user=lambda: "alice",
            is_root=lambda: True,
            run=_fake_run,
            which=lambda name: "/bin/systemctl" if name == "systemctl" else None,
        ),
    )

    assert result == 0
    assert commands[-1] == ("systemctl", "restart", "chemstack-queue-worker@alice.service")


def test_cmd_service_restart_uses_sudo_for_non_root_user() -> None:
    commands: list[tuple[str, ...]] = []

    def _fake_run(argv: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
        del check
        commands.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0)

    result = cli_systemd.cmd_service_restart(
        Namespace(target_user=None),
        deps=Namespace(
            _default_service_user=lambda: "alice",
            _restart_unit_for_user=lambda target_user, run: f"chemstack-runtime@{target_user}.target",
            is_root=lambda: False,
            run=_fake_run,
            which=lambda name: f"/usr/bin/{name}" if name in {"systemctl", "sudo"} else None,
        ),
    )

    assert result == 0
    assert commands == [("sudo", "systemctl", "restart", "chemstack-runtime@alice.target")]
