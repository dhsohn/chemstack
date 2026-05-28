from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

import yaml

from chemstack.cli_common import _dependency, _repo_root
from chemstack.core.utils.coercion import normalize_text


SYSTEMD_UNIT_NAMES = (
    "chemstack-queue-worker@.service",
    "chemstack-bot@.service",
    "chemstack-summary@.service",
    "chemstack-summary@.timer",
    "chemstack-runtime@.target",
)

DEFAULT_SYSTEMD_UNIT_DIR = Path("/etc/systemd/system")


@dataclass(frozen=True)
class RenderedUnit:
    name: str
    destination: Path
    content: str


@dataclass(frozen=True)
class SystemdInstallPlan:
    target_user: str
    repo: Path
    config: Path
    unit_dir: Path
    units: tuple[RenderedUnit, ...]
    commands: tuple[tuple[str, ...], ...]
    enabled_unit: str | None
    use_sudo: bool
    warnings: tuple[str, ...]


def _is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        return False
    return int(geteuid()) == 0


def _existing_parent(path: Path) -> Path:
    current = path
    while not current.exists() and current.parent != current:
        current = current.parent
    return current


def _needs_sudo(unit_dir: Path, *, is_root: Callable[[], bool] = _is_root) -> bool:
    if is_root():
        return False
    writable_target = unit_dir if unit_dir.exists() else _existing_parent(unit_dir)
    return not os.access(writable_target, os.W_OK)


def _template_dir(repo_root: Path) -> Path:
    return repo_root / "systemd"


def _read_unit_template(template_root: Path, name: str) -> str:
    return (template_root / name).read_text(encoding="utf-8")


def _render_unit_template(template: str, *, repo: Path, config: Path) -> str:
    repo_text = str(repo)
    config_text = str(config)
    rendered = template.replace("/home/%i/chemstack", repo_text)
    lines = []
    for line in rendered.splitlines():
        if line.startswith("Environment=CHEMSTACK_CONFIG="):
            lines.append(f"Environment=CHEMSTACK_CONFIG={config_text}")
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def _default_config_for_repo(repo: Path) -> Path:
    return repo / "config" / "chemstack.yaml"


def _normalize_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def _enabled_unit_for_args(*, target_user: str, worker_only: bool, no_enable: bool) -> str | None:
    if no_enable:
        return None
    if worker_only:
        return f"chemstack-queue-worker@{target_user}.service"
    return f"chemstack-runtime@{target_user}.target"


def _systemctl_enable_command(enabled_unit: str, *, no_start: bool) -> tuple[str, ...]:
    if no_start:
        return ("systemctl", "enable", enabled_unit)
    return ("systemctl", "enable", "--now", enabled_unit)


def _telegram_runtime_warning(config: Path, *, worker_only: bool) -> str | None:
    if worker_only or not config.exists():
        return None
    try:
        parsed = yaml.safe_load(config.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        return f"could not read Telegram settings from {config}: {exc}"
    if not isinstance(parsed, dict):
        return f"could not read Telegram settings from {config}: top-level YAML is not a mapping"
    telegram = parsed.get("telegram") or {}
    if not isinstance(telegram, dict):
        return f"could not read Telegram settings from {config}: telegram section is not a mapping"
    if not normalize_text(telegram.get("bot_token")) or not normalize_text(telegram.get("chat_id")):
        return (
            "full runtime target includes the Telegram bot, but telegram.bot_token or "
            "telegram.chat_id is empty; use --worker-only if Telegram is not ready"
        )
    return None


def _telegram_configured(config: Path) -> bool:
    if not config.exists():
        return False
    try:
        parsed = yaml.safe_load(config.read_text(encoding="utf-8")) or {}
    except Exception:
        return False
    if not isinstance(parsed, dict):
        return False
    telegram = parsed.get("telegram") or {}
    if not isinstance(telegram, dict):
        return False
    return bool(normalize_text(telegram.get("bot_token")) and normalize_text(telegram.get("chat_id")))


def _auto_worker_only(config: Path, *, worker_only: bool, no_enable: bool) -> bool:
    if worker_only or no_enable:
        return worker_only
    return not _telegram_configured(config)


def _collect_warnings(
    repo: Path,
    config: Path,
    *,
    worker_only: bool,
    auto_selected_worker_only: bool,
) -> tuple[str, ...]:
    warnings: list[str] = []
    if not repo.exists():
        warnings.append(f"repo path does not exist yet: {repo}")
    elif not repo.is_dir():
        warnings.append(f"repo path is not a directory: {repo}")
    python_path = repo / ".venv" / "bin" / "python"
    if not python_path.exists():
        warnings.append(f"service Python does not exist yet: {python_path}")
    if not config.exists():
        warnings.append(f"config file does not exist yet: {config}")
    if not auto_selected_worker_only:
        telegram_warning = _telegram_runtime_warning(config, worker_only=worker_only)
        if telegram_warning:
            warnings.append(telegram_warning)
    if auto_selected_worker_only and not worker_only:
        warnings.append(
            "Telegram is not fully configured, so the installer will enable only the "
            "queue worker; rerun the same command after setting telegram.bot_token and "
            "telegram.chat_id to enable the full runtime target"
        )
    return tuple(warnings)


def build_systemd_install_plan(
    *,
    target_user: str,
    repo: str | Path,
    config: str | Path | None = None,
    unit_dir: str | Path = DEFAULT_SYSTEMD_UNIT_DIR,
    worker_only: bool = False,
    no_enable: bool = False,
    no_start: bool = False,
    no_sudo: bool = False,
    repo_root: Path | None = None,
    is_root: Callable[[], bool] = _is_root,
) -> SystemdInstallPlan:
    user_text = normalize_text(target_user)
    if not user_text:
        raise ValueError("--user is required")

    repo_path = _normalize_path(repo)
    config_path = _normalize_path(config or _default_config_for_repo(repo_path))
    unit_dir_path = _normalize_path(unit_dir)
    template_root = _template_dir(repo_root or _repo_root())

    units = tuple(
        RenderedUnit(
            name=name,
            destination=unit_dir_path / name,
            content=_render_unit_template(
                _read_unit_template(template_root, name),
                repo=repo_path,
                config=config_path,
            ),
        )
        for name in SYSTEMD_UNIT_NAMES
    )

    effective_worker_only = _auto_worker_only(
        config_path,
        worker_only=worker_only,
        no_enable=no_enable,
    )
    enabled_unit = _enabled_unit_for_args(
        target_user=user_text,
        worker_only=effective_worker_only,
        no_enable=no_enable,
    )
    commands: list[tuple[str, ...]] = [("systemctl", "daemon-reload")]
    if enabled_unit:
        commands.append(_systemctl_enable_command(enabled_unit, no_start=no_start))

    return SystemdInstallPlan(
        target_user=user_text,
        repo=repo_path,
        config=config_path,
        unit_dir=unit_dir_path,
        units=units,
        commands=tuple(commands),
        enabled_unit=enabled_unit,
        use_sudo=False if no_sudo else _needs_sudo(unit_dir_path, is_root=is_root),
        warnings=_collect_warnings(
            repo_path,
            config_path,
            worker_only=worker_only,
            auto_selected_worker_only=effective_worker_only,
        ),
    )


def _format_command(command: Sequence[str], *, use_sudo: bool) -> str:
    parts = (("sudo",) if use_sudo else ()) + tuple(command)
    return " ".join(parts)


def _print_plan(plan: SystemdInstallPlan) -> None:
    print("systemd install plan:")
    print(f"  user: {plan.target_user}")
    print(f"  repo: {plan.repo}")
    print(f"  config: {plan.config}")
    print(f"  unit_dir: {plan.unit_dir}")
    if plan.enabled_unit:
        print(f"  enable: {plan.enabled_unit}")
    else:
        print("  enable: skipped")
    print("  write:")
    for unit in plan.units:
        print(f"    {unit.destination}")
    if plan.commands:
        print("  run:")
        for command in plan.commands:
            print(f"    {_format_command(command, use_sudo=plan.use_sudo)}")


def _print_warnings(plan: SystemdInstallPlan) -> None:
    for warning in plan.warnings:
        print(f"warning: {warning}")


def _run_command(
    command: Sequence[str],
    *,
    use_sudo: bool,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> int:
    argv = [*command]
    if use_sudo:
        argv.insert(0, "sudo")
    print(f"$ {' '.join(argv)}")
    completed = run(argv, check=False)
    return int(completed.returncode)


def _write_units_direct(plan: SystemdInstallPlan) -> None:
    plan.unit_dir.mkdir(parents=True, exist_ok=True)
    for unit in plan.units:
        unit.destination.write_text(unit.content, encoding="utf-8")
        unit.destination.chmod(0o644)
        print(f"installed: {unit.destination}")


def _write_units_with_sudo(
    plan: SystemdInstallPlan,
    *,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> int:
    with tempfile.TemporaryDirectory(prefix="chemstack-systemd-") as tmp_dir_text:
        tmp_dir = Path(tmp_dir_text)
        for unit in plan.units:
            (tmp_dir / unit.name).write_text(unit.content, encoding="utf-8")

        rc = _run_command(("mkdir", "-p", str(plan.unit_dir)), use_sudo=True, run=run)
        if rc != 0:
            return rc
        for unit in plan.units:
            rc = _run_command(
                ("install", "-m", "0644", str(tmp_dir / unit.name), str(unit.destination)),
                use_sudo=True,
                run=run,
            )
            if rc != 0:
                return rc
            print(f"installed: {unit.destination}")
    return 0


def apply_systemd_install_plan(
    plan: SystemdInstallPlan,
    *,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> int:
    if plan.use_sudo and shutil.which("sudo") is None:
        print("error: sudo is required to write system units; rerun as root or use --no-sudo")
        return 1

    if plan.use_sudo:
        rc = _write_units_with_sudo(plan, run=run)
    else:
        try:
            _write_units_direct(plan)
        except OSError as exc:
            print(f"error: failed to write systemd units: {exc}")
            return 1
        rc = 0
    if rc != 0:
        return rc

    for command in plan.commands:
        rc = _run_command(command, use_sudo=plan.use_sudo, run=run)
        if rc != 0:
            return rc

    if plan.enabled_unit:
        print(f"enabled: {plan.enabled_unit}")
    else:
        print("installed systemd units; enable/start skipped")
    return 0


def cmd_systemd_install(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    build_plan = _dependency(deps, "build_systemd_install_plan", build_systemd_install_plan)
    apply_plan = _dependency(deps, "apply_systemd_install_plan", apply_systemd_install_plan)
    run = _dependency(deps, "run", subprocess.run)
    is_root = _dependency(deps, "is_root", _is_root)

    try:
        plan = build_plan(
            target_user=getattr(args, "target_user", None),
            repo=getattr(args, "repo", None),
            config=getattr(args, "config", None),
            unit_dir=getattr(args, "unit_dir", DEFAULT_SYSTEMD_UNIT_DIR),
            worker_only=bool(getattr(args, "worker_only", False)),
            no_enable=bool(getattr(args, "no_enable", False)),
            no_start=bool(getattr(args, "no_start", False)),
            no_sudo=bool(getattr(args, "no_sudo", False)),
            is_root=is_root,
        )
    except (OSError, ValueError) as exc:
        print(f"error: {exc}")
        return 1

    _print_warnings(plan)
    if bool(getattr(args, "dry_run", False)):
        _print_plan(plan)
        return 0
    return int(apply_plan(plan, run=run))


__all__ = [
    "DEFAULT_SYSTEMD_UNIT_DIR",
    "SYSTEMD_UNIT_NAMES",
    "RenderedUnit",
    "SystemdInstallPlan",
    "apply_systemd_install_plan",
    "build_systemd_install_plan",
    "cmd_systemd_install",
]
