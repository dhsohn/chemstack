from __future__ import annotations

import argparse
import getpass
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from chemstack.cli_common import _dependency, _repo_root
from chemstack.core.config.files import load_yaml_mapping
from chemstack.core.utils.coercion import normalize_text


SYSTEMD_UNIT_NAMES = (
    "chemstack-queue-worker@.service",
    "chemstack-bot@.service",
    "chemstack-summary@.service",
    "chemstack-summary@.timer",
    "chemstack-runtime@.target",
)

DEFAULT_SYSTEMD_UNIT_DIR = Path("/etc/systemd/system")

SERVICE_UNIT_ORDER = (
    ("runtime", "chemstack-runtime@{user}.target"),
    ("worker", "chemstack-queue-worker@{user}.service"),
    ("bot", "chemstack-bot@{user}.service"),
    ("summary", "chemstack-summary@{user}.timer"),
)


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


@dataclass(frozen=True)
class ServiceUnitStatus:
    label: str
    unit: str
    active: str
    enabled: str


def _is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    if geteuid is None:
        return False
    return int(geteuid()) == 0


@dataclass(frozen=True)
class SystemdInstallOptions:
    target_user: str
    repo: Path
    config: Path
    unit_dir: Path
    worker_only: bool = False
    no_enable: bool = False
    no_start: bool = False
    no_sudo: bool = False
    repo_root: Path | None = None
    is_root: Callable[[], bool] = _is_root


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


def _telegram_mapping(config: Path) -> dict[str, Any]:
    _, parsed = load_yaml_mapping(
        config,
        invalid_message=(
            "could not read Telegram settings from {path}: top-level YAML is not a mapping"
        ),
    )
    telegram = parsed.get("telegram") or {}
    if not isinstance(telegram, dict):
        raise ValueError(
            f"could not read Telegram settings from {config}: telegram section is not a mapping"
        )
    return telegram


def _telegram_credentials_configured(telegram: dict[str, Any]) -> bool:
    return bool(
        normalize_text(telegram.get("bot_token")) and normalize_text(telegram.get("chat_id"))
    )


def _telegram_runtime_warning(config: Path, *, worker_only: bool) -> str | None:
    if worker_only or not config.exists():
        return None
    try:
        telegram = _telegram_mapping(config)
    except ValueError as exc:
        return str(exc)
    except Exception as exc:
        return f"could not read Telegram settings from {config}: {exc}"
    if not _telegram_credentials_configured(telegram):
        return (
            "full runtime target includes the Telegram bot, but telegram.bot_token or "
            "telegram.chat_id is empty; use --worker-only if Telegram is not ready"
        )
    return None


def _telegram_configured(config: Path) -> bool:
    if not config.exists():
        return False
    try:
        telegram = _telegram_mapping(config)
    except Exception:
        return False
    return _telegram_credentials_configured(telegram)


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


def _build_systemd_install_plan(options: SystemdInstallOptions) -> SystemdInstallPlan:
    template_root = _template_dir(options.repo_root or _repo_root())
    units = tuple(
        RenderedUnit(
            name=name,
            destination=options.unit_dir / name,
            content=_render_unit_template(
                _read_unit_template(template_root, name),
                repo=options.repo,
                config=options.config,
            ),
        )
        for name in SYSTEMD_UNIT_NAMES
    )

    effective_worker_only = _auto_worker_only(
        options.config,
        worker_only=options.worker_only,
        no_enable=options.no_enable,
    )
    enabled_unit = _enabled_unit_for_args(
        target_user=options.target_user,
        worker_only=effective_worker_only,
        no_enable=options.no_enable,
    )
    commands: list[tuple[str, ...]] = [("systemctl", "daemon-reload")]
    if enabled_unit:
        commands.append(_systemctl_enable_command(enabled_unit, no_start=options.no_start))

    return SystemdInstallPlan(
        target_user=options.target_user,
        repo=options.repo,
        config=options.config,
        unit_dir=options.unit_dir,
        units=units,
        commands=tuple(commands),
        enabled_unit=enabled_unit,
        use_sudo=False
        if options.no_sudo
        else _needs_sudo(options.unit_dir, is_root=options.is_root),
        warnings=_collect_warnings(
            options.repo,
            options.config,
            worker_only=options.worker_only,
            auto_selected_worker_only=effective_worker_only,
        ),
    )


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
    options = SystemdInstallOptions(
        target_user=user_text,
        repo=repo_path,
        config=_normalize_path(config or _default_config_for_repo(repo_path)),
        unit_dir=_normalize_path(unit_dir),
        worker_only=worker_only,
        no_enable=no_enable,
        no_start=no_start,
        no_sudo=no_sudo,
        repo_root=repo_root,
        is_root=is_root,
    )
    return _build_systemd_install_plan(options)


def _format_command(command: Sequence[str], *, use_sudo: bool) -> str:
    parts = (("sudo",) if use_sudo else ()) + tuple(command)
    return " ".join(parts)


def _default_service_user() -> str:
    return getpass.getuser()


def _service_units_for_user(target_user: str) -> tuple[tuple[str, str], ...]:
    user_text = normalize_text(target_user)
    if not user_text:
        raise ValueError("service user is required")
    return tuple((label, template.format(user=user_text)) for label, template in SERVICE_UNIT_ORDER)


def _single_line_command_output(completed: subprocess.CompletedProcess[Any]) -> str:
    output = normalize_text(getattr(completed, "stdout", ""))
    if not output:
        output = normalize_text(getattr(completed, "stderr", ""))
    if not output:
        output = f"exit {completed.returncode}"
    return output.splitlines()[0]


def _query_systemctl(
    action: str,
    unit: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    try:
        completed = run(
            ["systemctl", action, unit],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as exc:
        return f"error: {exc}"
    return _single_line_command_output(completed)


def collect_service_status(
    target_user: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> tuple[ServiceUnitStatus, ...]:
    return tuple(
        ServiceUnitStatus(
            label=label,
            unit=unit,
            active=_query_systemctl("is-active", unit, run=run),
            enabled=_query_systemctl("is-enabled", unit, run=run),
        )
        for label, unit in _service_units_for_user(target_user)
    )


def _print_service_status(target_user: str, statuses: Sequence[ServiceUnitStatus]) -> None:
    print(f"ChemStack service status for {target_user}:")
    print(f"{'Name':<10} {'Active':<14} {'Enabled':<14} Unit")
    for status in statuses:
        print(f"{status.label:<10} {status.active:<14} {status.enabled:<14} {status.unit}")


def _systemctl_available(*, which: Callable[[str], str | None] = shutil.which) -> bool:
    return which("systemctl") is not None


def _sudo_available(*, which: Callable[[str], str | None] = shutil.which) -> bool:
    return which("sudo") is not None


def _runtime_unit_for_user(target_user: str) -> str:
    return f"chemstack-runtime@{target_user}.target"


def _worker_unit_for_user(target_user: str) -> str:
    return f"chemstack-queue-worker@{target_user}.service"


def _restart_unit_for_user(
    target_user: str,
    *,
    run: Callable[..., subprocess.CompletedProcess[Any]] = subprocess.run,
) -> str:
    runtime_unit = _runtime_unit_for_user(target_user)
    runtime_active = _query_systemctl("is-active", runtime_unit, run=run)
    runtime_enabled = _query_systemctl("is-enabled", runtime_unit, run=run)
    if runtime_active == "active" or runtime_enabled == "enabled":
        return runtime_unit
    return _worker_unit_for_user(target_user)


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


def _service_target_user(args: argparse.Namespace, *, deps: Any | None = None) -> str:
    default_user = _dependency(deps, "_default_service_user", _default_service_user)
    return normalize_text(getattr(args, "target_user", None)) or normalize_text(default_user())


def cmd_service_status(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    which = _dependency(deps, "which", shutil.which)
    collect_status = _dependency(deps, "collect_service_status", collect_service_status)
    if not _systemctl_available(which=which):
        print("error: systemctl is not available in this environment")
        return 1

    target_user = _service_target_user(args, deps=deps)
    try:
        statuses = collect_status(target_user, run=_dependency(deps, "run", subprocess.run))
    except ValueError as exc:
        print(f"error: {exc}")
        return 1
    _print_service_status(target_user, statuses)
    return 1 if any(status.active == "failed" for status in statuses) else 0


def cmd_service_restart(args: argparse.Namespace, *, deps: Any | None = None) -> int:
    which = _dependency(deps, "which", shutil.which)
    run = _dependency(deps, "run", subprocess.run)
    is_root = _dependency(deps, "is_root", _is_root)
    restart_unit_for_user = _dependency(deps, "_restart_unit_for_user", _restart_unit_for_user)

    if not _systemctl_available(which=which):
        print("error: systemctl is not available in this environment")
        return 1
    use_sudo = not is_root()
    if use_sudo and not _sudo_available(which=which):
        print("error: sudo is required to restart system services; rerun as root")
        return 1

    target_user = _service_target_user(args, deps=deps)
    try:
        unit = restart_unit_for_user(target_user, run=run)
    except ValueError as exc:
        print(f"error: {exc}")
        return 1

    print(f"Restarting {unit}")
    rc = _run_command(("systemctl", "restart", unit), use_sudo=use_sudo, run=run)
    if rc == 0:
        print("Restart requested successfully.")
        print("Check status with: chemstack service status")
    return rc


__all__ = [
    "DEFAULT_SYSTEMD_UNIT_DIR",
    "SERVICE_UNIT_ORDER",
    "SYSTEMD_UNIT_NAMES",
    "RenderedUnit",
    "ServiceUnitStatus",
    "SystemdInstallOptions",
    "SystemdInstallPlan",
    "apply_systemd_install_plan",
    "build_systemd_install_plan",
    "cmd_service_restart",
    "cmd_service_status",
    "cmd_systemd_install",
    "collect_service_status",
]
