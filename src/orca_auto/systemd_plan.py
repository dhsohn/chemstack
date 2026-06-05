from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from orca_auto.cli_common import _repo_root
from orca_auto.core.config.files import YAML_CONFIG_LOAD_EXCEPTIONS, load_yaml_mapping
from orca_auto.core.utils.coercion import normalize_text

SYSTEMD_UNIT_NAMES = (
    "orca_auto-queue-worker@.service",
    "orca_auto-bot@.service",
    "orca_auto-runtime@.target",
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
    rendered = template.replace("/home/%i/orca_auto", repo_text)
    lines = []
    for line in rendered.splitlines():
        if line.startswith("Environment=ORCA_AUTO_CONFIG="):
            lines.append(f"Environment=ORCA_AUTO_CONFIG={config_text}")
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def _default_config_for_repo(repo: Path) -> Path:
    return repo / "config" / "orca_auto.yaml"


def _normalize_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve(strict=False)


def _enabled_unit_for_args(*, target_user: str, worker_only: bool, no_enable: bool) -> str | None:
    if no_enable:
        return None
    if worker_only:
        return f"orca_auto-queue-worker@{target_user}.service"
    return f"orca_auto-runtime@{target_user}.target"


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
    except YAML_CONFIG_LOAD_EXCEPTIONS as exc:
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
    except YAML_CONFIG_LOAD_EXCEPTIONS:
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


def _systemd_command_argv(command: Sequence[str], *, use_sudo: bool) -> tuple[str, ...]:
    parts = (("sudo",) if use_sudo else ()) + tuple(command)
    return parts


def _format_command(command: Sequence[str], *, use_sudo: bool) -> str:
    return " ".join(_systemd_command_argv(command, use_sudo=use_sudo))


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


__all__ = [
    "DEFAULT_SYSTEMD_UNIT_DIR",
    "SYSTEMD_UNIT_NAMES",
    "RenderedUnit",
    "SystemdInstallOptions",
    "SystemdInstallPlan",
    "build_systemd_install_plan",
]
