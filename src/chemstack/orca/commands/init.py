from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from ..config import _default_organized_root, load_config
from ..pathing import is_rejected_windows_path, is_subpath
from ._helpers import default_config_path

logger = logging.getLogger(__name__)


def _prompt_text(label: str, default: str | None = None) -> str:
    suffix = f" [{default}]" if default is not None and default != "" else ""
    value = input(f"{label}{suffix}: ").strip()
    if value:
        return value
    return default or ""


def _prompt_yes_no(label: str, *, default: bool) -> bool:
    hint = "Y/n" if default else "y/N"
    while True:
        value = input(f"{label} [{hint}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        print("Please answer y or n.")


def _normalize_linux_path(raw: str, *, label: str) -> Path | None:
    if not raw.strip():
        print(f"{label} is required.")
        return None
    if is_rejected_windows_path(raw):
        print(f"{label} must be a Linux path, not a Windows path: {raw}")
        return None

    path = Path(raw).expanduser()
    if not path.is_absolute():
        print(f"{label} must be an absolute Linux path.")
        return None
    return path.resolve(strict=False)


def _prompt_executable_path(prompt_label: str, *, label: str) -> str:
    while True:
        raw = _prompt_text(prompt_label)
        path = _normalize_linux_path(raw, label=label)
        if path is None:
            continue
        if str(path).lower().endswith(".exe"):
            print(f"{label} must point to a Linux binary, not a Windows .exe.")
            continue
        if not path.exists():
            print(f"File not found: {path}")
            continue
        if not path.is_file():
            print(f"Path is not a file: {path}")
            continue
        return str(path)


def _prompt_orca_executable() -> str:
    return _prompt_executable_path("ORCA executable path", label="orca_executable")


def _prompt_xtb_executable() -> str:
    return _prompt_executable_path("xTB executable path", label="xtb_executable")


def _prompt_crest_executable() -> str:
    return _prompt_executable_path("CREST executable path", label="crest_executable")


def _prompt_directory_path(label: str, *, default: str | None = None) -> Path:
    while True:
        raw = _prompt_text(label, default)
        path = _normalize_linux_path(raw, label=label)
        if path is None:
            continue
        if path.exists() and not path.is_dir():
            print(f"{label} is not a directory: {path}")
            continue
        return path


def _ensure_directory(path: Path, *, label: str) -> bool:
    if path.exists():
        return True
    if not _prompt_yes_no(f"{label} does not exist. Create it now?", default=True):
        print(f"{label} was not created.")
        return False
    path.mkdir(parents=True, exist_ok=True)
    return True


def _default_engine_organized_root(allowed_root: Path, *, engine_key: str) -> str:
    if engine_key == "orca":
        return _default_organized_root(str(allowed_root))
    return str(allowed_root.parent / f"{engine_key}_outputs")


def _prompt_organized_root(allowed_root: Path, *, engine_key: str, engine_label: str) -> str:
    default_path = _default_engine_organized_root(allowed_root, engine_key=engine_key)
    while True:
        path = _prompt_directory_path(f"{engine_label} organized_root directory", default=default_path)
        if is_subpath(path, allowed_root) or is_subpath(allowed_root, path):
            print(
                "organized_root and allowed_root must not contain each other. "
                f"allowed_root={allowed_root}, organized_root={path}"
            )
            continue
        if not _ensure_directory(path, label=f"{engine_key}.organized_root"):
            continue
        return str(path)


def _prompt_int(label: str, *, default: str, minimum: int) -> int:
    while True:
        raw = _prompt_text(label, default)
        try:
            value = int(raw)
        except ValueError:
            print(f"{label} must be an integer >= {minimum}.")
            continue
        if value < minimum:
            print(f"{label} must be an integer >= {minimum}.")
            continue
        return value


def _prompt_default_max_retries() -> int:
    return _prompt_int("default_max_retries", default="2", minimum=0)


def _prompt_max_active_simulations() -> int:
    return _prompt_int("max_active_simulations", default="4", minimum=1)


def _prompt_telegram_config() -> dict[str, str]:
    if not _prompt_yes_no("Configure Telegram notifications now?", default=False):
        return {"bot_token": "", "chat_id": ""}

    while True:
        bot_token = _prompt_text("Telegram bot token")
        chat_id = _prompt_text("Telegram chat id")
        if bot_token and chat_id:
            return {"bot_token": bot_token, "chat_id": chat_id}
        print("Both Telegram bot token and chat id are required, or choose not to configure Telegram.")


def _prompt_workflow_root() -> str:
    workflow_root = _prompt_directory_path("workflow.root directory")
    while not _ensure_directory(workflow_root, label="workflow.root"):
        workflow_root = _prompt_directory_path("workflow.root directory")
    return str(workflow_root)


def _prompt_engine_runtime(*, engine_key: str, engine_label: str, executable_prompt: Any) -> dict[str, str]:
    executable = str(executable_prompt())
    allowed_root = _prompt_directory_path(f"{engine_label} allowed_root directory")
    while not _ensure_directory(allowed_root, label=f"{engine_key}.allowed_root"):
        allowed_root = _prompt_directory_path(f"{engine_label} allowed_root directory")
    organized_root = _prompt_organized_root(
        allowed_root,
        engine_key=engine_key,
        engine_label=engine_label,
    )
    return {
        "allowed_root": str(allowed_root),
        "organized_root": organized_root,
        "executable": executable,
    }


def _prompt_orca_runtime() -> dict[str, object]:
    payload: dict[str, object] = _prompt_engine_runtime(
        engine_key="orca",
        engine_label="ORCA",
        executable_prompt=_prompt_orca_executable,
    )
    payload["default_max_retries"] = _prompt_default_max_retries()
    return payload


def _prompt_xtb_runtime() -> dict[str, str]:
    return {"executable": _prompt_xtb_executable()}


def _prompt_crest_runtime() -> dict[str, str]:
    return {"executable": _prompt_crest_executable()}


def _validate_generated_config(config_path: str) -> None:
    from chemstack.crest.config import load_config as load_crest_config
    from chemstack.xtb.config import load_config as load_xtb_config

    load_config(config_path)
    load_xtb_config(config_path)
    load_crest_config(config_path)


def _write_config(config_path: Path, payload: dict[str, object]) -> None:
    config_path.parent.mkdir(parents=True, exist_ok=True)
    rendered = yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    config_path.write_text(f"# Generated by chemstack init\n{rendered}", encoding="utf-8")


def cmd_init(args: Any) -> int:
    force = bool(getattr(args, "force", False))
    raw_config_path = str(getattr(args, "config", "") or "").strip() or default_config_path()
    config_path = Path(raw_config_path).expanduser().resolve()

    if config_path.exists() and not force:
        overwrite = _prompt_yes_no(
            f"Config already exists at {config_path}. Overwrite it?",
            default=False,
        )
        if not overwrite:
            print("Cancelled.")
            return 0

    print(f"Creating config at: {config_path}")

    try:
        workflow_root = _prompt_workflow_root()
        orca_runtime = _prompt_orca_runtime()
        xtb_runtime = _prompt_xtb_runtime()
        crest_runtime = _prompt_crest_runtime()
        max_active_simulations = _prompt_max_active_simulations()
        telegram = _prompt_telegram_config()
    except (EOFError, KeyboardInterrupt):
        print("\nCancelled.")
        return 1

    payload: dict[str, object] = {
        "resources": {
            "max_cores_per_task": 8,
            "max_memory_gb_per_task": 32,
        },
        "behavior": {
            "auto_organize_on_terminal": False,
        },
        "scheduler": {
            "max_active_simulations": max_active_simulations,
        },
        "workflow": {
            "root": workflow_root,
            "paths": {
                "xtb_executable": str(xtb_runtime["executable"]),
                "crest_executable": str(crest_runtime["executable"]),
            },
        },
        "telegram": telegram,
        "orca": {
            "runtime": {
                "allowed_root": str(orca_runtime["allowed_root"]),
                "organized_root": str(orca_runtime["organized_root"]),
                "default_max_retries": int(orca_runtime["default_max_retries"]),
            },
            "paths": {
                "orca_executable": str(orca_runtime["executable"]),
            },
        },
    }

    try:
        _write_config(config_path, payload)
        _validate_generated_config(str(config_path))
    except Exception as exc:
        logger.exception("Failed to generate config: %s", exc)
        print(f"Failed to generate config: {exc}")
        return 1

    print("Config created successfully.")
    print(f"  config: {config_path}")
    print(f"  workflow_root: {workflow_root}")
    print(f"  max_active_simulations: {max_active_simulations}")
    print(f"  orca_allowed_root: {orca_runtime['allowed_root']}")
    print(f"  xtb_executable: {xtb_runtime['executable']}")
    print(f"  crest_executable: {crest_runtime['executable']}")
    return 0
