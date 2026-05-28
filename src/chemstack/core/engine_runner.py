from __future__ import annotations

import os
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.config import engines as _config_engines

ExecutableErrorMessage = str | Callable[[Path], str]


def _executable_error_message(message: ExecutableErrorMessage, path: Path) -> str:
    if callable(message):
        return message(path)
    return message


def validate_executable_file(
    path_value: str | Path,
    *,
    missing_message: ExecutableErrorMessage,
    not_file_message: ExecutableErrorMessage,
    not_executable_message: ExecutableErrorMessage,
    access_fn: Callable[[str, int], bool] = os.access,
) -> Path:
    path = Path(path_value).expanduser().resolve()
    if not path.exists():
        raise ValueError(_executable_error_message(missing_message, path))
    if not path.is_file():
        raise ValueError(_executable_error_message(not_file_message, path))
    if not access_fn(str(path), os.X_OK):
        raise ValueError(_executable_error_message(not_executable_message, path))
    return path


def resolve_configured_executable(
    cfg: Any,
    *,
    path_attr: str,
    executable_name: str,
    display_name: str,
) -> str:
    configured = str(getattr(cfg.paths, path_attr, "")).strip()
    if configured:
        path = validate_executable_file(
            configured,
            missing_message=lambda resolved: (
                f"Configured {display_name} executable not found: {resolved}"
            ),
            not_file_message=lambda resolved: (
                f"Configured {display_name} executable is not a file: {resolved}"
            ),
            not_executable_message=lambda resolved: (
                f"Configured {display_name} executable is not executable: {resolved}"
            ),
        )
        return str(path)

    discovered = shutil.which(executable_name)
    if discovered:
        return discovered
    raise ValueError(f"{display_name} executable not configured and not found on PATH.")


def resource_actual_dict(resource_request: dict[str, int]) -> dict[str, int]:
    return _config_engines.resource_actual_from_request(resource_request)


def bool_flag(manifest: dict[str, Any], key: str) -> bool:
    return _config_engines.as_bool(manifest.get(key), False)


def manifest_int(
    manifest: dict[str, Any],
    key: str,
    *,
    zero_is_absent: bool = False,
) -> int | None:
    value = manifest.get(key)
    absent_values = (None, "", 0, "0") if zero_is_absent else (None, "")
    if value in absent_values:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped or (zero_is_absent and stripped == "0"):
            return None
        return int(stripped)
    if isinstance(value, (int, float)):
        return int(value)
    raise ValueError(f"Manifest field {key!r} must be an integer-compatible value.")


def manifest_scalar_text(manifest: dict[str, Any], key: str) -> str | None:
    value = manifest.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, bool):
        return "true" if value else None
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).strip()
    return text or None


def append_solvent_option(command: list[str], manifest: dict[str, Any]) -> None:
    solvent_model = str(manifest.get("solvent_model", "")).strip().lower()
    solvent = str(manifest.get("solvent", "")).strip()
    if solvent and solvent_model in {"gbsa", "alpb"}:
        command.extend([f"--{solvent_model}", solvent])


__all__ = [
    "append_solvent_option",
    "bool_flag",
    "manifest_int",
    "manifest_scalar_text",
    "resolve_configured_executable",
    "resource_actual_dict",
    "validate_executable_file",
]
