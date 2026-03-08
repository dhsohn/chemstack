"""Configuration validation and normalization helpers.

Extracted from config.py to reduce its size while keeping dataclasses
and ``load_config()`` in the main module.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from .pathing import is_subpath, is_rejected_windows_path


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any, default: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return default


def _validate_config(cfg: Any) -> None:
    """Validate core path constraints on an AppConfig instance."""
    for label, path_val in [
        ("allowed_root", cfg.runtime.allowed_root),
        ("organized_root", cfg.runtime.organized_root),
        ("orca_executable", cfg.paths.orca_executable),
    ]:
        if is_rejected_windows_path(path_val):
            raise ValueError(
                f"{label} must be a Linux path (Windows legacy paths are no longer supported): {path_val!r}"
            )
        if not Path(path_val).is_absolute():
            raise ValueError(
                f"{label} must be an absolute Linux path: {path_val!r}"
            )
    if cfg.paths.orca_executable.lower().endswith(".exe"):
        raise ValueError(
            f"orca_executable must point to Linux ORCA binary, not Windows executable: {cfg.paths.orca_executable!r}"
        )

    orca_path = Path(cfg.paths.orca_executable)
    if not orca_path.exists():
        raise ValueError(
            f"orca_executable not found: {cfg.paths.orca_executable!r}. "
            "Verify the path points to an existing ORCA binary."
        )
    if not orca_path.is_file():
        raise ValueError(
            f"orca_executable is not a file: {cfg.paths.orca_executable!r}"
        )

    allowed_root = Path(cfg.runtime.allowed_root)
    if not allowed_root.exists():
        raise ValueError(
            f"allowed_root directory not found: {cfg.runtime.allowed_root!r}. "
            "Create the directory or update the config."
        )
    if not allowed_root.is_dir():
        raise ValueError(
            f"allowed_root is not a directory: {cfg.runtime.allowed_root!r}"
        )

    ar = Path(cfg.runtime.allowed_root).resolve()
    org = Path(cfg.runtime.organized_root).resolve()
    if is_subpath(org, ar) or is_subpath(ar, org):
        raise ValueError(
            f"organized_root and allowed_root must not contain each other: "
            f"allowed_root={ar}, organized_root={org}"
        )
