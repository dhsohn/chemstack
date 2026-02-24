"""Configuration validation and normalization helpers.

Extracted from config.py to reduce its size while keeping dataclasses
and ``load_config()`` in the main module.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, List

from .pathing import is_subpath, is_windows_style_path

_FALLBACK_KEEP_EXTENSIONS = [".inp", ".out", ".xyz", ".gbw", ".hess"]


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_str(value: Any, default: str) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return default


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _validate_config(cfg: Any) -> None:
    """Validate core path constraints on an AppConfig instance."""
    for label, path_val in [
        ("allowed_root", cfg.runtime.allowed_root),
        ("organized_root", cfg.runtime.organized_root),
        ("orca_executable", cfg.paths.orca_executable),
    ]:
        if is_windows_style_path(path_val):
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
    ar = Path(cfg.runtime.allowed_root).resolve()
    org = Path(cfg.runtime.organized_root).resolve()
    if is_subpath(org, ar) or is_subpath(ar, org):
        raise ValueError(
            f"organized_root and allowed_root must not contain each other: "
            f"allowed_root={ar}, organized_root={org}"
        )


def _normalize_extensions(raw: Any, defaults: List[str] | None = None) -> List[str]:
    if defaults is None:
        defaults = _FALLBACK_KEEP_EXTENSIONS
    if not isinstance(raw, list):
        return list(defaults)
    seen: set[str] = set()
    result: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        ext = item.strip().lower()
        if not ext:
            continue
        if not ext.startswith("."):
            ext = "." + ext
        if ext not in seen:
            seen.add(ext)
            result.append(ext)
    return result


def _normalize_string_list(raw: Any, defaults: List[str]) -> List[str]:
    if not isinstance(raw, list):
        return list(defaults)
    seen: set[str] = set()
    result: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            continue
        val = item.strip()
        if not val:
            continue
        if val not in seen:
            seen.add(val)
            result.append(val)
    return result


def _validate_cleanup_config(cleanup: Any) -> None:
    if not cleanup.keep_extensions:
        raise ValueError(
            "cleanup.keep_extensions must not be empty (data loss risk)"
        )
    if not cleanup.keep_filenames:
        raise ValueError(
            "cleanup.keep_filenames must not be empty (data loss risk)"
        )


def _validate_disk_monitor_config(dm: Any) -> None:
    if dm.threshold_gb <= 0:
        raise ValueError(f"disk_monitor.threshold_gb must be > 0, got {dm.threshold_gb}")
    if dm.interval_sec < 10:
        raise ValueError(f"disk_monitor.interval_sec must be >= 10, got {dm.interval_sec}")
    if not (1 <= dm.top_n <= 100):
        raise ValueError(f"disk_monitor.top_n must be 1-100, got {dm.top_n}")


def _validate_monitoring_config(mon: Any) -> None:
    t = mon.telegram
    if not (1 <= t.timeout_sec <= 30):
        raise ValueError(f"monitoring.telegram.timeout_sec must be 1-30, got {t.timeout_sec}")
    if not (0 <= t.retry_count <= 5):
        raise ValueError(f"monitoring.telegram.retry_count must be 0-5, got {t.retry_count}")
    d = mon.delivery
    if not (100 <= d.queue_size <= 10000):
        raise ValueError(f"monitoring.delivery.queue_size must be 100-10000, got {d.queue_size}")
    h = mon.heartbeat
    if h.enabled and h.interval_sec < 60:
        raise ValueError(f"monitoring.heartbeat.interval_sec must be >= 60, got {h.interval_sec}")
