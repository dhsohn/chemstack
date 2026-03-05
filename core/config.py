from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

from .config_validation import (
    _as_float,
    _as_int,
    _as_str,
    _normalize_extensions,
    _normalize_string_list,
    _validate_cleanup_config,
    _validate_config,
    _validate_disk_monitor_config,
)


def _default_allowed_root() -> str:
    return str(Path.home() / "orca_runs")


def _default_organized_root() -> str:
    return str(Path.home() / "orca_outputs")


def _default_orca_executable() -> str:
    return str(Path.home() / "opt" / "orca" / "orca")


@dataclass
class RuntimeConfig:
    allowed_root: str = ""
    organized_root: str = ""
    # max retry count, not total execution count
    default_max_retries: int = 2

    def __post_init__(self) -> None:
        if not self.allowed_root:
            self.allowed_root = _default_allowed_root()
        if not self.organized_root:
            self.organized_root = _default_organized_root()


@dataclass
class PathsConfig:
    orca_executable: str = ""

    def __post_init__(self) -> None:
        if not self.orca_executable:
            self.orca_executable = _default_orca_executable()


_DEFAULT_KEEP_EXTENSIONS = [".inp", ".out", ".xyz", ".gbw", ".hess"]
_DEFAULT_KEEP_FILENAMES = ["run_state.json", "run_report.json", "run_report.md"]
_DEFAULT_REMOVE_PATTERNS = [
    "*.retry*.inp", "*.retry*.out", "*_trj.xyz",
    "*.densities", "*.engrad", "*.tmp", "*.prop", "*.scfp", "*.opt",
    "*.cis", "*.mdci", "*.mrci", "*.autoci", "*.cipsi",
    "*.loc", "*.nbo", "*.eprnmr", "*.compound",
    "*.bas", "*.one", "*.two",
]


@dataclass
class DiskMonitorConfig:
    threshold_gb: float = 50.0
    interval_sec: int = 300
    top_n: int = 10


@dataclass
class CleanupConfig:
    keep_extensions: List[str] = field(default_factory=lambda: list(_DEFAULT_KEEP_EXTENSIONS))
    keep_filenames: List[str] = field(default_factory=lambda: list(_DEFAULT_KEEP_FILENAMES))
    remove_patterns: List[str] = field(default_factory=lambda: list(_DEFAULT_REMOVE_PATTERNS))
    remove_overrides_keep: bool = False


@dataclass
class AppConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    cleanup: CleanupConfig = field(default_factory=CleanupConfig)
    disk_monitor: DiskMonitorConfig = field(default_factory=DiskMonitorConfig)


def load_config(config_path: str) -> AppConfig:
    path = Path(config_path).expanduser().resolve()
    raw: Dict[str, Any] = {}
    if path.exists():
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
            if isinstance(parsed, dict):
                raw = parsed
    else:
        logger.warning("Config file not found, using defaults: %s", path)

    runtime_raw = raw.get("runtime", {}) if isinstance(raw.get("runtime", {}), dict) else {}
    paths_raw = raw.get("paths", {}) if isinstance(raw.get("paths", {}), dict) else {}
    cleanup_raw = raw.get("cleanup", {}) if isinstance(raw.get("cleanup", {}), dict) else {}
    disk_monitor_raw = raw.get("disk_monitor", {}) if isinstance(raw.get("disk_monitor", {}), dict) else {}

    if "platform_mode" in runtime_raw:
        raise ValueError(
            "runtime.platform_mode is removed. orca_auto is Linux-only; delete this legacy key from config."
        )

    allowed_root = _as_str(
        runtime_raw.get("allowed_root"),
        _default_allowed_root(),
    )
    organized_root = _as_str(
        runtime_raw.get("organized_root"),
        _default_organized_root(),
    )
    default_max_retries = _as_int(
        runtime_raw.get("default_max_retries"),
        RuntimeConfig.default_max_retries,
    )

    cleanup_cfg = CleanupConfig(
        keep_extensions=_normalize_extensions(cleanup_raw.get("keep_extensions"), _DEFAULT_KEEP_EXTENSIONS),
        keep_filenames=_normalize_string_list(
            cleanup_raw.get("keep_filenames"), _DEFAULT_KEEP_FILENAMES,
        ),
        remove_patterns=_normalize_string_list(
            cleanup_raw.get("remove_patterns"), _DEFAULT_REMOVE_PATTERNS,
        ),
        remove_overrides_keep=(
            cleanup_raw.get("remove_overrides_keep")
            if isinstance(cleanup_raw.get("remove_overrides_keep"), bool)
            else CleanupConfig.remove_overrides_keep
        ),
    )

    disk_monitor_cfg = DiskMonitorConfig(
        threshold_gb=_as_float(disk_monitor_raw.get("threshold_gb"), DiskMonitorConfig.threshold_gb),
        interval_sec=_as_int(disk_monitor_raw.get("interval_sec"), DiskMonitorConfig.interval_sec),
        top_n=_as_int(disk_monitor_raw.get("top_n"), DiskMonitorConfig.top_n),
    )

    cfg = AppConfig(
        runtime=RuntimeConfig(
            allowed_root=allowed_root,
            organized_root=organized_root,
            default_max_retries=max(0, default_max_retries),
        ),
        paths=PathsConfig(
            orca_executable=_as_str(paths_raw.get("orca_executable"), _default_orca_executable()),
        ),
        cleanup=cleanup_cfg,
        disk_monitor=disk_monitor_cfg,
    )
    _validate_config(cfg)
    _validate_cleanup_config(cfg.cleanup)
    _validate_disk_monitor_config(cfg.disk_monitor)

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, orca_executable=%s",
        cfg.runtime.allowed_root, cfg.runtime.organized_root, cfg.paths.orca_executable,
    )
    return cfg
