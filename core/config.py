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
    _validate_monitoring_config,
)
from .pathing import to_local_path


@dataclass
class RuntimeConfig:
    allowed_root: str = "/home/daehyupsohn/orca_runs"
    organized_root: str = "/home/daehyupsohn/orca_outputs"
    # max retry count, not total execution count
    default_max_retries: int = 2


@dataclass
class PathsConfig:
    orca_executable: str = "/home/daehyupsohn/opt/orca/orca"


@dataclass
class TelegramTransportConfig:
    bot_token_env: str = "ORCA_AUTO_TELEGRAM_BOT_TOKEN"
    chat_id_env: str = "ORCA_AUTO_TELEGRAM_CHAT_ID"
    timeout_sec: int = 5
    retry_count: int = 2
    retry_backoff_sec: float = 1.0
    retry_jitter_sec: float = 0.3


@dataclass
class DeliveryConfig:
    async_enabled: bool = True
    queue_size: int = 1000
    worker_flush_timeout_sec: float = 3.0
    dedup_ttl_sec: int = 86400


@dataclass
class MonitoringConfig:
    enabled: bool = False
    telegram: TelegramTransportConfig = field(default_factory=TelegramTransportConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)


_DEFAULT_KEEP_EXTENSIONS = [".inp", ".out", ".xyz", ".gbw", ".hess"]
_DEFAULT_KEEP_FILENAMES = ["run_state.json", "run_report.json", "run_report.md"]
_DEFAULT_REMOVE_PATTERNS = ["*.retry*.inp", "*.retry*.out", "*_trj.xyz"]


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
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    cleanup: CleanupConfig = field(default_factory=CleanupConfig)
    disk_monitor: DiskMonitorConfig = field(default_factory=DiskMonitorConfig)


def load_config(config_path: str) -> AppConfig:
    path = Path(to_local_path(config_path))
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
    monitoring_raw = raw.get("monitoring", {}) if isinstance(raw.get("monitoring", {}), dict) else {}
    telegram_raw = monitoring_raw.get("telegram", {}) if isinstance(monitoring_raw.get("telegram", {}), dict) else {}
    delivery_raw = monitoring_raw.get("delivery", {}) if isinstance(monitoring_raw.get("delivery", {}), dict) else {}
    cleanup_raw = raw.get("cleanup", {}) if isinstance(raw.get("cleanup", {}), dict) else {}
    disk_monitor_raw = raw.get("disk_monitor", {}) if isinstance(raw.get("disk_monitor", {}), dict) else {}

    if "platform_mode" in runtime_raw:
        raise ValueError(
            "runtime.platform_mode is removed. orca_auto is Linux-only; delete this legacy key from config."
        )

    allowed_root = _as_str(
        runtime_raw.get("allowed_root"),
        RuntimeConfig.allowed_root,
    )
    organized_root = _as_str(
        runtime_raw.get("organized_root"),
        RuntimeConfig.organized_root,
    )
    raw_max_retries = runtime_raw.get("default_max_retries")
    if raw_max_retries is None and "default_max_attempts" in runtime_raw:
        logger.warning(
            "Config key 'default_max_attempts' is deprecated; use 'default_max_retries' instead."
        )
        raw_max_retries = runtime_raw["default_max_attempts"]
    default_max_retries = _as_int(raw_max_retries, RuntimeConfig.default_max_retries)

    monitoring_cfg = MonitoringConfig(
        enabled=bool(monitoring_raw.get("enabled", False)),
        telegram=TelegramTransportConfig(
            bot_token_env=_as_str(telegram_raw.get("bot_token_env"), TelegramTransportConfig.bot_token_env),
            chat_id_env=_as_str(telegram_raw.get("chat_id_env"), TelegramTransportConfig.chat_id_env),
            timeout_sec=_as_int(telegram_raw.get("timeout_sec"), TelegramTransportConfig.timeout_sec),
            retry_count=_as_int(telegram_raw.get("retry_count"), TelegramTransportConfig.retry_count),
            retry_backoff_sec=_as_float(telegram_raw.get("retry_backoff_sec"), TelegramTransportConfig.retry_backoff_sec),
            retry_jitter_sec=_as_float(telegram_raw.get("retry_jitter_sec"), TelegramTransportConfig.retry_jitter_sec),
        ),
        delivery=DeliveryConfig(
            async_enabled=bool(delivery_raw.get("async_enabled", DeliveryConfig.async_enabled)),
            queue_size=_as_int(delivery_raw.get("queue_size"), DeliveryConfig.queue_size),
            worker_flush_timeout_sec=_as_float(
                delivery_raw.get("worker_flush_timeout_sec"), DeliveryConfig.worker_flush_timeout_sec,
            ),
            dedup_ttl_sec=_as_int(delivery_raw.get("dedup_ttl_sec"), DeliveryConfig.dedup_ttl_sec),
        ),
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
            orca_executable=_as_str(paths_raw.get("orca_executable"), PathsConfig.orca_executable),
        ),
        monitoring=monitoring_cfg,
        cleanup=cleanup_cfg,
        disk_monitor=disk_monitor_cfg,
    )
    _validate_config(cfg)
    _validate_cleanup_config(cfg.cleanup)
    _validate_disk_monitor_config(cfg.disk_monitor)

    if cfg.monitoring.enabled:
        try:
            _validate_monitoring_config(cfg.monitoring)
        except ValueError as exc:
            logger.warning("Monitoring config invalid, disabling: %s", exc)
            cfg = AppConfig(
                runtime=cfg.runtime,
                paths=cfg.paths,
                monitoring=MonitoringConfig(enabled=False),
                cleanup=cfg.cleanup,
                disk_monitor=cfg.disk_monitor,
            )

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, orca_executable=%s",
        cfg.runtime.allowed_root, cfg.runtime.organized_root, cfg.paths.orca_executable,
    )
    return cfg
