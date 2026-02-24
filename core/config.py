from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

import yaml

logger = logging.getLogger(__name__)

from .pathing import is_subpath, is_windows_style_path, to_local_path


@dataclass
class RuntimeConfig:
    allowed_root: str = "/home/daehyupsohn/orca_runs"
    organized_root: str = "/home/daehyupsohn/orca_outputs"
    # max retry count, not total execution count
    default_max_retries: int = 5


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
class HeartbeatConfig:
    enabled: bool = True
    interval_sec: int = 1800


@dataclass
class MonitoringConfig:
    enabled: bool = False
    telegram: TelegramTransportConfig = field(default_factory=TelegramTransportConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)


_DEFAULT_KEEP_EXTENSIONS = [".inp", ".out", ".xyz", ".gbw", ".hess"]
_DEFAULT_KEEP_FILENAMES = ["run_state.json", "run_report.json", "run_report.md"]
_DEFAULT_REMOVE_PATTERNS = ["*.retry*.inp", "*.retry*.out", "*_trj.xyz"]


@dataclass
class CleanupConfig:
    keep_extensions: List[str] = field(default_factory=lambda: list(_DEFAULT_KEEP_EXTENSIONS))
    keep_filenames: List[str] = field(default_factory=lambda: list(_DEFAULT_KEEP_FILENAMES))
    remove_patterns: List[str] = field(default_factory=lambda: list(_DEFAULT_REMOVE_PATTERNS))


@dataclass
class AppConfig:
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    paths: PathsConfig = field(default_factory=PathsConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    cleanup: CleanupConfig = field(default_factory=CleanupConfig)


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


def _validate_config(cfg: AppConfig) -> None:
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


def _normalize_extensions(raw: Any) -> List[str]:
    if not isinstance(raw, list):
        return list(_DEFAULT_KEEP_EXTENSIONS)
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


def _validate_cleanup_config(cleanup: CleanupConfig) -> None:
    if not cleanup.keep_extensions:
        raise ValueError(
            "cleanup.keep_extensions must not be empty (data loss risk)"
        )
    if not cleanup.keep_filenames:
        raise ValueError(
            "cleanup.keep_filenames must not be empty (data loss risk)"
        )


def _validate_monitoring_config(mon: MonitoringConfig) -> None:
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
    heartbeat_raw = monitoring_raw.get("heartbeat", {}) if isinstance(monitoring_raw.get("heartbeat", {}), dict) else {}
    cleanup_raw = raw.get("cleanup", {}) if isinstance(raw.get("cleanup", {}), dict) else {}

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
        heartbeat=HeartbeatConfig(
            enabled=bool(heartbeat_raw.get("enabled", HeartbeatConfig.enabled)),
            interval_sec=_as_int(heartbeat_raw.get("interval_sec"), HeartbeatConfig.interval_sec),
        ),
    )

    cleanup_cfg = CleanupConfig(
        keep_extensions=_normalize_extensions(cleanup_raw.get("keep_extensions")),
        keep_filenames=_normalize_string_list(
            cleanup_raw.get("keep_filenames"), _DEFAULT_KEEP_FILENAMES,
        ),
        remove_patterns=_normalize_string_list(
            cleanup_raw.get("remove_patterns"), _DEFAULT_REMOVE_PATTERNS,
        ),
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
    )
    _validate_config(cfg)
    _validate_cleanup_config(cfg.cleanup)

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
            )

    logger.info(
        "Config loaded: allowed_root=%s, organized_root=%s, orca_executable=%s",
        cfg.runtime.allowed_root, cfg.runtime.organized_root, cfg.paths.orca_executable,
    )
    return cfg
