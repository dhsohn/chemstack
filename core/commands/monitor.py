from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict

from ..config import load_config
from ..disk_monitor import DiskReport, scan_disk_usage
from ._helpers import _human_bytes

logger = logging.getLogger(__name__)


def _report_to_dict(report: DiskReport) -> Dict[str, Any]:
    d: Dict[str, Any] = {
        "allowed_root": report.allowed_root,
        "allowed_root_bytes": report.allowed_root_bytes,
        "allowed_root_human": _human_bytes(report.allowed_root_bytes),
        "organized_root": report.organized_root,
        "organized_root_bytes": report.organized_root_bytes,
        "organized_root_human": _human_bytes(report.organized_root_bytes),
        "combined_bytes": report.combined_bytes,
        "combined_human": _human_bytes(report.combined_bytes),
        "threshold_gb": report.threshold_gb,
        "threshold_exceeded": report.threshold_exceeded,
        "top_dirs": [
            {"path": td.path, "size_bytes": td.size_bytes, "size_human": _human_bytes(td.size_bytes)}
            for td in report.top_dirs
        ],
        "timestamp": report.timestamp,
    }
    if report.filesystem is not None:
        d["filesystem"] = {
            "total_bytes": report.filesystem.total_bytes,
            "used_bytes": report.filesystem.used_bytes,
            "free_bytes": report.filesystem.free_bytes,
            "usage_percent": report.filesystem.usage_percent,
        }
    return d


def _emit_monitor(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in ["allowed_root", "allowed_root_human", "organized_root", "organized_root_human",
                 "combined_human", "threshold_gb", "threshold_exceeded", "timestamp"]:
        if key in payload:
            print(f"{key}: {payload[key]}")
    for td in payload.get("top_dirs", []):
        print(f"  {td['path']}: {td['size_human']}")
    fs = payload.get("filesystem")
    if fs:
        print(f"filesystem: total={_human_bytes(fs['total_bytes'])}, used={_human_bytes(fs['used_bytes'])}, free={_human_bytes(fs['free_bytes'])}, usage={fs['usage_percent']}%")


def cmd_monitor(args: Any) -> int:
    cfg = load_config(args.config)
    as_json = getattr(args, "json", False)
    watch = getattr(args, "watch", False)

    threshold_gb = getattr(args, "threshold_gb", None)
    if threshold_gb is None:
        threshold_gb = cfg.disk_monitor.threshold_gb

    interval_sec = getattr(args, "interval_sec", None)
    if interval_sec is None:
        interval_sec = cfg.disk_monitor.interval_sec

    top_n = getattr(args, "top_n", None)
    if top_n is None:
        top_n = cfg.disk_monitor.top_n

    if threshold_gb <= 0:
        logger.error("threshold_gb must be > 0, got %s", threshold_gb)
        return 1
    if interval_sec < 10:
        logger.error("interval_sec must be >= 10, got %s", interval_sec)
        return 1
    if not (1 <= top_n <= 100):
        logger.error("top_n must be 1-100, got %s", top_n)
        return 1

    allowed_root = cfg.runtime.allowed_root
    organized_root = cfg.runtime.organized_root

    if not watch:
        report = scan_disk_usage(allowed_root, organized_root, threshold_gb, top_n)
        _emit_monitor(_report_to_dict(report), as_json)
        return 1 if report.threshold_exceeded else 0

    # Watch mode
    try:
        while True:
            report = scan_disk_usage(allowed_root, organized_root, threshold_gb, top_n)
            _emit_monitor(_report_to_dict(report), as_json)
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        logger.info("Monitor watch stopped by user")
        return 0
