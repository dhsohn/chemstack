"""monitor command — send discovery alerts from periodic filesystem scans.

Runs hourly via cron to report only newly discovered DFT results and scan
failures. Run lifecycle notifications are emitted directly from ``run-inp``,
and workstation state snapshots belong to ``summary``.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from ..config import AppConfig, load_config
from ..dft_index import DFTIndex
from ..dft_monitor import DFTMonitor, MonitorResult, ScanReport
from ..run_snapshot import status_icon
from ..telegram_notifier import escape_html, send_message
from ._helpers import _to_resolved_local

logger = logging.getLogger(__name__)

_STATE_FILE = ".dft_monitor_state.json"
_DFT_DB = "dft.db"


def _notifiable_dft_results(report: ScanReport) -> list[MonitorResult]:
    return [
        result
        for result in report.new_results
        if str(result.status).strip().lower() != "running"
    ]


def _format_dft_section(report: ScanReport) -> str | None:
    results = _notifiable_dft_results(report)
    if not results:
        return None

    lines: list[str] = []
    for result in results:
        icon = status_icon(result.status)
        calc_label = result.calc_type.upper() if result.calc_type else "-"
        note = f"\n   \u26a0\ufe0f {escape_html(result.note.strip('() '))}" if result.note else ""
        lines.append(
            f"{icon} <b>{escape_html(result.formula)}</b>  [{escape_html(calc_label)}]\n"
            f"   \U0001f9ec {escape_html(result.method_basis)}\n"
            f"   \u26a1 {escape_html(result.energy)}\n"
            f"   \U0001f4c2 <code>{escape_html(result.path)}</code>"
            f"{note}"
        )

    header = f"\U0001f9ea <b>New Calculations Detected</b>  ({len(results)})"
    return header + "\n\n" + "\n\n".join(lines)


def _format_failure_section(report: ScanReport) -> str | None:
    if not report.failures:
        return None

    lines: list[str] = []
    for failure in report.failures[:5]:
        lines.append(
            f"\u274c <code>{escape_html(failure.path)}</code>\n"
            f"   {escape_html(failure.error_type)}: {escape_html(failure.error)}"
        )

    count = len(report.failures)
    header = f"\u26a0\ufe0f <b>Scan Parse Failures</b>  ({count})"
    body = "\n\n".join(lines)
    if count > 5:
        body += f"\n\n   ... and {count - 5} more"
    return header + "\n\n" + body


def _build_message(report: ScanReport) -> str:
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = f"\u2699\ufe0f <b>orca_auto monitor</b>  <code>{now}</code>"
    divider = "\u2500" * 28
    scope = (
        "\U0001f50d <b>Scope</b>\n"
        "Filesystem discovery only. "
        "Use run-inp alerts for immediate lifecycle events and summary for periodic state digests."
    )

    sections: list[str] = [header, divider, scope]

    dft = _format_dft_section(report)
    if dft:
        sections.append(dft)

    fail = _format_failure_section(report)
    if fail:
        sections.append(fail)

    return "\n\n".join(sections)


def _run_monitor(cfg: AppConfig) -> int:
    tg = cfg.telegram
    if not tg.enabled:
        logger.error("Telegram is not configured.")
        return 1

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    state_file = str(allowed_root / _STATE_FILE)
    db_path = str(allowed_root / _DFT_DB)
    dft_index = DFTIndex()
    dft_index.initialize(db_path)
    monitor = DFTMonitor(
        dft_index=dft_index,
        kb_dirs=[str(allowed_root)],
        state_file=state_file,
    )
    report = monitor.scan()
    notifiable_dft_results = _notifiable_dft_results(report)

    should_send = bool(notifiable_dft_results or report.failures)
    if not should_send:
        logger.info("No new monitor discoveries to send.")
        return 0

    message = _build_message(report)
    success = send_message(tg, message)
    if not success:
        logger.error("Failed to send Telegram notification")
        return 1

    logger.info("Telegram notification sent successfully")
    return 0


def cmd_monitor(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_monitor(cfg)
