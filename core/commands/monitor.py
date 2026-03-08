"""monitor command — scan simulation status and send Telegram summary.

Runs hourly via cron to report currently running simulations
and newly detected DFT calculation results via Telegram.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..config import AppConfig, load_config
from ..dft_index import DFTIndex
from ..dft_monitor import DFTMonitor, ScanReport
from ..telegram_notifier import escape_html, send_message
from ..types import RunInfo
from ._helpers import _to_resolved_local
from .list_runs import _collect_runs

logger = logging.getLogger(__name__)

_STATE_FILE = ".dft_monitor_state.json"
_DFT_DB = "dft.db"

_ICON = {
    "completed": "\u2705",
    "running": "\u23f3",
    "failed": "\u274c",
    "retrying": "\U0001f504",
    "created": "\U0001f195",
}


def _status_icon(status: str) -> str:
    return _ICON.get(status, "\u2753")


def _format_running_section(runs: list[RunInfo]) -> str | None:
    """Build HTML block for running/retrying simulations."""
    active = [r for r in runs if r["status"] in ("running", "retrying")]
    if not active:
        return None

    lines: list[str] = []
    for r in active:
        icon = _status_icon(r["status"])
        inp_name = r["inp"] or "-"
        attempt_info = f"(attempt #{r['attempts']})" if r["attempts"] > 1 else ""
        lines.append(
            f"{icon} <b>{escape_html(r['dir'])}</b> {attempt_info}\n"
            f"   \U0001f4c4 {escape_html(inp_name)}\n"
            f"   \u23f1 Elapsed: {escape_html(r['elapsed_text'])}"
        )

    header = f"\u23f3 <b>Running</b>  ({len(active)})"
    return header + "\n\n" + "\n\n".join(lines)


def _format_dft_section(report: ScanReport) -> str | None:
    """Build HTML block for newly detected DFT calculation results."""
    if not report.new_results:
        return None

    lines: list[str] = []
    for r in report.new_results:
        icon = _status_icon(r.status)
        calc_label = r.calc_type.upper() if r.calc_type else "-"
        note = f"\n   \u26a0\ufe0f {escape_html(r.note.strip('() '))}" if r.note else ""
        lines.append(
            f"{icon} <b>{escape_html(r.formula)}</b>  [{escape_html(calc_label)}]\n"
            f"   \U0001f9ec {escape_html(r.method_basis)}\n"
            f"   \u26a1 {escape_html(r.energy)}\n"
            f"   \U0001f4c2 <code>{escape_html(r.path)}</code>"
            f"{note}"
        )

    header = f"\U0001f9ea <b>New Calculations Detected</b>  ({len(report.new_results)})"
    return header + "\n\n" + "\n\n".join(lines)


def _format_failure_section(report: ScanReport) -> str | None:
    """Build HTML block for parse failures, if any."""
    if not report.failures:
        return None

    lines: list[str] = []
    for f in report.failures[:5]:  # cap at 5 to avoid message bloat
        lines.append(
            f"\u274c <code>{escape_html(f.path)}</code>\n"
            f"   {escape_html(f.error_type)}: {escape_html(f.error)}"
        )

    count = len(report.failures)
    header = f"\u26a0\ufe0f <b>Parse Failures</b>  ({count})"
    body = "\n\n".join(lines)
    if count > 5:
        body += f"\n\n   ... and {count - 5} more"
    return header + "\n\n" + body


def _format_overall_summary(runs: list[RunInfo]) -> str:
    """Build a one-line summary of all simulation stats."""
    counts: dict[str, int] = {}
    for r in runs:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    parts: list[str] = []
    for status in ("running", "retrying", "completed", "failed", "created"):
        n = counts.get(status, 0)
        if n > 0:
            parts.append(f"{_status_icon(status)} {status} {n}")

    total = len(runs)
    summary = " | ".join(parts) if parts else "No runs"
    return f"\U0001f4ca <b>Overview</b>  (total {total})\n{summary}"


def _build_message(
    runs: list[RunInfo],
    report: ScanReport,
) -> str:
    """Compose the full Telegram message."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = f"\u2699\ufe0f <b>orca_auto monitor</b>  <code>{now}</code>"
    divider = "\u2500" * 28

    sections: list[str] = [header, divider]

    running = _format_running_section(runs)
    if running:
        sections.append(running)

    dft = _format_dft_section(report)
    if dft:
        sections.append(dft)

    fail = _format_failure_section(report)
    if fail:
        sections.append(fail)

    sections.append(divider)
    sections.append(_format_overall_summary(runs))

    return "\n\n".join(sections)


def _run_monitor(cfg: AppConfig) -> int:
    """Execute a single scan and send Telegram notification."""
    tg = cfg.telegram
    if not tg.enabled:
        logger.error("Telegram is not configured.")
        return 1

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    # 1) Collect current simulations
    runs = _collect_runs(allowed_root)

    # 2) DFT Monitor scan (detect newly changed calculations)
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

    if report.baseline_seeded:
        logger.info("DFT Monitor baseline seeded (first run). Changes will be detected from next scan.")

    # 3) Compose and send message
    message = _build_message(runs, report)
    success = send_message(tg, message)

    if success:
        logger.info("Telegram notification sent successfully")
    else:
        logger.error("Failed to send Telegram notification")
        return 1

    return 0


def cmd_monitor(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_monitor(cfg)
