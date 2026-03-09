"""Telegram notification sender.

Sends DFT monitor scan results as Telegram messages.
Uses only urllib with no external dependencies.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config import TelegramConfig
    from core.dft_monitor import ScanReport
    from core.types import RetryNotification

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}"
_MAX_MESSAGE_LENGTH = 4096


def escape_html(text: str) -> str:
    """Escape HTML special characters for Telegram HTML messages."""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def send_message(
    config: TelegramConfig,
    text: str,
    *,
    parse_mode: str | None = "HTML",
) -> bool:
    """Send a Telegram message. Returns True on success."""
    if not config.enabled:
        logger.debug("telegram_notifier_disabled")
        return False

    url = f"{_API_BASE.format(token=config.bot_token)}/sendMessage"
    payload: dict = {
        "chat_id": config.chat_id,
        "text": text[:_MAX_MESSAGE_LENGTH],
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            if not result.get("ok"):
                logger.warning("telegram_send_api_error: %s", result)
                return False
            return True
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.warning("telegram_send_http_error: status=%d body=%s", exc.code, body)
        return False
    except Exception as exc:
        logger.warning("telegram_send_failed: %s", exc)
        return False


def format_scan_report(report: ScanReport) -> str | None:
    """Format a ScanReport as a Telegram HTML message. Returns None if nothing to report."""
    if not report.new_results:
        return None

    lines: list[str] = [f"<b>DFT Calculation Alert</b> ({len(report.new_results)} new)\n"]

    for r in report.new_results:
        status_icon = _status_icon(r.status)
        line = (
            f"{status_icon} <b>{escape_html(r.formula)}</b>"
            f" | {escape_html(r.method_basis)}"
            f" | {escape_html(r.energy)}"
        )
        if r.calc_type:
            line += f" | {escape_html(r.calc_type)}"
        if r.note:
            line += f" {escape_html(r.note)}"
        line += f"\n<code>{escape_html(r.path)}</code>"
        lines.append(line)

    return "\n\n".join(lines)


def notify_scan_report(config: TelegramConfig, report: ScanReport) -> bool:
    """Send a Telegram notification if the ScanReport contains new results."""
    text = format_scan_report(report)
    if text is None:
        return False
    return send_message(config, text)


def format_retry_event(event: RetryNotification) -> str:
    """Format a retry event as a Telegram HTML message."""
    reaction_dir = Path(event["reaction_dir"])
    failed_inp = Path(event["failed_inp"])
    next_inp = Path(event["next_inp"])
    lines = [
        "<b>ORCA Auto Retry</b>",
        f"<b>Job</b>: {escape_html(reaction_dir.name or reaction_dir.as_posix())}",
        (
            f"<b>Attempt</b>: {event['attempt_index']} failed; "
            f"retry {event['retry_number']}/{event['max_retries']} is starting"
        ),
        (
            f"<b>Reason</b>: <code>{escape_html(event['analyzer_status'])}</code> "
            f"({escape_html(event['analyzer_reason'])})"
        ),
        f"<b>Failed input</b>: <code>{escape_html(failed_inp.name)}</code>",
        f"<b>Restart input</b>: <code>{escape_html(next_inp.name)}</code>",
    ]
    patch_summary = _format_patch_actions(event.get("patch_actions", []))
    if patch_summary:
        lines.append(f"<b>Applied patches</b>: {patch_summary}")
    if event.get("resumed"):
        lines.append("<b>Mode</b>: resumed run")
    lines.append(f"<b>Directory</b>: <code>{escape_html(event['reaction_dir'])}</code>")
    return "\n".join(lines)


def notify_retry_event(config: TelegramConfig, event: RetryNotification) -> bool:
    """Send a Telegram notification when an automatic retry is scheduled."""
    if not config.enabled:
        logger.debug("telegram_retry_notification_disabled")
        return False

    sent = send_message(config, format_retry_event(event))
    if sent:
        logger.info(
            "telegram_retry_notification_sent: reaction_dir=%s retry=%d",
            event["reaction_dir"],
            event["retry_number"],
        )
    else:
        logger.warning(
            "telegram_retry_notification_failed: reaction_dir=%s retry=%d",
            event["reaction_dir"],
            event["retry_number"],
        )
    return sent


def _status_icon(status: str) -> str:
    icons = {
        "completed": "\u2705",
        "running": "\u23f3",
        "failed": "\u274c",
        "error": "\u274c",
    }
    return icons.get(status, "\u2753")


def _format_patch_actions(actions: list[str]) -> str | None:
    rendered: list[str] = []
    for action in actions[:4]:
        text = action.strip()
        if not text:
            continue
        rendered.append(escape_html(_humanize_patch_action(text)))
    if not rendered:
        return None
    if len(actions) > len(rendered):
        rendered.append("...")
    return ", ".join(rendered)


def _humanize_patch_action(action: str) -> str:
    labels = {
        "route_add_tightscf_slowconv": "TightSCF + SlowConv",
        "scf_maxiter_300": "SCF MaxIter 300",
        "geom_hessian_and_maxiter": "Geom Hessian + MaxIter 300",
        "geom_hessian_and_maxiter_500": "Geom Hessian + MaxIter 500",
        "maxcore_increased": "MaxCore increased",
        "route_add_looseopt": "LooseOpt",
        "geometry_restart_not_applied": "geometry restart not applied",
        "no_previous_xyz_file_found": "no previous xyz file found",
        "no_geometry_file_found": "no geometry file found",
        "no_recipe_applied": "no retry recipe applied",
    }
    if action.startswith("geometry_restart_from_"):
        source = action.removeprefix("geometry_restart_from_")
        return f"geometry restart from {source}"
    return labels.get(action, action.replace("_", " "))
