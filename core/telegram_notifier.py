"""Telegram notification sender and formatter utilities."""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config import TelegramConfig
    from core.dft_monitor import ScanReport
    from core.types import QueueEnqueuedNotification, RetryNotification, RunFinishedNotification, RunStartedNotification

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


def has_monitor_updates(report: ScanReport) -> bool:
    return bool(_notifiable_monitor_results(report) or report.failures)


def _notifiable_monitor_results(report: ScanReport) -> list:
    return [
        result
        for result in report.new_results
        if str(result.status).strip().lower() != "running"
    ]


def _format_monitor_dft_section(report: ScanReport) -> str | None:
    results = _notifiable_monitor_results(report)
    if not results:
        return None

    lines: list[str] = []
    for result in results:
        icon = _status_icon(str(result.status))
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


def _format_monitor_failure_section(report: ScanReport) -> str | None:
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


def format_monitor_message(report: ScanReport, *, now: datetime | None = None) -> str:
    if now is None:
        current_time = datetime.now().astimezone()
    elif now.tzinfo is None:
        current_time = now.astimezone()
    else:
        current_time = now
    header = f"\u2699\ufe0f <b>orca_auto monitor</b>  <code>{current_time.strftime('%Y-%m-%d %H:%M %Z')}</code>"
    divider = "\u2500" * 28
    scope = (
        "\U0001f50d <b>Scope</b>\n"
        "Filesystem discovery only. "
        "Use run-dir alerts for immediate lifecycle events and summary for periodic state digests."
    )

    sections: list[str] = [header, divider, scope]

    dft_section = _format_monitor_dft_section(report)
    if dft_section:
        sections.append(dft_section)

    failure_section = _format_monitor_failure_section(report)
    if failure_section:
        sections.append(failure_section)

    return "\n\n".join(sections)


def notify_monitor_report(config: TelegramConfig, report: ScanReport) -> bool:
    if not has_monitor_updates(report):
        return False
    return send_message(config, format_monitor_message(report))


def format_run_started_event(event: RunStartedNotification) -> str:
    """Format an immediate run-start notification."""
    reaction_dir = Path(event["reaction_dir"])
    current_inp = Path(event["current_inp"])
    status = str(event["status"]).strip().lower()
    title = "ORCA Auto Resumed" if event.get("resumed") else "ORCA Auto Started"
    lines = [
        f"<b>{escape_html(title)}</b>",
        f"<b>Job</b>: {escape_html(reaction_dir.name or reaction_dir.as_posix())}",
        (
            f"<b>Attempt</b>: #{event['attempt_index']} "
            f"(<code>{escape_html(status or 'running')}</code>)"
        ),
        f"<b>Input</b>: <code>{escape_html(current_inp.name)}</code>",
        f"<b>Max retries</b>: {event['max_retries']}",
    ]
    if event.get("resumed"):
        lines.append("<b>Mode</b>: resumed run")
    lines.append(f"<b>Directory</b>: <code>{escape_html(event['reaction_dir'])}</code>")
    return "\n".join(lines)


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


def format_run_finished_event(event: RunFinishedNotification) -> str:
    """Format an immediate terminal run notification."""
    reaction_dir = Path(event["reaction_dir"])
    status = str(event["status"]).strip().lower()
    title = "ORCA Auto Completed" if status == "completed" else "ORCA Auto Failed"
    status_text = status or "unknown"
    lines = [
        f"<b>{escape_html(title)}</b>",
        f"<b>Job</b>: {escape_html(reaction_dir.name or reaction_dir.as_posix())}",
        f"<b>Result</b>: <code>{escape_html(status_text)}</code>",
        f"<b>Attempts</b>: {event['attempt_count']}",
        f"<b>Reason</b>: <code>{escape_html(event['reason'])}</code>",
        f"<b>Analyzer</b>: <code>{escape_html(event['analyzer_status'])}</code>",
    ]
    last_out_path = event.get("last_out_path")
    if isinstance(last_out_path, str) and last_out_path.strip():
        lines.append(f"<b>Output</b>: <code>{escape_html(Path(last_out_path).name)}</code>")
    if event.get("skipped_execution"):
        lines.append("<b>Mode</b>: reused existing output")
    elif event.get("resumed"):
        lines.append("<b>Mode</b>: resumed run")
    lines.append(f"<b>Directory</b>: <code>{escape_html(event['reaction_dir'])}</code>")
    return "\n".join(lines)


def notify_run_started_event(config: TelegramConfig, event: RunStartedNotification) -> bool:
    """Send a Telegram notification when a run attempt starts."""
    if not config.enabled:
        logger.debug("telegram_run_started_notification_disabled")
        return False

    sent = send_message(config, format_run_started_event(event))
    if sent:
        logger.info(
            "telegram_run_started_notification_sent: reaction_dir=%s attempt=%d",
            event["reaction_dir"],
            event["attempt_index"],
        )
    else:
        logger.warning(
            "telegram_run_started_notification_failed: reaction_dir=%s attempt=%d",
            event["reaction_dir"],
            event["attempt_index"],
        )
    return sent


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


def notify_run_finished_event(config: TelegramConfig, event: RunFinishedNotification) -> bool:
    """Send a Telegram notification when a run reaches a terminal state."""
    if not config.enabled:
        logger.debug("telegram_run_finished_notification_disabled")
        return False

    sent = send_message(config, format_run_finished_event(event))
    if sent:
        logger.info(
            "telegram_run_finished_notification_sent: reaction_dir=%s status=%s",
            event["reaction_dir"],
            event["status"],
        )
    else:
        logger.warning(
            "telegram_run_finished_notification_failed: reaction_dir=%s status=%s",
            event["reaction_dir"],
            event["status"],
        )
    return sent


def _status_icon(status: str) -> str:
    icons = {
        "completed": "\u2705",
        "running": "\u23f3",
        "retrying": "\U0001f504",
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


def format_queue_enqueued_event(event: QueueEnqueuedNotification) -> str:
    """Format a queue-enqueued notification as a Telegram HTML message."""
    reaction_dir = Path(event["reaction_dir"])
    lines = [
        "<b>ORCA Auto Queued</b>",
        f"<b>Job</b>: {escape_html(reaction_dir.name or reaction_dir.as_posix())}",
        f"<b>Queue ID</b>: <code>{escape_html(event['queue_id'])}</code>",
        f"<b>Priority</b>: {event['priority']}",
    ]
    if event.get("force"):
        lines.append("<b>Mode</b>: force re-enqueue")
    lines.append(f"<b>Directory</b>: <code>{escape_html(event['reaction_dir'])}</code>")
    return "\n".join(lines)


def notify_queue_enqueued_event(config: TelegramConfig, event: QueueEnqueuedNotification) -> bool:
    """Send a Telegram notification when a job is added to the queue."""
    if not config.enabled:
        logger.debug("telegram_queue_enqueued_notification_disabled")
        return False

    sent = send_message(config, format_queue_enqueued_event(event))
    if sent:
        logger.info(
            "telegram_queue_enqueued_notification_sent: queue_id=%s reaction_dir=%s",
            event["queue_id"],
            event["reaction_dir"],
        )
    else:
        logger.warning(
            "telegram_queue_enqueued_notification_failed: queue_id=%s reaction_dir=%s",
            event["queue_id"],
            event["reaction_dir"],
        )
    return sent
