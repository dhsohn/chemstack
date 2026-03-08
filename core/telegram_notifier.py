"""텔레그램 봇 알림 전송.

DFT 모니터 스캔 결과를 텔레그램 메시지로 전송한다.
외부 의존성 없이 urllib만 사용.

ollama_bot 텔레그램 알림 로직에서 이식됨.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config import TelegramConfig
    from core.dft_monitor import ScanReport

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}"
_MAX_MESSAGE_LENGTH = 4096


def send_message(
    config: TelegramConfig,
    text: str,
    *,
    parse_mode: str | None = "HTML",
) -> bool:
    """텔레그램 메시지 전송. 성공 시 True 반환."""
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
    """ScanReport를 텔레그램 HTML 메시지로 포맷. 알릴 내용이 없으면 None."""
    if not report.new_results:
        return None

    lines: list[str] = [f"<b>DFT 계산 알림</b> ({len(report.new_results)}건)\n"]

    for r in report.new_results:
        status_icon = _status_icon(r.status)
        line = (
            f"{status_icon} <b>{_escape_html(r.formula)}</b>"
            f" | {_escape_html(r.method_basis)}"
            f" | {_escape_html(r.energy)}"
        )
        if r.calc_type:
            line += f" | {_escape_html(r.calc_type)}"
        if r.note:
            line += f" {_escape_html(r.note)}"
        line += f"\n<code>{_escape_html(r.path)}</code>"
        lines.append(line)

    return "\n\n".join(lines)


def notify_scan_report(config: TelegramConfig, report: ScanReport) -> bool:
    """ScanReport에 새 결과가 있으면 텔레그램으로 전송."""
    text = format_scan_report(report)
    if text is None:
        return False
    return send_message(config, text)


def _status_icon(status: str) -> str:
    icons = {
        "completed": "\u2705",
        "running": "\u23f3",
        "failed": "\u274c",
        "error": "\u274c",
    }
    return icons.get(status, "\u2753")


def _escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
