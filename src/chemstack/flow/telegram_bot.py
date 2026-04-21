"""Telegram bot for unified chem_flow activity control."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable

from chemstack.core.app_ids import (
    CHEMSTACK_REPO_ROOT_ENV_VAR,
    LEGACY_ORCA_REPO_ROOT_ENV_VAR,
)
from chemstack.core.config import TelegramConfig

from .activity import _discover_sibling_config, _discover_workflow_root
from .operations import cancel_activity, list_activities

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}"
_POLL_TIMEOUT_SECONDS = 30
_MAX_MESSAGE_LENGTH = 4096


@dataclass(frozen=True)
class TelegramBotSettings:
    telegram: TelegramConfig
    workflow_root: str | None
    crest_auto_config: str | None
    xtb_auto_config: str | None
    orca_auto_config: str | None
    orca_auto_repo_root: str | None

    @property
    def enabled(self) -> bool:
        return self.telegram.enabled


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _status_icon(status: str) -> str:
    normalized = str(status).strip().lower()
    return {
        "pending": "⏳",
        "created": "🆕",
        "running": "▶",
        "retrying": "🔄",
        "cancel_requested": "⏹",
        "completed": "✅",
        "failed": "❌",
        "cancelled": "⛔",
    }.get(normalized, "•")


def settings_from_env() -> TelegramBotSettings:
    shared_config = _discover_sibling_config(None, app_name="chemstack")
    return TelegramBotSettings(
        telegram=TelegramConfig(
            bot_token=os.getenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("CHEM_FLOW_TELEGRAM_CHAT_ID", "").strip(),
        ),
        workflow_root=_discover_workflow_root(None),
        crest_auto_config=shared_config,
        xtb_auto_config=shared_config,
        orca_auto_config=shared_config,
        orca_auto_repo_root=(
            os.getenv(CHEMSTACK_REPO_ROOT_ENV_VAR, "").strip()
            or os.getenv(LEGACY_ORCA_REPO_ROOT_ENV_VAR, "").strip()
            or None
        ),
    )


def _api_call(
    token: str,
    method: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = _POLL_TIMEOUT_SECONDS + 5,
) -> Any | None:
    url = f"{_API_BASE.format(token=token)}/{method}"
    data = json.dumps(payload or {}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
            if result.get("ok"):
                return result.get("result")
            logger.warning("telegram_api_error: method=%s response=%s", method, result)
            return None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        logger.warning("telegram_api_http_error: method=%s status=%d body=%s", method, exc.code, body)
        return None
    except Exception as exc:
        logger.warning("telegram_api_failed: method=%s error=%s", method, exc)
        return None


def _send_message(token: str, chat_id: str, text: str, *, parse_mode: str | None = "HTML") -> bool:
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text[:_MAX_MESSAGE_LENGTH]}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    return _api_call(token, "sendMessage", payload) is not None


def _activity_payload(settings: TelegramBotSettings) -> dict[str, Any]:
    return list_activities(
        workflow_root=settings.workflow_root,
        crest_auto_config=settings.crest_auto_config,
        xtb_auto_config=settings.xtb_auto_config,
        orca_auto_config=settings.orca_auto_config,
        orca_auto_repo_root=settings.orca_auto_repo_root,
    )


def _format_activity_rows(rows: list[dict[str, Any]], *, limit: int = 12) -> str:
    lines: list[str] = []
    for item in rows[:limit]:
        label = escape_html(str(item.get("label", "")).strip() or str(item.get("activity_id", "-")))
        engine = escape_html(str(item.get("engine", "-")).strip())
        source = escape_html(str(item.get("source", "-")).strip())
        status = escape_html(str(item.get("status", "-")).strip())
        lines.append(f"{_status_icon(status)} <b>{label}</b>  {engine}  {status}  [{source}]")
    if len(rows) > limit:
        lines.append(f"... and {len(rows) - limit} more")
    return "\n".join(lines)


def _handle_list(settings: TelegramBotSettings, args: str) -> str:
    payload = _activity_payload(settings)
    rows = list(payload.get("activities", []))

    filter_status = args.strip().lower()
    if filter_status:
        rows = [item for item in rows if str(item.get("status", "")).strip().lower() == filter_status]

    if not rows:
        return "No activities found."

    header = f"<b>Activities</b> ({len(rows)})"
    return header + "\n\n" + _format_activity_rows(rows)


def _handle_cancel(settings: TelegramBotSettings, args: str) -> str:
    target = args.strip()
    if not target:
        return "Usage: /cancel &lt;target&gt;"
    try:
        payload = cancel_activity(
            target=target,
            workflow_root=settings.workflow_root,
            crest_auto_config=settings.crest_auto_config,
            xtb_auto_config=settings.xtb_auto_config,
            orca_auto_config=settings.orca_auto_config,
            orca_auto_repo_root=settings.orca_auto_repo_root,
        )
    except (LookupError, ValueError) as exc:
        return escape_html(str(exc))

    label = escape_html(str(payload.get("label", payload.get("activity_id", target))))
    status = escape_html(str(payload.get("status", "unknown")))
    return f"{_status_icon(status)} <b>{label}</b>\nstatus: <code>{status}</code>"


def _handle_help(settings: TelegramBotSettings, args: str) -> str:
    return (
        "<b>chem_flow bot commands</b>\n\n"
        "/list — Show unified activities\n"
        "/list running — Running activities only\n"
        "/list failed — Failed activities only\n"
        "/cancel &lt;target&gt; — Cancel a workflow or standalone job\n"
        "/help — This help message"
    )


_HANDLERS: dict[str, Callable[[TelegramBotSettings, str], str]] = {
    "list": _handle_list,
    "cancel": _handle_cancel,
    "help": _handle_help,
    "start": _handle_help,
}


def _set_bot_commands(token: str) -> None:
    commands = [
        {"command": "list", "description": "Show unified activity list"},
        {"command": "cancel", "description": "Cancel a workflow or job"},
        {"command": "help", "description": "Help"},
    ]
    _api_call(token, "setMyCommands", {"commands": commands})


def run_bot(settings: TelegramBotSettings | None = None) -> int:
    resolved = settings or settings_from_env()
    if not resolved.enabled:
        logger.error("Telegram is not configured. Set CHEM_FLOW_TELEGRAM_BOT_TOKEN and CHEM_FLOW_TELEGRAM_CHAT_ID.")
        return 1

    _set_bot_commands(resolved.telegram.bot_token)
    logger.info("chem_flow Telegram bot started (chat_id=%s)", resolved.telegram.chat_id)

    offset = 0
    while True:
        try:
            updates = _api_call(
                resolved.telegram.bot_token,
                "getUpdates",
                {"offset": offset, "timeout": _POLL_TIMEOUT_SECONDS, "allowed_updates": ["message"]},
                timeout=_POLL_TIMEOUT_SECONDS + 5,
            )
            if not isinstance(updates, list) or not updates:
                continue

            for update in updates:
                if not isinstance(update, dict):
                    continue
                update_id = int(update.get("update_id", 0) or 0)
                offset = max(offset, update_id + 1)

                message = update.get("message")
                if not isinstance(message, dict):
                    continue
                chat = message.get("chat")
                chat_dict = chat if isinstance(chat, dict) else {}
                if str(chat_dict.get("id", "")).strip() != resolved.telegram.chat_id:
                    continue

                text_value = message.get("text")
                text = text_value.strip() if isinstance(text_value, str) else ""
                if not text.startswith("/"):
                    continue

                parts = text.split(maxsplit=1)
                command = parts[0].lstrip("/").split("@")[0].lower()
                cmd_args = parts[1] if len(parts) > 1 else ""
                handler = _HANDLERS.get(command)
                if handler is None:
                    response = f"Unknown command: /{escape_html(command)}\nType /help for available commands."
                else:
                    try:
                        response = handler(resolved, cmd_args)
                    except Exception as exc:
                        logger.exception("telegram_bot_handler_error: cmd=%s", command)
                        response = f"Error: {escape_html(str(exc))}"
                _send_message(resolved.telegram.bot_token, resolved.telegram.chat_id, response)
        except KeyboardInterrupt:
            logger.info("chem_flow Telegram bot stopped")
            return 0
        except Exception as exc:
            logger.exception("telegram_bot_poll_error: %s", exc)
            time.sleep(5)


__all__ = [
    "TelegramBotSettings",
    "escape_html",
    "run_bot",
    "settings_from_env",
]
