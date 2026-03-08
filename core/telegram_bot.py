"""Telegram bot — receives commands via long polling and responds.

Uses only urllib with no external dependencies.
"""

from __future__ import annotations

import json
import logging
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from .commands.list_runs import _collect_runs
from .config import AppConfig
from .telegram_notifier import escape_html

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}"
_POLL_TIMEOUT = 30  # long polling timeout (seconds)
_MAX_MESSAGE_LENGTH = 4096


def _api_call(
    token: str,
    method: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 35,
) -> Any | None:
    """Call the Telegram Bot API."""
    url = f"{_API_BASE.format(token=token)}/{method}"
    data = json.dumps(payload or {}).encode("utf-8")
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            result = json.loads(resp.read().decode("utf-8"))
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


# -- Command handlers ------------------------------------------------


def _status_icon(status: str) -> str:
    return {"completed": "\u2705", "running": "\u23f3", "failed": "\u274c",
            "retrying": "\U0001f504", "created": "\U0001f195"}.get(status, "\u2753")


def _handle_list(cfg: AppConfig, args: str) -> str:
    """Handle ``/list [filter]`` command."""
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    if not allowed_root.is_dir():
        return "allowed_root not found."

    runs = _collect_runs(allowed_root)

    filter_status = args.strip().lower() if args.strip() else None
    if filter_status:
        runs = [r for r in runs if r["status"] == filter_status]

    if not runs:
        return "No registered runs found."

    lines: list[str] = [f"<b>Simulation List</b> ({len(runs)})\n"]
    for r in runs:
        icon = _status_icon(r["status"])
        line = (
            f"{icon} <b>{escape_html(r['dir'])}</b>"
            f"  {escape_html(r['status'])}"
            f"  {escape_html(r['elapsed_text'])}"
        )
        if r["inp"]:
            line += f"  <code>{escape_html(r['inp'])}</code>"
        lines.append(line)

    return "\n".join(lines)


def _handle_help(cfg: AppConfig, args: str) -> str:
    return (
        "<b>orca_auto bot commands</b>\n\n"
        "/list \u2014 Show all simulations\n"
        "/list running \u2014 Running jobs only\n"
        "/list completed \u2014 Completed jobs only\n"
        "/list failed \u2014 Failed jobs only\n"
        "/help \u2014 This help message"
    )


_HANDLERS: dict[str, Callable[[AppConfig, str], str]] = {
    "list": _handle_list,
    "help": _handle_help,
    "start": _handle_help,
}


# -- Bot main loop ---------------------------------------------------


def _set_bot_commands(token: str) -> None:
    """Register bot command autocomplete."""
    commands = [
        {"command": "list", "description": "Show simulation list"},
        {"command": "help", "description": "Help"},
    ]
    _api_call(token, "setMyCommands", {"commands": commands})


def run_bot(cfg: AppConfig) -> int:
    """Run the Telegram bot long-polling loop. Exit with Ctrl+C."""
    tg = cfg.telegram
    if not tg.enabled:
        logger.error("Telegram is not configured. Check bot_token/chat_id.")
        return 1

    _set_bot_commands(tg.bot_token)
    logger.info("Telegram bot started (chat_id=%s)", tg.chat_id)

    offset = 0
    while True:
        try:
            updates = _api_call(
                tg.bot_token, "getUpdates",
                {"offset": offset, "timeout": _POLL_TIMEOUT, "allowed_updates": ["message"]},
                timeout=_POLL_TIMEOUT + 5,
            )
            if not isinstance(updates, list) or not updates:
                continue

            for update in updates:
                if not isinstance(update, dict):
                    continue
                update_id = update.get("update_id", 0)
                offset = max(offset, update_id + 1)

                message = update.get("message")
                if not isinstance(message, dict):
                    continue

                # Validate chat_id — only respond to authorized user
                chat = message.get("chat")
                chat_dict = chat if isinstance(chat, dict) else {}
                msg_chat_id = str(chat_dict.get("id", ""))
                if msg_chat_id != tg.chat_id:
                    logger.debug("telegram_bot_ignored_chat: %s", msg_chat_id)
                    continue

                text_value = message.get("text")
                text = text_value.strip() if isinstance(text_value, str) else ""
                if not text.startswith("/"):
                    continue

                parts = text.split(maxsplit=1)
                cmd_raw = parts[0].lstrip("/").split("@")[0].lower()
                cmd_args = parts[1] if len(parts) > 1 else ""

                handler = _HANDLERS.get(cmd_raw)
                if handler:
                    try:
                        response = handler(cfg, cmd_args)
                    except Exception as exc:
                        logger.exception("telegram_bot_handler_error: cmd=%s", cmd_raw)
                        response = f"Error: {exc}"
                    _send_message(tg.bot_token, tg.chat_id, response)
                else:
                    _send_message(tg.bot_token, tg.chat_id,
                                  f"Unknown command: /{escape_html(cmd_raw)}\nType /help for available commands.")

        except KeyboardInterrupt:
            logger.info("Telegram bot stopped")
            return 0
        except Exception as exc:
            logger.exception("telegram_bot_poll_error: %s", exc)
            time.sleep(5)
