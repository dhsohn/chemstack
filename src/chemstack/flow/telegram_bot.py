"""Telegram bot for unified chem_flow activity control."""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.activity_view import activity_display_fields, count_global_active_simulations, queue_list_display_rows
from chemstack.core.app_ids import (
    CHEMSTACK_REPO_ROOT_ENV_VAR,
    LEGACY_ORCA_REPO_ROOT_ENV_VAR,
)
from chemstack.core.config import TelegramConfig
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.core.notifications.telegram import urlopen_with_ipv4_fallback
import yaml

from .activity import _discover_sibling_config, _discover_workflow_root
from .operations import cancel_activity, clear_activities, list_activities

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


def _telegram_from_config_path(config_path: str | None) -> TelegramConfig:
    config_text = str(config_path or "").strip()
    if not config_text:
        return TelegramConfig()

    try:
        path = Path(config_text).expanduser().resolve()
    except OSError:
        return TelegramConfig()
    if not path.exists():
        return TelegramConfig()

    try:
        with path.open("r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
    except Exception:
        return TelegramConfig()
    if not isinstance(parsed, dict):
        return TelegramConfig()

    telegram_raw = parsed.get("telegram")
    if not isinstance(telegram_raw, dict):
        return TelegramConfig()

    return TelegramConfig(
        bot_token=str(telegram_raw.get("bot_token", "")).strip(),
        chat_id=str(telegram_raw.get("chat_id", "")).strip(),
    )


def settings_from_config(config_path: str | None = None) -> TelegramBotSettings:
    shared_config = _discover_sibling_config(config_path, app_name="chemstack")
    telegram = _telegram_from_config_path(shared_config)
    if not telegram.enabled:
        telegram = TelegramConfig(
            bot_token=os.getenv("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("CHEM_FLOW_TELEGRAM_CHAT_ID", "").strip(),
        )
    workflow_root = shared_workflow_root_from_config(shared_config) or _discover_workflow_root(None)
    return TelegramBotSettings(
        telegram=telegram,
        workflow_root=workflow_root,
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
        with urlopen_with_ipv4_fallback(request, timeout=timeout) as response:
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


def _message_chunks(text: str, *, limit: int = _MAX_MESSAGE_LENGTH) -> list[str]:
    if limit <= 0:
        raise ValueError("message chunk limit must be positive")

    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(line) > limit:
            if current:
                chunks.append(current.rstrip("\n"))
                current = ""
            remaining = line
            while remaining:
                piece = remaining[:limit]
                chunks.append(piece.rstrip("\n"))
                remaining = remaining[len(piece) :]
            continue
        if current and len(current) + len(line) > limit:
            chunks.append(current.rstrip("\n"))
            current = line
            continue
        current += line

    if current:
        chunks.append(current.rstrip("\n"))
    return [chunk for chunk in chunks if chunk]


def _send_response(
    token: str,
    chat_id: str,
    text: str,
    *,
    parse_mode: str | None = "HTML",
    limit: int = _MAX_MESSAGE_LENGTH,
) -> bool:
    sent_any = False
    for chunk in _message_chunks(text, limit=limit):
        if _send_message(token, chat_id, chunk, parse_mode=parse_mode):
            sent_any = True
            continue
        if parse_mode and _send_message(token, chat_id, chunk, parse_mode=None):
            sent_any = True
            continue
        return False
    return sent_any


def _activity_payload(settings: TelegramBotSettings) -> dict[str, Any]:
    return list_activities(
        workflow_root=settings.workflow_root,
        crest_auto_config=settings.crest_auto_config,
        xtb_auto_config=settings.xtb_auto_config,
        orca_auto_config=settings.orca_auto_config,
        orca_auto_repo_root=settings.orca_auto_repo_root,
    )


def _format_activity_rows(rows: list[tuple[int, dict[str, Any]]], *, limit: int | None = None) -> str:
    lines: list[str] = []
    visible_rows = rows if limit is None or limit <= 0 else rows[:limit]
    for indent, item in visible_rows:
        indent_prefix = "\u00A0\u00A0" * max(0, int(indent))
        activity_id = escape_html(str(item.get("activity_id", "-")).strip() or "-")
        kind = escape_html(str(item.get("kind", "-")).strip() or "-")
        engine = escape_html(str(item.get("engine", "-")).strip() or "-")
        status = escape_html(str(item.get("status", "-")).strip() or "-")
        label = escape_html(str(item.get("label", "-")).strip() or "-")
        source = escape_html(str(item.get("source", "-")).strip() or "-")
        details = "".join(
            f" {escape_html(key)}=<code>{escape_html(value)}</code>"
            for key, value in activity_display_fields(item)
        )
        lines.append(
            f"{indent_prefix}- <code>{activity_id}</code>"
            f" kind=<code>{kind}</code>"
            f" engine=<code>{engine}</code>"
            f" status=<code>{status}</code>"
            f" label=<code>{label}</code>"
            f" source=<code>{source}</code>"
            f"{details}"
        )
    if limit is not None and limit > 0 and len(rows) > limit:
        lines.append(f"... and {len(rows) - limit} more")
    return "\n".join(lines)


def _activity_counter_config_path(
    payload: dict[str, Any],
    *,
    settings: TelegramBotSettings,
) -> str | None:
    sources = payload.get("sources")
    if isinstance(sources, dict):
        for key in ("orca_auto_config", "crest_auto_config", "xtb_auto_config"):
            source_text = str(sources.get(key, "")).strip()
            if source_text:
                return source_text
    for value in (settings.orca_auto_config, settings.crest_auto_config, settings.xtb_auto_config):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _handle_list(settings: TelegramBotSettings, args: str) -> str:
    action = args.strip().lower()
    if action == "clear":
        payload = clear_activities(
            workflow_root=settings.workflow_root,
            crest_auto_config=settings.crest_auto_config,
            xtb_auto_config=settings.xtb_auto_config,
            orca_auto_config=settings.orca_auto_config,
            orca_auto_repo_root=settings.orca_auto_repo_root,
        )
        total_cleared = int(payload.get("total_cleared", 0) or 0)
        if total_cleared <= 0:
            return "Nothing to clear."

        lines = [
            f"\u2705 Cleared <code>{total_cleared}</code> completed/failed/cancelled entries."
        ]
        cleared = payload.get("cleared")
        if isinstance(cleared, dict):
            labels = (
                ("workflows", "workflows"),
                ("xtb_queue_entries", "xTB queue entries"),
                ("crest_queue_entries", "CREST queue entries"),
                ("orca_queue_entries", "ORCA queue entries"),
                ("orca_run_states", "ORCA run states"),
            )
            for key, label in labels:
                count = int(cleared.get(key, 0) or 0)
                if count > 0:
                    lines.append(f"{escape_html(label)}: <code>{count}</code>")
        return "\n".join(lines)

    payload = _activity_payload(settings)
    all_rows = list(payload.get("activities", []))
    rows = list(all_rows)

    filter_status = action
    if filter_status:
        rows = [item for item in rows if str(item.get("status", "")).strip().lower() == filter_status]

    if not rows:
        return "No activities found."

    header = (
        f"<b>active_simulations</b>: <code>{count_global_active_simulations(all_rows, config_path=_activity_counter_config_path(payload, settings=settings))}</code>"
    )
    display_rows = queue_list_display_rows(
        all_items=all_rows,
        visible_items=rows,
        show_workflow_context=True,
    )
    return header + "\n\n" + _format_activity_rows(display_rows)


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
        "/list clear — Remove completed/failed/cancelled entries\n"
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
        logger.error(
            "Telegram is not configured. Set telegram.bot_token/chat_id in chemstack.yaml "
            "or CHEM_FLOW_TELEGRAM_BOT_TOKEN and CHEM_FLOW_TELEGRAM_CHAT_ID."
        )
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
                _send_response(resolved.telegram.bot_token, resolved.telegram.chat_id, response)
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
    "settings_from_config",
    "settings_from_env",
]
