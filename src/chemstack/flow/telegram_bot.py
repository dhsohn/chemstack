"""Telegram bot for unified chemstack_flow activity control."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.activity_rendering import queue_clear_lines, queue_list_text_lines
from chemstack.activity_presenter import (
    QueueListPresentationDeps,
    QueueListPresentationRequest,
    queue_list_text_presentation,
)
from chemstack.core.activity_icons import activity_status_icon
from chemstack.activity_view import (
    activity_counter_config_path,
    count_global_active_simulations,
    queue_list_default_visible_items,
    queue_list_display_rows,
)
from chemstack.core.app_ids import (
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_REPO_ROOT_ENV_VAR,
)
from chemstack.core.config import TelegramConfig
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.core.notifications import (
    MAX_TELEGRAM_MESSAGE_LENGTH,
    TelegramApiClient,
    build_telegram_transport,
    escape_html,
    load_telegram_config_from_file,
    send_preformatted_telegram_message,
    send_telegram_message,
)

from . import _activity_sources
from . import telegram_interactive as _interactive
from .activity import cancel_activity, clear_activities, list_activities
from .telegram_keyboards import (
    _MAX_LIST_CANCEL_BUTTONS as _MAX_LIST_CANCEL_BUTTONS,
    _cancel_confirm_keyboard as _cancel_confirm_keyboard,
    _list_action_keyboard as _list_action_keyboard,
)

logger = logging.getLogger(__name__)

_API_BASE = "https://api.telegram.org/bot{token}"
_POLL_TIMEOUT_SECONDS = 30
_MAX_MESSAGE_LENGTH = MAX_TELEGRAM_MESSAGE_LENGTH


@dataclass(frozen=True)
class TelegramBotSettings:
    telegram: TelegramConfig
    workflow_root: str | None
    crest_config: str | None
    xtb_config: str | None
    orca_config: str | None
    orca_repo_root: str | None

    @property
    def enabled(self) -> bool:
        return self.telegram.enabled


def _status_icon(status: str) -> str:
    return activity_status_icon(status)


def settings_from_env() -> TelegramBotSettings:
    shared_config = _activity_sources.discover_shared_config(None)
    return TelegramBotSettings(
        telegram=TelegramConfig(
            bot_token=os.getenv("CHEMSTACK_FLOW_TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("CHEMSTACK_FLOW_TELEGRAM_CHAT_ID", "").strip(),
        ),
        workflow_root=_activity_sources.discover_workflow_root(None),
        crest_config=shared_config,
        xtb_config=shared_config,
        orca_config=shared_config,
        orca_repo_root=os.getenv(CHEMSTACK_REPO_ROOT_ENV_VAR, "").strip() or None,
    )


def _telegram_from_config_path(config_path: str | None) -> TelegramConfig:
    config_text = str(config_path or "").strip()
    if config_text:
        try:
            Path(config_text).expanduser().resolve()
        except OSError:
            return TelegramConfig()
    return load_telegram_config_from_file(config_path)


def settings_from_config(config_path: str | None = None) -> TelegramBotSettings:
    shared_config = _activity_sources.discover_shared_config(config_path)
    telegram = _telegram_from_config_path(shared_config)
    if not telegram.enabled:
        telegram = TelegramConfig(
            bot_token=os.getenv("CHEMSTACK_FLOW_TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("CHEMSTACK_FLOW_TELEGRAM_CHAT_ID", "").strip(),
        )
    workflow_root = shared_workflow_root_from_config(
        shared_config
    ) or _activity_sources.discover_workflow_root(None)
    return TelegramBotSettings(
        telegram=telegram,
        workflow_root=workflow_root,
        crest_config=shared_config,
        xtb_config=shared_config,
        orca_config=shared_config,
        orca_repo_root=os.getenv(CHEMSTACK_REPO_ROOT_ENV_VAR, "").strip() or None,
    )


def _api_call(
    token: str,
    method: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = _POLL_TIMEOUT_SECONDS + 5,
) -> Any | None:
    client = TelegramApiClient(
        token=token,
        timeout=timeout,
        base_url=_API_BASE.removesuffix("/bot{token}"),
        logger=logger,
    )
    return client.api_call(method, payload, timeout=timeout)


def _send_response(
    config: TelegramConfig,
    text: str,
    *,
    parse_mode: str | None = "HTML",
    limit: int = _MAX_MESSAGE_LENGTH,
) -> bool:
    return send_telegram_message(
        config,
        text,
        parse_mode=parse_mode,
        limit=limit,
        logger=logger,
        transport_factory=build_telegram_transport,
    )


def _send_preformatted_response(
    config: TelegramConfig,
    text: str,
    *,
    limit: int = _MAX_MESSAGE_LENGTH,
) -> bool:
    return send_preformatted_telegram_message(
        config,
        text,
        limit=limit,
        logger=logger,
        transport_factory=build_telegram_transport,
    )


def _activity_payload(
    settings: TelegramBotSettings,
    *,
    child_job_engines: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    return list_activities(
        workflow_root=settings.workflow_root,
        crest_config=settings.crest_config,
        xtb_config=settings.xtb_config,
        orca_config=settings.orca_config,
        child_job_engines=child_job_engines,
    )


def _activity_counter_config_path(
    payload: dict[str, Any],
    *,
    settings: TelegramBotSettings,
) -> str | None:
    return activity_counter_config_path(
        payload,
        config_hints=(settings.orca_config, settings.crest_config, settings.xtb_config),
    )


def _handle_list(settings: TelegramBotSettings, args: str) -> str:
    action = args.strip().lower()
    if action == "clear":
        payload = clear_activities(
            workflow_root=settings.workflow_root,
            crest_config=settings.crest_config,
            xtb_config=settings.xtb_config,
            orca_config=settings.orca_config,
        )
        return "\n".join(queue_clear_lines(payload))

    filter_status = action
    payload = _activity_payload(
        settings,
        child_job_engines=() if not filter_status else None,
    )
    all_rows = list(payload.get("activities", []))
    rows = list(all_rows)
    if filter_status:
        rows = [
            item for item in rows if str(item.get("status", "")).strip().lower() == filter_status
        ]
    presentation = queue_list_text_presentation(
        payload,
        request=QueueListPresentationRequest(
            visible_items=rows,
            config_hints=(settings.orca_config, settings.crest_config, settings.xtb_config),
            default_visible_items=not filter_status,
            show_workflow_context=True,
            visible_workflow_child_engines=("orca",) if not filter_status else None,
            include_id=False,
        ),
        deps=QueueListPresentationDeps(
            activity_counter_config_path=activity_counter_config_path,
            count_global_active_simulations=count_global_active_simulations,
            queue_list_default_visible_items=queue_list_default_visible_items,
            queue_list_display_rows=queue_list_display_rows,
            queue_list_text_lines=queue_list_text_lines,
        ),
    )
    return "\n".join(presentation.lines)


def _handle_cancel(settings: TelegramBotSettings, args: str) -> str:
    target = args.strip()
    if not target:
        return "Usage: /cancel &lt;target&gt;"
    try:
        payload = cancel_activity(
            target=target,
            workflow_root=settings.workflow_root,
            crest_config=settings.crest_config,
            xtb_config=settings.xtb_config,
            orca_config=settings.orca_config,
            orca_repo_root=settings.orca_repo_root,
        )
    except (LookupError, ValueError) as exc:
        return escape_html(str(exc))

    label = escape_html(str(payload.get("label", payload.get("activity_id", target))))
    status = escape_html(str(payload.get("status", "unknown")))
    return f"{_status_icon(status)} <b>{label}</b>\nstatus: <code>{status}</code>"


def _handle_help(_settings: TelegramBotSettings, _args: str) -> str:
    return (
        "<b>chemstack_flow bot commands</b>\n\n"
        "/list — Show unified activities\n"
        "/list clear — Remove completed/failed/cancelled entries\n"
        "/list running — Running activities only\n"
        "/list failed — Failed activities only\n"
        "/cancel &lt;target&gt; — Cancel a workflow or queued job (asks to confirm)\n"
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


def _send_message(
    settings: TelegramBotSettings,
    text: str,
    *,
    parse_mode: str | None = "HTML",
    reply_markup: dict[str, Any] | None = None,
) -> Any | None:
    payload: dict[str, Any] = {"chat_id": settings.telegram.chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    return _api_call(settings.telegram.bot_token, "sendMessage", payload)


def _edit_message(
    settings: TelegramBotSettings,
    *,
    chat_id: Any,
    message_id: Any,
    text: str,
) -> Any | None:
    return _api_call(
        settings.telegram.bot_token,
        "editMessageText",
        {"chat_id": chat_id, "message_id": message_id, "text": text, "parse_mode": "HTML"},
    )


def _answer_callback(settings: TelegramBotSettings, callback_id: Any) -> Any | None:
    return _api_call(
        settings.telegram.bot_token,
        "answerCallbackQuery",
        {"callback_query_id": callback_id},
    )


def _send_cancel_confirmation(settings: TelegramBotSettings, target: str) -> None:
    _interactive.send_cancel_confirmation(
        settings,
        target,
        send_response=_send_response,
        send_message=_send_message,
        handle_cancel=_handle_cancel,
    )


def _callback_response(settings: TelegramBotSettings, data: str) -> str:
    return _interactive.callback_response(settings, data, handle_cancel=_handle_cancel)


def _active_cancel_targets(settings: TelegramBotSettings) -> list[dict[str, Any]]:
    return _interactive.active_cancel_targets(settings, activity_payload=_activity_payload)


def _send_list_actions(settings: TelegramBotSettings) -> None:
    _interactive.send_list_actions(
        settings,
        active_cancel_targets_fn=_active_cancel_targets,
        send_message=_send_message,
        logger=logger,
    )


def _send_list_response(settings: TelegramBotSettings) -> None:
    _interactive.send_list_response(
        settings,
        send_preformatted_response=_send_preformatted_response,
        handle_list=_handle_list,
        send_list_actions_fn=_send_list_actions,
    )


def _poll_updates(token: str, offset: int) -> list[Any]:
    updates = _api_call(
        token,
        "getUpdates",
        {
            "offset": offset,
            "timeout": _POLL_TIMEOUT_SECONDS,
            "allowed_updates": ["message", "callback_query"],
        },
        timeout=_POLL_TIMEOUT_SECONDS + 5,
    )
    return updates if isinstance(updates, list) else []


def _message_from_update(update: Any, *, chat_id: str) -> tuple[int | None, dict[str, Any] | None]:
    if not isinstance(update, dict):
        return None, None

    update_id = int(update.get("update_id", 0) or 0)
    message = update.get("message")
    if not isinstance(message, dict):
        return update_id, None

    chat = message.get("chat")
    chat_dict = chat if isinstance(chat, dict) else {}
    if str(chat_dict.get("id", "")).strip() != chat_id:
        return update_id, None
    return update_id, message


def _command_from_message(message: dict[str, Any]) -> tuple[str, str] | None:
    text_value = message.get("text")
    text = text_value.strip() if isinstance(text_value, str) else ""
    if not text.startswith("/"):
        return None

    parts = text.split(maxsplit=1)
    command = parts[0].lstrip("/").split("@")[0].lower()
    return command, parts[1] if len(parts) > 1 else ""


def _response_for_command(
    settings: TelegramBotSettings, command: str, args: str
) -> tuple[str, bool]:
    handler = _HANDLERS.get(command)
    if handler is None:
        return (
            f"Unknown command: /{escape_html(command)}\nType /help for available commands.",
            False,
        )

    try:
        return handler(settings, args), command == "list"
    except Exception as exc:
        logger.exception("telegram_bot_handler_error: cmd=%s", command)
        return f"Error: {escape_html(str(exc))}", False


def _send_bot_response(settings: TelegramBotSettings, response: str, *, preformatted: bool) -> None:
    if preformatted:
        _send_preformatted_response(settings.telegram, response)
    else:
        _send_response(settings.telegram, response)


def _clear_finished(settings: TelegramBotSettings) -> None:
    _send_response(settings.telegram, _handle_list(settings, "clear"))


def _dispatch_callback_query(settings: TelegramBotSettings, update: dict[str, Any]) -> int | None:
    return _interactive.dispatch_callback_query(
        settings,
        update,
        answer_callback=_answer_callback,
        send_list_response_fn=_send_list_response,
        send_cancel_confirmation_fn=_send_cancel_confirmation,
        callback_response_fn=_callback_response,
        edit_message=_edit_message,
        send_response=_send_response,
        clear_finished_fn=_clear_finished,
    )


def _dispatch_update(settings: TelegramBotSettings, update: Any) -> int | None:
    if isinstance(update, dict) and isinstance(update.get("callback_query"), dict):
        return _dispatch_callback_query(settings, update)

    update_id, message = _message_from_update(update, chat_id=settings.telegram.chat_id)
    if message is None:
        return update_id

    command_parts = _command_from_message(message)
    if command_parts is None:
        return update_id

    command, args = command_parts
    if command == "cancel":
        # Cancellation is destructive, so confirm via inline buttons instead of
        # acting immediately; the actual cancel runs on the callback.
        _send_cancel_confirmation(settings, args)
        return update_id

    response, preformatted = _response_for_command(settings, command, args)
    _send_bot_response(settings, response, preformatted=preformatted)
    if command == "list" and args.strip().lower() != "clear":
        # Attach per-activity cancel + refresh buttons after the table.
        _send_list_actions(settings)
    return update_id


def run_bot(settings: TelegramBotSettings | None = None) -> int:
    resolved = settings or settings_from_env()
    if not resolved.enabled:
        logger.error(
            "Telegram is not configured. Set telegram.bot_token/chat_id in chemstack.yaml "
            "or CHEMSTACK_FLOW_TELEGRAM_BOT_TOKEN and CHEMSTACK_FLOW_TELEGRAM_CHAT_ID."
        )
        return 1

    _set_bot_commands(resolved.telegram.bot_token)
    logger.info("chemstack_flow Telegram bot started (chat_id=%s)", resolved.telegram.chat_id)

    offset = 0
    while True:
        try:
            for update in _poll_updates(resolved.telegram.bot_token, offset):
                update_id = _dispatch_update(resolved, update)
                if update_id is not None:
                    offset = max(offset, update_id + 1)
        except KeyboardInterrupt:
            logger.info("chemstack_flow Telegram bot stopped")
            return 0
        except Exception as exc:
            logger.exception("telegram_bot_poll_error: %s", exc)
            time.sleep(5)


def main() -> int:
    config_path = str(os.getenv(CHEMSTACK_CONFIG_ENV_VAR, "")).strip() or None
    return int(run_bot(settings_from_config(config_path)))


__all__ = [
    "TelegramBotSettings",
    "escape_html",
    "main",
    "run_bot",
    "settings_from_config",
    "settings_from_env",
]


if __name__ == "__main__":
    raise SystemExit(main())
