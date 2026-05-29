"""Shared notification transport and helper functions."""

from .telegram import (
    DEFAULT_TELEGRAM_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_TELEGRAM_MESSAGE_LENGTH,
    TelegramApiClient,
    TelegramSendResult,
    TelegramTransport,
    build_telegram_transport,
    escape_html,
    html_code,
    load_telegram_config_from_file,
    log_telegram_send_failure,
    send_preformatted_telegram_message,
    send_telegram_message,
    split_telegram_message,
    telegram_send_result_ok,
)

__all__ = [
    "DEFAULT_TELEGRAM_BASE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_TELEGRAM_MESSAGE_LENGTH",
    "TelegramApiClient",
    "TelegramSendResult",
    "TelegramTransport",
    "build_telegram_transport",
    "escape_html",
    "html_code",
    "load_telegram_config_from_file",
    "log_telegram_send_failure",
    "send_preformatted_telegram_message",
    "send_telegram_message",
    "split_telegram_message",
    "telegram_send_result_ok",
]
