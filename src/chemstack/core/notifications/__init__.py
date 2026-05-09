"""Shared notification transport and helper functions."""

from .telegram import (
    DEFAULT_TELEGRAM_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_TELEGRAM_MESSAGE_LENGTH,
    TelegramSendResult,
    TelegramTransport,
    build_telegram_transport,
    escape_html,
    html_code,
    load_telegram_config_from_file,
    split_telegram_message,
)

__all__ = [
    "DEFAULT_TELEGRAM_BASE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "MAX_TELEGRAM_MESSAGE_LENGTH",
    "TelegramSendResult",
    "TelegramTransport",
    "build_telegram_transport",
    "escape_html",
    "html_code",
    "load_telegram_config_from_file",
    "split_telegram_message",
]
