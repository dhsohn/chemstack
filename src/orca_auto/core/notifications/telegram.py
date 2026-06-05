from __future__ import annotations

import logging
from urllib.request import Request, urlopen

from .telegram_api import TelegramApiClient
from .telegram_config import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_RETRY_BACKOFF_SECONDS,
    DEFAULT_TELEGRAM_BASE_URL,
    DEFAULT_TIMEOUT_SECONDS,
    TelegramConfigLike,
    load_telegram_config_from_file,
)
from .telegram_format import (
    MAX_TELEGRAM_MESSAGE_LENGTH,
    _append_telegram_line,
    _flush_telegram_chunk,
    _split_long_segment,
    escape_html,
    html_code,
    split_telegram_message,
)
from .telegram_network import (
    _force_ipv4_resolution,
    _is_network_unreachable_error,
    _is_retryable_http_status,
    _is_temporary_dns_error,
    _is_timeout_error,
    _iter_exception_chain,
    _read_http_error_body,
    _should_retry_url_error,
)
from .telegram_network import (
    urlopen_with_ipv4_fallback as _network_urlopen_with_ipv4_fallback,
)
from .telegram_transport import (
    TelegramSendResult,
    TelegramTransport,
    TelegramTransportFactory,
    _send_telegram_chunks,
    _sleep_before_retry,
    _telegram_transport_or_none,
    _TelegramChunkSendRequest,
    build_telegram_transport,
    log_telegram_send_failure,
    send_preformatted_telegram_message,
    send_telegram_message,
    telegram_send_result_ok,
)

LOGGER = logging.getLogger(__name__)


def urlopen_with_ipv4_fallback(request: Request, *, timeout: float):
    return _network_urlopen_with_ipv4_fallback(request, timeout=timeout, urlopen_fn=urlopen)

__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_RETRY_BACKOFF_SECONDS",
    "DEFAULT_TELEGRAM_BASE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "LOGGER",
    "MAX_TELEGRAM_MESSAGE_LENGTH",
    "TelegramApiClient",
    "TelegramConfigLike",
    "TelegramSendResult",
    "TelegramTransport",
    "TelegramTransportFactory",
    "_TelegramChunkSendRequest",
    "_append_telegram_line",
    "_flush_telegram_chunk",
    "_force_ipv4_resolution",
    "_is_network_unreachable_error",
    "_is_retryable_http_status",
    "_is_temporary_dns_error",
    "_is_timeout_error",
    "_iter_exception_chain",
    "_read_http_error_body",
    "_send_telegram_chunks",
    "_should_retry_url_error",
    "_sleep_before_retry",
    "_split_long_segment",
    "_telegram_transport_or_none",
    "build_telegram_transport",
    "escape_html",
    "html_code",
    "load_telegram_config_from_file",
    "log_telegram_send_failure",
    "send_preformatted_telegram_message",
    "send_telegram_message",
    "split_telegram_message",
    "telegram_send_result_ok",
    "urlopen",
    "urlopen_with_ipv4_fallback",
]
