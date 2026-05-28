from __future__ import annotations

import socket
import time
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

import yaml

from chemstack.core.config.schema import TelegramConfig, telegram_config_from_mapping
from chemstack.core.utils.coercion import (
    normalize_text as _normalize_text,
)

DEFAULT_TELEGRAM_BASE_URL = "https://api.telegram.org"
DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_MAX_ATTEMPTS = 2
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5
MAX_TELEGRAM_MESSAGE_LENGTH = 4096
LOGGER = logging.getLogger(__name__)


class TelegramConfigLike(Protocol):
    @property
    def bot_token(self) -> str: ...

    @property
    def chat_id(self) -> str: ...

    @property
    def timeout_seconds(self) -> float: ...

    @property
    def max_attempts(self) -> int: ...

    @property
    def retry_backoff_seconds(self) -> float: ...

    @property
    def enabled(self) -> bool: ...


def _iter_exception_chain(exc: BaseException) -> list[BaseException]:
    seen: set[int] = set()
    stack: list[BaseException] = [exc]
    rows: list[BaseException] = []
    while stack:
        current = stack.pop()
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        rows.append(current)
        reason = getattr(current, "reason", None)
        if isinstance(reason, BaseException):
            stack.append(reason)
        cause = getattr(current, "__cause__", None)
        if isinstance(cause, BaseException):
            stack.append(cause)
        context = getattr(current, "__context__", None)
        if isinstance(context, BaseException):
            stack.append(context)
    return rows


def _is_network_unreachable_error(exc: BaseException) -> bool:
    for current in _iter_exception_chain(exc):
        errno = getattr(current, "errno", None)
        if errno in {101, 113}:
            return True
        text = str(current).strip().lower()
        if "network is unreachable" in text or "no route to host" in text:
            return True
    return False


def _is_timeout_error(exc: BaseException) -> bool:
    for current in _iter_exception_chain(exc):
        if isinstance(current, (TimeoutError, socket.timeout)):
            return True
        text = str(current).strip().lower()
        if "timed out" in text:
            return True
    return False


def _is_temporary_dns_error(exc: BaseException) -> bool:
    for current in _iter_exception_chain(exc):
        if isinstance(current, socket.gaierror) and current.errno == getattr(
            socket, "EAI_AGAIN", None
        ):
            return True
        text = str(current).strip().lower()
        if "temporary failure in name resolution" in text or "temporary dns failure" in text:
            return True
    return False


def _should_retry_url_error(exc: BaseException) -> bool:
    return (
        _is_timeout_error(exc) or _is_network_unreachable_error(exc) or _is_temporary_dns_error(exc)
    )


def _is_retryable_http_status(status_code: int | None) -> bool:
    return status_code in {429, 500, 502, 503, 504}


def escape_html(value: Any) -> str:
    text = _normalize_text(value)
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def html_code(value: Any) -> str:
    return f"<code>{escape_html(value)}</code>"


def _split_long_segment(text: str, *, limit: int) -> list[str]:
    pieces: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            pieces.append(remaining)
            break
        split_at = remaining.rfind(" ", 0, limit + 1)
        if split_at <= 0:
            split_at = limit
        else:
            split_at += 1
        pieces.append(remaining[:split_at])
        remaining = remaining[split_at:]
    return pieces


def _append_telegram_line(chunks: list[str], current: str, line: str, *, limit: int) -> str:
    if current and len(current) + len(line) > limit:
        chunk = current.strip()
        if chunk:
            chunks.append(chunk)
        return line
    return current + line


def _flush_telegram_chunk(chunks: list[str], current: str) -> str:
    chunk = current.strip()
    if chunk:
        chunks.append(chunk)
    return ""


def split_telegram_message(
    text: str,
    *,
    limit: int = MAX_TELEGRAM_MESSAGE_LENGTH,
) -> list[str]:
    """Split a Telegram message without cutting across normal line boundaries."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    message = str(text).strip()
    if not message:
        return []
    if len(message) <= limit:
        return [message]

    chunks: list[str] = []
    current = ""

    for line in message.splitlines(keepends=True):
        if len(line) > limit:
            current = _flush_telegram_chunk(chunks, current)
            for piece in _split_long_segment(line, limit=limit):
                _flush_telegram_chunk(chunks, piece)
            continue
        current = _append_telegram_line(chunks, current, line, limit=limit)

    _flush_telegram_chunk(chunks, current)
    return chunks


def load_telegram_config_from_file(config_path: str | Path | None) -> TelegramConfig:
    config_text = _normalize_text(config_path)
    if not config_text:
        return TelegramConfig()

    try:
        path = Path(config_text).expanduser().resolve()
    except OSError:
        return TelegramConfig()
    if not path.exists():
        return TelegramConfig()

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        LOGGER.debug("failed to load telegram config file: %s", path, exc_info=True)
        return TelegramConfig()
    if not isinstance(raw, dict):
        return TelegramConfig()

    return telegram_config_from_mapping(raw.get("telegram"))


@contextmanager
def _force_ipv4_resolution(hostname: str):
    target = str(hostname).strip().lower()
    original_getaddrinfo = socket.getaddrinfo

    def _ipv4_only_getaddrinfo(
        host: bytes | str | None,
        port: bytes | str | int | None,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ):
        results = original_getaddrinfo(host, port, family, type, proto, flags)
        if str(host).strip().lower() != target:
            return results
        filtered = [item for item in results if item[0] == socket.AF_INET]
        return filtered or results

    setattr(socket, "getaddrinfo", _ipv4_only_getaddrinfo)
    try:
        yield
    finally:
        setattr(socket, "getaddrinfo", original_getaddrinfo)


def urlopen_with_ipv4_fallback(request: Request, *, timeout: float):
    try:
        return urlopen(request, timeout=timeout)
    except BaseException as exc:
        if not _is_network_unreachable_error(exc):
            raise
        hostname = urlsplit(getattr(request, "full_url", "")).hostname or ""
        if not hostname:
            raise
        with _force_ipv4_resolution(hostname):
            return urlopen(request, timeout=timeout)


@dataclass(frozen=True)
class TelegramSendResult:
    sent: bool
    skipped: bool = False
    status_code: int | None = None
    response_text: str = ""
    error: str = ""
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TelegramTransport:
    config: TelegramConfigLike
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS
    base_url: str = DEFAULT_TELEGRAM_BASE_URL

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def _send_message_url(self, token: str) -> str:
        return f"{self.base_url.rstrip('/')}/bot{token}/sendMessage"

    def _send_text_payload(
        self,
        message: str,
        *,
        chat_id: str | None = None,
        disable_web_page_preview: bool = True,
        silent: bool = False,
        parse_mode: str | None = None,
    ) -> tuple[str, dict[str, Any]] | TelegramSendResult:
        resolved_chat_id = str(chat_id or self.config.chat_id).strip()
        token = str(self.config.bot_token).strip()
        if not token or not resolved_chat_id:
            return TelegramSendResult(sent=False, skipped=True, error="telegram_config_incomplete")

        payload: dict[str, Any] = {
            "chat_id": resolved_chat_id,
            "text": message,
            "disable_web_page_preview": "true" if disable_web_page_preview else "false",
            "disable_notification": "true" if silent else "false",
        }
        if parse_mode:
            payload["parse_mode"] = str(parse_mode).strip()

        return token, payload

    def _send_text_once(self, request: Request, payload: dict[str, Any]) -> TelegramSendResult:
        with urlopen_with_ipv4_fallback(request, timeout=float(self.timeout)) as response:
            body = response.read().decode("utf-8", errors="replace")
            status_code = int(getattr(response, "status", response.getcode()))
            return TelegramSendResult(
                sent=200 <= status_code < 300,
                skipped=False,
                status_code=status_code,
                response_text=body,
                error="" if 200 <= status_code < 300 else f"telegram_http_{status_code}",
                payload=payload,
            )

    def _send_text_attempt(
        self, request: Request, payload: dict[str, Any]
    ) -> tuple[TelegramSendResult, bool]:
        try:
            result = self._send_text_once(request, payload)
            return result, False
        except HTTPError as exc:
            result = TelegramSendResult(
                sent=False,
                skipped=False,
                status_code=getattr(exc, "code", None),
                response_text=_read_http_error_body(exc),
                error=f"telegram_http_error:{exc}",
                payload=payload,
            )
            return result, _is_retryable_http_status(result.status_code)
        except URLError as exc:
            return TelegramSendResult(
                sent=False,
                skipped=False,
                error=f"telegram_url_error:{exc}",
                payload=payload,
            ), _should_retry_url_error(exc)
        except Exception as exc:
            return TelegramSendResult(
                sent=False,
                skipped=False,
                error=f"telegram_error:{exc}",
                payload=payload,
            ), _is_timeout_error(exc)

    def _retry_send_text(self, request: Request, payload: dict[str, Any]) -> TelegramSendResult:
        attempts = max(1, int(self.max_attempts))
        for attempt_index in range(1, attempts + 1):
            result, retryable = self._send_text_attempt(request, payload)
            if result.sent or attempt_index >= attempts or not retryable:
                return result
            _sleep_before_retry(self.retry_backoff_seconds)
        return TelegramSendResult(
            sent=False, skipped=False, error="telegram_retry_exhausted", payload=payload
        )

    def send_text(
        self,
        text: str,
        *,
        chat_id: str | None = None,
        disable_web_page_preview: bool = True,
        silent: bool = False,
        parse_mode: str | None = None,
    ) -> TelegramSendResult:
        message = str(text).strip()
        if not self.enabled:
            return TelegramSendResult(sent=False, skipped=True, error="telegram_disabled")
        if not message:
            return TelegramSendResult(sent=False, skipped=True, error="empty_message")

        payload_result = self._send_text_payload(
            message,
            chat_id=chat_id,
            disable_web_page_preview=disable_web_page_preview,
            silent=silent,
            parse_mode=parse_mode,
        )
        if isinstance(payload_result, TelegramSendResult):
            return payload_result

        token, payload = payload_result
        request = Request(
            self._send_message_url(token),
            data=urlencode(payload).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        return self._retry_send_text(request, payload)


def _read_http_error_body(exc: HTTPError) -> str:
    try:
        return exc.read().decode("utf-8", errors="replace")
    except Exception:
        LOGGER.debug("failed to read Telegram HTTP error body", exc_info=True)
        return ""


def _sleep_before_retry(backoff_seconds: float) -> None:
    delay = max(0.0, float(backoff_seconds))
    if delay > 0:
        time.sleep(delay)


def build_telegram_transport(
    config: TelegramConfigLike,
    *,
    timeout: float | None = None,
    max_attempts: int | None = None,
    retry_backoff_seconds: float | None = None,
    base_url: str = DEFAULT_TELEGRAM_BASE_URL,
) -> TelegramTransport:
    resolved_timeout = float(
        timeout
        if timeout is not None
        else getattr(config, "timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    )
    resolved_attempts = max(
        1,
        int(
            max_attempts
            if max_attempts is not None
            else getattr(config, "max_attempts", DEFAULT_MAX_ATTEMPTS)
        ),
    )
    resolved_backoff = max(
        0.0,
        float(
            retry_backoff_seconds
            if retry_backoff_seconds is not None
            else getattr(config, "retry_backoff_seconds", DEFAULT_RETRY_BACKOFF_SECONDS)
        ),
    )
    return TelegramTransport(
        config=config,
        timeout=resolved_timeout,
        max_attempts=resolved_attempts,
        retry_backoff_seconds=resolved_backoff,
        base_url=base_url,
    )
