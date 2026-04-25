from __future__ import annotations

import socket
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

from ..config.schema import TelegramConfig

DEFAULT_TELEGRAM_BASE_URL = "https://api.telegram.org"
DEFAULT_TIMEOUT_SECONDS = 10.0


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


@contextmanager
def _force_ipv4_resolution(hostname: str):
    target = str(hostname).strip().lower()
    original_getaddrinfo = socket.getaddrinfo

    def _ipv4_only_getaddrinfo(host: str, port: Any, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0):
        results = original_getaddrinfo(host, port, family, type, proto, flags)
        if str(host).strip().lower() != target:
            return results
        filtered = [item for item in results if item[0] == socket.AF_INET]
        return filtered or results

    socket.getaddrinfo = _ipv4_only_getaddrinfo
    try:
        yield
    finally:
        socket.getaddrinfo = original_getaddrinfo


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
    config: TelegramConfig
    timeout: float = DEFAULT_TIMEOUT_SECONDS
    base_url: str = DEFAULT_TELEGRAM_BASE_URL

    @property
    def enabled(self) -> bool:
        return self.config.enabled

    def _send_message_url(self, token: str) -> str:
        return f"{self.base_url.rstrip('/')}/bot{token}/sendMessage"

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

        token = str(self.config.bot_token).strip()
        resolved_chat_id = str(chat_id or self.config.chat_id).strip()
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

        request = Request(
            self._send_message_url(token),
            data=urlencode(payload).encode("utf-8"),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        try:
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
        except HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            return TelegramSendResult(
                sent=False,
                skipped=False,
                status_code=getattr(exc, "code", None),
                response_text=body,
                error=f"telegram_http_error:{exc}",
                payload=payload,
            )
        except URLError as exc:
            return TelegramSendResult(
                sent=False,
                skipped=False,
                error=f"telegram_url_error:{exc}",
                payload=payload,
            )


def build_telegram_transport(
    config: TelegramConfig,
    *,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    base_url: str = DEFAULT_TELEGRAM_BASE_URL,
) -> TelegramTransport:
    return TelegramTransport(config=config, timeout=timeout, base_url=base_url)
