from __future__ import annotations

import sys
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Literal
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chemstack.core.config.schema import TelegramConfig
from chemstack.core.notifications import telegram as telegram_mod


@dataclass
class _FakeResponse:
    body: str
    status: int

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        return False

    def read(self) -> bytes:
        return self.body.encode("utf-8")

    def getcode(self) -> int:
        return self.status


def _make_transport(
    *,
    bot_token: str = "bot-token",
    chat_id: str = "chat-id",
    timeout: float = 2.5,
    base_url: str = "https://example.test/",
) -> telegram_mod.TelegramTransport:
    return telegram_mod.TelegramTransport(
        config=TelegramConfig(bot_token=bot_token, chat_id=chat_id),
        timeout=timeout,
        base_url=base_url,
    )


def test_disabled_transport_skips_without_calling_urlopen(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fake_urlopen(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("urlopen should not be called for disabled transport")

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport(bot_token="", chat_id="chat-id")

    result = transport.send_text("hello")

    assert result == telegram_mod.TelegramSendResult(
        sent=False,
        skipped=True,
        error="telegram_disabled",
    )
    assert called is False


def test_empty_message_skips_without_calling_urlopen(monkeypatch: pytest.MonkeyPatch) -> None:
    called = False

    def fake_urlopen(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("urlopen should not be called for empty messages")

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("   ")

    assert result == telegram_mod.TelegramSendResult(
        sent=False,
        skipped=True,
        error="empty_message",
    )
    assert called is False


def test_incomplete_config_skips_without_calling_urlopen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called = False

    def fake_urlopen(*args, **kwargs):
        nonlocal called
        called = True
        raise AssertionError("urlopen should not be called for incomplete config")

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("hello", chat_id="   ")

    assert result == telegram_mod.TelegramSendResult(
        sent=False,
        skipped=True,
        error="telegram_config_incomplete",
    )
    assert called is False


def test_successful_send_path_uses_urlopen_and_returns_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen = {}

    def fake_urlopen(request, timeout):
        seen["request"] = request
        seen["timeout"] = timeout
        return _FakeResponse(body='{"ok":true}', status=200)

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport(timeout=7.5, base_url="https://api.telegram.org/")

    result = transport.send_text(
        "  hello world  ",
        silent=True,
        parse_mode="MarkdownV2",
    )

    assert result.sent is True
    assert result.skipped is False
    assert result.status_code == 200
    assert result.response_text == '{"ok":true}'
    assert result.error == ""
    assert result.payload == {
        "chat_id": "chat-id",
        "text": "hello world",
        "disable_web_page_preview": "true",
        "disable_notification": "true",
        "parse_mode": "MarkdownV2",
    }

    request = seen["request"]
    assert request.full_url == "https://api.telegram.org/botbot-token/sendMessage"
    assert seen["timeout"] == 7.5
    assert request.data is not None
    parsed = parse_qs(request.data.decode("utf-8"))
    assert parsed == {
        "chat_id": ["chat-id"],
        "text": ["hello world"],
        "disable_web_page_preview": ["true"],
        "disable_notification": ["true"],
        "parse_mode": ["MarkdownV2"],
    }


def test_non_2xx_response_status_sets_http_error_and_preserves_body(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_urlopen(request, timeout):
        return _FakeResponse(body="service unavailable", status=503)

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("hello")

    assert result.sent is False
    assert result.skipped is False
    assert result.status_code == 503
    assert result.response_text == "service unavailable"
    assert result.error == "telegram_http_503"


def test_http_error_handling_reads_error_body(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request, timeout):
        raise HTTPError(
            url=request.full_url,
            code=401,
            msg="Unauthorized",
            hdrs=None,
            fp=BytesIO(b"bad token"),
        )

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("hello")

    assert result.sent is False
    assert result.skipped is False
    assert result.status_code == 401
    assert result.response_text == "bad token"
    assert result.error == "telegram_http_error:HTTP Error 401: Unauthorized"


def test_http_error_handling_leaves_response_text_empty_when_read_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenErrorBody:
        def read(self) -> bytes:
            raise OSError("cannot read body")

        def close(self) -> None:
            pass

    def fake_urlopen(request, timeout):
        raise HTTPError(
            url=request.full_url,
            code=502,
            msg="Bad Gateway",
            hdrs=None,
            fp=_BrokenErrorBody(),
        )

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("hello")

    assert result.sent is False
    assert result.skipped is False
    assert result.status_code == 502
    assert result.response_text == ""
    assert result.error == "telegram_http_error:HTTP Error 502: Bad Gateway"


def test_url_error_handling_returns_url_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(request, timeout):
        raise URLError("temporary DNS failure")

    monkeypatch.setattr(telegram_mod, "urlopen", fake_urlopen)

    transport = _make_transport()

    result = transport.send_text("hello")

    assert result.sent is False
    assert result.skipped is False
    assert result.status_code is None
    assert result.response_text == ""
    assert result.error == "telegram_url_error:<urlopen error temporary DNS failure>"


def test_build_telegram_transport_uses_defaults() -> None:
    config = TelegramConfig(bot_token="bot-token", chat_id="chat-id")

    transport = telegram_mod.build_telegram_transport(config)

    assert transport.config is config
    assert transport.timeout == telegram_mod.DEFAULT_TIMEOUT_SECONDS
    assert transport.base_url == telegram_mod.DEFAULT_TELEGRAM_BASE_URL
