from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

TELEGRAM_API_BASE = "https://api.telegram.org"
MAX_MESSAGE_LENGTH = 3500
_TOKEN_PLACEHOLDER = "***"


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str
    timeout_sec: int = 5
    retry_count: int = 2
    retry_backoff_sec: float = 1.0
    retry_jitter_sec: float = 0.3


@dataclass(frozen=True)
class SendResult:
    success: bool
    status_code: int
    error: Optional[str] = None
    retry_after: Optional[float] = None


def _sanitize_token_from_message(msg: str, token: str) -> str:
    if token:
        return msg.replace(token, _TOKEN_PLACEHOLDER)
    return msg


def _truncate_text(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> str:
    if len(text) <= max_length:
        return text
    suffix = "\n... [truncated]"
    return text[: max_length - len(suffix)] + suffix


def send_message(config: TelegramConfig, text: str) -> SendResult:
    text = _truncate_text(text)
    url = f"{TELEGRAM_API_BASE}/bot{config.bot_token}/sendMessage"
    payload = json.dumps({
        "chat_id": config.chat_id,
        "text": text,
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=config.timeout_sec) as resp:
            return SendResult(success=True, status_code=resp.status)
    except urllib.error.HTTPError as exc:
        retry_after = None
        if exc.code == 429:
            try:
                body = json.loads(exc.read().decode("utf-8", errors="replace"))
                retry_after = body.get("parameters", {}).get("retry_after")
            except Exception:
                pass
        error_msg = _sanitize_token_from_message(str(exc), config.bot_token)
        return SendResult(
            success=False,
            status_code=exc.code,
            error=error_msg,
            retry_after=float(retry_after) if retry_after else None,
        )
    except (urllib.error.URLError, OSError, TimeoutError) as exc:
        error_msg = _sanitize_token_from_message(str(exc), config.bot_token)
        return SendResult(success=False, status_code=0, error=error_msg)


def _should_retry(result: SendResult) -> bool:
    if result.success:
        return False
    if result.status_code == 429:
        return True
    if result.status_code == 0:
        return True
    if 500 <= result.status_code < 600:
        return True
    return False


def _compute_delay(
    config: TelegramConfig,
    last_result: SendResult,
    attempt_idx: int,
) -> float:
    if last_result.retry_after is not None and last_result.retry_after > 0:
        return last_result.retry_after
    base = config.retry_backoff_sec * (2 ** attempt_idx)
    jitter = random.uniform(0, config.retry_jitter_sec)
    return base + jitter


def send_with_retry(config: TelegramConfig, text: str) -> SendResult:
    last_result = send_message(config, text)
    if last_result.success:
        return last_result

    for attempt_idx in range(config.retry_count):
        if not _should_retry(last_result):
            return last_result

        delay = _compute_delay(config, last_result, attempt_idx)
        time.sleep(delay)

        last_result = send_message(config, text)
        if last_result.success:
            return last_result

    return last_result
