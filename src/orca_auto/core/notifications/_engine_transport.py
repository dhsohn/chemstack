from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .telegram_format import split_telegram_message
from .telegram_transport import build_telegram_transport


def send_lines(
    cfg: Any,
    lines: list[str],
    *,
    build_transport: Callable[[Any], Any] = build_telegram_transport,
) -> bool:
    transport = build_transport(cfg.telegram)
    chunks = split_telegram_message("\n".join(lines))
    if not chunks:
        return False
    for chunk in chunks:
        result = transport.send_text(chunk)
        if not bool(result.sent or result.skipped):
            return False
    return True


def telegram_line_sender(
    build_transport_getter: Callable[[], Callable[[Any], Any]],
) -> Callable[[Any, list[str]], bool]:
    def send(cfg: Any, lines: list[str]) -> bool:
        return send_lines(cfg, lines, build_transport=build_transport_getter())

    return send
