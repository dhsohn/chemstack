from __future__ import annotations

from typing import Any


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def positive_int(value: Any) -> int | None:
    parsed = safe_int(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed
