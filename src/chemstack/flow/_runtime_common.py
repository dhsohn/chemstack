from __future__ import annotations

from typing import Any

from chemstack.core.utils.coercion import (
    normalize_text as normalize_text,
    safe_int as _shared_safe_int,
)


def safe_int(value: Any) -> int | None:
    return _shared_safe_int(value, default=None)


def positive_int(value: Any) -> int | None:
    parsed = safe_int(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed
