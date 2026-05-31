from __future__ import annotations

from typing import Any

from chemstack.core.utils.coercion import normalize_text as _normalize_text


def normalize_index_text(value: Any) -> str:
    return _normalize_text(value, none="None")


__all__ = ["normalize_index_text"]
