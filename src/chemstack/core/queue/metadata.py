from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any


def mapping_metadata(entry: Mapping[str, Any] | None) -> dict[str, Any]:
    metadata = (entry or {}).get("metadata")
    return dict(metadata) if isinstance(metadata, dict) else {}


def mapping_metadata_value(entry: Mapping[str, Any] | None, key: str) -> Any:
    return mapping_metadata(entry).get(key)


def entry_metadata_value(entry: Any, key: str, default: Any = "") -> Any:
    metadata = getattr(entry, "metadata", {})
    getter = getattr(metadata, "get", None)
    if getter is None:
        return default
    return getter(key, default)


def entry_metadata_text(entry: Any, key: str, default: Any = "") -> str:
    return str(entry_metadata_value(entry, key, default)).strip()


def entry_metadata_resolved_path(entry: Any, key: str, default: Any = "") -> Path:
    return Path(str(entry_metadata_value(entry, key, default))).expanduser().resolve()


def entry_metadata_dict(entry: Any, key: str) -> dict[str, Any]:
    payload = entry_metadata_value(entry, key, {})
    return dict(payload) if isinstance(payload, dict) else {}


__all__ = [
    "entry_metadata_dict",
    "entry_metadata_resolved_path",
    "entry_metadata_text",
    "entry_metadata_value",
    "mapping_metadata",
    "mapping_metadata_value",
]
