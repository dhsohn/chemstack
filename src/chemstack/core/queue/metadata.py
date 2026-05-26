from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def mapping_metadata(entry: Mapping[str, Any] | None) -> dict[str, Any]:
    metadata = (entry or {}).get("metadata")
    return dict(metadata) if isinstance(metadata, dict) else {}


def mapping_metadata_value(entry: Mapping[str, Any] | None, key: str) -> Any:
    return mapping_metadata(entry).get(key)


__all__ = ["mapping_metadata", "mapping_metadata_value"]
