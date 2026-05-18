from __future__ import annotations

from typing import Any


def normalize_queue_text(value: object | None) -> str:
    return str(value or "").strip()


def metadata_with_run_id(metadata: dict[str, Any], run_id: object | None) -> dict[str, Any]:
    normalized = dict(metadata)
    run_id_text = normalize_queue_text(run_id)
    if run_id_text:
        normalized.setdefault("run_id", run_id_text)
    return normalized


def coerce_queue_status(status_cls: Any, value: object | None, *, default: str) -> Any:
    status_text = normalize_queue_text(value) or default
    try:
        return status_cls(status_text)
    except ValueError:
        return status_cls(default)


__all__ = [
    "coerce_queue_status",
    "metadata_with_run_id",
    "normalize_queue_text",
]
