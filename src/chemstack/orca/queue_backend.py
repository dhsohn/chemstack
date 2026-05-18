from __future__ import annotations

from typing import Any

from chemstack.core.queue import compat as _core_queue_compat

from . import queue_entry_model as _queue_entry_model
from .statuses import QueueStatus
from .types import QueueEntry

QUEUE_ENGINE = "orca"
QUEUE_TASK_KIND = "orca_run_inp"


def _normalize_text(value: object | None) -> str:
    return _queue_entry_model.normalize_text(value)


def _normalize_entry(entry: QueueEntry) -> QueueEntry:
    return _queue_entry_model.normalize_entry(entry)


def to_core_entry(entry: QueueEntry, *, backend: Any) -> Any:
    normalized = _normalize_entry(entry)
    metadata = _core_queue_compat.metadata_with_run_id(
        _queue_entry_model.queue_entry_metadata(normalized),
        _queue_entry_model.queue_entry_run_id(normalized),
    )
    status = _core_queue_compat.coerce_queue_status(
        backend.QueueStatus,
        _queue_entry_model.queue_entry_status(normalized),
        default=QueueStatus.PENDING.value,
    )
    queue_id = _queue_entry_model.queue_entry_id(normalized)
    return backend.QueueEntry(
        queue_id=queue_id,
        app_name=_queue_entry_model.queue_entry_app_name(normalized),
        task_id=_queue_entry_model.queue_entry_task_id(normalized) or queue_id,
        task_kind=_normalize_text(normalized.get("task_kind")) or QUEUE_TASK_KIND,
        engine=_normalize_text(normalized.get("engine")) or QUEUE_ENGINE,
        status=status,
        priority=_queue_entry_model.queue_entry_priority(normalized),
        enqueued_at=_normalize_text(normalized.get("enqueued_at")),
        started_at=_normalize_text(normalized.get("started_at")),
        finished_at=_normalize_text(normalized.get("finished_at")),
        cancel_requested=bool(normalized.get("cancel_requested", False)),
        error=_normalize_text(normalized.get("error")),
        metadata=metadata,
    )


def entry_dict(entry: QueueEntry, *, backend: Any) -> dict[str, Any]:
    normalized = _normalize_entry(entry)
    serialize_entry = getattr(backend, "entry_to_dict", None)
    if not callable(serialize_entry):
        serialize_entry = backend._entry_to_dict
    serialized = dict(serialize_entry(to_core_entry(normalized, backend=backend)))

    reaction_dir = _queue_entry_model.queue_entry_reaction_dir(normalized)
    if reaction_dir:
        serialized["reaction_dir"] = reaction_dir
    serialized["force"] = _queue_entry_model.queue_entry_force(normalized)
    serialized["started_at"] = normalized.get("started_at")
    serialized["finished_at"] = normalized.get("finished_at")
    serialized["error"] = normalized.get("error")
    serialized["run_id"] = _queue_entry_model.queue_entry_run_id(normalized)
    return serialized


def entries_payload(entries: list[QueueEntry], *, backend: Any | None) -> list[dict[str, Any]]:
    if backend is None:
        return [dict(_normalize_entry(entry)) for entry in entries]
    return [entry_dict(entry, backend=backend) for entry in entries]


__all__ = [
    "entries_payload",
    "entry_dict",
    "to_core_entry",
]
