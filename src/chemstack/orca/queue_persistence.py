"""ORCA queue persistence backed by the shared core queue store."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from chemstack.core.queue import store as _core_queue

from .queue_entries import entry_from_json_payload
from .types import QueueEntry


QueueStoreCorruptError = _core_queue.QueueStoreCorruptError


def load_entries(allowed_root: Path) -> list[QueueEntry]:
    return _core_queue.load_entries(
        allowed_root,
        entry_from_dict_fn=entry_from_json_payload,
        corrupt_error=QueueStoreCorruptError,
    )


def save_entries(
    allowed_root: Path,
    entries: Sequence[QueueEntry],
) -> None:
    _core_queue.save_entries(allowed_root, entries)


def mutate_entries(
    allowed_root: Path,
    mutator: Any,
) -> Any:
    return _core_queue.mutate_entries(
        allowed_root,
        mutator,
        load_entries_fn=load_entries,
        save_entries_fn=save_entries,
    )


queue_lock = _core_queue.queue_lock
