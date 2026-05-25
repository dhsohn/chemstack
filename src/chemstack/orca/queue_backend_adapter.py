from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from chemstack.core.utils.persistence import load_json_list_file

from . import queue_backend as _queue_backend
from .types import QueueEntry


@dataclass(frozen=True)
class QueueBackendAdapter:
    queue_path: Any
    corrupt_error: Any
    atomic_write_json: Any
    backend_module: Any
    normalize_entry: Any

    def load_entries(self, allowed_root: Path) -> list[QueueEntry]:
        raw = load_json_list_file(
            self.queue_path(allowed_root),
            corrupt_error=self.corrupt_error,
            description="Queue file",
        )
        return [cast(QueueEntry, entry) for entry in raw if isinstance(entry, dict)]

    def save_entries(self, allowed_root: Path, entries: list[QueueEntry]) -> None:
        normalized_entries = [self.normalize_entry(entry) for entry in entries]
        serialized_entries = _queue_backend.entries_payload(
            normalized_entries,
            backend=self.backend_module(),
        )
        self.atomic_write_json(
            self.queue_path(allowed_root),
            serialized_entries,
            ensure_ascii=True,
            indent=2,
        )


__all__ = ["QueueBackendAdapter"]
