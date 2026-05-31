from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.commands.queue import QueueRuntime

from .worker import (
    dequeue_next_across_roots,
    queue_entry_by_id as _queue_entry_by_id,
    read_worker_pid_file,
    resolve_admission_root,
)


@dataclass(frozen=True)
class EngineQueueRuntime:
    load_config: Callable[[Any], Any]
    runtime_roots_for_cfg: Callable[[Any], tuple[Path, ...]]
    list_queue: Callable[[str | Path], list[Any]]
    dequeue_next: Callable[[Path], Any | None]
    worker_pid_file_name: str
    dequeue_next_across_roots: Callable[..., tuple[Path, Any] | None] = (
        dequeue_next_across_roots
    )

    def _queue_runtime(self) -> QueueRuntime:
        return QueueRuntime(
            load_config_fn=self.load_config,
            runtime_roots_for_cfg_fn=self.runtime_roots_for_cfg,
            list_queue_fn=self.list_queue,
            dequeue_next_fn=self.dequeue_next,
            dequeue_next_across_roots_fn=self.dequeue_next_across_roots,
        )

    def queue_roots(self, cfg: Any) -> tuple[Path, ...]:
        return self._queue_runtime().queue_roots(cfg)

    def queue_entries_with_roots(self, cfg: Any) -> list[tuple[Path, Any]]:
        return self._queue_runtime().queue_entries_with_roots(cfg)

    def dequeue_next_entry(self, cfg: Any) -> tuple[Path, Any] | None:
        return self._queue_runtime().dequeue_next_entry(cfg)

    def queue_entry_by_id(self, queue_root: Path | str, queue_id: str) -> Any | None:
        return _queue_entry_by_id(
            queue_root,
            queue_id,
            list_queue_fn=self.list_queue,
        )

    def admission_root(self, cfg: Any) -> str:
        return resolve_admission_root(cfg)

    def read_worker_pid(self, allowed_root: Path) -> int | None:
        return read_worker_pid_file(allowed_root, self.worker_pid_file_name)


__all__ = ["EngineQueueRuntime"]
