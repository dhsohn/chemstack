from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .internal_engine_runtime import InternalEngineQueueRuntime
from .internal_engine_spec import InternalEngineSpec
from .internal_engine_worker_facade import (
    InternalEngineQueueWorkerDeps,
    InternalEngineQueueWorkerFacade,
)


@dataclass(frozen=True)
class InternalEngineQueueModule:
    runtime: InternalEngineQueueRuntime
    facade: InternalEngineQueueWorkerFacade

    @classmethod
    def create(
        cls,
        *,
        spec: InternalEngineSpec,
        load_config: Callable[[Any], Any],
        runtime_roots_for_cfg: Callable[[Any], tuple[Path, ...]],
        list_queue: Callable[[str | Path], list[Any]],
        dequeue_next: Callable[[Path], Any | None],
        poll_interval_seconds: int,
        shutdown_grace_seconds: float,
        deps: InternalEngineQueueWorkerDeps,
    ) -> InternalEngineQueueModule:
        runtime = InternalEngineQueueRuntime.create(
            spec=spec,
            load_config=load_config,
            runtime_roots_for_cfg=runtime_roots_for_cfg,
            list_queue=list_queue,
            dequeue_next=dequeue_next,
        )
        return cls(
            runtime=runtime,
            facade=InternalEngineQueueWorkerFacade(
                runtime=runtime,
                poll_interval_seconds=poll_interval_seconds,
                shutdown_grace_seconds=shutdown_grace_seconds,
                deps=deps,
            ),
        )

    @property
    def queue_roots(self) -> Callable[[Any], tuple[Path, ...]]:
        return self.runtime.queue_roots

    @property
    def queue_entries_with_roots(self) -> Callable[[Any], list[tuple[Path, Any]]]:
        return self.runtime.queue_entries_with_roots

    @property
    def dequeue_next_entry(self) -> Callable[[Any], tuple[Path, Any] | None]:
        return self.runtime.dequeue_next_entry

    @property
    def read_worker_pid(self) -> Callable[[Path], int | None]:
        return self.runtime.read_worker_pid

    @property
    def queue_entry_by_id(self) -> Callable[[Path | str, str], Any | None]:
        return self.runtime.queue_entry_by_id

    @property
    def admission_root(self) -> Callable[[Any], str]:
        return self.runtime.admission_root


__all__ = ["InternalEngineQueueModule"]
