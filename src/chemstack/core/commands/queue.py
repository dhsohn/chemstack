from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def display_status(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    normalized = str(status_value).strip() or "unknown"
    if getattr(entry, "cancel_requested", False) and normalized == "running":
        return "cancel_requested"
    return normalized


@dataclass(frozen=True)
class QueueRuntime:
    load_config_fn: Callable[[Any], Any]
    runtime_roots_for_cfg_fn: Callable[[Any], tuple[Path, ...]]
    list_queue_fn: Callable[[Path], list[Any]]
    dequeue_next_fn: Callable[[Path], Any | None]
    dequeue_next_across_roots_fn: Callable[..., tuple[Path, Any] | None]

    def queue_roots(self, cfg: Any) -> tuple[Path, ...]:
        return queue_roots(
            cfg,
            runtime_roots_for_cfg_fn=self.runtime_roots_for_cfg_fn,
        )

    def queue_entries_with_roots(self, cfg: Any) -> list[tuple[Path, Any]]:
        return queue_entries_with_roots(
            cfg,
            queue_roots_fn=self.queue_roots,
            list_queue_fn=self.list_queue_fn,
        )

    def dequeue_next_entry(self, cfg: Any) -> tuple[Path, Any] | None:
        return dequeue_next_entry(
            cfg,
            queue_roots_fn=self.queue_roots,
            list_queue_fn=self.list_queue_fn,
            dequeue_next_fn=self.dequeue_next_fn,
            dequeue_next_across_roots_fn=self.dequeue_next_across_roots_fn,
        )

    def process_one(
        self,
        cfg: Any,
        *,
        reserve_slot_fn: Callable[[Any], str | None],
        admission_root_fn: Callable[[Any], str | Path],
        execute_entry_fn: Callable[[Path, Any], Any],
        release_slot_fn: Callable[[str | Path, str], Any],
        after_execute_fn: Callable[[Any, Any], Any] | None = None,
    ) -> str:
        return process_one_entry(
            cfg,
            reserve_slot_fn=reserve_slot_fn,
            admission_root_fn=admission_root_fn,
            dequeue_next_entry_fn=self.dequeue_next_entry,
            execute_entry_fn=execute_entry_fn,
            release_slot_fn=release_slot_fn,
            after_execute_fn=after_execute_fn,
        )


def queue_roots(
    cfg: Any,
    *,
    runtime_roots_for_cfg_fn: Callable[[Any], tuple[Path, ...]],
) -> tuple[Path, ...]:
    try:
        return tuple(runtime_roots_for_cfg_fn(cfg))
    except Exception:
        return (Path(cfg.runtime.allowed_root).expanduser().resolve(),)


def queue_entries_with_roots(
    cfg: Any,
    *,
    queue_roots_fn: Callable[[Any], tuple[Path, ...]],
    list_queue_fn: Callable[[Path], list[Any]],
) -> list[tuple[Path, Any]]:
    rows: list[tuple[Path, Any]] = []
    for root in queue_roots_fn(cfg):
        for entry in list_queue_fn(root):
            rows.append((root, entry))
    return rows


def dequeue_next_entry(
    cfg: Any,
    *,
    queue_roots_fn: Callable[[Any], tuple[Path, ...]],
    list_queue_fn: Callable[[Path], list[Any]],
    dequeue_next_fn: Callable[[Path], Any | None],
    dequeue_next_across_roots_fn: Callable[..., tuple[Path, Any] | None],
) -> tuple[Path, Any] | None:
    return dequeue_next_across_roots_fn(
        queue_roots_fn(cfg),
        list_queue_fn=list_queue_fn,
        dequeue_next_fn=dequeue_next_fn,
    )


def process_one_entry(
    cfg: Any,
    *,
    reserve_slot_fn: Callable[[Any], str | None],
    admission_root_fn: Callable[[Any], str | Path],
    dequeue_next_entry_fn: Callable[[Any], tuple[Path, Any] | None],
    execute_entry_fn: Callable[[Path, Any], Any],
    release_slot_fn: Callable[[str | Path, str], Any],
    after_execute_fn: Callable[[Any, Any], Any] | None = None,
) -> str:
    slot_token = reserve_slot_fn(cfg)
    if slot_token is None:
        return "blocked"

    try:
        dequeued = dequeue_next_entry_fn(cfg)
        if dequeued is None:
            return "idle"
        queue_root, entry = dequeued
        outcome = execute_entry_fn(queue_root, entry)
        if after_execute_fn is not None:
            after_execute_fn(entry, outcome)
        return "processed"
    finally:
        release_slot_fn(admission_root_fn(cfg), slot_token)


def run_queue_worker_command(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    config_path_fn: Callable[[Any], str],
    worker_factory: Callable[..., Any],
    existing_pid_fn: Callable[[Any], int | None] | None = None,
    existing_pid_report_fn: Callable[[int], Any] | None = None,
    max_concurrent_fn: Callable[[Any], int] | None = None,
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))

    if existing_pid_fn is not None:
        existing_pid = existing_pid_fn(cfg)
        if existing_pid is not None:
            if existing_pid_report_fn is not None:
                existing_pid_report_fn(existing_pid)
            else:
                print(f"error: queue worker already running (pid={existing_pid})")
            return 1

    worker_kwargs: dict[str, Any] = {}
    if max_concurrent_fn is not None:
        worker_kwargs["max_concurrent"] = max_concurrent_fn(cfg)

    worker = worker_factory(
        cfg,
        config_path_fn(args),
        **worker_kwargs,
    )
    return worker.run()
