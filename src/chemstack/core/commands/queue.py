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


def entry_matches_target(entry: Any, target: str) -> bool:
    return entry.queue_id == target or entry.task_id == target


def find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    for entry in entries:
        if entry_matches_target(entry, target):
            return entry
    return None


def find_entry_with_root_by_target(
    entries_with_roots: list[tuple[Path, Any]],
    target: str,
) -> tuple[Path, Any] | None:
    for queue_root, entry in entries_with_roots:
        if entry_matches_target(entry, target):
            return queue_root, entry
    return None


@dataclass(frozen=True)
class QueueRuntime:
    load_config_fn: Callable[[Any], Any]
    runtime_roots_for_cfg_fn: Callable[[Any], tuple[Path, ...]]
    list_queue_fn: Callable[[Path], list[Any]]
    dequeue_next_fn: Callable[[Path], Any | None]
    dequeue_next_across_roots_fn: Callable[..., tuple[Path, Any] | None]
    request_cancel_fn: Callable[[Path, str], Any | None]
    display_status_fn: Callable[[Any], str] = display_status

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

    def cmd_queue_cancel(self, args: Any) -> int:
        return cmd_queue_cancel(
            args,
            load_config_fn=self.load_config_fn,
            queue_entries_with_roots_fn=self.queue_entries_with_roots,
            request_cancel_fn=self.request_cancel_fn,
            display_status_fn=self.display_status_fn,
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


def cmd_queue_cancel(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    queue_entries_with_roots_fn: Callable[[Any], list[tuple[Path, Any]]],
    request_cancel_fn: Callable[[Path, str], Any | None],
    display_status_fn: Callable[[Any], str],
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: queue cancel requires a queue_id or job_id")
        return 1

    entries_with_roots = queue_entries_with_roots_fn(cfg)
    entry_with_root = find_entry_with_root_by_target(entries_with_roots, target)
    if entry_with_root is None:
        print(f"error: queue target not found: {target}")
        return 1
    queue_root, entry = entry_with_root

    updated = request_cancel_fn(queue_root, entry.queue_id)
    if updated is None:
        print(f"error: queue target already terminal: {target}")
        return 1

    print(f"status: {display_status_fn(updated)}")
    print(f"queue_id: {updated.queue_id}")
    print(f"job_id: {updated.task_id}")
    return 0


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
