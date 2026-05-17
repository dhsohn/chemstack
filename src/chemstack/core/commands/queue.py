from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any


def display_status(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    normalized = str(status_value).strip() or "unknown"
    if getattr(entry, "cancel_requested", False) and normalized == "running":
        return "cancel_requested"
    return normalized


def find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    for entry in entries:
        if entry.queue_id == target or entry.task_id == target:
            return entry
    return None


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


def queue_entry_sort_key(entry: Any) -> tuple[int, str, str]:
    return (
        int(getattr(entry, "priority", 10) or 10),
        str(getattr(entry, "enqueued_at", "")),
        str(getattr(entry, "queue_id", "")),
    )


def sorted_queue_entries(
    cfg: Any,
    *,
    runtime_roots_for_cfg_fn: Callable[[Any], tuple[Path, ...]],
    list_queue_fn: Callable[[Path], list[Any]],
) -> list[Any]:
    entries = [
        entry
        for _root, entry in queue_entries_with_roots(
            cfg,
            queue_roots_fn=lambda cfg_obj: queue_roots(
                cfg_obj,
                runtime_roots_for_cfg_fn=runtime_roots_for_cfg_fn,
            ),
            list_queue_fn=list_queue_fn,
        )
    ]
    entries.sort(key=queue_entry_sort_key)
    return entries


def metadata_text(entry: Any, key: str, *, default: str = "") -> str:
    value = str(getattr(entry, "metadata", {}).get(key, "")).strip()
    return value or default


def metadata_path_name(entry: Any, key: str, *, default: str = "-") -> str:
    value = metadata_text(entry, key)
    return Path(value).name if value else default


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

    entry_with_root = None
    for queue_root, entry in queue_entries_with_roots_fn(cfg):
        if entry.queue_id == target or entry.task_id == target:
            entry_with_root = (queue_root, entry)
            break
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
