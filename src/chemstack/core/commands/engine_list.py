from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.commands import queue as _shared_queue


@dataclass(frozen=True)
class EngineListColumn:
    value: Callable[[Any], Any]
    width: int | None = None


@dataclass(frozen=True)
class EngineListSpec:
    engine_label: str
    header: str
    separator: str
    columns: tuple[EngineListColumn, ...]


def format_cell(value: Any, width: int | None) -> str:
    text = str(value)
    if width is None:
        return text
    return f"{text:<{width}}"


def format_row(entry: Any, columns: tuple[EngineListColumn, ...]) -> str:
    return " ".join(format_cell(column.value(entry), column.width) for column in columns)


def metadata_text_column(key: str, *, default: str = "-") -> Callable[[Any], str]:
    def value(entry: Any) -> str:
        return _shared_queue.metadata_text(entry, key, default=default)

    return value


def metadata_path_name_column(key: str, *, default: str = "-") -> Callable[[Any], str]:
    def value(entry: Any) -> str:
        return _shared_queue.metadata_path_name(entry, key, default=default)

    return value


def cmd_list(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    runtime_roots_for_cfg_fn: Callable[[Any], tuple[Path, ...]],
    list_queue_fn: Callable[[Path], list[Any]],
    spec: EngineListSpec,
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    entries = _shared_queue.sorted_queue_entries(
        cfg,
        runtime_roots_for_cfg_fn=runtime_roots_for_cfg_fn,
        list_queue_fn=list_queue_fn,
    )

    if not entries:
        print(f"No {spec.engine_label} jobs found.")
        return 0

    print(f"{spec.engine_label} queue: {len(entries)} entries\n")
    print(spec.header)
    print(spec.separator)
    for entry in entries:
        print(format_row(entry, spec.columns))
    return 0
