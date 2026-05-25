from __future__ import annotations

from pathlib import Path
from typing import Any


def normalize_work_dir(value: str | Path | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def normalize_slot(slot: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(slot)
    raw_work_dir = normalized.get("work_dir")
    if raw_work_dir in {None, ""}:
        raw_work_dir = normalized.get("reaction_dir")
    work_dir_input: str | Path | None
    if isinstance(raw_work_dir, (str, Path)):
        work_dir_input = raw_work_dir
    else:
        work_dir_input = None
    work_dir = normalize_work_dir(work_dir_input)
    if work_dir is not None:
        normalized["work_dir"] = work_dir
        normalized["reaction_dir"] = work_dir
    return normalized


def slot_reaction_dir(slot: dict[str, Any]) -> str | None:
    raw_reaction_dir = slot.get("reaction_dir")
    if raw_reaction_dir in {None, ""}:
        raw_reaction_dir = slot.get("work_dir")
    if not isinstance(raw_reaction_dir, (str, Path)):
        return None
    return normalize_work_dir(raw_reaction_dir)


def normalize_reaction_dir_set(reaction_dirs: set[str] | None) -> set[str]:
    normalized: set[str] = set()
    for reaction_dir in reaction_dirs or set():
        resolved = normalize_work_dir(reaction_dir)
        if resolved is not None:
            normalized.add(resolved)
    return normalized


__all__ = [
    "normalize_reaction_dir_set",
    "normalize_slot",
    "normalize_work_dir",
    "slot_reaction_dir",
]
