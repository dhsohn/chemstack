from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any


def normalize_text(value: Any) -> str:
    return str(value).strip()


def load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def direct_dir_target(
    target: str,
    *,
    path_factory: Callable[[str], Any] = Path,
) -> Path | None:
    raw = normalize_text(target)
    if not raw:
        return None
    try:
        candidate = path_factory(raw).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() and candidate.is_dir() else None


def resolved_dir_candidates(
    values: Iterable[Any],
    *,
    path_factory: Callable[[str], Any] = Path,
) -> list[Path]:
    candidates: list[Path] = []
    for value in values:
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            candidates.append(path_factory(raw).expanduser().resolve())
        except OSError:
            continue
    return candidates
