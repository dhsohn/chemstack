from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import yaml

from chemstack.core.admission import active_slot_count
from chemstack.flow.submitters.common import sibling_runtime_paths

from ._runtime_common import normalize_text, positive_int


def submission_admission_limit_from_config(
    config_path: str | Path,
    *,
    positive_int_fn: Callable[[Any], int | None] = positive_int,
) -> int | None:
    try:
        path = Path(config_path).expanduser().resolve()
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None

    scheduler = raw.get("scheduler")
    if not isinstance(scheduler, dict):
        scheduler = {}
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        runtime = {}

    for candidate in (
        scheduler.get("max_active_simulations"),
        scheduler.get("admission_limit"),
        runtime.get("admission_limit"),
        runtime.get("admission_max_concurrent"),
        runtime.get("max_concurrent"),
    ):
        parsed = positive_int_fn(candidate)
        if parsed is not None:
            return parsed
    return None


def submission_admission_has_capacity(
    config_path: str | Path,
    *,
    submission_admission_limit_from_config_fn: Callable[[str | Path], int | None]
    = submission_admission_limit_from_config,
    active_slot_count_fn: Callable[[Path], int] = active_slot_count,
    sibling_runtime_paths_fn: Callable[..., dict[str, Any]] = sibling_runtime_paths,
) -> bool | None:
    limit = submission_admission_limit_from_config_fn(config_path)
    if limit is None:
        return None
    admission_root: Path | None = None
    for engine in (None, "xtb", "crest", "orca"):
        try:
            runtime_paths = sibling_runtime_paths_fn(str(config_path), engine=engine)
        except Exception:
            continue
        candidate = runtime_paths.get("admission_root")
        if isinstance(candidate, Path):
            admission_root = candidate
            break
    if not isinstance(admission_root, Path):
        return None
    try:
        return active_slot_count_fn(admission_root) < limit
    except Exception:
        return None


def workflow_submission_has_capacity(
    *config_paths: str | Path | None,
    submission_admission_has_capacity_fn: Callable[[str | Path], bool | None]
    | None = None,
    normalize_text_fn: Callable[[Any], str] = normalize_text,
) -> bool:
    has_capacity_fn = submission_admission_has_capacity_fn or submission_admission_has_capacity
    for config_path in config_paths:
        config_text = normalize_text_fn(config_path)
        if not config_text:
            continue
        has_capacity = has_capacity_fn(config_text)
        if has_capacity is not None:
            return has_capacity
    return True
