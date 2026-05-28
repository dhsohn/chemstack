from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

import yaml
from chemstack.core.admission import active_slot_count
from chemstack.core.config.files import (
    default_shared_admission_root,
    engine_config_mapping,
    workflow_root_from_mapping,
)

from . import _runtime_common


def submission_admission_limit_from_config(
    config_path: str | Path,
    *,
    positive_int_fn: Callable[[Any], int | None] = _runtime_common.positive_int,
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
    return positive_int_fn(scheduler.get("max_active_simulations"))


def _mapping_section(raw: dict[str, Any], key: str) -> dict[str, Any]:
    section = raw.get(key)
    return section if isinstance(section, dict) else {}


def _resolve_configured_path(value: Any) -> Path | None:
    text = _runtime_common.normalize_text(value)
    return Path(text).expanduser().resolve() if text else None


def _submission_admission_root_from_config(
    config_path: str | Path,
    *,
    engine: str | None = None,
) -> Path | None:
    path = Path(config_path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        return None

    if engine in {"xtb", "crest"}:
        workflow_root = workflow_root_from_mapping(raw)
        if not workflow_root:
            return None
        admission_root = _resolve_configured_path(
            _mapping_section(raw, "scheduler").get("admission_root")
        )
        if admission_root is None:
            admission_root = _resolve_configured_path(default_shared_admission_root(path))
        return admission_root

    if engine:
        raw = engine_config_mapping(raw, engine, inherit_keys=("scheduler", "workflow"))
    runtime = raw.get("runtime")
    scheduler = _mapping_section(raw, "scheduler")
    if isinstance(runtime, dict):
        admission_root = _resolve_configured_path(runtime.get("admission_root"))
        if admission_root is not None:
            return admission_root
    admission_root = _resolve_configured_path(scheduler.get("admission_root"))
    if admission_root is None and scheduler:
        admission_root = _resolve_configured_path(default_shared_admission_root(path))
    return admission_root


def submission_admission_has_capacity(
    config_path: str | Path,
    *,
    submission_admission_limit_from_config_fn: Callable[[str | Path], int | None]
    = submission_admission_limit_from_config,
    active_slot_count_fn: Callable[[Path], int] = active_slot_count,
    engine_runtime_paths_fn: Callable[..., dict[str, Any]] | None = None,
) -> bool | None:
    limit = submission_admission_limit_from_config_fn(config_path)
    if limit is None:
        return None
    admission_root: Path | None = None
    for engine in (None, "xtb", "crest", "orca"):
        try:
            if engine_runtime_paths_fn is None:
                candidate = _submission_admission_root_from_config(config_path, engine=engine)
            else:
                runtime_paths = engine_runtime_paths_fn(str(config_path), engine=engine)
                candidate = runtime_paths.get("admission_root")
        except Exception:
            continue
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
    normalize_text_fn: Callable[[Any], str] = _runtime_common.normalize_text,
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
