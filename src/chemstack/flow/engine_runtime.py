from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.config.files import (
    default_shared_admission_root,
    engine_config_mapping,
    workflow_root_from_mapping,
)
from chemstack.core.utils.coercion import normalize_text as normalize_text


def _load_engine_config(config_path: str) -> tuple[Path, dict[str, Any]]:
    import yaml  # type: ignore[import-untyped]

    path = Path(config_path).expanduser().resolve()
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"Invalid engine config file: {path}")
    return path, raw


def _mapping_section(raw: dict[str, Any], key: str) -> dict[str, Any]:
    section = raw.get(key)
    return section if isinstance(section, dict) else {}


def _resolve_configured_path(value: Any) -> Path | None:
    text = normalize_text(value)
    return Path(text).expanduser().resolve() if text else None


def _runtime_section_label(engine: str | None) -> str:
    return f"{engine}.runtime" if engine else "runtime"


def _runtime_allowed_root_label(engine: str | None) -> str:
    return f"{_runtime_section_label(engine)}.allowed_root"


def _internal_engine_runtime_paths(path: Path, raw: dict[str, Any]) -> dict[str, Path]:
    workflow_root = workflow_root_from_mapping(raw)
    if not workflow_root:
        raise ValueError(f"Missing workflow.root in config: {path}")
    resolved_workflow_root = Path(workflow_root).expanduser().resolve()
    resolved = {
        "workflow_root": resolved_workflow_root,
        "allowed_root": resolved_workflow_root,
        "organized_root": resolved_workflow_root,
    }
    admission_root = _resolve_configured_path(
        _mapping_section(raw, "scheduler").get("admission_root")
    )
    if admission_root is None:
        admission_root = _resolve_configured_path(default_shared_admission_root(path))
    if admission_root is not None:
        resolved["admission_root"] = admission_root
    return resolved


def _runtime_admission_root(
    path: Path, runtime: dict[str, Any], scheduler: dict[str, Any]
) -> Path | None:
    admission_root = _resolve_configured_path(runtime.get("admission_root"))
    if admission_root is None:
        admission_root = _resolve_configured_path(scheduler.get("admission_root"))
    if admission_root is None and scheduler:
        admission_root = _resolve_configured_path(default_shared_admission_root(path))
    return admission_root


def _configured_runtime_paths(
    path: Path, raw: dict[str, Any], *, engine: str | None = None
) -> dict[str, Path]:
    runtime = raw.get("runtime")
    if not isinstance(runtime, dict):
        raise ValueError(f"Missing {_runtime_section_label(engine)} section in config: {path}")
    scheduler = _mapping_section(raw, "scheduler")

    resolved_runtime_paths: dict[str, Path] = {}
    for key in ("allowed_root", "organized_root"):
        resolved_path = _resolve_configured_path(runtime.get(key))
        if resolved_path is not None:
            resolved_runtime_paths[key] = resolved_path

    if "allowed_root" not in resolved_runtime_paths:
        raise ValueError(f"Missing {_runtime_allowed_root_label(engine)} in config: {path}")

    admission_root = _runtime_admission_root(path, runtime, scheduler)
    if admission_root is not None:
        resolved_runtime_paths["admission_root"] = admission_root
    return resolved_runtime_paths


def engine_runtime_paths(config_path: str, *, engine: str | None = None) -> dict[str, Path]:
    path, raw = _load_engine_config(config_path)
    if engine in {"xtb", "crest"}:
        return _internal_engine_runtime_paths(path, raw)

    if engine:
        raw = engine_config_mapping(raw, engine, inherit_keys=("scheduler", "workflow"))
    return _configured_runtime_paths(path, raw, engine=engine)


__all__ = [
    "engine_runtime_paths",
]
