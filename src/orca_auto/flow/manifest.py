from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from orca_auto.core.utils import normalize_text

FLOW_MANIFEST_FILENAMES = ("flow.yaml",)


def manifest_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if normalize_text(key)}


def load_flow_manifest(
    directory: Path,
    *,
    filenames: tuple[str, ...] = FLOW_MANIFEST_FILENAMES,
    description: str = "Workflow manifest",
) -> dict[str, Any]:
    for name in filenames:
        candidate = directory / name
        if not candidate.is_file():
            continue
        parsed = yaml.safe_load(candidate.read_text(encoding="utf-8"))
        if parsed is None:
            return {}
        if not isinstance(parsed, dict):
            raise ValueError(f"{description} must contain a mapping: {candidate}")
        return dict(parsed)
    return {}


def resolve_manifest_file_value(base_dir: Path, value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return str(candidate.resolve())


def resolve_engine_manifest(base_dir: Path, manifest: dict[str, Any], key: str) -> dict[str, Any]:
    section = manifest_mapping(manifest.get(key))
    if not section:
        return {}
    resolved = dict(section)
    if "xcontrol_file" in resolved:
        resolved["xcontrol_file"] = resolve_manifest_file_value(
            base_dir,
            resolved.get("xcontrol_file"),
        )
    return resolved


def resolve_engine_manifest_with_presence(
    base_dir: Path,
    manifest: dict[str, Any],
    key: str,
) -> tuple[bool, dict[str, Any]]:
    if not isinstance(manifest.get(key), dict):
        return False, {}
    return True, resolve_engine_manifest(base_dir, manifest, key)


def resolve_endpoint_pairing_manifest(
    manifest: dict[str, Any],
    xtb_manifest: dict[str, Any],
) -> dict[str, Any]:
    xtb_section = manifest_mapping(xtb_manifest.pop("endpoint_pairing", None))
    top_level = manifest_mapping(manifest.get("endpoint_pairing"))
    resolved = dict(xtb_section)
    resolved.update(top_level)
    return resolved


__all__ = [
    "FLOW_MANIFEST_FILENAMES",
    "load_flow_manifest",
    "manifest_mapping",
    "resolve_endpoint_pairing_manifest",
    "resolve_engine_manifest",
    "resolve_engine_manifest_with_presence",
    "resolve_manifest_file_value",
]
