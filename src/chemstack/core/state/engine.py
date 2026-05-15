from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.utils import atomic_write_json

RECOVERY_PENDING_REASONS = frozenset({"worker_shutdown", "crashed_recovery"})


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def coerce_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def coerce_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def write_json_artifact(job_dir: Path, filename: str, payload: dict[str, Any]) -> Path:
    path = job_dir / filename
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_text_artifact(job_dir: Path, filename: str, lines: list[str]) -> Path:
    path = job_dir / filename
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def load_json_mapping_artifact(job_dir: Path, filename: str) -> dict[str, Any] | None:
    path = job_dir / filename
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def state_matches_fields(state: dict[str, Any] | None, fields: dict[str, Any]) -> bool:
    if not isinstance(state, dict):
        return False
    for key, value in fields.items():
        if normalize_text(state.get(key)) != normalize_text(value):
            return False
    return True


def is_recovery_pending_state(state: dict[str, Any] | None) -> bool:
    if not isinstance(state, dict):
        return False
    if bool(state.get("recovery_pending")):
        return True
    status = normalize_text(state.get("status")).lower()
    reason = normalize_text(state.get("reason"))
    return status == "queued" and reason in RECOVERY_PENDING_REASONS


def manifest_path_from_existing(
    job_dir: Path,
    existing: dict[str, Any],
    *,
    manifest_filename: str,
) -> str:
    manifest_path = normalize_text(existing.get("manifest_path"))
    if manifest_path:
        return manifest_path
    manifest = (job_dir / manifest_filename).resolve()
    return str(manifest) if manifest.exists() else ""


def recovery_pending_payload(
    job_dir: Path,
    *,
    existing: dict[str, Any],
    job_id: str,
    selected_input_xyz: str | Path,
    reason: str,
    now: str,
    manifest_filename: str,
    identity_fields: dict[str, Any],
    retained_fields: dict[str, Any],
    resource_request: dict[str, Any] | None,
    resource_actual: dict[str, Any] | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "job_id": normalize_text(existing.get("job_id")) or normalize_text(job_id),
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": normalize_text(selected_input_xyz),
        **{str(key): value for key, value in identity_fields.items()},
        "status": "queued",
        "reason": normalize_text(reason),
        "created_at": normalize_text(existing.get("created_at")) or now,
        "started_at": normalize_text(existing.get("started_at")),
        "updated_at": now,
        **{str(key): value for key, value in retained_fields.items()},
        "manifest_path": manifest_path_from_existing(job_dir, existing, manifest_filename=manifest_filename),
        "resource_request": coerce_dict(resource_request) or coerce_dict(existing.get("resource_request")),
        "resource_actual": coerce_dict(resource_actual) or coerce_dict(existing.get("resource_actual")),
        "recovery_pending": True,
        "recovery_reason": normalize_text(reason),
        "recovery_count": int(existing.get("recovery_count", 0) or 0) + 1,
    }
    return payload
