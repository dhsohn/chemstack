from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.utils import atomic_write_json, now_utc_iso

STATE_FILE_NAME = "job_state.json"
REPORT_JSON_FILE_NAME = "job_report.json"
REPORT_MD_FILE_NAME = "job_report.md"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"
RECOVERY_PENDING_REASONS = frozenset({"worker_shutdown", "crashed_recovery"})


def write_state(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / STATE_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_report_json(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / REPORT_JSON_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def write_report_md(job_dir: Path, *, job_id: str, status: str, reason: str, selected_xyz: str) -> Path:
    path = job_dir / REPORT_MD_FILE_NAME
    lines = [
        "# crest_auto Report",
        "",
        f"- Job ID: `{job_id}`",
        f"- Status: `{status}`",
        f"- Reason: `{reason}`",
        f"- Selected XYZ: `{selected_xyz}`",
        f"- Updated At: `{now_utc_iso()}`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_report_md_lines(job_dir: Path, lines: list[str]) -> Path:
    path = job_dir / REPORT_MD_FILE_NAME
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_organized_ref(job_dir: Path, payload: dict[str, Any]) -> Path:
    path = job_dir / ORGANIZED_REF_FILE_NAME
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def load_state(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / STATE_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def load_report_json(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / REPORT_JSON_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def load_organized_ref(job_dir: Path) -> dict[str, Any] | None:
    path = job_dir / ORGANIZED_REF_FILE_NAME
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    return raw


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _coerce_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def state_matches_job(
    state: dict[str, Any] | None,
    *,
    selected_input_xyz: str | Path,
    mode: str,
    molecule_key: str,
) -> bool:
    if not isinstance(state, dict):
        return False
    selected_text = _normalize_text(selected_input_xyz)
    if _normalize_text(state.get("selected_input_xyz")) != selected_text:
        return False
    if _normalize_text(state.get("mode")) != _normalize_text(mode):
        return False
    return _normalize_text(state.get("molecule_key")) == _normalize_text(molecule_key)


def is_recovery_pending(state: dict[str, Any] | None) -> bool:
    if not isinstance(state, dict):
        return False
    if bool(state.get("recovery_pending")):
        return True
    status = _normalize_text(state.get("status")).lower()
    reason = _normalize_text(state.get("reason"))
    return status == "queued" and reason in RECOVERY_PENDING_REASONS


def mark_recovery_pending(
    job_dir: Path,
    *,
    job_id: str,
    selected_input_xyz: str | Path,
    mode: str,
    molecule_key: str,
    resource_request: dict[str, Any] | None,
    resource_actual: dict[str, Any] | None,
    reason: str,
) -> dict[str, Any]:
    now = now_utc_iso()
    existing = load_state(job_dir) or {}
    retained_paths = _coerce_list(existing.get("retained_conformer_paths"))
    manifest_path = _normalize_text(existing.get("manifest_path"))
    if not manifest_path:
        manifest = (job_dir / "crest_job.yaml").resolve()
        manifest_path = str(manifest) if manifest.exists() else ""
    recovery_count = int(existing.get("recovery_count", 0) or 0) + 1
    payload = {
        "job_id": _normalize_text(existing.get("job_id")) or _normalize_text(job_id),
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": _normalize_text(selected_input_xyz),
        "molecule_key": _normalize_text(molecule_key),
        "mode": _normalize_text(mode),
        "status": "queued",
        "reason": _normalize_text(reason),
        "created_at": _normalize_text(existing.get("created_at")) or now,
        "started_at": _normalize_text(existing.get("started_at")),
        "updated_at": now,
        "retained_conformer_count": int(existing.get("retained_conformer_count", 0) or 0),
        "retained_conformer_paths": retained_paths,
        "manifest_path": manifest_path,
        "resource_request": _coerce_dict(resource_request) or _coerce_dict(existing.get("resource_request")),
        "resource_actual": _coerce_dict(resource_actual) or _coerce_dict(existing.get("resource_actual")),
        "recovery_pending": True,
        "recovery_reason": _normalize_text(reason),
        "recovery_count": recovery_count,
    }
    write_state(job_dir, payload)
    return payload
