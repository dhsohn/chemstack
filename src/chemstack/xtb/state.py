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


def write_report_md(job_dir: Path, *, job_id: str, status: str, reason: str, selected_input: str) -> Path:
    path = job_dir / REPORT_MD_FILE_NAME
    lines = [
        "# xtb_auto Report",
        "",
        f"- Job ID: `{job_id}`",
        f"- Status: `{status}`",
        f"- Reason: `{reason}`",
        f"- Selected Input: `{selected_input}`",
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
    job_type: str,
    reaction_key: str,
) -> bool:
    if not isinstance(state, dict):
        return False
    selected_text = _normalize_text(selected_input_xyz)
    if _normalize_text(state.get("selected_input_xyz")) != selected_text:
        return False
    if _normalize_text(state.get("job_type")) != _normalize_text(job_type):
        return False
    return _normalize_text(state.get("reaction_key")) == _normalize_text(reaction_key)


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
    job_type: str,
    reaction_key: str,
    input_summary: dict[str, Any] | None,
    resource_request: dict[str, Any] | None,
    resource_actual: dict[str, Any] | None,
    reason: str,
) -> dict[str, Any]:
    now = now_utc_iso()
    existing = load_state(job_dir) or {}
    input_summary_payload = _coerce_dict(input_summary)
    candidate_paths = _coerce_list(existing.get("candidate_paths")) or _coerce_list(input_summary_payload.get("candidate_paths"))
    selected_candidate_paths = _coerce_list(existing.get("selected_candidate_paths"))
    candidate_details = _coerce_list(existing.get("candidate_details"))
    analysis_summary = _coerce_dict(existing.get("analysis_summary"))
    manifest_path = _normalize_text(existing.get("manifest_path"))
    if not manifest_path:
        manifest = (job_dir / "xtb_job.yaml").resolve()
        manifest_path = str(manifest) if manifest.exists() else ""
    recovery_count = int(existing.get("recovery_count", 0) or 0) + 1
    payload = {
        "job_id": _normalize_text(existing.get("job_id")) or _normalize_text(job_id),
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": _normalize_text(selected_input_xyz),
        "job_type": _normalize_text(job_type),
        "reaction_key": _normalize_text(reaction_key),
        "input_summary": input_summary_payload,
        "status": "queued",
        "reason": _normalize_text(reason),
        "created_at": _normalize_text(existing.get("created_at")) or now,
        "started_at": _normalize_text(existing.get("started_at")),
        "updated_at": now,
        "candidate_count": int(existing.get("candidate_count", 0) or 0),
        "candidate_paths": candidate_paths,
        "selected_candidate_paths": selected_candidate_paths,
        "candidate_details": candidate_details,
        "analysis_summary": analysis_summary,
        "manifest_path": manifest_path,
        "resource_request": _coerce_dict(resource_request) or _coerce_dict(existing.get("resource_request")),
        "resource_actual": _coerce_dict(resource_actual) or _coerce_dict(existing.get("resource_actual")),
        "recovery_pending": True,
        "recovery_reason": _normalize_text(reason),
        "recovery_count": recovery_count,
    }
    write_state(job_dir, payload)
    return payload
