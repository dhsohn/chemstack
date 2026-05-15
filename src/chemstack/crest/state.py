from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.state import engine as _engine_state
from chemstack.core.utils import now_utc_iso

STATE_FILE_NAME = "job_state.json"
REPORT_JSON_FILE_NAME = "job_report.json"
REPORT_MD_FILE_NAME = "job_report.md"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"
RECOVERY_PENDING_REASONS = _engine_state.RECOVERY_PENDING_REASONS


def write_state(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _engine_state.write_json_artifact(job_dir, STATE_FILE_NAME, payload)


def write_report_json(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _engine_state.write_json_artifact(job_dir, REPORT_JSON_FILE_NAME, payload)


def write_report_md(job_dir: Path, *, job_id: str, status: str, reason: str, selected_xyz: str) -> Path:
    lines = [
        "# crest_auto Report",
        "",
        f"- Job ID: `{job_id}`",
        f"- Status: `{status}`",
        f"- Reason: `{reason}`",
        f"- Selected XYZ: `{selected_xyz}`",
        f"- Updated At: `{now_utc_iso()}`",
    ]
    return _engine_state.write_text_artifact(job_dir, REPORT_MD_FILE_NAME, lines)


def write_report_md_lines(job_dir: Path, lines: list[str]) -> Path:
    return _engine_state.write_text_artifact(job_dir, REPORT_MD_FILE_NAME, lines)


def write_organized_ref(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _engine_state.write_json_artifact(job_dir, ORGANIZED_REF_FILE_NAME, payload)


def load_state(job_dir: Path) -> dict[str, Any] | None:
    return _engine_state.load_json_mapping_artifact(job_dir, STATE_FILE_NAME)


def load_report_json(job_dir: Path) -> dict[str, Any] | None:
    return _engine_state.load_json_mapping_artifact(job_dir, REPORT_JSON_FILE_NAME)


def load_organized_ref(job_dir: Path) -> dict[str, Any] | None:
    return _engine_state.load_json_mapping_artifact(job_dir, ORGANIZED_REF_FILE_NAME)


def _normalize_text(value: Any) -> str:
    return _engine_state.normalize_text(value)


def _coerce_dict(value: Any) -> dict[str, Any]:
    return _engine_state.coerce_dict(value)


def _coerce_list(value: Any) -> list[Any]:
    return _engine_state.coerce_list(value)


def state_matches_job(
    state: dict[str, Any] | None,
    *,
    selected_input_xyz: str | Path,
    mode: str,
    molecule_key: str,
) -> bool:
    return _engine_state.state_matches_fields(
        state,
        {
            "selected_input_xyz": selected_input_xyz,
            "mode": mode,
            "molecule_key": molecule_key,
        },
    )


def is_recovery_pending(state: dict[str, Any] | None) -> bool:
    return _engine_state.is_recovery_pending_state(state)


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
    payload = _engine_state.recovery_pending_payload(
        job_dir,
        existing=existing,
        job_id=job_id,
        selected_input_xyz=selected_input_xyz,
        reason=reason,
        now=now,
        manifest_filename="crest_job.yaml",
        identity_fields={
            "molecule_key": _normalize_text(molecule_key),
            "mode": _normalize_text(mode),
        },
        retained_fields={
            "retained_conformer_count": int(existing.get("retained_conformer_count", 0) or 0),
            "retained_conformer_paths": retained_paths,
        },
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    write_state(job_dir, payload)
    return payload
