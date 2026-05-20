from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.artifacts import (
    JOB_REPORT_JSON_FILE,
    JOB_REPORT_MD_FILE,
    JOB_STATE_FILE,
    ORGANIZED_REF_FILE,
)
from chemstack.core.state import engine as _engine_state
from chemstack.core.utils import now_utc_iso

STATE_FILE_NAME = JOB_STATE_FILE
REPORT_JSON_FILE_NAME = JOB_REPORT_JSON_FILE
REPORT_MD_FILE_NAME = JOB_REPORT_MD_FILE
ORGANIZED_REF_FILE_NAME = ORGANIZED_REF_FILE
RECOVERY_PENDING_REASONS = _engine_state.RECOVERY_PENDING_REASONS
_STATE_FILES = _engine_state.EngineStateFiles(
    state_file_name=STATE_FILE_NAME,
    report_json_file_name=REPORT_JSON_FILE_NAME,
    report_md_file_name=REPORT_MD_FILE_NAME,
    organized_ref_file_name=ORGANIZED_REF_FILE_NAME,
)


def write_state(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _STATE_FILES.write_state(job_dir, payload)


def write_report_json(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _STATE_FILES.write_report_json(job_dir, payload)


def write_report_md(
    job_dir: Path, *, job_id: str, status: str, reason: str, selected_input: str
) -> Path:
    lines = [
        "# xtb_auto Report",
        "",
        f"- Job ID: `{job_id}`",
        f"- Status: `{status}`",
        f"- Reason: `{reason}`",
        f"- Selected Input: `{selected_input}`",
        f"- Updated At: `{now_utc_iso()}`",
    ]
    return _engine_state.write_text_artifact(job_dir, REPORT_MD_FILE_NAME, lines)


def write_report_md_lines(job_dir: Path, lines: list[str]) -> Path:
    return _STATE_FILES.write_report_md_lines(job_dir, lines)


def write_organized_ref(job_dir: Path, payload: dict[str, Any]) -> Path:
    return _STATE_FILES.write_organized_ref(job_dir, payload)


def load_state(job_dir: Path) -> dict[str, Any] | None:
    return _STATE_FILES.load_state(job_dir)


def load_report_json(job_dir: Path) -> dict[str, Any] | None:
    return _STATE_FILES.load_report_json(job_dir)


def load_organized_ref(job_dir: Path) -> dict[str, Any] | None:
    return _STATE_FILES.load_organized_ref(job_dir)


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
    job_type: str,
    reaction_key: str,
) -> bool:
    return _engine_state.state_matches_fields(
        state,
        {
            "selected_input_xyz": selected_input_xyz,
            "job_type": job_type,
            "reaction_key": reaction_key,
        },
    )


def is_recovery_pending(state: dict[str, Any] | None) -> bool:
    return _engine_state.is_recovery_pending_state(state)


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
    candidate_paths = _coerce_list(existing.get("candidate_paths")) or _coerce_list(
        input_summary_payload.get("candidate_paths")
    )
    selected_candidate_paths = _coerce_list(existing.get("selected_candidate_paths"))
    candidate_details = _coerce_list(existing.get("candidate_details"))
    analysis_summary = _coerce_dict(existing.get("analysis_summary"))
    payload = _engine_state.recovery_pending_payload(
        job_dir,
        existing=existing,
        job_id=job_id,
        selected_input_xyz=selected_input_xyz,
        reason=reason,
        now=now,
        manifest_filename="xtb_job.yaml",
        identity_fields={
            "job_type": _normalize_text(job_type),
            "reaction_key": _normalize_text(reaction_key),
            "input_summary": input_summary_payload,
        },
        retained_fields={
            "candidate_count": int(existing.get("candidate_count", 0) or 0),
            "candidate_paths": candidate_paths,
            "selected_candidate_paths": selected_candidate_paths,
            "candidate_details": candidate_details,
            "analysis_summary": analysis_summary,
        },
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    write_state(job_dir, payload)
    return payload
