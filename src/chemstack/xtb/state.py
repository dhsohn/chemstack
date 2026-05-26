from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.artifacts import (
    XTB_JOB_MANIFEST_FILE,
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
_STATE_ACCESS = _engine_state.EngineStateAccess(
    files=_STATE_FILES,
    report_title="ChemStack xTB Report",
    selected_input_label="Selected Input",
    now_fn=lambda: now_utc_iso(),
)
write_state = _STATE_ACCESS.write_state
write_report_json = _STATE_ACCESS.write_report_json
write_report_md_lines = _STATE_ACCESS.write_report_md_lines
write_organized_ref = _STATE_ACCESS.write_organized_ref
load_state = _STATE_ACCESS.load_state
load_report_json = _STATE_ACCESS.load_report_json
load_organized_ref = _STATE_ACCESS.load_organized_ref


def write_report_md(
    job_dir: Path, *, job_id: str, status: str, reason: str, selected_input: str
) -> Path:
    return _STATE_ACCESS.write_report_md(
        job_dir,
        job_id=job_id,
        status=status,
        reason=reason,
        selected_input=selected_input,
    )


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
        manifest_filename=XTB_JOB_MANIFEST_FILE,
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
