from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.artifacts import (
    CREST_JOB_MANIFEST_FILE,
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
    report_title="ChemStack CREST Report",
    selected_input_label="Selected XYZ",
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
    job_dir: Path, *, job_id: str, status: str, reason: str, selected_xyz: str
) -> Path:
    return _STATE_ACCESS.write_report_md(
        job_dir,
        job_id=job_id,
        status=status,
        reason=reason,
        selected_input=selected_xyz,
    )


_normalize_text = _engine_state.normalize_text
_coerce_dict = _engine_state.coerce_dict
_coerce_list = _engine_state.coerce_list


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


is_recovery_pending = _engine_state.is_recovery_pending_state


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
        manifest_filename=CREST_JOB_MANIFEST_FILE,
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
