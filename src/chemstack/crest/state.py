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
_STATE_BINDINGS = _engine_state.create_engine_state_bindings(
    state_file_name=STATE_FILE_NAME,
    report_json_file_name=REPORT_JSON_FILE_NAME,
    report_md_file_name=REPORT_MD_FILE_NAME,
    organized_ref_file_name=ORGANIZED_REF_FILE_NAME,
    manifest_file_name=CREST_JOB_MANIFEST_FILE,
    report_title="ChemStack CREST Report",
    selected_input_label="Selected XYZ",
    now_fn=lambda: now_utc_iso(),
)
_STATE_ACCESS = _STATE_BINDINGS.access
_RECOVERY_PENDING = _STATE_BINDINGS.recovery_pending
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


def _recovery_retained_fields(existing: dict[str, Any]) -> dict[str, Any]:
    return {
        "retained_conformer_count": int(existing.get("retained_conformer_count", 0) or 0),
        "retained_conformer_paths": _engine_state.coerce_list(
            existing.get("retained_conformer_paths")
        ),
    }


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
    return _RECOVERY_PENDING.write(
        job_dir,
        job_id=job_id,
        selected_input_xyz=selected_input_xyz,
        reason=reason,
        identity_fields={
            "molecule_key": _engine_state.normalize_text(molecule_key),
            "mode": _engine_state.normalize_text(mode),
        },
        retained_fields=_recovery_retained_fields,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
