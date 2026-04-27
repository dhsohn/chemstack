from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location

from ..contracts.crest import CrestArtifactContract, CrestDownstreamPolicy, _coerce_resource_dict, to_workflow_stage_inputs
from ..contracts.xtb import WorkflowStageInput

REPORT_JSON_FILE_NAME = "job_report.json"
STATE_FILE_NAME = "job_state.json"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"
_ACTIVE_PAYLOAD_STATUSES = frozenset({"queued", "running", "submitted", "cancel_requested", "retrying"})


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return raw if isinstance(raw, dict) else {}


def _select_artifact_payload(
    *,
    report: dict[str, Any],
    state: dict[str, Any],
    organized_ref: dict[str, Any],
) -> dict[str, Any]:
    state_status = _normalize_text(state.get("status")).lower()
    if state and state_status in _ACTIVE_PAYLOAD_STATUSES:
        return state

    report_job_id = _normalize_text(report.get("job_id"))
    state_job_id = _normalize_text(state.get("job_id"))
    if state and report_job_id and state_job_id and report_job_id != state_job_id:
        return state

    return report or state or organized_ref


def _direct_path_target(target: str) -> Path | None:
    raw = _normalize_text(target)
    if not raw:
        return None
    try:
        candidate = Path(raw).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() and candidate.is_dir() else None


def _resolve_job_dir(index_root: Path, target: str) -> tuple[Path, JobLocationRecord | None]:
    record = resolve_job_location(index_root, target)
    candidates: list[Path] = []
    if record is not None:
        for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
            raw = _normalize_text(value)
            if not raw:
                continue
            try:
                candidate = Path(raw).expanduser().resolve()
            except OSError:
                continue
            candidates.append(candidate)
    direct = _direct_path_target(target)
    if direct is not None:
        candidates.append(direct)

    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate, record
    raise FileNotFoundError(f"CREST job directory not found for target: {target}")


def _retained_paths(payload: dict[str, Any]) -> tuple[str, ...]:
    raw = payload.get("retained_conformer_paths")
    if not isinstance(raw, list):
        return ()
    return tuple(_normalize_text(item) for item in raw if _normalize_text(item))


def _artifact_roots(job_dir: Path, organized_output_dir: str) -> tuple[Path, ...]:
    roots: list[Path] = []
    for candidate in (organized_output_dir, str(job_dir)):
        text = _normalize_text(candidate)
        if not text:
            continue
        try:
            resolved = Path(text).expanduser().resolve()
        except OSError:
            continue
        if resolved not in roots:
            roots.append(resolved)
    return tuple(roots)


def _resolve_artifact_path(value: Any, *, roots: tuple[Path, ...]) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        resolved = Path(text).expanduser().resolve()
    except OSError:
        return text
    if resolved.exists():
        return str(resolved)
    for root in roots:
        remapped = root / resolved.name
        if remapped.exists():
            return str(remapped.resolve())
    return str(resolved)


def load_crest_artifact_contract(*, crest_index_root: str | Path, target: str) -> CrestArtifactContract:
    index_root = Path(crest_index_root).expanduser().resolve()
    job_dir, record = _resolve_job_dir(index_root, target)

    report = _load_json_dict(job_dir / REPORT_JSON_FILE_NAME)
    state = _load_json_dict(job_dir / STATE_FILE_NAME)
    organized_ref = _load_json_dict(job_dir / ORGANIZED_REF_FILE_NAME)
    payload = _select_artifact_payload(report=report, state=state, organized_ref=organized_ref)
    if not payload:
        raise FileNotFoundError(f"CREST artifact files not found in job directory: {job_dir}")

    if record is not None and record.app_name and record.app_name != "crest_auto":
        raise ValueError(f"Expected crest_auto index record, got: {record.app_name}")

    retained_paths = _retained_paths(payload)
    retained_count = int(payload.get("retained_conformer_count", len(retained_paths)) or len(retained_paths))
    status = _normalize_text(payload.get("status") or (record.status if record is not None else "")) or "unknown"
    reason = _normalize_text(payload.get("reason"))
    job_id = _normalize_text(payload.get("job_id") or (record.job_id if record is not None else ""))
    mode = _normalize_text(payload.get("mode") or (record.job_type if record is not None else "")) or "standard"
    molecule_key = _normalize_text(payload.get("molecule_key") or (record.molecule_key if record is not None else ""))
    selected_input_xyz = _normalize_text(payload.get("selected_input_xyz") or (record.selected_input_xyz if record is not None else ""))
    organized_output_dir = _normalize_text(
        payload.get("organized_output_dir")
        or organized_ref.get("organized_output_dir")
        or (record.organized_output_dir if record is not None else "")
    )
    artifact_roots = _artifact_roots(job_dir, organized_output_dir)
    selected_input_xyz = _resolve_artifact_path(selected_input_xyz, roots=artifact_roots)
    remapped_retained_paths: list[str] = []
    for path in retained_paths:
        remapped = _resolve_artifact_path(path, roots=artifact_roots)
        if remapped:
            remapped_retained_paths.append(remapped)
    retained_paths = tuple(remapped_retained_paths)
    latest_known_path = _normalize_text((record.latest_known_path if record is not None else "") or str(job_dir))
    resource_request = (
        _coerce_resource_dict(payload.get("resource_request"))
        or _coerce_resource_dict(record.resource_request if record is not None else {})
    )
    resource_actual = (
        _coerce_resource_dict(payload.get("resource_actual"))
        or _coerce_resource_dict(record.resource_actual if record is not None else {})
        or dict(resource_request)
    )

    return CrestArtifactContract(
        job_id=job_id,
        mode=mode,
        status=status,
        reason=reason,
        job_dir=str(job_dir),
        latest_known_path=latest_known_path,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        selected_input_xyz=selected_input_xyz,
        retained_conformer_count=retained_count,
        retained_conformer_paths=retained_paths,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def select_crest_downstream_inputs(
    contract: CrestArtifactContract,
    *,
    policy: CrestDownstreamPolicy | None = None,
) -> tuple[WorkflowStageInput, ...]:
    return to_workflow_stage_inputs(contract, policy=policy)


__all__ = [
    "load_crest_artifact_contract",
    "select_crest_downstream_inputs",
]
