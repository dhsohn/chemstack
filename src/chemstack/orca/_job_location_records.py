from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME
from chemstack.core.indexing import (
    JobLocationRecord,
    get_job_location,
    list_job_locations,
    upsert_job_location,
)
from chemstack.core.indexing import engine_artifacts as _engine_artifacts
from chemstack.core.indexing import engines as _engine_locations

from ._job_location_utils import (
    TERMINAL_STATUSES,
    derive_selected_input_xyz,
    normalize_path_text,
    normalize_text,
    resource_dict_from_any,
)
from .config import AppConfig
from .molecule_key import resolve_molecule_key
from .result_organizer import detect_job_type
from .state import load_organized_ref, load_report_json, load_state

_MOLECULE_KEY_RE = re.compile(r"[^A-Za-z0-9._-]+")


def index_root_for_cfg(cfg: AppConfig) -> Path:
    return Path(cfg.runtime.allowed_root).expanduser().resolve()


def job_type_identifier(job_type: str) -> str:
    normalized = normalize_text(job_type).lower()
    if normalized.startswith("orca_"):
        return normalized
    return f"orca_{normalized or 'other'}"


def normalize_molecule_key(value: str) -> str:
    collapsed = _MOLECULE_KEY_RE.sub("_", normalize_text(value)).strip("._-")
    return collapsed or "unknown"


def molecule_key_from_selected_inp(selected_inp: str, job_dir: Path) -> str:
    raw = normalize_text(selected_inp)
    if raw:
        try:
            candidate = Path(raw).expanduser()
            resolved = candidate.resolve()
        except OSError:
            resolved = None
        if resolved is not None and resolved.exists():
            return resolve_molecule_key(resolved).key
        stem = Path(raw).stem.strip()
        if stem:
            return normalize_molecule_key(stem)
    return normalize_molecule_key(job_dir.name)


def resolve_job_metadata(selected_inp: str, job_dir: Path) -> tuple[str, str]:
    job_type = "other"
    raw = normalize_text(selected_inp)
    if raw:
        try:
            candidate = Path(raw).expanduser()
            resolved = candidate.resolve()
        except OSError:
            resolved = None
        if resolved is not None and resolved.exists():
            job_type = detect_job_type(resolved)
    molecule_key = molecule_key_from_selected_inp(raw, job_dir)
    return job_type, molecule_key


def resource_dict(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return _engine_locations.resource_dict(max_cores, max_memory_gb)


def build_job_location_record(
    *,
    existing: JobLocationRecord | None = None,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    selected_input_text = normalize_path_text(selected_input_xyz)
    return _engine_locations.build_job_location_record(
        existing=existing,
        job_id=job_id,
        app_name=CHEMSTACK_ORCA_APP_NAME,
        job_type=job_type_identifier(job_type),
        status=status or "unknown",
        job_dir=job_dir,
        selected_input_xyz=selected_input_text,
        molecule_key=molecule_key,
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
        default_molecule_key_fn=lambda original_run_dir, selected: molecule_key_from_selected_inp(
            selected,
            original_run_dir,
        ),
    )


def upsert_job_record(
    cfg: AppConfig,
    *,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    root = index_root_for_cfg(cfg)
    existing = get_job_location(root, job_id)
    record = build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    return upsert_job_location(root, record)


def list_job_location_records(index_root: str | Path) -> list[JobLocationRecord]:
    return list(list_job_locations(index_root))


def resolve_record_job_dir(record: JobLocationRecord) -> Path | None:
    for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            resolved = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if resolved.exists() and resolved.is_dir():
            return resolved
    return None


def is_terminal_status(status: str) -> bool:
    return normalize_text(status).lower() in TERMINAL_STATUSES


def _artifact_record_identity(
    *,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
    fallback_job_id: str,
) -> tuple[str, str, str]:
    sources = (report, state, organized_ref)
    job_id = (
        _engine_artifacts.first_artifact_text(sources, "job_id")
        or normalize_text(fallback_job_id)
        or normalize_text(existing.job_id if existing else "")
        or _engine_artifacts.first_artifact_text(sources, "run_id")
    )
    status = (
        _engine_artifacts.first_artifact_text(sources, "status")
        or "unknown"
    )
    selected_inp = normalize_path_text(
        _engine_artifacts.first_artifact_value((report, state, organized_ref), "selected_inp")
    )
    selected_input_xyz = normalize_path_text(
        _engine_artifacts.first_artifact_value(
            (report, state, organized_ref),
            "selected_input_xyz",
        )
    )
    if not selected_input_xyz.lower().endswith(".xyz"):
        selected_input_xyz = derive_selected_input_xyz(selected_inp)
    selected_input_xyz = selected_input_xyz or selected_inp or (
        existing.selected_input_xyz if existing else ""
    )
    return job_id, status, selected_input_xyz


def _artifact_job_metadata(
    *,
    job_dir: Path,
    selected_input_xyz: str,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
    default_job_type: str,
) -> tuple[str, str]:
    derived_job_type, derived_molecule_key = resolve_job_metadata(selected_input_xyz, job_dir)
    job_type = (
        normalize_text(
            report.get("job_type")
            or state.get("job_type")
            or organized_ref.get("job_type")
            or derived_job_type
            or default_job_type
        )
        or default_job_type
    )
    molecule_key = normalize_text(
        report.get("molecule_key")
        or state.get("molecule_key")
        or organized_ref.get("molecule_key")
        or (existing.molecule_key if existing else "")
        or derived_molecule_key
    )
    return job_type, molecule_key


def _artifact_resources(
    *,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
) -> tuple[dict[str, int], dict[str, int]]:
    return _engine_artifacts.artifact_resources(
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        resource_mapping_fn=resource_dict_from_any,
    )


def _artifact_dirs(
    *,
    job_dir: Path,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    existing: JobLocationRecord | None,
) -> tuple[str, str]:
    sources = (report, state, organized_ref)
    original_run_dir = (
        _engine_artifacts.first_artifact_text(sources, "original_run_dir")
        or normalize_text(existing.original_run_dir if existing else "")
        or str(job_dir)
    )
    organized_output_dir = (
        _engine_artifacts.first_artifact_text(sources, "organized_output_dir")
        or normalize_text(existing.organized_output_dir if existing else "")
    )
    return original_run_dir, organized_output_dir


def record_from_artifacts(
    *,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    fallback_job_id: str = "",
    default_job_type: str = "other",
) -> JobLocationRecord | None:
    state = state or {}
    report = report or {}
    organized_ref = organized_ref or {}

    job_id, status, selected_input_xyz = _artifact_record_identity(
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        fallback_job_id=fallback_job_id,
    )
    if not job_id:
        return None

    job_type, molecule_key = _artifact_job_metadata(
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        default_job_type=default_job_type,
    )
    resource_request, resource_actual = _artifact_resources(
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
    )
    original_run_dir, organized_output_dir = _artifact_dirs(
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
    )

    return build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=Path(original_run_dir),
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=Path(organized_output_dir).expanduser().resolve()
        if organized_output_dir
        else None,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def collect_reindex_payload(job_dir: Path) -> dict[str, Any] | None:
    resolved_job_dir = job_dir.expanduser().resolve()
    state_data = load_state(resolved_job_dir)
    state = dict(state_data) if state_data is not None else {}
    report = load_report_json(resolved_job_dir) or {}
    organized_ref = load_organized_ref(resolved_job_dir) or {}

    record = record_from_artifacts(
        job_dir=resolved_job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
    )
    if record is None:
        return None

    return {
        "job_id": record.job_id,
        "status": record.status,
        "job_type": record.job_type,
        "job_dir": record.original_run_dir,
        "selected_input_xyz": record.selected_input_xyz,
        "molecule_key": record.molecule_key,
        "organized_output_dir": record.organized_output_dir,
        "resource_request": dict(record.resource_request),
        "resource_actual": dict(record.resource_actual),
    }


def reindex_job_locations(cfg: AppConfig) -> int:
    root = index_root_for_cfg(cfg)
    if not root.exists():
        return 0

    candidate_dirs: set[Path] = set()
    for pattern in ("run_state.json", "run_report.json", "organized_ref.json"):
        for artifact in root.rglob(pattern):
            candidate_dirs.add(artifact.parent)

    updated = 0
    for job_dir in sorted(candidate_dirs):
        state_data = load_state(job_dir)
        state = dict(state_data) if state_data is not None else None
        report = load_report_json(job_dir)
        organized_ref = load_organized_ref(job_dir)
        record = record_from_artifacts(
            job_dir=job_dir,
            state=state,
            report=report,
            organized_ref=organized_ref,
        )
        if record is None:
            continue
        upsert_job_location(root, record)
        updated += 1
    return updated
