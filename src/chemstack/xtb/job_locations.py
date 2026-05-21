from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import (
    JobLocationRecord,
    get_job_location,
    list_job_locations,
    resolve_job_location,
    upsert_job_location,
)
from chemstack.core.indexing.engine_job_locations import build_engine_job_location_api
from chemstack.core.indexing import engines as _engine_locations

from .config import AppConfig
from .state import load_organized_ref, load_report_json, load_state

_APP_NAME = "xtb_auto"
_ENGINE = "xtb"
_UNKNOWN_KEY = "unknown_key"
_LOCATION_SPEC = _engine_locations.EngineLocationSpec(
    app_name=_APP_NAME,
    job_type_from_payload=lambda job_type: job_type_identifier(job_type),
    default_molecule_key=lambda original_run_dir, _selected: reaction_key_from_job_dir(original_run_dir),
    payload_kind_key="job_type",
    payload_kind_default="path_search",
    molecule_key_name="reaction_key",
)
_LOCATION_API = build_engine_job_location_api(
    engine=_ENGINE,
    spec=_LOCATION_SPEC,
    load_state_fn=load_state,
    load_report_json_fn=load_report_json,
    load_organized_ref_fn=load_organized_ref,
    payload_kind_kwarg="job_type",
    molecule_key_kwarg="reaction_key",
    default_payload_kind_kwarg="default_job_type",
    get_job_location_fn=lambda: get_job_location,
    list_job_locations_fn=lambda: list_job_locations,
    resolve_job_location_fn=lambda: resolve_job_location,
    upsert_job_location_fn=lambda: upsert_job_location,
    load_state_supplier=lambda: load_state,
    load_report_json_supplier=lambda: load_report_json,
)
_LOCATION_FACADE = _LOCATION_API.facade

_normalize_text = _engine_locations.normalize_text
index_root_for_cfg = _LOCATION_FACADE.index_root_for_cfg
runtime_roots_for_cfg = _LOCATION_FACADE.runtime_roots_for_cfg
index_root_for_path = _LOCATION_FACADE.index_root_for_path
_lookup_roots_for_target = _LOCATION_FACADE.lookup_roots_for_target


def list_job_records_for_cfg(cfg: AppConfig) -> list[tuple[Path, JobLocationRecord]]:
    return _LOCATION_API.list_job_records_for_cfg(cfg)


def resolve_job_location_for_cfg(
    cfg: AppConfig,
    target: str,
) -> tuple[Path | None, JobLocationRecord | None]:
    return _LOCATION_API.resolve_job_location_for_cfg(cfg, target)


def job_type_identifier(job_type: str) -> str:
    normalized = _normalize_text(job_type).lower() or "unknown"
    return f"xtb_{normalized}"


def normalize_key(value: str) -> str:
    return _engine_locations.normalize_identifier(value, default=_UNKNOWN_KEY)


def reaction_key_from_job_dir(job_dir: Path) -> str:
    return normalize_key(job_dir.name)


def reaction_key_from_selected_xyz(selected_input_xyz: str, job_dir: Path) -> str:
    raw = _normalize_text(selected_input_xyz)
    if raw:
        stem = Path(raw).stem.strip()
        if stem and stem.lower() not in {"r1", "reactant", "input"}:
            return normalize_key(stem)
    return reaction_key_from_job_dir(job_dir)


resource_dict = _engine_locations.resource_dict


def build_job_location_record(
    *,
    existing: JobLocationRecord | None = None,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    reaction_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    return _LOCATION_API.build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


_build_job_location_record_from_kind = _LOCATION_FACADE.build_job_location_record


def upsert_job_record(
    cfg: AppConfig,
    *,
    job_id: str,
    status: str,
    job_dir: Path,
    job_type: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    reaction_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    return _LOCATION_API.upsert_job_record(
        cfg,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    return _LOCATION_API.resolve_latest_job_dir(index_root, target)


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    return _LOCATION_API.load_job_artifacts(index_root, target)


def load_job_artifacts_for_cfg(
    cfg: AppConfig,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None, JobLocationRecord | None]:
    return _LOCATION_API.load_job_artifacts_for_cfg(cfg, target)


is_terminal_status = _engine_locations.is_terminal_status


def record_from_artifacts(
    *,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    default_job_type: str = "path_search",
) -> JobLocationRecord | None:
    return _LOCATION_API.record_from_artifacts(
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        default_job_type=default_job_type,
    )


collect_reindex_payload = _LOCATION_FACADE.collect_reindex_payload
