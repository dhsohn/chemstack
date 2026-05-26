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

_APP_NAME = "crest_auto"
_ENGINE = "crest"
_UNKNOWN_MOLECULE = "unknown_molecule"
_LOCATION_SPEC = _engine_locations.EngineLocationSpec(
    app_name=_APP_NAME,
    job_type_from_payload=lambda mode: job_type_for_mode(mode),
    default_molecule_key=lambda original_run_dir, selected: molecule_key_from_selected_xyz(
        selected,
        original_run_dir,
    ),
    payload_kind_key="mode",
    payload_kind_default="standard",
    molecule_key_name="molecule_key",
)
_LOCATION_API = build_engine_job_location_api(
    engine=_ENGINE,
    spec=_LOCATION_SPEC,
    load_state_fn=load_state,
    load_report_json_fn=load_report_json,
    load_organized_ref_fn=load_organized_ref,
    payload_kind_kwarg="mode",
    molecule_key_kwarg="molecule_key",
    default_payload_kind_kwarg="default_mode",
    get_job_location_fn=lambda: get_job_location,
    list_job_locations_fn=lambda: list_job_locations,
    resolve_job_location_fn=lambda: resolve_job_location,
    upsert_job_location_fn=lambda: upsert_job_location,
    load_state_supplier=lambda: load_state,
    load_report_json_supplier=lambda: load_report_json,
)
_LOCATION_SERVICE = _LOCATION_API.service

_normalize_text = _engine_locations.normalize_text
index_root_for_cfg = _LOCATION_SERVICE.index_root_for_cfg
runtime_roots_for_cfg = _LOCATION_SERVICE.runtime_roots_for_cfg
index_root_for_path = _LOCATION_SERVICE.index_root_for_path
_lookup_roots_for_target = _LOCATION_SERVICE.lookup_roots_for_target


def list_job_records_for_cfg(cfg: AppConfig) -> list[tuple[Path, JobLocationRecord]]:
    return _LOCATION_API.list_job_records_for_cfg(cfg)


def resolve_job_location_for_cfg(
    cfg: AppConfig,
    target: str,
) -> tuple[Path | None, JobLocationRecord | None]:
    return _LOCATION_API.resolve_job_location_for_cfg(cfg, target)


def job_type_for_mode(mode: str) -> str:
    normalized = _normalize_text(mode).lower()
    return "crest_nci_conformer_search" if normalized == "nci" else "crest_standard_conformer_search"


def normalize_molecule_key(value: str) -> str:
    return _engine_locations.normalize_identifier(value, default=_UNKNOWN_MOLECULE)


def molecule_key_from_selected_xyz(selected_input_xyz: str, job_dir: Path) -> str:
    raw = _normalize_text(selected_input_xyz)
    source = Path(raw).name if raw else job_dir.name
    stem = Path(source).stem or job_dir.name
    return normalize_molecule_key(stem)


resource_dict = _engine_locations.resource_dict


def build_job_location_record(
    *,
    existing: JobLocationRecord | None = None,
    job_id: str,
    status: str,
    job_dir: Path,
    mode: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    return _LOCATION_API.build_job_location_record(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        mode=mode,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


_build_job_location_record_from_kind = _LOCATION_SERVICE.build_job_location_record


def upsert_job_record(
    cfg: AppConfig,
    *,
    job_id: str,
    status: str,
    job_dir: Path,
    mode: str,
    selected_input_xyz: str,
    organized_output_dir: Path | None = None,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> JobLocationRecord:
    return _LOCATION_API.upsert_job_record(
        cfg,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        mode=mode,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
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
    default_mode: str = "standard",
) -> JobLocationRecord | None:
    return _LOCATION_API.record_from_artifacts(
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        default_mode=default_mode,
    )


collect_reindex_payload = _LOCATION_SERVICE.collect_reindex_payload
