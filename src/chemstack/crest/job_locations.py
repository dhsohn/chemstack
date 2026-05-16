from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, get_job_location, list_job_locations, resolve_job_location, upsert_job_location
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


def _normalize_text(value: Any) -> str:
    return _engine_locations.normalize_text(value)


def index_root_for_cfg(cfg: AppConfig) -> Path:
    return _engine_locations.index_root_for_cfg(cfg)


def runtime_roots_for_cfg(cfg: AppConfig) -> tuple[Path, ...]:
    return _engine_locations.runtime_roots_for_cfg(cfg, engine=_ENGINE)


def index_root_for_path(
    cfg: AppConfig,
    *paths: str | Path | None,
) -> Path:
    return _engine_locations.index_root_for_path(cfg, *paths, engine=_ENGINE)


def _lookup_roots_for_target(cfg: AppConfig, target: str) -> tuple[Path, ...]:
    return _engine_locations.lookup_roots_for_target(cfg, target, engine=_ENGINE)


def list_job_records_for_cfg(cfg: AppConfig) -> list[tuple[Path, JobLocationRecord]]:
    return _engine_locations.list_job_records_for_cfg(
        cfg,
        engine=_ENGINE,
        list_job_locations_fn=list_job_locations,
    )


def resolve_job_location_for_cfg(
    cfg: AppConfig,
    target: str,
) -> tuple[Path | None, JobLocationRecord | None]:
    return _engine_locations.resolve_job_location_for_cfg(
        cfg,
        target,
        engine=_ENGINE,
        resolve_job_location_fn=resolve_job_location,
    )


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


def resource_dict(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return _engine_locations.resource_dict(max_cores, max_memory_gb)


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
    return _build_job_location_record_from_kind(
        existing=existing,
        job_id=job_id,
        status=status,
        job_dir=job_dir,
        payload_kind=mode,
        selected_input_xyz=selected_input_xyz,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


_build_job_location_record_from_kind = _engine_locations.make_engine_record_builder(_LOCATION_SPEC)


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
    root = index_root_for_path(cfg, job_dir, organized_output_dir)
    existing = get_job_location(root, job_id)
    record = build_job_location_record(
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
    return upsert_job_location(root, record)


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    return _engine_locations.resolve_latest_job_dir(
        index_root,
        target,
        resolve_job_location_fn=resolve_job_location,
    )


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    return _engine_locations.load_job_artifacts(
        index_root,
        target,
        load_state_fn=load_state,
        load_report_json_fn=load_report_json,
        resolve_latest_job_dir_fn=resolve_latest_job_dir,
    )


def load_job_artifacts_for_cfg(
    cfg: AppConfig,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None, JobLocationRecord | None]:
    return _engine_locations.load_job_artifacts_for_cfg(
        cfg,
        target,
        engine=_ENGINE,
        load_state_fn=load_state,
        load_report_json_fn=load_report_json,
        resolve_latest_job_dir_fn=resolve_latest_job_dir,
        resolve_job_location_fn=resolve_job_location,
    )


def is_terminal_status(status: str) -> bool:
    return _engine_locations.is_terminal_status(status)


def record_from_artifacts(
    *,
    job_dir: Path,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
    existing: JobLocationRecord | None = None,
    default_mode: str = "standard",
) -> JobLocationRecord | None:
    return _engine_locations.engine_record_from_artifacts(
        spec=_LOCATION_SPEC,
        build_record_fn=_build_job_location_record_from_kind,
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
        existing=existing,
        default_payload_kind=default_mode,
    )


def collect_reindex_payload(job_dir: Path) -> dict[str, Any] | None:
    return _engine_locations.collect_engine_reindex_payload_for_dir(
        spec=_LOCATION_SPEC,
        job_dir=job_dir,
        load_state_fn=load_state,
        load_report_json_fn=load_report_json,
        load_organized_ref_fn=load_organized_ref,
    )
