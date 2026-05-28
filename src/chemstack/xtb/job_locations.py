from __future__ import annotations

from pathlib import Path

from chemstack.core.indexing.engine_job_locations import build_store_backed_engine_job_location_api
from chemstack.core.indexing import engines as _engine_locations

from .state import load_organized_ref, load_report_json, load_state


def job_type_identifier(job_type: str) -> str:
    normalized = _engine_locations.normalize_text(job_type).lower() or "unknown"
    return f"xtb_{normalized}"


def normalize_key(value: str) -> str:
    return _engine_locations.normalize_identifier(value, default="unknown_key")


def reaction_key_from_job_dir(job_dir: Path) -> str:
    return normalize_key(job_dir.name)


def reaction_key_from_selected_xyz(selected_input_xyz: str, job_dir: Path) -> str:
    raw = _engine_locations.normalize_text(selected_input_xyz)
    if raw:
        stem = Path(raw).stem.strip()
        if stem and stem.lower() not in {"r1", "reactant", "input"}:
            return normalize_key(stem)
    return reaction_key_from_job_dir(job_dir)


_LOCATION_API = build_store_backed_engine_job_location_api(
    engine="xtb",
    spec=_engine_locations.EngineLocationSpec(
        app_name="chemstack_xtb",
        job_type_from_payload=job_type_identifier,
        default_molecule_key=lambda original_run_dir, _selected: reaction_key_from_job_dir(
            original_run_dir
        ),
        payload_kind_key="job_type",
        payload_kind_default="path_search",
        molecule_key_name="reaction_key",
    ),
    load_state_fn=load_state,
    load_report_json_fn=load_report_json,
    load_organized_ref_fn=load_organized_ref,
    payload_kind_kwarg="job_type",
    molecule_key_kwarg="reaction_key",
    default_payload_kind_kwarg="default_job_type",
)

index_root_for_cfg = _LOCATION_API.service.index_root_for_cfg
runtime_roots_for_cfg = _LOCATION_API.service.runtime_roots_for_cfg
index_root_for_path = _LOCATION_API.service.index_root_for_path
list_job_records_for_cfg = _LOCATION_API.list_job_records_for_cfg
resolve_job_location_for_cfg = _LOCATION_API.resolve_job_location_for_cfg

build_job_location_record = _LOCATION_API.build_job_location_record
upsert_job_record = _LOCATION_API.upsert_job_record
resolve_latest_job_dir = _LOCATION_API.resolve_latest_job_dir
load_job_artifacts = _LOCATION_API.load_job_artifacts
load_job_artifacts_for_cfg = _LOCATION_API.load_job_artifacts_for_cfg
record_from_artifacts = _LOCATION_API.record_from_artifacts
