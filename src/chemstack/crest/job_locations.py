from __future__ import annotations

from pathlib import Path

from chemstack.core.indexing.engine_job_locations import build_store_backed_engine_job_location_api
from chemstack.core.indexing import engines as _engine_locations

from .state import load_organized_ref, load_report_json, load_state

_APP_NAME = "chemstack_crest"
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
_LOCATION_API = build_store_backed_engine_job_location_api(
    engine=_ENGINE,
    spec=_LOCATION_SPEC,
    load_state_fn=load_state,
    load_report_json_fn=load_report_json,
    load_organized_ref_fn=load_organized_ref,
    payload_kind_kwarg="mode",
    molecule_key_kwarg="molecule_key",
    default_payload_kind_kwarg="default_mode",
)
_LOCATION_SERVICE = _LOCATION_API.service

_normalize_text = _engine_locations.normalize_text
index_root_for_cfg = _LOCATION_SERVICE.index_root_for_cfg
runtime_roots_for_cfg = _LOCATION_SERVICE.runtime_roots_for_cfg
index_root_for_path = _LOCATION_SERVICE.index_root_for_path
list_job_records_for_cfg = _LOCATION_API.list_job_records_for_cfg
resolve_job_location_for_cfg = _LOCATION_API.resolve_job_location_for_cfg


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


build_job_location_record = _LOCATION_API.build_job_location_record
upsert_job_record = _LOCATION_API.upsert_job_record
resolve_latest_job_dir = _LOCATION_API.resolve_latest_job_dir
load_job_artifacts = _LOCATION_API.load_job_artifacts
load_job_artifacts_for_cfg = _LOCATION_API.load_job_artifacts_for_cfg
record_from_artifacts = _LOCATION_API.record_from_artifacts
