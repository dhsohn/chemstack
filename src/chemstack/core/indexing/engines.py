from __future__ import annotations

from .engine_adapters import EngineLocationArtifacts, EngineLocationRoots, EngineLocationStore
from .engine_module_adapter import EngineLocationModule
from .engine_records import (
    EngineLocationRecordRequest,
    EngineLocationSpec,
    build_engine_job_location_record,
    build_job_location_record,
    resource_dict,
)
from .engine_service import EngineLocationService
from .location import JobLocationRecord as JobLocationRecord
from .roots import (
    append_unique_root,
    index_root_for_cfg,
    index_root_for_path,
    list_job_records_for_cfg,
    load_job_artifacts,
    load_job_artifacts_for_cfg,
    lookup_roots_for_target,
    normalize_identifier,
    normalize_text,
    resolve_job_location_for_cfg,
    resolve_latest_job_dir,
    runtime_roots_for_cfg,
)
from .store import (
    get_job_location as get_job_location,
    list_job_locations as list_job_locations,
    resolve_job_location as resolve_job_location,
    upsert_job_location as upsert_job_location,
)

__all__ = [
    "EngineLocationArtifacts",
    "EngineLocationModule",
    "EngineLocationRecordRequest",
    "EngineLocationRoots",
    "EngineLocationService",
    "EngineLocationSpec",
    "EngineLocationStore",
    "append_unique_root",
    "build_engine_job_location_record",
    "build_job_location_record",
    "index_root_for_cfg",
    "index_root_for_path",
    "list_job_records_for_cfg",
    "load_job_artifacts",
    "load_job_artifacts_for_cfg",
    "lookup_roots_for_target",
    "normalize_identifier",
    "normalize_text",
    "resolve_job_location_for_cfg",
    "resolve_latest_job_dir",
    "resource_dict",
    "runtime_roots_for_cfg",
]
