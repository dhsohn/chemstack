from __future__ import annotations

from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location
from chemstack.core.utils.coercion import (
    normalize_bool as _shared_normalize_bool,
    normalize_text as _shared_normalize_text,
    safe_int as _shared_safe_int,
)

from ._orca_contract_assembly import (
    OrcaContractLoaderDeps,
    contract_from_orca_payload_impl,
    load_orca_artifact_contract_impl,
)
from ._orca_contract_status import (
    attempt_count_impl,
    coerce_attempts_impl,
    final_result_payload_impl,
    max_retries_impl,
    status_from_payloads_impl,
)
from ._orca_local_lookup import (
    find_organized_record_impl,
    find_queue_entry_impl,
    load_json_dict_impl,
    load_json_list_impl,
    load_jsonl_records_impl,
    load_tracked_organized_ref_impl,
    organized_dir_from_record_impl,
    queue_entry_metadata_value_impl,
    record_organized_dir_impl,
    resolve_job_dir_impl,
)
from ._orca_path_helpers import (
    derive_selected_input_xyz_impl,
    direct_dir_target_impl,
    is_subpath_impl,
    iter_existing_dirs_impl,
    prefer_orca_optimized_xyz_impl,
    resolve_artifact_path_impl,
    resolve_candidate_path_impl,
)
from ._orca_tracking import (
    import_orca_module_impl,
    load_orca_contract_payload_impl,
    orca_job_locations_module_impl,
    sibling_orca_repo_root_impl,
    tracked_artifact_context_impl,
    tracked_runtime_context_impl,
)
from ..contracts.orca import OrcaArtifactContract, _coerce_resource_dict

QUEUE_FILE_NAME = "queue.json"
STATE_FILE_NAME = "run_state.json"
REPORT_JSON_FILE_NAME = "run_report.json"
REPORT_MD_FILE_NAME = "run_report.md"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"
INDEX_DIR_NAME = "index"
RECORDS_FILE_NAME = "records.jsonl"


def _normalize_text(value: Any) -> str:
    return _shared_normalize_text(value)


def _normalize_bool(value: Any) -> bool:
    return _shared_normalize_bool(value)


def _safe_int(value: Any, *, default: int = 0) -> int:
    return _shared_safe_int(value, default=default)


def _safe_int_callback(value: Any, default: int = 0) -> int:
    return _shared_safe_int(value, default=default)


_load_json_dict = load_json_dict_impl
_load_json_list = load_json_list_impl
_load_jsonl_records = load_jsonl_records_impl
_sibling_orca_repo_root = sibling_orca_repo_root_impl
_import_orca_module = import_orca_module_impl


_orca_job_locations_module = lru_cache(maxsize=1)(orca_job_locations_module_impl)
_resolve_candidate_path = resolve_candidate_path_impl
_direct_dir_target = direct_dir_target_impl
_tracked_artifact_context = tracked_artifact_context_impl
_tracked_runtime_context = tracked_runtime_context_impl
_tracked_contract_payload = load_orca_contract_payload_impl
_resolve_job_dir = resolve_job_dir_impl
_find_queue_entry = find_queue_entry_impl
_queue_entry_metadata_value = queue_entry_metadata_value_impl
_find_organized_record = find_organized_record_impl
_organized_dir_from_record = organized_dir_from_record_impl
_resolve_artifact_path = resolve_artifact_path_impl
_record_organized_dir = record_organized_dir_impl
_load_tracked_organized_ref = load_tracked_organized_ref_impl
_derive_selected_input_xyz = derive_selected_input_xyz_impl
_iter_existing_dirs = iter_existing_dirs_impl
_is_subpath = is_subpath_impl
_prefer_orca_optimized_xyz = prefer_orca_optimized_xyz_impl


def _attempt_count(state: dict[str, Any], report: dict[str, Any]) -> int:
    return attempt_count_impl(
        state,
        report,
        safe_int_fn=_safe_int_callback,
    )


def _max_retries(state: dict[str, Any], report: dict[str, Any]) -> int:
    return max_retries_impl(
        state,
        report,
        safe_int_fn=_safe_int_callback,
    )


def _coerce_attempts(state: dict[str, Any], report: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    return coerce_attempts_impl(
        state,
        report,
        normalize_text_fn=_normalize_text,
        safe_int_fn=_safe_int_callback,
    )


def _final_result_payload(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    return final_result_payload_impl(state, report)


def _status_from_payloads(
    *,
    queue_entry: dict[str, Any] | None,
    state: dict[str, Any],
    report: dict[str, Any],
) -> tuple[str, str, str, str]:
    return status_from_payloads_impl(
        queue_entry=queue_entry,
        state=state,
        report=report,
        normalize_text_fn=_normalize_text,
        normalize_bool_fn=_normalize_bool,
    )


def _contract_loader_deps() -> OrcaContractLoaderDeps:
    return OrcaContractLoaderDeps(
        path_type=Path,
        normalize_text_fn=_normalize_text,
        normalize_bool_fn=_normalize_bool,
        safe_int_fn=_safe_int_callback,
        tracked_runtime_context_fn=_tracked_runtime_context,
        tracked_artifact_context_fn=_tracked_artifact_context,
        find_queue_entry_fn=_find_queue_entry,
        queue_entry_metadata_value_fn=_queue_entry_metadata_value,
        resolve_candidate_path_fn=_resolve_candidate_path,
        direct_dir_target_fn=_direct_dir_target,
        record_organized_dir_fn=_record_organized_dir,
        load_json_dict_fn=_load_json_dict,
        load_tracked_organized_ref_fn=_load_tracked_organized_ref,
        status_from_payloads_fn=_status_from_payloads,
        resolve_artifact_path_fn=_resolve_artifact_path,
        derive_selected_input_xyz_fn=_derive_selected_input_xyz,
        prefer_orca_optimized_xyz_fn=_prefer_orca_optimized_xyz,
        is_subpath_fn=_is_subpath,
        coerce_resource_dict_fn=_coerce_resource_dict,
        attempt_count_fn=_attempt_count,
        max_retries_fn=_max_retries,
        coerce_attempts_fn=_coerce_attempts,
        final_result_payload_fn=_final_result_payload,
        contract_cls=OrcaArtifactContract,
    )


def load_orca_artifact_contract(
    *,
    target: str,
    orca_allowed_root: str | Path | None = None,
    orca_organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> OrcaArtifactContract:
    deps = _contract_loader_deps()
    allowed_root = Path(orca_allowed_root).expanduser().resolve() if orca_allowed_root else None
    organized_root = (
        Path(orca_organized_root).expanduser().resolve() if orca_organized_root else None
    )
    payload = _tracked_contract_payload(
        index_root=allowed_root,
        organized_root=organized_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    if payload is not None:
        return contract_from_orca_payload_impl(
            payload=payload,
            target=target,
            queue_id=queue_id,
            run_id=run_id,
            reaction_dir=reaction_dir,
            deps=deps,
        )

    return load_orca_artifact_contract_impl(
        target=target,
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=deps,
    )


__all__ = [
    "JobLocationRecord",
    "import_module",
    "load_orca_artifact_contract",
    "resolve_job_location",
]
