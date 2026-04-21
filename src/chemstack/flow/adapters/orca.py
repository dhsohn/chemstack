from __future__ import annotations

import json
import sys
from functools import lru_cache
from importlib import import_module
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location

from ._orca_contract_assembly import (
    attempt_count_impl,
    coerce_attempts_impl,
    final_result_payload_impl,
    load_orca_artifact_contract_impl,
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
    import_orca_auto_module_impl,
    orca_auto_tracking_module_impl,
    sibling_orca_auto_repo_root_impl,
    tracked_artifact_context_impl,
    tracked_contract_payload_impl,
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

# Keep these names on the facade module so private helper modules and tests can
# monkeypatch the original surface after the implementation split.
_FACADE_COMPAT = (json, sys, import_module, resolve_job_location)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize_text(value).lower() in {"1", "true", "yes", "y", "on"}


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _load_json_dict(path: Path) -> dict[str, Any]:
    return load_json_dict_impl(path)


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    return load_json_list_impl(path)


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    return load_jsonl_records_impl(path)


def _sibling_orca_auto_repo_root() -> Path:
    return sibling_orca_auto_repo_root_impl()


def _import_orca_auto_module(module_name: str) -> Any | None:
    return import_orca_auto_module_impl(module_name)


@lru_cache(maxsize=1)
def _orca_auto_tracking_module() -> Any | None:
    return orca_auto_tracking_module_impl()


def _resolve_candidate_path(path_text: Any) -> Path | None:
    return resolve_candidate_path_impl(path_text)


def _direct_dir_target(target: str) -> Path | None:
    return direct_dir_target_impl(target)


def _tracked_artifact_context(
    *,
    index_root: Path | None,
    targets: tuple[str, ...],
) -> tuple[Path | None, JobLocationRecord | None, dict[str, Any], dict[str, Any], dict[str, Any]]:
    return tracked_artifact_context_impl(index_root=index_root, targets=targets)


def _tracked_runtime_context(
    *,
    index_root: Path | None,
    organized_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> tuple[Path | None, JobLocationRecord | None, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any] | None, Path | None] | None:
    return tracked_runtime_context_impl(
        index_root=index_root,
        organized_root=organized_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )


def _tracked_contract_payload(
    *,
    index_root: Path | None,
    organized_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    return tracked_contract_payload_impl(
        index_root=index_root,
        organized_root=organized_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )


def _resolve_job_dir(index_root: Path | None, target: str) -> tuple[Path | None, JobLocationRecord | None]:
    return resolve_job_dir_impl(index_root, target)


def _find_queue_entry(
    *,
    allowed_root: Path | None,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    return find_queue_entry_impl(
        allowed_root=allowed_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )


def _find_organized_record(
    *,
    organized_root: Path | None,
    target: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    return find_organized_record_impl(
        organized_root=organized_root,
        target=target,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )


def _organized_dir_from_record(organized_root: Path | None, record: dict[str, Any] | None) -> Path | None:
    return organized_dir_from_record_impl(organized_root, record)


def _resolve_artifact_path(path_value: Any, base_dir: Path | None) -> str:
    return resolve_artifact_path_impl(path_value, base_dir)


def _record_organized_dir(record: JobLocationRecord | None) -> Path | None:
    return record_organized_dir_impl(record)


def _load_tracked_organized_ref(record: JobLocationRecord | None, current_dir: Path | None) -> dict[str, Any]:
    return load_tracked_organized_ref_impl(record, current_dir)


def _derive_selected_input_xyz(selected_inp: str) -> str:
    return derive_selected_input_xyz_impl(selected_inp)


def _iter_existing_dirs(*candidates: Path | None) -> list[Path]:
    return iter_existing_dirs_impl(*candidates)


def _is_subpath(candidate: Path, root: Path | None) -> bool:
    return is_subpath_impl(candidate, root)


def _prefer_orca_optimized_xyz(
    *,
    selected_inp: str,
    selected_input_xyz: str,
    current_dir: Path | None,
    organized_dir: Path | None,
    latest_known_path: str,
    last_out_path: str,
) -> str:
    return prefer_orca_optimized_xyz_impl(
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        current_dir=current_dir,
        organized_dir=organized_dir,
        latest_known_path=latest_known_path,
        last_out_path=last_out_path,
    )


def _attempt_count(state: dict[str, Any], report: dict[str, Any]) -> int:
    return attempt_count_impl(
        state,
        report,
        safe_int_fn=lambda value, default: _safe_int(value, default=default),
    )


def _max_retries(state: dict[str, Any], report: dict[str, Any]) -> int:
    return max_retries_impl(
        state,
        report,
        safe_int_fn=lambda value, default: _safe_int(value, default=default),
    )


def _coerce_attempts(state: dict[str, Any], report: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    return coerce_attempts_impl(
        state,
        report,
        normalize_text_fn=_normalize_text,
        safe_int_fn=lambda value, default: _safe_int(value, default=default),
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


def load_orca_artifact_contract(
    *,
    target: str,
    orca_allowed_root: str | Path | None = None,
    orca_organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> OrcaArtifactContract:
    return load_orca_artifact_contract_impl(
        target=target,
        orca_allowed_root=orca_allowed_root,
        orca_organized_root=orca_organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        path_type=Path,
        normalize_text_fn=_normalize_text,
        normalize_bool_fn=_normalize_bool,
        safe_int_fn=lambda value, default: _safe_int(value, default=default),
        tracked_contract_payload_fn=_tracked_contract_payload,
        tracked_runtime_context_fn=_tracked_runtime_context,
        tracked_artifact_context_fn=_tracked_artifact_context,
        resolve_job_dir_fn=_resolve_job_dir,
        find_queue_entry_fn=_find_queue_entry,
        resolve_candidate_path_fn=_resolve_candidate_path,
        direct_dir_target_fn=_direct_dir_target,
        record_organized_dir_fn=_record_organized_dir,
        find_organized_record_fn=_find_organized_record,
        organized_dir_from_record_fn=_organized_dir_from_record,
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


__all__ = [
    "load_orca_artifact_contract",
]
