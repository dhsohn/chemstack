from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location

from . import _engine_adapter_helpers as _adapter_helpers
from ..contracts.xtb import (
    WorkflowStageInput,
    XtbArtifactContract,
    XtbCandidateArtifact,
    XtbDownstreamPolicy,
    _coerce_resource_dict,
)
from ..xyz_utils import has_xyz_geometry

REPORT_JSON_FILE_NAME = "job_report.json"
STATE_FILE_NAME = "job_state.json"
ORGANIZED_REF_FILE_NAME = "organized_ref.json"


def _normalize_text(value: Any) -> str:
    return _adapter_helpers.normalize_text(value)


def _job_type_from_record(record: JobLocationRecord | None, fallback: str) -> str:
    if record is None:
        return fallback
    value = _normalize_text(record.job_type)
    if value.startswith("xtb_"):
        value = value[4:]
    return value or fallback


def _load_json_dict(path: Path) -> dict[str, Any]:
    return _adapter_helpers.load_json_dict(path)


def _direct_path_target(target: str) -> Path | None:
    return _adapter_helpers.direct_dir_target(target, path_factory=Path)


def _resolve_job_dir(index_root: Path, target: str) -> tuple[Path, JobLocationRecord | None]:
    return _adapter_helpers.resolve_indexed_job_dir(
        index_root,
        target,
        resolve_job_location_fn=resolve_job_location,
        direct_path_target_fn=_direct_path_target,
        missing_label="xTB",
        path_factory=Path,
    )


def _load_candidate_details(payload: dict[str, Any]) -> tuple[XtbCandidateArtifact, ...]:
    raw_items = payload.get("candidate_details")
    if not isinstance(raw_items, list):
        return ()
    details: list[XtbCandidateArtifact] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            continue
        detail = XtbCandidateArtifact.from_raw(raw)
        if detail.path:
            details.append(detail)
    return tuple(details)


def _fallback_details_from_paths(
    contract_payload: dict[str, Any],
) -> tuple[XtbCandidateArtifact, ...]:
    raw_paths = contract_payload.get("selected_candidate_paths")
    if not isinstance(raw_paths, list):
        return ()
    details: list[XtbCandidateArtifact] = []
    for index, raw in enumerate(raw_paths, start=1):
        path = _normalize_text(raw)
        if not path:
            continue
        details.append(
            XtbCandidateArtifact(
                rank=index,
                kind="candidate",
                path=path,
                selected=True,
            )
        )
    return tuple(details)


def _ordered_xtb_candidate_details(
    contract: XtbArtifactContract,
    policy: XtbDownstreamPolicy,
    *,
    require_geometry: bool,
) -> list[XtbCandidateArtifact]:
    selected_order = {path: index for index, path in enumerate(contract.selected_candidate_paths)}
    kind_priority = {kind: index for index, kind in enumerate(policy.preferred_kinds)}
    allowed_kinds = {kind for kind in policy.allowed_kinds if kind}
    details = list(contract.candidate_details)

    if policy.selected_only:
        filtered = [item for item in details if item.selected or item.path in selected_order]
        details = filtered or details

    def _sort_key(item: XtbCandidateArtifact) -> tuple[int, int, int, str]:
        path_rank = selected_order.get(item.path, 10_000)
        kind_rank = kind_priority.get(item.kind, len(kind_priority) + 100)
        item_rank = item.rank if item.rank > 0 else 10_000
        return (path_rank, kind_rank, item_rank, item.path)

    details = sorted(details, key=_sort_key)
    if policy.preferred_kinds:
        preferred = [item for item in details if item.kind in kind_priority]
        other = [item for item in details if item.kind not in kind_priority]
        details = preferred + other
    if allowed_kinds:
        details = [item for item in details if item.kind in allowed_kinds]
    if require_geometry:
        details = [item for item in details if has_xyz_geometry(item.path)]
    return details


def _stage_input_from_xtb_candidate(
    contract: XtbArtifactContract,
    detail: XtbCandidateArtifact,
) -> WorkflowStageInput:
    return WorkflowStageInput(
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        selected_input_xyz=contract.selected_input_xyz,
        rank=detail.rank,
        kind=detail.kind,
        artifact_path=detail.path,
        selected=detail.selected,
        score=detail.score,
        metadata=dict(detail.metadata),
    )


def _fallback_xtb_downstream_inputs(
    contract: XtbArtifactContract,
    policy: XtbDownstreamPolicy,
    *,
    require_geometry: bool,
) -> tuple[WorkflowStageInput, ...]:
    fallback_paths = contract.selected_candidate_paths
    if require_geometry:
        fallback_paths = tuple(path for path in fallback_paths if has_xyz_geometry(path))

    fallback: list[WorkflowStageInput] = []
    for index, path in enumerate(fallback_paths, start=1):
        fallback.append(
            WorkflowStageInput(
                source_job_id=contract.job_id,
                source_job_type=contract.job_type,
                reaction_key=contract.reaction_key,
                selected_input_xyz=contract.selected_input_xyz,
                rank=index,
                kind="candidate",
                artifact_path=path,
                selected=True,
                metadata={},
            )
        )
        if len(fallback) >= policy.max_candidates:
            break
    return tuple(fallback)


def load_xtb_artifact_contract(*, xtb_index_root: str | Path, target: str) -> XtbArtifactContract:
    bundle = _adapter_helpers.load_contract_artifact_bundle(
        index_root=xtb_index_root,
        target=target,
        resolve_job_dir_fn=_resolve_job_dir,
        load_json_dict_fn=_load_json_dict,
        report_filename=REPORT_JSON_FILE_NAME,
        state_filename=STATE_FILE_NAME,
        organized_ref_filename=ORGANIZED_REF_FILE_NAME,
        missing_label="xTB",
        expected_app_name="xtb_auto",
        coerce_resource_dict_fn=_coerce_resource_dict,
    )
    job_dir = bundle.job_dir
    record = bundle.record
    organized_ref = bundle.organized_ref
    payload = bundle.payload

    candidate_details = _load_candidate_details(payload) or _fallback_details_from_paths(payload)

    selected_candidate_paths_raw = payload.get("selected_candidate_paths")
    selected_candidate_paths: tuple[str, ...]
    if isinstance(selected_candidate_paths_raw, list):
        selected_candidate_paths = tuple(
            _normalize_text(item) for item in selected_candidate_paths_raw if _normalize_text(item)
        )
    else:
        selected_candidate_paths = tuple(item.path for item in candidate_details if item.selected)

    job_type = _normalize_text(payload.get("job_type")) or _job_type_from_record(record, "unknown")
    status = (
        _normalize_text(payload.get("status") or (record.status if record is not None else ""))
        or "unknown"
    )
    reason = _normalize_text(payload.get("reason"))
    job_id = _normalize_text(payload.get("job_id") or (record.job_id if record is not None else ""))
    reaction_key = _normalize_text(
        payload.get("reaction_key") or (record.molecule_key if record is not None else "")
    )
    selected_input_xyz = _normalize_text(
        payload.get("selected_input_xyz")
        or (record.selected_input_xyz if record is not None else "")
    )
    organized_output_dir = _normalize_text(
        payload.get("organized_output_dir")
        or organized_ref.get("organized_output_dir")
        or (record.organized_output_dir if record is not None else "")
    )
    latest_known_path = bundle.latest_known_path

    analysis_summary = payload.get("analysis_summary")
    if not isinstance(analysis_summary, dict):
        analysis_summary = {}

    return XtbArtifactContract(
        job_id=job_id,
        job_type=job_type,
        status=status,
        reason=reason,
        job_dir=str(job_dir),
        latest_known_path=latest_known_path,
        organized_output_dir=organized_output_dir,
        reaction_key=reaction_key,
        selected_input_xyz=selected_input_xyz,
        selected_candidate_paths=selected_candidate_paths,
        candidate_details=candidate_details,
        analysis_summary=dict(analysis_summary),
        resource_request=bundle.resource_request,
        resource_actual=bundle.resource_actual,
    )


def select_xtb_downstream_inputs(
    contract: XtbArtifactContract,
    *,
    policy: XtbDownstreamPolicy | None = None,
    require_geometry: bool = False,
) -> tuple[WorkflowStageInput, ...]:
    active_policy = policy or XtbDownstreamPolicy.build()
    details = _ordered_xtb_candidate_details(
        contract, active_policy, require_geometry=require_geometry
    )

    selected_inputs: list[WorkflowStageInput] = []
    for detail in details:
        selected_inputs.append(_stage_input_from_xtb_candidate(contract, detail))
        if len(selected_inputs) >= active_policy.max_candidates:
            break

    if selected_inputs or not active_policy.fallback_to_selected_paths:
        return tuple(selected_inputs)
    return _fallback_xtb_downstream_inputs(
        contract, active_policy, require_geometry=require_geometry
    )


__all__ = [
    "load_xtb_artifact_contract",
    "select_xtb_downstream_inputs",
]
