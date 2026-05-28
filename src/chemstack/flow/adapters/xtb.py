from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location
from chemstack.core.utils.coercion import coerce_int_mapping

from . import _engine_adapter_helpers as _adapter_helpers
from ..contracts.xtb import (
    WorkflowStageInput,
    XtbArtifactContract,
    XtbCandidateArtifact,
    XtbDownstreamPolicy,
)
from ..xyz_utils import has_xyz_geometry


def _job_type_from_record(record: JobLocationRecord | None, fallback: str) -> str:
    if record is None:
        return fallback
    value = _adapter_helpers.normalize_text(record.job_type)
    if value.startswith("xtb_"):
        value = value[4:]
    return value or fallback


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
    details: list[XtbCandidateArtifact] = []
    for index, path in enumerate(
        _adapter_helpers.normalized_text_sequence(contract_payload.get("selected_candidate_paths")),
        start=1,
    ):
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
        resolve_job_location_fn=resolve_job_location,
        load_json_dict_fn=_adapter_helpers.load_json_dict,
        report_filename="job_report.json",
        state_filename="job_state.json",
        organized_ref_filename="organized_ref.json",
        missing_label="xTB",
        expected_app_name="chemstack_xtb",
        coerce_resource_dict_fn=coerce_int_mapping,
    )
    job_dir = bundle.job_dir
    record = bundle.record
    organized_ref = bundle.organized_ref
    payload = bundle.payload

    candidate_details = _load_candidate_details(payload) or _fallback_details_from_paths(payload)

    selected_candidate_paths = _adapter_helpers.normalized_text_sequence(
        payload.get("selected_candidate_paths")
    ) or tuple(item.path for item in candidate_details if item.selected)

    job_type = _adapter_helpers.first_normalized_text(
        payload.get("job_type"),
        default=_job_type_from_record(record, "unknown"),
    )
    status = _adapter_helpers.first_normalized_text(
        payload.get("status"),
        record.status if record is not None else "",
        default="unknown",
    )
    reason = _adapter_helpers.normalize_text(payload.get("reason"))
    job_id = _adapter_helpers.first_normalized_text(
        payload.get("job_id"),
        record.job_id if record is not None else "",
    )
    reaction_key = _adapter_helpers.first_normalized_text(
        payload.get("reaction_key"),
        record.molecule_key if record is not None else "",
    )
    selected_input_xyz = _adapter_helpers.first_normalized_text(
        payload.get("selected_input_xyz"),
        record.selected_input_xyz if record is not None else "",
    )
    organized_output_dir = _adapter_helpers.first_normalized_text(
        payload.get("organized_output_dir"),
        organized_ref.get("organized_output_dir"),
        record.organized_output_dir if record is not None else "",
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
