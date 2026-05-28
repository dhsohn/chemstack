from __future__ import annotations

from functools import partial
from pathlib import Path

from chemstack.core.indexing import resolve_job_location
from chemstack.core.utils.coercion import coerce_int_mapping

from . import _engine_adapter_helpers as _adapter_helpers
from ..contracts.crest import CrestArtifactContract, CrestDownstreamPolicy, to_workflow_stage_inputs
from ..contracts.xtb import WorkflowStageInput

_ACTIVE_PAYLOAD_STATUSES = frozenset({"queued", "running", "submitted", "cancel_requested", "retrying"})


def load_crest_artifact_contract(*, crest_index_root: str | Path, target: str) -> CrestArtifactContract:
    bundle = _adapter_helpers.load_contract_artifact_bundle(
        index_root=crest_index_root,
        target=target,
        resolve_job_location_fn=resolve_job_location,
        load_json_dict_fn=_adapter_helpers.load_json_dict,
        report_filename="job_report.json",
        state_filename="job_state.json",
        organized_ref_filename="organized_ref.json",
        missing_label="CREST",
        expected_app_name="chemstack_crest",
        coerce_resource_dict_fn=coerce_int_mapping,
        select_payload_fn=partial(
            _adapter_helpers.select_active_artifact_payload,
            active_statuses=_ACTIVE_PAYLOAD_STATUSES,
        ),
    )
    job_dir = bundle.job_dir
    record = bundle.record
    organized_ref = bundle.organized_ref
    payload = bundle.payload

    retained_paths = _adapter_helpers.normalized_text_sequence(
        payload.get("retained_conformer_paths")
    )
    retained_count = int(payload.get("retained_conformer_count", len(retained_paths)) or len(retained_paths))
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
    mode = _adapter_helpers.first_normalized_text(
        payload.get("mode"),
        record.job_type if record is not None else "",
        default="standard",
    )
    molecule_key = _adapter_helpers.first_normalized_text(
        payload.get("molecule_key"),
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
    artifact_roots = _adapter_helpers.artifact_roots(job_dir, organized_output_dir)
    selected_input_xyz = _adapter_helpers.resolve_artifact_path(
        selected_input_xyz, roots=artifact_roots
    )
    remapped_retained_paths: list[str] = []
    for path in retained_paths:
        remapped = _adapter_helpers.resolve_artifact_path(path, roots=artifact_roots)
        if remapped:
            remapped_retained_paths.append(remapped)
    retained_paths = tuple(remapped_retained_paths)
    latest_known_path = bundle.latest_known_path

    return CrestArtifactContract(
        job_id=job_id,
        mode=mode,
        status=status,
        reason=reason,
        job_dir=str(job_dir),
        latest_known_path=latest_known_path,
        organized_output_dir=organized_output_dir,
        molecule_key=molecule_key,
        selected_input_xyz=selected_input_xyz,
        retained_conformer_count=retained_count,
        retained_conformer_paths=retained_paths,
        resource_request=bundle.resource_request,
        resource_actual=bundle.resource_actual,
    )


def select_crest_downstream_inputs(
    contract: CrestArtifactContract,
    *,
    policy: CrestDownstreamPolicy | None = None,
) -> tuple[WorkflowStageInput, ...]:
    return to_workflow_stage_inputs(contract, policy=policy)


__all__ = [
    "load_crest_artifact_contract",
    "select_crest_downstream_inputs",
]
