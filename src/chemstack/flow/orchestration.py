from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from chemstack.core.utils import (
    normalize_text,
    now_utc_iso,
    timestamped_token,
)

from ._orchestration_advance import (
    advance_workflow,
    cancel_materialized_workflow,
)
from ._orchestration_builders import (
    _copy_input_impl,
    new_crest_stage_impl,
    new_xtb_stage_impl,
)
from . import orchestration_factories as _workflow_factories
from .orchestration_factories import WorkflowFactoryDeps
from ._orchestration_requests import (
    ConformerScreeningWorkflowRequest,
    ReactionTsSearchWorkflowRequest,
)
from ._orchestration_deps import OrchestrationDeps
from ._orchestration_lifecycle import (
    effective_stage_status_impl,
    recompute_workflow_status_impl,
    stage_failure_is_recoverable_impl,
    workflow_has_active_children_impl,
    workflow_sync_only_impl,
)
from ._orchestration_stage_materialization import (
    append_crest_orca_stages_impl,
    append_reaction_orca_stages_impl,
    append_reaction_xtb_stages_impl,
)
from ._orchestration_stage_runtime_crest import (
    completed_crest_roles_impl,
    completed_crest_stage_impl,
    ensure_crest_job_dir_impl,
    sync_crest_stage_impl,
)
from ._orchestration_stage_runtime_orca import (
    completed_orca_stage_impl,
    sync_orca_stage_impl,
)
from ._orchestration_stage_runtime_shared import append_unique_artifact_impl
from ._orchestration_stage_runtime_xtb_handoff import (
    stage_has_xtb_candidates_impl,
    xtb_handoff_status_impl,
)
from ._orchestration_stage_runtime_xtb_path_jobs import (
    ensure_xtb_job_dir_impl,
    write_xtb_path_job_impl,
)
from ._orchestration_stage_runtime_xtb_retry import (
    xtb_attempt_record_impl,
    xtb_attempt_rows_impl,
    xtb_current_attempt_number_impl,
    xtb_path_retry_limit_impl,
    xtb_retry_recipe_impl,
)
from ._orchestration_stage_runtime_xtb_sync import sync_xtb_stage_impl
from ._orchestration_support import (
    clear_reaction_xtb_handoff_error_if_recovering_impl,
    load_config_organized_root_impl,
    load_config_root_impl,
    reaction_orca_allows_next_candidate_impl,
    reaction_orca_source_candidate_path_impl,
    reaction_ts_guess_error_impl,
    stage_metadata_impl,
    submission_target_impl,
    task_payload_dict_impl,
)
from ._workflow_phases import phase_finished
from .adapters import (
    load_crest_artifact_contract,
    load_orca_artifact_contract,
    load_xtb_artifact_contract,
    select_crest_downstream_inputs,
    select_xtb_downstream_inputs,
)
from .contracts import (
    CrestDownstreamPolicy,
    WorkflowStageInput,
    XtbDownstreamPolicy,
)
from .endpoint_pairing import EndpointPairingPolicy, select_endpoint_pairs
from .registry import sync_workflow_registry
from .state import (
    acquire_workflow_lock,
    load_workflow_payload,
    resolve_workflow_workspace,
    workflow_has_active_downstream,
    write_workflow_payload,
)
from .submitters.common import sibling_allowed_root, sibling_runtime_paths
from .submitters.crest import (
    cancel_target as crest_cancel_target,
    submit_job_dir as submit_crest_job_dir,
)
from .submitters.orca import (
    cancel_target as orca_cancel_target,
    submit_reaction_dir,
)
from .submitters.xtb import (
    cancel_target as xtb_cancel_target,
    submit_job_dir as submit_xtb_job_dir,
)
from .workflow_notifications import maybe_notify_workflow_phase_summary
from ._orca_stage_materialization import build_materialized_orca_stage, safe_name
from .xyz_utils import choose_orca_geometry_frame, load_xyz_atom_sequence

def _write_workflow_payload_side_effect(workspace_dir: Path, payload: dict[str, Any]) -> None:
    write_workflow_payload(workspace_dir, payload)


def _sync_workflow_registry_side_effect(
    workflow_root: Path,
    workspace_dir: Path,
    payload: dict[str, Any],
) -> None:
    sync_workflow_registry(workflow_root, workspace_dir, payload)


def _workflow_factory_deps() -> WorkflowFactoryDeps:
    return WorkflowFactoryDeps(
        normalize_text=normalize_text,
        workflow_id_factory=timestamped_token,
        copy_input_fn=_copy_input_impl,
        now_utc_iso_fn=now_utc_iso,
        new_crest_stage_fn=_new_crest_stage,
        write_workflow_payload_fn=_write_workflow_payload_side_effect,
        sync_workflow_registry_fn=_sync_workflow_registry_side_effect,
        load_xyz_atom_sequence_fn=load_xyz_atom_sequence,
    )


def _persist_workflow_progress(
    workflow_root: Path,
    workspace_dir: Path,
    payload: dict[str, Any],
    *,
    sync_only: bool,
) -> None:
    if not sync_only:
        status = normalize_text(payload.get("status")).lower()
        if status not in {"completed", "failed", "cancel_requested", "cancelled", "cancel_failed"}:
            payload["status"] = "running"
    write_workflow_payload(workspace_dir, payload)
    sync_workflow_registry(workflow_root, workspace_dir, payload)


def _workflow_sync_only(payload: dict[str, Any]) -> bool:
    return workflow_sync_only_impl(payload, normalize_text_fn=normalize_text)


def _workflow_has_active_children(payload: dict[str, Any]) -> bool:
    return workflow_has_active_children_impl(
        payload,
        normalize_text_fn=normalize_text,
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
    )


def _maybe_notify_workflow_phase_summary(
    payload: dict[str, Any],
    *,
    config_path: str | None,
    phase_engine: str,
    extra_lines: list[str] | None = None,
) -> bool:
    return maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=config_path,
        phase_engine=phase_engine,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable,
        extra_lines=extra_lines,
    )


_new_crest_stage = cast(Callable[..., dict[str, Any]], new_crest_stage_impl)
_new_xtb_stage = cast(Callable[..., dict[str, Any]], new_xtb_stage_impl)


def create_reaction_ts_search_workflow_from_request(
    request: ReactionTsSearchWorkflowRequest,
) -> dict[str, Any]:
    return _workflow_factories.create_reaction_ts_search_workflow_from_request(
        request,
        deps=_workflow_factory_deps(),
    )


def create_conformer_screening_workflow_from_request(
    request: ConformerScreeningWorkflowRequest,
) -> dict[str, Any]:
    return _workflow_factories.create_conformer_screening_workflow_from_request(
        request,
        deps=_workflow_factory_deps(),
    )


def create_reaction_ts_search_workflow(
    *,
    reactant_xyz: str,
    product_xyz: str,
    workflow_root: str | Path,
    workflow_id: str | None = None,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_crest_candidates: int = 3,
    max_xtb_stages: int = 3,
    max_xtb_handoff_retries: int = 2,
    max_orca_stages: int = 3,
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
    xtb_job_manifest: dict[str, Any] | None = None,
    endpoint_pairing: dict[str, Any] | None = None,
    source_job_id: str = "",
    source_job_type: str = "",
) -> dict[str, Any]:
    return create_reaction_ts_search_workflow_from_request(
        ReactionTsSearchWorkflowRequest(
            reactant_xyz=reactant_xyz,
            product_xyz=product_xyz,
            workflow_root=workflow_root,
            workflow_id=workflow_id,
            crest_mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            max_crest_candidates=max_crest_candidates,
            max_xtb_stages=max_xtb_stages,
            max_xtb_handoff_retries=max_xtb_handoff_retries,
            max_orca_stages=max_orca_stages,
            orca_route_line=orca_route_line,
            charge=charge,
            multiplicity=multiplicity,
            crest_job_manifest=crest_job_manifest,
            xtb_job_manifest=xtb_job_manifest,
            endpoint_pairing=endpoint_pairing,
            source_job_id=source_job_id,
            source_job_type=source_job_type,
        )
    )


def create_conformer_screening_workflow(
    *,
    input_xyz: str,
    workflow_root: str | Path,
    workflow_id: str | None = None,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_orca_stages: int = 20,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return create_conformer_screening_workflow_from_request(
        ConformerScreeningWorkflowRequest(
            input_xyz=input_xyz,
            workflow_root=workflow_root,
            workflow_id=workflow_id,
            crest_mode=crest_mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            max_orca_stages=max_orca_stages,
            orca_route_line=orca_route_line,
            charge=charge,
            multiplicity=multiplicity,
            crest_job_manifest=crest_job_manifest,
        )
    )


_submission_target = submission_target_impl
_load_config_root = load_config_root_impl
_load_config_organized_root = load_config_organized_root_impl
_stage_metadata = stage_metadata_impl
_task_payload_dict = task_payload_dict_impl
_xtb_attempt_rows = xtb_attempt_rows_impl
_xtb_attempt_record = xtb_attempt_record_impl
_xtb_retry_recipe = xtb_retry_recipe_impl
_xtb_path_retry_limit = xtb_path_retry_limit_impl
_xtb_current_attempt_number = xtb_current_attempt_number_impl
_write_xtb_path_job = write_xtb_path_job_impl
_xtb_handoff_status = xtb_handoff_status_impl
_stage_has_xtb_candidates = stage_has_xtb_candidates_impl


def _stage_failure_is_recoverable(stage: dict[str, Any]) -> bool:
    return stage_failure_is_recoverable_impl(
        stage,
        normalize_text_fn=normalize_text,
        stage_metadata_fn=_stage_metadata,
    )


_reaction_ts_guess_error = reaction_ts_guess_error_impl
_reaction_orca_source_candidate_path = reaction_orca_source_candidate_path_impl
_reaction_orca_allows_next_candidate = reaction_orca_allows_next_candidate_impl
_clear_reaction_xtb_handoff_error_if_recovering = (
    clear_reaction_xtb_handoff_error_if_recovering_impl
)
_append_unique_artifact = append_unique_artifact_impl
_ensure_crest_job_dir = ensure_crest_job_dir_impl
_ensure_xtb_job_dir = ensure_xtb_job_dir_impl
_sync_crest_stage = sync_crest_stage_impl
_sync_xtb_stage = sync_xtb_stage_impl
_sync_orca_stage = sync_orca_stage_impl
_completed_crest_roles = completed_crest_roles_impl
_completed_crest_stage = completed_crest_stage_impl
_completed_orca_stage = completed_orca_stage_impl
_append_reaction_xtb_stages = append_reaction_xtb_stages_impl


def _append_reaction_orca_stages(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_config: str | None,
    orca_config: str | None,
    deps: OrchestrationDeps | None = None,
) -> bool:
    if not phase_finished(payload.get("stages", []), engine="xtb"):
        return False
    return append_reaction_orca_stages_impl(
        payload,
        workspace_dir=workspace_dir,
        xtb_config=xtb_config,
        orca_config=orca_config,
        deps=deps,
    )


_append_crest_orca_stages = append_crest_orca_stages_impl


def _recompute_workflow_status(payload: dict[str, Any]) -> str:
    return recompute_workflow_status_impl(
        payload,
        normalize_text_fn=normalize_text,
        effective_stage_status_fn=lambda stage: effective_stage_status_impl(
            stage,
            normalize_text_fn=normalize_text,
            stage_failure_is_recoverable_fn=_stage_failure_is_recoverable,
        ),
    )

__all__ = [
    "ConformerScreeningWorkflowRequest",
    "CrestDownstreamPolicy",
    "EndpointPairingPolicy",
    "ReactionTsSearchWorkflowRequest",
    "WorkflowStageInput",
    "XtbDownstreamPolicy",
    "acquire_workflow_lock",
    "advance_workflow",
    "build_materialized_orca_stage",
    "cancel_materialized_workflow",
    "choose_orca_geometry_frame",
    "create_conformer_screening_workflow",
    "create_conformer_screening_workflow_from_request",
    "create_reaction_ts_search_workflow",
    "create_reaction_ts_search_workflow_from_request",
    "crest_cancel_target",
    "load_crest_artifact_contract",
    "load_orca_artifact_contract",
    "load_workflow_payload",
    "load_xtb_artifact_contract",
    "load_xyz_atom_sequence",
    "orca_cancel_target",
    "resolve_workflow_workspace",
    "safe_name",
    "select_crest_downstream_inputs",
    "select_endpoint_pairs",
    "select_xtb_downstream_inputs",
    "sibling_allowed_root",
    "sibling_runtime_paths",
    "submit_crest_job_dir",
    "submit_reaction_dir",
    "submit_xtb_job_dir",
    "xtb_cancel_target",
]
