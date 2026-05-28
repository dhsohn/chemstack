from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from . import _orchestration_dep_builders as _dep_builders

AnyCallable = Callable[..., Any]


@dataclass(frozen=True)
class OrchestrationContractDeps:
    CrestDownstreamPolicy: type[Any]
    EndpointPairingPolicy: type[Any]
    WorkflowStageInput: type[Any]
    XtbDownstreamPolicy: type[Any]


@dataclass(frozen=True)
class OrchestrationPersistenceDeps:
    acquire_workflow_lock: AnyCallable
    load_workflow_payload: AnyCallable
    now_utc_iso: Callable[[], str]
    resolve_workflow_workspace: AnyCallable
    sync_workflow_registry: AnyCallable
    write_workflow_payload: AnyCallable


@dataclass(frozen=True)
class OrchestrationEngineDeps:
    build_materialized_orca_stage: AnyCallable
    choose_orca_geometry_frame: AnyCallable
    crest_cancel_target: AnyCallable
    load_crest_artifact_contract: AnyCallable
    load_orca_artifact_contract: AnyCallable
    load_xtb_artifact_contract: AnyCallable
    orca_cancel_target: AnyCallable
    safe_name: AnyCallable
    select_crest_downstream_inputs: AnyCallable
    select_endpoint_pairs: AnyCallable
    select_xtb_downstream_inputs: AnyCallable
    engine_runtime_paths: AnyCallable
    submit_crest_job_dir: AnyCallable
    submit_reaction_dir: AnyCallable
    submit_xtb_job_dir: AnyCallable
    xtb_cancel_target: AnyCallable


@dataclass(frozen=True)
class OrchestrationStageDeps:
    _append_unique_artifact: AnyCallable
    _append_crest_orca_stages: AnyCallable
    _append_reaction_orca_stages: AnyCallable
    _append_reaction_xtb_stages: AnyCallable
    _clear_reaction_xtb_handoff_error_if_recovering: AnyCallable
    _coerce_mapping: Callable[[Any], dict[str, Any]]
    _completed_crest_roles: AnyCallable
    _completed_crest_stage: AnyCallable
    _ensure_crest_job_dir: AnyCallable
    _ensure_xtb_job_dir: AnyCallable
    _load_config_organized_root: AnyCallable
    _load_config_root: AnyCallable
    _maybe_notify_workflow_phase_summary: AnyCallable
    _new_xtb_stage: AnyCallable
    _normalize_text: Callable[[Any], str]
    _persist_workflow_progress: AnyCallable
    _reaction_orca_source_candidate_path: AnyCallable
    _reaction_ts_guess_error: AnyCallable
    _recompute_workflow_status: Callable[[dict[str, Any]], str]
    _safe_int: AnyCallable
    _stage_failure_is_recoverable: Callable[[dict[str, Any]], bool]
    _stage_metadata: Callable[[dict[str, Any]], dict[str, Any]]
    _submission_target: AnyCallable
    _sync_crest_stage: AnyCallable
    _sync_orca_stage: AnyCallable
    _sync_xtb_stage: AnyCallable
    _task_payload_dict: AnyCallable
    _workflow_has_active_children: Callable[[dict[str, Any]], bool]
    _workflow_sync_only: Callable[[dict[str, Any]], bool]
    _write_xtb_path_job: AnyCallable
    _xtb_attempt_record: AnyCallable
    _xtb_attempt_rows: AnyCallable
    _xtb_current_attempt_number: AnyCallable
    _xtb_handoff_status: AnyCallable
    _xtb_path_retry_limit: AnyCallable
    _xtb_retry_recipe: AnyCallable


@dataclass(frozen=True)
class OrchestrationAdvanceDeps:
    _cancel_active_workflow_stages: AnyCallable
    _cancel_stage_activity: AnyCallable


@dataclass(frozen=True)
class OrchestrationDeps:
    contracts: OrchestrationContractDeps
    persistence: OrchestrationPersistenceDeps
    engines: OrchestrationEngineDeps
    stages: OrchestrationStageDeps
    advance: OrchestrationAdvanceDeps


def orchestration_deps(overrides: Mapping[str, Any] | None = None) -> OrchestrationDeps:
    return OrchestrationDeps(
        contracts=_dep_builders._build_contract_deps(overrides),
        persistence=_dep_builders._build_persistence_deps(overrides),
        engines=_dep_builders._build_engine_deps(overrides),
        stages=_dep_builders._build_stage_deps(overrides),
        advance=_dep_builders._build_advance_deps(overrides),
    )


__all__ = [
    "OrchestrationAdvanceDeps",
    "OrchestrationContractDeps",
    "OrchestrationDeps",
    "OrchestrationEngineDeps",
    "OrchestrationPersistenceDeps",
    "OrchestrationStageDeps",
    "orchestration_deps",
]
