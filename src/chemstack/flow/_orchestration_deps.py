from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

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
class OrchestrationStageBuilderDeps:
    _new_xtb_stage: AnyCallable


@dataclass(frozen=True)
class OrchestrationStageMaterializationDeps:
    _append_crest_orca_stages: AnyCallable
    _append_reaction_orca_stages: AnyCallable
    _append_reaction_xtb_stages: AnyCallable


@dataclass(frozen=True)
class OrchestrationStageRuntimeDeps:
    _append_unique_artifact: AnyCallable
    _completed_crest_roles: AnyCallable
    _completed_crest_stage: AnyCallable
    _ensure_crest_job_dir: AnyCallable
    _ensure_xtb_job_dir: AnyCallable
    _sync_crest_stage: AnyCallable
    _sync_orca_stage: AnyCallable
    _sync_xtb_stage: AnyCallable
    _write_xtb_path_job: AnyCallable
    _xtb_attempt_record: AnyCallable
    _xtb_attempt_rows: AnyCallable
    _xtb_current_attempt_number: AnyCallable
    _xtb_handoff_status: AnyCallable
    _xtb_path_retry_limit: AnyCallable
    _xtb_retry_recipe: AnyCallable


@dataclass(frozen=True)
class OrchestrationStageSupportDeps:
    _clear_reaction_xtb_handoff_error_if_recovering: AnyCallable
    _coerce_mapping: Callable[[Any], dict[str, Any]]
    _load_config_organized_root: AnyCallable
    _load_config_root: AnyCallable
    _normalize_text: Callable[[Any], str]
    _reaction_orca_source_candidate_path: AnyCallable
    _reaction_ts_guess_error: AnyCallable
    _safe_int: AnyCallable
    _stage_metadata: Callable[[dict[str, Any]], dict[str, Any]]
    _submission_target: AnyCallable
    _task_payload_dict: AnyCallable


@dataclass(frozen=True)
class OrchestrationStageWorkflowDeps:
    _maybe_notify_workflow_phase_summary: AnyCallable
    _persist_workflow_progress: AnyCallable
    _recompute_workflow_status: Callable[[dict[str, Any]], str]
    _stage_failure_is_recoverable: Callable[[dict[str, Any]], bool]
    _workflow_has_active_children: Callable[[dict[str, Any]], bool]
    _workflow_sync_only: Callable[[dict[str, Any]], bool]


_ORCHESTRATION_STAGE_DEP_GROUPS: Mapping[str, tuple[str, ...]] = {
    "builders": ("_new_xtb_stage",),
    "materialization": (
        "_append_crest_orca_stages",
        "_append_reaction_orca_stages",
        "_append_reaction_xtb_stages",
    ),
    "runtime": (
        "_append_unique_artifact",
        "_completed_crest_roles",
        "_completed_crest_stage",
        "_ensure_crest_job_dir",
        "_ensure_xtb_job_dir",
        "_sync_crest_stage",
        "_sync_orca_stage",
        "_sync_xtb_stage",
        "_write_xtb_path_job",
        "_xtb_attempt_record",
        "_xtb_attempt_rows",
        "_xtb_current_attempt_number",
        "_xtb_handoff_status",
        "_xtb_path_retry_limit",
        "_xtb_retry_recipe",
    ),
    "support": (
        "_clear_reaction_xtb_handoff_error_if_recovering",
        "_coerce_mapping",
        "_load_config_organized_root",
        "_load_config_root",
        "_normalize_text",
        "_reaction_orca_source_candidate_path",
        "_reaction_ts_guess_error",
        "_safe_int",
        "_stage_metadata",
        "_submission_target",
        "_task_payload_dict",
    ),
    "workflow": (
        "_maybe_notify_workflow_phase_summary",
        "_persist_workflow_progress",
        "_recompute_workflow_status",
        "_stage_failure_is_recoverable",
        "_workflow_has_active_children",
        "_workflow_sync_only",
    ),
}

_ORCHESTRATION_STAGE_DEP_TARGETS: Mapping[str, str] = {
    dep_name: group_name
    for group_name, dep_names in _ORCHESTRATION_STAGE_DEP_GROUPS.items()
    for dep_name in dep_names
}


@dataclass(frozen=True)
class OrchestrationStageDeps:
    builders: OrchestrationStageBuilderDeps
    materialization: OrchestrationStageMaterializationDeps
    runtime: OrchestrationStageRuntimeDeps
    support: OrchestrationStageSupportDeps
    workflow: OrchestrationStageWorkflowDeps

    _PASSTHROUGH_TARGETS: ClassVar[Mapping[str, str]] = _ORCHESTRATION_STAGE_DEP_TARGETS

    def __getattr__(self, name: str) -> Any:
        group_name = self._PASSTHROUGH_TARGETS.get(name)
        if group_name is None:
            raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")
        return getattr(getattr(self, group_name), name)


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
    deps_provider = _dep_builders._LazyOrchestrationDeps(overrides)
    deps = OrchestrationDeps(
        contracts=_dep_builders._build_contract_deps(overrides),
        persistence=_dep_builders._build_persistence_deps(overrides),
        engines=_dep_builders._build_engine_deps(overrides),
        stages=_dep_builders._build_stage_deps(overrides, deps_provider=deps_provider),
        advance=_dep_builders._build_advance_deps(overrides, deps_provider=deps_provider),
    )
    deps_provider.resolve_to(deps)
    return deps


__all__ = [
    "OrchestrationAdvanceDeps",
    "OrchestrationContractDeps",
    "OrchestrationDeps",
    "OrchestrationEngineDeps",
    "OrchestrationPersistenceDeps",
    "OrchestrationStageBuilderDeps",
    "OrchestrationStageDeps",
    "OrchestrationStageMaterializationDeps",
    "OrchestrationStageRuntimeDeps",
    "OrchestrationStageSupportDeps",
    "OrchestrationStageWorkflowDeps",
    "orchestration_deps",
]
