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


@dataclass(frozen=True)
class OrchestrationStageDeps:
    builders: OrchestrationStageBuilderDeps
    materialization: OrchestrationStageMaterializationDeps
    runtime: OrchestrationStageRuntimeDeps
    support: OrchestrationStageSupportDeps
    workflow: OrchestrationStageWorkflowDeps

    @property
    def _append_unique_artifact(self) -> AnyCallable:
        return self.runtime._append_unique_artifact

    @property
    def _append_crest_orca_stages(self) -> AnyCallable:
        return self.materialization._append_crest_orca_stages

    @property
    def _append_reaction_orca_stages(self) -> AnyCallable:
        return self.materialization._append_reaction_orca_stages

    @property
    def _append_reaction_xtb_stages(self) -> AnyCallable:
        return self.materialization._append_reaction_xtb_stages

    @property
    def _clear_reaction_xtb_handoff_error_if_recovering(self) -> AnyCallable:
        return self.support._clear_reaction_xtb_handoff_error_if_recovering

    @property
    def _coerce_mapping(self) -> Callable[[Any], dict[str, Any]]:
        return self.support._coerce_mapping

    @property
    def _completed_crest_roles(self) -> AnyCallable:
        return self.runtime._completed_crest_roles

    @property
    def _completed_crest_stage(self) -> AnyCallable:
        return self.runtime._completed_crest_stage

    @property
    def _ensure_crest_job_dir(self) -> AnyCallable:
        return self.runtime._ensure_crest_job_dir

    @property
    def _ensure_xtb_job_dir(self) -> AnyCallable:
        return self.runtime._ensure_xtb_job_dir

    @property
    def _load_config_organized_root(self) -> AnyCallable:
        return self.support._load_config_organized_root

    @property
    def _load_config_root(self) -> AnyCallable:
        return self.support._load_config_root

    @property
    def _maybe_notify_workflow_phase_summary(self) -> AnyCallable:
        return self.workflow._maybe_notify_workflow_phase_summary

    @property
    def _new_xtb_stage(self) -> AnyCallable:
        return self.builders._new_xtb_stage

    @property
    def _normalize_text(self) -> Callable[[Any], str]:
        return self.support._normalize_text

    @property
    def _persist_workflow_progress(self) -> AnyCallable:
        return self.workflow._persist_workflow_progress

    @property
    def _reaction_orca_source_candidate_path(self) -> AnyCallable:
        return self.support._reaction_orca_source_candidate_path

    @property
    def _reaction_ts_guess_error(self) -> AnyCallable:
        return self.support._reaction_ts_guess_error

    @property
    def _recompute_workflow_status(self) -> Callable[[dict[str, Any]], str]:
        return self.workflow._recompute_workflow_status

    @property
    def _safe_int(self) -> AnyCallable:
        return self.support._safe_int

    @property
    def _stage_failure_is_recoverable(self) -> Callable[[dict[str, Any]], bool]:
        return self.workflow._stage_failure_is_recoverable

    @property
    def _stage_metadata(self) -> Callable[[dict[str, Any]], dict[str, Any]]:
        return self.support._stage_metadata

    @property
    def _submission_target(self) -> AnyCallable:
        return self.support._submission_target

    @property
    def _sync_crest_stage(self) -> AnyCallable:
        return self.runtime._sync_crest_stage

    @property
    def _sync_orca_stage(self) -> AnyCallable:
        return self.runtime._sync_orca_stage

    @property
    def _sync_xtb_stage(self) -> AnyCallable:
        return self.runtime._sync_xtb_stage

    @property
    def _task_payload_dict(self) -> AnyCallable:
        return self.support._task_payload_dict

    @property
    def _workflow_has_active_children(self) -> Callable[[dict[str, Any]], bool]:
        return self.workflow._workflow_has_active_children

    @property
    def _workflow_sync_only(self) -> Callable[[dict[str, Any]], bool]:
        return self.workflow._workflow_sync_only

    @property
    def _write_xtb_path_job(self) -> AnyCallable:
        return self.runtime._write_xtb_path_job

    @property
    def _xtb_attempt_record(self) -> AnyCallable:
        return self.runtime._xtb_attempt_record

    @property
    def _xtb_attempt_rows(self) -> AnyCallable:
        return self.runtime._xtb_attempt_rows

    @property
    def _xtb_current_attempt_number(self) -> AnyCallable:
        return self.runtime._xtb_current_attempt_number

    @property
    def _xtb_handoff_status(self) -> AnyCallable:
        return self.runtime._xtb_handoff_status

    @property
    def _xtb_path_retry_limit(self) -> AnyCallable:
        return self.runtime._xtb_path_retry_limit

    @property
    def _xtb_retry_recipe(self) -> AnyCallable:
        return self.runtime._xtb_retry_recipe


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
    "OrchestrationStageBuilderDeps",
    "OrchestrationStageDeps",
    "OrchestrationStageMaterializationDeps",
    "OrchestrationStageRuntimeDeps",
    "OrchestrationStageSupportDeps",
    "OrchestrationStageWorkflowDeps",
    "orchestration_deps",
]
