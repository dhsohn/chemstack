from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class OrchestrationContractDeps:
    CrestDownstreamPolicy: Any
    EndpointPairingPolicy: Any
    WorkflowStageInput: Any
    XtbDownstreamPolicy: Any


@dataclass(frozen=True)
class OrchestrationPersistenceDeps:
    acquire_workflow_lock: Any
    load_workflow_payload: Any
    now_utc_iso: Any
    resolve_workflow_workspace: Any
    sync_workflow_registry: Any
    write_workflow_payload: Any


@dataclass(frozen=True)
class OrchestrationEngineDeps:
    build_materialized_orca_stage: Any
    choose_orca_geometry_frame: Any
    crest_cancel_target: Any
    load_crest_artifact_contract: Any
    load_orca_artifact_contract: Any
    load_xtb_artifact_contract: Any
    orca_cancel_target: Any
    safe_name: Any
    select_crest_downstream_inputs: Any
    select_endpoint_pairs: Any
    select_xtb_downstream_inputs: Any
    sibling_runtime_paths: Any
    submit_crest_job_dir: Any
    submit_reaction_dir: Any
    submit_xtb_job_dir: Any
    xtb_cancel_target: Any


@dataclass(frozen=True)
class OrchestrationStageDeps:
    _append_unique_artifact: Any
    _append_crest_orca_stages: Any
    _append_reaction_orca_stages: Any
    _append_reaction_xtb_stages: Any
    _clear_reaction_xtb_handoff_error_if_recovering: Any
    _coerce_mapping: Any
    _completed_crest_roles: Any
    _completed_crest_stage: Any
    _ensure_crest_job_dir: Any
    _ensure_xtb_job_dir: Any
    _load_config_organized_root: Any
    _load_config_root: Any
    _maybe_notify_workflow_phase_summary: Any
    _new_xtb_stage: Any
    _normalize_text: Any
    _persist_workflow_progress: Any
    _reaction_orca_source_candidate_path: Any
    _reaction_ts_guess_error: Any
    _recompute_workflow_status: Any
    _safe_int: Any
    _stage_failure_is_recoverable: Any
    _stage_metadata: Any
    _submission_target: Any
    _sync_crest_stage: Any
    _sync_orca_stage: Any
    _sync_xtb_stage: Any
    _task_payload_dict: Any
    _workflow_has_active_children: Any
    _workflow_sync_only: Any
    _write_xtb_path_job: Any
    _xtb_attempt_record: Any
    _xtb_attempt_rows: Any
    _xtb_current_attempt_number: Any
    _xtb_handoff_status: Any
    _xtb_path_retry_limit: Any
    _xtb_retry_recipe: Any


@dataclass(frozen=True)
class OrchestrationAdvanceDeps:
    _cancel_active_workflow_stages: Any
    _cancel_stage_activity: Any


@dataclass(frozen=True)
class OrchestrationDeps:
    contracts: OrchestrationContractDeps
    persistence: OrchestrationPersistenceDeps
    engines: OrchestrationEngineDeps
    stages: OrchestrationStageDeps
    advance: OrchestrationAdvanceDeps

    def __getattr__(self, name: str) -> Any:
        for group in (self.contracts, self.persistence, self.engines, self.stages, self.advance):
            if hasattr(group, name):
                return getattr(group, name)
        raise AttributeError(name)


def orchestration_deps() -> OrchestrationDeps:
    from . import _orchestration_advance
    from . import orchestration

    return OrchestrationDeps(
        contracts=OrchestrationContractDeps(
            CrestDownstreamPolicy=orchestration.CrestDownstreamPolicy,
            EndpointPairingPolicy=orchestration.EndpointPairingPolicy,
            WorkflowStageInput=orchestration.WorkflowStageInput,
            XtbDownstreamPolicy=orchestration.XtbDownstreamPolicy,
        ),
        persistence=OrchestrationPersistenceDeps(
            acquire_workflow_lock=orchestration.acquire_workflow_lock,
            load_workflow_payload=orchestration.load_workflow_payload,
            now_utc_iso=orchestration.now_utc_iso,
            resolve_workflow_workspace=orchestration.resolve_workflow_workspace,
            sync_workflow_registry=orchestration.sync_workflow_registry,
            write_workflow_payload=orchestration.write_workflow_payload,
        ),
        engines=OrchestrationEngineDeps(
            build_materialized_orca_stage=orchestration.build_materialized_orca_stage,
            choose_orca_geometry_frame=orchestration.choose_orca_geometry_frame,
            crest_cancel_target=orchestration.crest_cancel_target,
            load_crest_artifact_contract=orchestration.load_crest_artifact_contract,
            load_orca_artifact_contract=orchestration.load_orca_artifact_contract,
            load_xtb_artifact_contract=orchestration.load_xtb_artifact_contract,
            orca_cancel_target=orchestration.orca_cancel_target,
            safe_name=orchestration.safe_name,
            select_crest_downstream_inputs=orchestration.select_crest_downstream_inputs,
            select_endpoint_pairs=orchestration.select_endpoint_pairs,
            select_xtb_downstream_inputs=orchestration.select_xtb_downstream_inputs,
            sibling_runtime_paths=orchestration.sibling_runtime_paths,
            submit_crest_job_dir=orchestration.submit_crest_job_dir,
            submit_reaction_dir=orchestration.submit_reaction_dir,
            submit_xtb_job_dir=orchestration.submit_xtb_job_dir,
            xtb_cancel_target=orchestration.xtb_cancel_target,
        ),
        stages=OrchestrationStageDeps(
            _append_unique_artifact=orchestration._append_unique_artifact,
            _append_crest_orca_stages=orchestration._append_crest_orca_stages,
            _append_reaction_orca_stages=orchestration._append_reaction_orca_stages,
            _append_reaction_xtb_stages=orchestration._append_reaction_xtb_stages,
            _clear_reaction_xtb_handoff_error_if_recovering=(
                orchestration._clear_reaction_xtb_handoff_error_if_recovering
            ),
            _coerce_mapping=orchestration._coerce_mapping,
            _completed_crest_roles=orchestration._completed_crest_roles,
            _completed_crest_stage=orchestration._completed_crest_stage,
            _ensure_crest_job_dir=orchestration._ensure_crest_job_dir,
            _ensure_xtb_job_dir=orchestration._ensure_xtb_job_dir,
            _load_config_organized_root=orchestration._load_config_organized_root,
            _load_config_root=orchestration._load_config_root,
            _maybe_notify_workflow_phase_summary=orchestration._maybe_notify_workflow_phase_summary,
            _new_xtb_stage=orchestration._new_xtb_stage,
            _normalize_text=orchestration._normalize_text,
            _persist_workflow_progress=orchestration._persist_workflow_progress,
            _reaction_orca_source_candidate_path=orchestration._reaction_orca_source_candidate_path,
            _reaction_ts_guess_error=orchestration._reaction_ts_guess_error,
            _recompute_workflow_status=orchestration._recompute_workflow_status,
            _safe_int=orchestration._safe_int,
            _stage_failure_is_recoverable=orchestration._stage_failure_is_recoverable,
            _stage_metadata=orchestration._stage_metadata,
            _submission_target=orchestration._submission_target,
            _sync_crest_stage=orchestration._sync_crest_stage,
            _sync_orca_stage=orchestration._sync_orca_stage,
            _sync_xtb_stage=orchestration._sync_xtb_stage,
            _task_payload_dict=orchestration._task_payload_dict,
            _workflow_has_active_children=orchestration._workflow_has_active_children,
            _workflow_sync_only=orchestration._workflow_sync_only,
            _write_xtb_path_job=orchestration._write_xtb_path_job,
            _xtb_attempt_record=orchestration._xtb_attempt_record,
            _xtb_attempt_rows=orchestration._xtb_attempt_rows,
            _xtb_current_attempt_number=orchestration._xtb_current_attempt_number,
            _xtb_handoff_status=orchestration._xtb_handoff_status,
            _xtb_path_retry_limit=orchestration._xtb_path_retry_limit,
            _xtb_retry_recipe=orchestration._xtb_retry_recipe,
        ),
        advance=OrchestrationAdvanceDeps(
            _cancel_active_workflow_stages=_orchestration_advance._cancel_active_workflow_stages,
            _cancel_stage_activity=_orchestration_advance._cancel_stage_activity,
        ),
    )


def call_engine_aware(func: Any, config_path: str | None, *, engine: str) -> Any:
    try:
        return func(config_path, engine=engine)
    except TypeError as exc:
        if "engine" not in str(exc):
            raise
        return func(config_path)


__all__ = [
    "OrchestrationAdvanceDeps",
    "OrchestrationContractDeps",
    "OrchestrationDeps",
    "OrchestrationEngineDeps",
    "OrchestrationPersistenceDeps",
    "OrchestrationStageDeps",
    "call_engine_aware",
    "orchestration_deps",
]
