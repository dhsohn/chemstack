from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_ORCHESTRATION_FACADE_MODULE = "chemstack.flow.orchestration"


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


def _facade_override(name: str, fallback: Any) -> Any:
    facade = sys.modules.get(_ORCHESTRATION_FACADE_MODULE)
    if facade is None:
        return fallback
    return getattr(facade, name, fallback)


def orchestration_deps() -> OrchestrationDeps:
    from chemstack.core.utils import mapping_or_empty, normalize_text, now_utc_iso, safe_int

    from . import _orchestration_advance
    from ._orchestration_builders import new_xtb_stage_impl
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
    from ._orchestration_stage_runtime import (
        append_unique_artifact_impl,
        completed_crest_roles_impl,
        completed_crest_stage_impl,
        ensure_crest_job_dir_impl,
        ensure_xtb_job_dir_impl,
        sync_crest_stage_impl,
        sync_orca_stage_impl,
        sync_xtb_stage_impl,
        write_xtb_path_job_impl,
        xtb_attempt_record_impl,
        xtb_attempt_rows_impl,
        xtb_current_attempt_number_impl,
        xtb_handoff_status_impl,
        xtb_path_retry_limit_impl,
        xtb_retry_recipe_impl,
    )
    from ._orchestration_support import (
        clear_reaction_xtb_handoff_error_if_recovering_impl,
        load_config_organized_root_impl,
        load_config_root_impl,
        reaction_orca_source_candidate_path_impl,
        reaction_ts_guess_error_impl,
        stage_metadata_impl,
        submission_target_impl,
        task_payload_dict_impl,
    )
    from ._workflow_phases import phase_finished
    from .adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
    from .adapters.orca import load_orca_artifact_contract
    from .adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
    from .contracts import CrestDownstreamPolicy, WorkflowStageInput, XtbDownstreamPolicy
    from .endpoint_pairing import EndpointPairingPolicy, select_endpoint_pairs
    from .registry import sync_workflow_registry
    from .state import (
        acquire_workflow_lock,
        load_workflow_payload,
        resolve_workflow_workspace,
        workflow_has_active_downstream,
        write_workflow_payload,
    )
    from .submitters.common import sibling_runtime_paths
    from .submitters.crest_auto import (
        cancel_target as crest_cancel_target,
        submit_job_dir as submit_crest_job_dir,
    )
    from .submitters.orca_auto import cancel_target as orca_cancel_target, submit_reaction_dir
    from .submitters.xtb_auto import (
        cancel_target as xtb_cancel_target,
        submit_job_dir as submit_xtb_job_dir,
    )
    from .workflow_notifications import maybe_notify_workflow_phase_summary
    from .workflows.orca_stage_utils import build_materialized_orca_stage, safe_name
    from .xyz_utils import choose_orca_geometry_frame

    def _coerce_mapping_fallback(value: Any) -> dict[str, Any]:
        return mapping_or_empty(value)

    def _normalize_text_fallback(value: Any) -> str:
        return normalize_text(value)

    def _safe_int_fallback(value: Any, *, default: int = 0) -> int:
        return safe_int(value, default=default)

    def _workflow_sync_only_fallback(payload: dict[str, Any]) -> bool:
        return workflow_sync_only_impl(
            payload,
            normalize_text_fn=_facade_override("_normalize_text", _normalize_text_fallback),
        )

    def _workflow_has_active_children_fallback(payload: dict[str, Any]) -> bool:
        return workflow_has_active_children_impl(
            payload,
            normalize_text_fn=_facade_override("_normalize_text", _normalize_text_fallback),
            workflow_has_active_downstream_fn=_facade_override(
                "workflow_has_active_downstream",
                workflow_has_active_downstream,
            ),
        )

    def _stage_failure_is_recoverable_fallback(stage: dict[str, Any]) -> bool:
        return stage_failure_is_recoverable_impl(
            stage,
            normalize_text_fn=_facade_override("_normalize_text", _normalize_text_fallback),
            stage_metadata_fn=_facade_override("_stage_metadata", stage_metadata_impl),
        )

    def _effective_stage_status_fallback(stage: dict[str, Any]) -> str:
        return effective_stage_status_impl(
            stage,
            normalize_text_fn=_facade_override("_normalize_text", _normalize_text_fallback),
            stage_failure_is_recoverable_fn=_facade_override(
                "_stage_failure_is_recoverable",
                _stage_failure_is_recoverable_fallback,
            ),
        )

    def _recompute_workflow_status_fallback(payload: dict[str, Any]) -> str:
        return recompute_workflow_status_impl(
            payload,
            normalize_text_fn=_facade_override("_normalize_text", _normalize_text_fallback),
            effective_stage_status_fn=_facade_override(
                "_effective_stage_status",
                _effective_stage_status_fallback,
            ),
        )

    def _persist_workflow_progress_fallback(
        workflow_root: Path,
        workspace_dir: Path,
        payload: dict[str, Any],
        *,
        sync_only: bool,
    ) -> None:
        normalize = _facade_override("_normalize_text", _normalize_text_fallback)
        if not sync_only:
            status = normalize(payload.get("status")).lower()
            if status not in {
                "completed",
                "failed",
                "cancel_requested",
                "cancelled",
                "cancel_failed",
            }:
                payload["status"] = "running"
        _facade_override("write_workflow_payload", write_workflow_payload)(workspace_dir, payload)
        _facade_override("sync_workflow_registry", sync_workflow_registry)(
            workflow_root,
            workspace_dir,
            payload,
        )

    def _maybe_notify_workflow_phase_summary_fallback(
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
            stage_failure_is_recoverable_fn=_facade_override(
                "_stage_failure_is_recoverable",
                _stage_failure_is_recoverable_fallback,
            ),
            extra_lines=extra_lines,
        )

    def _append_reaction_orca_stages_fallback(
        payload: dict[str, Any],
        *,
        workspace_dir: Path,
        xtb_auto_config: str | None,
        orca_auto_config: str | None,
    ) -> bool:
        if not phase_finished(payload.get("stages", []), engine="xtb"):
            return False
        return append_reaction_orca_stages_impl(
            payload,
            workspace_dir=workspace_dir,
            xtb_auto_config=xtb_auto_config,
            orca_auto_config=orca_auto_config,
        )

    return OrchestrationDeps(
        contracts=OrchestrationContractDeps(
            CrestDownstreamPolicy=_facade_override(
                "CrestDownstreamPolicy",
                CrestDownstreamPolicy,
            ),
            EndpointPairingPolicy=_facade_override(
                "EndpointPairingPolicy",
                EndpointPairingPolicy,
            ),
            WorkflowStageInput=_facade_override("WorkflowStageInput", WorkflowStageInput),
            XtbDownstreamPolicy=_facade_override("XtbDownstreamPolicy", XtbDownstreamPolicy),
        ),
        persistence=OrchestrationPersistenceDeps(
            acquire_workflow_lock=_facade_override(
                "acquire_workflow_lock",
                acquire_workflow_lock,
            ),
            load_workflow_payload=_facade_override(
                "load_workflow_payload",
                load_workflow_payload,
            ),
            now_utc_iso=_facade_override("now_utc_iso", now_utc_iso),
            resolve_workflow_workspace=_facade_override(
                "resolve_workflow_workspace",
                resolve_workflow_workspace,
            ),
            sync_workflow_registry=_facade_override(
                "sync_workflow_registry",
                sync_workflow_registry,
            ),
            write_workflow_payload=_facade_override(
                "write_workflow_payload",
                write_workflow_payload,
            ),
        ),
        engines=OrchestrationEngineDeps(
            build_materialized_orca_stage=_facade_override(
                "build_materialized_orca_stage",
                build_materialized_orca_stage,
            ),
            choose_orca_geometry_frame=_facade_override(
                "choose_orca_geometry_frame",
                choose_orca_geometry_frame,
            ),
            crest_cancel_target=_facade_override("crest_cancel_target", crest_cancel_target),
            load_crest_artifact_contract=_facade_override(
                "load_crest_artifact_contract",
                load_crest_artifact_contract,
            ),
            load_orca_artifact_contract=_facade_override(
                "load_orca_artifact_contract",
                load_orca_artifact_contract,
            ),
            load_xtb_artifact_contract=_facade_override(
                "load_xtb_artifact_contract",
                load_xtb_artifact_contract,
            ),
            orca_cancel_target=_facade_override("orca_cancel_target", orca_cancel_target),
            safe_name=_facade_override("safe_name", safe_name),
            select_crest_downstream_inputs=_facade_override(
                "select_crest_downstream_inputs",
                select_crest_downstream_inputs,
            ),
            select_endpoint_pairs=_facade_override("select_endpoint_pairs", select_endpoint_pairs),
            select_xtb_downstream_inputs=_facade_override(
                "select_xtb_downstream_inputs",
                select_xtb_downstream_inputs,
            ),
            sibling_runtime_paths=_facade_override("sibling_runtime_paths", sibling_runtime_paths),
            submit_crest_job_dir=_facade_override("submit_crest_job_dir", submit_crest_job_dir),
            submit_reaction_dir=_facade_override("submit_reaction_dir", submit_reaction_dir),
            submit_xtb_job_dir=_facade_override("submit_xtb_job_dir", submit_xtb_job_dir),
            xtb_cancel_target=_facade_override("xtb_cancel_target", xtb_cancel_target),
        ),
        stages=OrchestrationStageDeps(
            _append_unique_artifact=_facade_override(
                "_append_unique_artifact",
                append_unique_artifact_impl,
            ),
            _append_crest_orca_stages=_facade_override(
                "_append_crest_orca_stages",
                append_crest_orca_stages_impl,
            ),
            _append_reaction_orca_stages=_facade_override(
                "_append_reaction_orca_stages",
                _append_reaction_orca_stages_fallback,
            ),
            _append_reaction_xtb_stages=_facade_override(
                "_append_reaction_xtb_stages",
                append_reaction_xtb_stages_impl,
            ),
            _clear_reaction_xtb_handoff_error_if_recovering=_facade_override(
                "_clear_reaction_xtb_handoff_error_if_recovering",
                clear_reaction_xtb_handoff_error_if_recovering_impl,
            ),
            _coerce_mapping=_facade_override("_coerce_mapping", _coerce_mapping_fallback),
            _completed_crest_roles=_facade_override(
                "_completed_crest_roles",
                completed_crest_roles_impl,
            ),
            _completed_crest_stage=_facade_override(
                "_completed_crest_stage",
                completed_crest_stage_impl,
            ),
            _ensure_crest_job_dir=_facade_override(
                "_ensure_crest_job_dir",
                ensure_crest_job_dir_impl,
            ),
            _ensure_xtb_job_dir=_facade_override(
                "_ensure_xtb_job_dir",
                ensure_xtb_job_dir_impl,
            ),
            _load_config_organized_root=_facade_override(
                "_load_config_organized_root",
                load_config_organized_root_impl,
            ),
            _load_config_root=_facade_override("_load_config_root", load_config_root_impl),
            _maybe_notify_workflow_phase_summary=_facade_override(
                "_maybe_notify_workflow_phase_summary",
                _maybe_notify_workflow_phase_summary_fallback,
            ),
            _new_xtb_stage=_facade_override("_new_xtb_stage", new_xtb_stage_impl),
            _normalize_text=_facade_override("_normalize_text", _normalize_text_fallback),
            _persist_workflow_progress=_facade_override(
                "_persist_workflow_progress",
                _persist_workflow_progress_fallback,
            ),
            _reaction_orca_source_candidate_path=_facade_override(
                "_reaction_orca_source_candidate_path",
                reaction_orca_source_candidate_path_impl,
            ),
            _reaction_ts_guess_error=_facade_override(
                "_reaction_ts_guess_error",
                reaction_ts_guess_error_impl,
            ),
            _recompute_workflow_status=_facade_override(
                "_recompute_workflow_status",
                _recompute_workflow_status_fallback,
            ),
            _safe_int=_facade_override("_safe_int", _safe_int_fallback),
            _stage_failure_is_recoverable=_facade_override(
                "_stage_failure_is_recoverable",
                _stage_failure_is_recoverable_fallback,
            ),
            _stage_metadata=_facade_override("_stage_metadata", stage_metadata_impl),
            _submission_target=_facade_override("_submission_target", submission_target_impl),
            _sync_crest_stage=_facade_override("_sync_crest_stage", sync_crest_stage_impl),
            _sync_orca_stage=_facade_override("_sync_orca_stage", sync_orca_stage_impl),
            _sync_xtb_stage=_facade_override("_sync_xtb_stage", sync_xtb_stage_impl),
            _task_payload_dict=_facade_override("_task_payload_dict", task_payload_dict_impl),
            _workflow_has_active_children=_facade_override(
                "_workflow_has_active_children",
                _workflow_has_active_children_fallback,
            ),
            _workflow_sync_only=_facade_override(
                "_workflow_sync_only",
                _workflow_sync_only_fallback,
            ),
            _write_xtb_path_job=_facade_override(
                "_write_xtb_path_job",
                write_xtb_path_job_impl,
            ),
            _xtb_attempt_record=_facade_override("_xtb_attempt_record", xtb_attempt_record_impl),
            _xtb_attempt_rows=_facade_override("_xtb_attempt_rows", xtb_attempt_rows_impl),
            _xtb_current_attempt_number=_facade_override(
                "_xtb_current_attempt_number",
                xtb_current_attempt_number_impl,
            ),
            _xtb_handoff_status=_facade_override(
                "_xtb_handoff_status",
                xtb_handoff_status_impl,
            ),
            _xtb_path_retry_limit=_facade_override(
                "_xtb_path_retry_limit",
                xtb_path_retry_limit_impl,
            ),
            _xtb_retry_recipe=_facade_override("_xtb_retry_recipe", xtb_retry_recipe_impl),
        ),
        advance=OrchestrationAdvanceDeps(
            _cancel_active_workflow_stages=_facade_override(
                "_cancel_active_workflow_stages",
                _orchestration_advance._cancel_active_workflow_stages,
            ),
            _cancel_stage_activity=_facade_override(
                "_cancel_stage_activity",
                _orchestration_advance._cancel_stage_activity,
            ),
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
