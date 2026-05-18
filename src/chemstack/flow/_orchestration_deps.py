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


def _facade_overrides(items: dict[str, Any]) -> dict[str, Any]:
    return {name: _facade_override(name, fallback) for name, fallback in items.items()}


def _coerce_mapping_fallback(value: Any) -> dict[str, Any]:
    from chemstack.core.utils import mapping_or_empty

    return mapping_or_empty(value)


def _normalize_text_fallback(value: Any) -> str:
    from chemstack.core.utils import normalize_text

    return normalize_text(value)


def _safe_int_fallback(value: Any, *, default: int = 0) -> int:
    from chemstack.core.utils import safe_int

    return safe_int(value, default=default)


def _normalize_text_override() -> Any:
    return _facade_override("_normalize_text", _normalize_text_fallback)


def _stage_metadata_override() -> Any:
    from ._orchestration_support import stage_metadata_impl

    return _facade_override("_stage_metadata", stage_metadata_impl)


def _stage_failure_is_recoverable_override() -> Any:
    return _facade_override(
        "_stage_failure_is_recoverable",
        _stage_failure_is_recoverable_fallback,
    )


def _workflow_sync_only_fallback(payload: dict[str, Any]) -> bool:
    from ._orchestration_lifecycle import workflow_sync_only_impl

    return workflow_sync_only_impl(payload, normalize_text_fn=_normalize_text_override())


def _workflow_has_active_children_fallback(payload: dict[str, Any]) -> bool:
    from ._orchestration_lifecycle import workflow_has_active_children_impl
    from .state import workflow_has_active_downstream

    return workflow_has_active_children_impl(
        payload,
        normalize_text_fn=_normalize_text_override(),
        workflow_has_active_downstream_fn=_facade_override(
            "workflow_has_active_downstream",
            workflow_has_active_downstream,
        ),
    )


def _stage_failure_is_recoverable_fallback(stage: dict[str, Any]) -> bool:
    from ._orchestration_lifecycle import stage_failure_is_recoverable_impl

    return stage_failure_is_recoverable_impl(
        stage,
        normalize_text_fn=_normalize_text_override(),
        stage_metadata_fn=_stage_metadata_override(),
    )


def _effective_stage_status_fallback(stage: dict[str, Any]) -> str:
    from ._orchestration_lifecycle import effective_stage_status_impl

    return effective_stage_status_impl(
        stage,
        normalize_text_fn=_normalize_text_override(),
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(),
    )


def _recompute_workflow_status_fallback(payload: dict[str, Any]) -> str:
    from ._orchestration_lifecycle import recompute_workflow_status_impl

    return recompute_workflow_status_impl(
        payload,
        normalize_text_fn=_normalize_text_override(),
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
    from .registry import sync_workflow_registry
    from .state import write_workflow_payload

    normalize = _normalize_text_override()
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
    from .workflow_notifications import maybe_notify_workflow_phase_summary

    return maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=config_path,
        phase_engine=phase_engine,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(),
        extra_lines=extra_lines,
    )


def _append_reaction_orca_stages_fallback(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_auto_config: str | None,
    orca_auto_config: str | None,
) -> bool:
    from ._orchestration_stage_materialization import append_reaction_orca_stages_impl
    from ._workflow_phases import phase_finished

    if not phase_finished(payload.get("stages", []), engine="xtb"):
        return False
    return append_reaction_orca_stages_impl(
        payload,
        workspace_dir=workspace_dir,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )


def _build_contract_deps() -> OrchestrationContractDeps:
    from .contracts import CrestDownstreamPolicy, WorkflowStageInput, XtbDownstreamPolicy
    from .endpoint_pairing import EndpointPairingPolicy

    return OrchestrationContractDeps(
        **_facade_overrides(
            {
                "CrestDownstreamPolicy": CrestDownstreamPolicy,
                "EndpointPairingPolicy": EndpointPairingPolicy,
                "WorkflowStageInput": WorkflowStageInput,
                "XtbDownstreamPolicy": XtbDownstreamPolicy,
            }
        )
    )


def _build_persistence_deps() -> OrchestrationPersistenceDeps:
    from chemstack.core.utils import now_utc_iso

    from .registry import sync_workflow_registry
    from .state import acquire_workflow_lock, load_workflow_payload
    from .state import resolve_workflow_workspace, write_workflow_payload

    return OrchestrationPersistenceDeps(
        **_facade_overrides(
            {
                "acquire_workflow_lock": acquire_workflow_lock,
                "load_workflow_payload": load_workflow_payload,
                "now_utc_iso": now_utc_iso,
                "resolve_workflow_workspace": resolve_workflow_workspace,
                "sync_workflow_registry": sync_workflow_registry,
                "write_workflow_payload": write_workflow_payload,
            }
        )
    )


def _build_engine_deps() -> OrchestrationEngineDeps:
    from .adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
    from .adapters.orca import load_orca_artifact_contract
    from .adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
    from .endpoint_pairing import select_endpoint_pairs
    from .submitters.common import sibling_runtime_paths
    from .submitters.crest_auto import (
        cancel_target as crest_cancel_target,
        submit_job_dir as submit_crest_job_dir,
    )
    from .submitters.orca_auto import cancel_target as orca_cancel_target
    from .submitters.orca_auto import submit_reaction_dir
    from .submitters.xtb_auto import (
        cancel_target as xtb_cancel_target,
        submit_job_dir as submit_xtb_job_dir,
    )
    from .workflows.orca_stage_utils import build_materialized_orca_stage, safe_name
    from .xyz_utils import choose_orca_geometry_frame

    return OrchestrationEngineDeps(
        **_facade_overrides(
            {
                "build_materialized_orca_stage": build_materialized_orca_stage,
                "choose_orca_geometry_frame": choose_orca_geometry_frame,
                "crest_cancel_target": crest_cancel_target,
                "load_crest_artifact_contract": load_crest_artifact_contract,
                "load_orca_artifact_contract": load_orca_artifact_contract,
                "load_xtb_artifact_contract": load_xtb_artifact_contract,
                "orca_cancel_target": orca_cancel_target,
                "safe_name": safe_name,
                "select_crest_downstream_inputs": select_crest_downstream_inputs,
                "select_endpoint_pairs": select_endpoint_pairs,
                "select_xtb_downstream_inputs": select_xtb_downstream_inputs,
                "sibling_runtime_paths": sibling_runtime_paths,
                "submit_crest_job_dir": submit_crest_job_dir,
                "submit_reaction_dir": submit_reaction_dir,
                "submit_xtb_job_dir": submit_xtb_job_dir,
                "xtb_cancel_target": xtb_cancel_target,
            }
        )
    )


def _build_stage_deps() -> OrchestrationStageDeps:
    from ._orchestration_builders import new_xtb_stage_impl
    from ._orchestration_stage_materialization import (
        append_crest_orca_stages_impl,
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

    overrides = _facade_overrides(
        {
            "_append_unique_artifact": append_unique_artifact_impl,
            "_append_crest_orca_stages": append_crest_orca_stages_impl,
            "_append_reaction_orca_stages": _append_reaction_orca_stages_fallback,
            "_append_reaction_xtb_stages": append_reaction_xtb_stages_impl,
            "_clear_reaction_xtb_handoff_error_if_recovering": (
                clear_reaction_xtb_handoff_error_if_recovering_impl
            ),
            "_coerce_mapping": _coerce_mapping_fallback,
            "_completed_crest_roles": completed_crest_roles_impl,
            "_completed_crest_stage": completed_crest_stage_impl,
            "_ensure_crest_job_dir": ensure_crest_job_dir_impl,
            "_ensure_xtb_job_dir": ensure_xtb_job_dir_impl,
            "_load_config_organized_root": load_config_organized_root_impl,
            "_load_config_root": load_config_root_impl,
            "_maybe_notify_workflow_phase_summary": _maybe_notify_workflow_phase_summary_fallback,
            "_new_xtb_stage": new_xtb_stage_impl,
            "_normalize_text": _normalize_text_fallback,
            "_persist_workflow_progress": _persist_workflow_progress_fallback,
            "_reaction_orca_source_candidate_path": reaction_orca_source_candidate_path_impl,
            "_reaction_ts_guess_error": reaction_ts_guess_error_impl,
            "_recompute_workflow_status": _recompute_workflow_status_fallback,
            "_safe_int": _safe_int_fallback,
            "_stage_metadata": stage_metadata_impl,
            "_submission_target": submission_target_impl,
            "_sync_crest_stage": sync_crest_stage_impl,
            "_sync_orca_stage": sync_orca_stage_impl,
            "_sync_xtb_stage": sync_xtb_stage_impl,
            "_task_payload_dict": task_payload_dict_impl,
            "_workflow_has_active_children": _workflow_has_active_children_fallback,
            "_workflow_sync_only": _workflow_sync_only_fallback,
            "_write_xtb_path_job": write_xtb_path_job_impl,
            "_xtb_attempt_record": xtb_attempt_record_impl,
            "_xtb_attempt_rows": xtb_attempt_rows_impl,
            "_xtb_current_attempt_number": xtb_current_attempt_number_impl,
            "_xtb_handoff_status": xtb_handoff_status_impl,
            "_xtb_path_retry_limit": xtb_path_retry_limit_impl,
            "_xtb_retry_recipe": xtb_retry_recipe_impl,
        }
    )
    overrides["_stage_failure_is_recoverable"] = _stage_failure_is_recoverable_override()
    return OrchestrationStageDeps(**overrides)


def _build_advance_deps() -> OrchestrationAdvanceDeps:
    from . import _orchestration_advance

    return OrchestrationAdvanceDeps(
        _cancel_active_workflow_stages=_facade_override(
            "_cancel_active_workflow_stages",
            _orchestration_advance._cancel_active_workflow_stages,
        ),
        _cancel_stage_activity=_facade_override(
            "_cancel_stage_activity",
            _orchestration_advance._cancel_stage_activity,
        ),
    )


def orchestration_deps() -> OrchestrationDeps:
    return OrchestrationDeps(
        contracts=_build_contract_deps(),
        persistence=_build_persistence_deps(),
        engines=_build_engine_deps(),
        stages=_build_stage_deps(),
        advance=_build_advance_deps(),
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
