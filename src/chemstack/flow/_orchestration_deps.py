from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.facade import resolve_grouped_attr

_ORCHESTRATION_FACADE_MODULE = "chemstack.flow.orchestration"
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
    safe_name: Callable[[Any], str]
    select_crest_downstream_inputs: AnyCallable
    select_endpoint_pairs: AnyCallable
    select_xtb_downstream_inputs: AnyCallable
    sibling_runtime_paths: AnyCallable
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

    def __getattr__(self, name: str) -> Any:
        return resolve_grouped_attr(
            name,
            (self.contracts, self.persistence, self.engines, self.stages, self.advance),
        )


@dataclass(frozen=True)
class OrchestrationOverrideResolver:
    overrides: Mapping[str, Any] | None = None
    facade_module_name: str = _ORCHESTRATION_FACADE_MODULE

    def get(self, name: str, fallback: Any) -> Any:
        if self.overrides is not None and name in self.overrides:
            return self.overrides[name]
        facade = sys.modules.get(self.facade_module_name)
        if facade is None:
            return fallback
        return getattr(facade, name, fallback)

    def map(self, items: dict[str, Any]) -> dict[str, Any]:
        return {name: self.get(name, fallback) for name, fallback in items.items()}


def _override_resolver(
    overrides: Mapping[str, Any] | None = None,
    *,
    facade_module_name: str = _ORCHESTRATION_FACADE_MODULE,
) -> OrchestrationOverrideResolver:
    return OrchestrationOverrideResolver(
        overrides=overrides,
        facade_module_name=facade_module_name,
    )


def _facade_override(name: str, fallback: Any) -> Any:
    return _override_resolver().get(name, fallback)


def _facade_overrides(items: dict[str, Any]) -> dict[str, Any]:
    return _override_resolver().map(items)


def _coerce_mapping_fallback(value: Any) -> dict[str, Any]:
    from chemstack.core.utils import mapping_or_empty

    return mapping_or_empty(value)


def _normalize_text_fallback(value: Any) -> str:
    from chemstack.core.utils import normalize_text

    return normalize_text(value)


def _safe_int_fallback(value: Any, *, default: int = 0) -> int:
    from chemstack.core.utils import safe_int

    return safe_int(value, default=default)


def _normalize_text_override(resolver: OrchestrationOverrideResolver | None = None) -> Any:
    resolver = resolver or _override_resolver()
    return resolver.get("_normalize_text", _normalize_text_fallback)


def _stage_metadata_override(resolver: OrchestrationOverrideResolver | None = None) -> Any:
    from ._orchestration_support import stage_metadata_impl

    resolver = resolver or _override_resolver()
    return resolver.get("_stage_metadata", stage_metadata_impl)


def _stage_failure_is_recoverable_override(
    resolver: OrchestrationOverrideResolver | None = None,
) -> Any:
    resolver = resolver or _override_resolver()
    override = resolver.get("_stage_failure_is_recoverable", None)
    if override is not None:
        return override

    def stage_failure_is_recoverable(stage: dict[str, Any]) -> bool:
        return _stage_failure_is_recoverable_fallback(stage, resolver=resolver)

    return stage_failure_is_recoverable


def _workflow_sync_only_fallback(
    payload: dict[str, Any],
    *,
    resolver: OrchestrationOverrideResolver | None = None,
) -> bool:
    from ._orchestration_lifecycle import workflow_sync_only_impl

    return workflow_sync_only_impl(
        payload,
        normalize_text_fn=_normalize_text_override(resolver),
    )


def _workflow_has_active_children_fallback(
    payload: dict[str, Any],
    *,
    resolver: OrchestrationOverrideResolver | None = None,
) -> bool:
    from ._orchestration_lifecycle import workflow_has_active_children_impl
    from .state import workflow_has_active_downstream

    resolver = resolver or _override_resolver()
    return workflow_has_active_children_impl(
        payload,
        normalize_text_fn=_normalize_text_override(resolver),
        workflow_has_active_downstream_fn=resolver.get(
            "workflow_has_active_downstream",
            workflow_has_active_downstream,
        ),
    )


def _stage_failure_is_recoverable_fallback(
    stage: dict[str, Any],
    *,
    resolver: OrchestrationOverrideResolver | None = None,
) -> bool:
    from ._orchestration_lifecycle import stage_failure_is_recoverable_impl

    return stage_failure_is_recoverable_impl(
        stage,
        normalize_text_fn=_normalize_text_override(resolver),
        stage_metadata_fn=_stage_metadata_override(resolver),
    )


def _effective_stage_status_fallback(
    stage: dict[str, Any],
    *,
    resolver: OrchestrationOverrideResolver | None = None,
) -> str:
    from ._orchestration_lifecycle import effective_stage_status_impl

    resolver = resolver or _override_resolver()
    stage_failure_is_recoverable = _stage_failure_is_recoverable_override(resolver)
    return effective_stage_status_impl(
        stage,
        normalize_text_fn=_normalize_text_override(resolver),
        stage_failure_is_recoverable_fn=stage_failure_is_recoverable,
    )


def _recompute_workflow_status_fallback(
    payload: dict[str, Any],
    *,
    resolver: OrchestrationOverrideResolver | None = None,
) -> str:
    from ._orchestration_lifecycle import recompute_workflow_status_impl

    resolver = resolver or _override_resolver()
    effective_stage_status = resolver.get("_effective_stage_status", None)
    if effective_stage_status is None:

        def effective_stage_status(stage: dict[str, Any]) -> str:
            return _effective_stage_status_fallback(stage, resolver=resolver)

    return recompute_workflow_status_impl(
        payload,
        normalize_text_fn=_normalize_text_override(resolver),
        effective_stage_status_fn=effective_stage_status,
    )


def _persist_workflow_progress_fallback(
    workflow_root: Path,
    workspace_dir: Path,
    payload: dict[str, Any],
    *,
    sync_only: bool,
    resolver: OrchestrationOverrideResolver | None = None,
) -> None:
    from .registry import sync_workflow_registry
    from .state import write_workflow_payload

    resolver = resolver or _override_resolver()
    normalize = _normalize_text_override(resolver)
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
    resolver.get("write_workflow_payload", write_workflow_payload)(workspace_dir, payload)
    resolver.get("sync_workflow_registry", sync_workflow_registry)(
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
    resolver: OrchestrationOverrideResolver | None = None,
) -> bool:
    from .workflow_notifications import maybe_notify_workflow_phase_summary

    return maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=config_path,
        phase_engine=phase_engine,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(resolver),
        extra_lines=extra_lines,
    )


def _append_reaction_orca_stages_fallback(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_auto_config: str | None,
    orca_auto_config: str | None,
    resolver: OrchestrationOverrideResolver | None = None,
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


def _build_contract_deps(resolver: OrchestrationOverrideResolver) -> OrchestrationContractDeps:
    from .contracts import CrestDownstreamPolicy, WorkflowStageInput, XtbDownstreamPolicy
    from .endpoint_pairing import EndpointPairingPolicy

    return OrchestrationContractDeps(
        **resolver.map(
            {
                "CrestDownstreamPolicy": CrestDownstreamPolicy,
                "EndpointPairingPolicy": EndpointPairingPolicy,
                "WorkflowStageInput": WorkflowStageInput,
                "XtbDownstreamPolicy": XtbDownstreamPolicy,
            }
        )
    )


def _build_persistence_deps(resolver: OrchestrationOverrideResolver) -> OrchestrationPersistenceDeps:
    from chemstack.core.utils import now_utc_iso

    from .registry import sync_workflow_registry
    from .state import acquire_workflow_lock, load_workflow_payload
    from .state import resolve_workflow_workspace, write_workflow_payload

    return OrchestrationPersistenceDeps(
        **resolver.map(
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


def _build_engine_deps(resolver: OrchestrationOverrideResolver) -> OrchestrationEngineDeps:
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
        **resolver.map(
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


def _stage_builder_fallbacks() -> dict[str, Any]:
    from ._orchestration_builders import new_xtb_stage_impl

    return {
        "_new_xtb_stage": new_xtb_stage_impl,
    }


def _stage_materialization_fallbacks(
    resolver: OrchestrationOverrideResolver,
) -> dict[str, Any]:
    from ._orchestration_stage_materialization import (
        append_crest_orca_stages_impl,
        append_reaction_xtb_stages_impl,
    )

    def append_reaction_orca_stages(
        payload: dict[str, Any],
        *,
        workspace_dir: Path,
        xtb_auto_config: str | None,
        orca_auto_config: str | None,
    ) -> bool:
        return _append_reaction_orca_stages_fallback(
            payload,
            workspace_dir=workspace_dir,
            xtb_auto_config=xtb_auto_config,
            orca_auto_config=orca_auto_config,
            resolver=resolver,
        )

    return {
        "_append_crest_orca_stages": append_crest_orca_stages_impl,
        "_append_reaction_orca_stages": append_reaction_orca_stages,
        "_append_reaction_xtb_stages": append_reaction_xtb_stages_impl,
    }


def _stage_runtime_fallbacks() -> dict[str, Any]:
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

    return {
        "_append_unique_artifact": append_unique_artifact_impl,
        "_completed_crest_roles": completed_crest_roles_impl,
        "_completed_crest_stage": completed_crest_stage_impl,
        "_ensure_crest_job_dir": ensure_crest_job_dir_impl,
        "_ensure_xtb_job_dir": ensure_xtb_job_dir_impl,
        "_sync_crest_stage": sync_crest_stage_impl,
        "_sync_orca_stage": sync_orca_stage_impl,
        "_sync_xtb_stage": sync_xtb_stage_impl,
        "_write_xtb_path_job": write_xtb_path_job_impl,
        "_xtb_attempt_record": xtb_attempt_record_impl,
        "_xtb_attempt_rows": xtb_attempt_rows_impl,
        "_xtb_current_attempt_number": xtb_current_attempt_number_impl,
        "_xtb_handoff_status": xtb_handoff_status_impl,
        "_xtb_path_retry_limit": xtb_path_retry_limit_impl,
        "_xtb_retry_recipe": xtb_retry_recipe_impl,
    }


def _stage_support_fallbacks() -> dict[str, Any]:
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

    return {
        "_clear_reaction_xtb_handoff_error_if_recovering": (
            clear_reaction_xtb_handoff_error_if_recovering_impl
        ),
        "_load_config_organized_root": load_config_organized_root_impl,
        "_load_config_root": load_config_root_impl,
        "_reaction_orca_source_candidate_path": reaction_orca_source_candidate_path_impl,
        "_reaction_ts_guess_error": reaction_ts_guess_error_impl,
        "_stage_metadata": stage_metadata_impl,
        "_submission_target": submission_target_impl,
        "_task_payload_dict": task_payload_dict_impl,
    }


def _stage_workflow_fallbacks(
    resolver: OrchestrationOverrideResolver,
) -> dict[str, Any]:
    def workflow_sync_only(payload: dict[str, Any]) -> bool:
        return _workflow_sync_only_fallback(payload, resolver=resolver)

    def workflow_has_active_children(payload: dict[str, Any]) -> bool:
        return _workflow_has_active_children_fallback(payload, resolver=resolver)

    def recompute_workflow_status(payload: dict[str, Any]) -> str:
        return _recompute_workflow_status_fallback(payload, resolver=resolver)

    def persist_workflow_progress(
        workflow_root: Path,
        workspace_dir: Path,
        payload: dict[str, Any],
        *,
        sync_only: bool,
    ) -> None:
        _persist_workflow_progress_fallback(
            workflow_root,
            workspace_dir,
            payload,
            sync_only=sync_only,
            resolver=resolver,
        )

    def maybe_notify_phase_summary(
        payload: dict[str, Any],
        *,
        config_path: str | None,
        phase_engine: str,
        extra_lines: list[str] | None = None,
    ) -> bool:
        return _maybe_notify_workflow_phase_summary_fallback(
            payload,
            config_path=config_path,
            phase_engine=phase_engine,
            extra_lines=extra_lines,
            resolver=resolver,
        )

    return {
        "_coerce_mapping": _coerce_mapping_fallback,
        "_maybe_notify_workflow_phase_summary": maybe_notify_phase_summary,
        "_normalize_text": _normalize_text_fallback,
        "_persist_workflow_progress": persist_workflow_progress,
        "_recompute_workflow_status": recompute_workflow_status,
        "_safe_int": _safe_int_fallback,
        "_workflow_has_active_children": workflow_has_active_children,
        "_workflow_sync_only": workflow_sync_only,
    }


def _stage_dep_fallbacks(resolver: OrchestrationOverrideResolver) -> dict[str, Any]:
    fallbacks: dict[str, Any] = {}
    for group in (
        _stage_builder_fallbacks(),
        _stage_materialization_fallbacks(resolver),
        _stage_runtime_fallbacks(),
        _stage_support_fallbacks(),
        _stage_workflow_fallbacks(resolver),
    ):
        fallbacks.update(group)
    return fallbacks


def _build_stage_deps(resolver: OrchestrationOverrideResolver) -> OrchestrationStageDeps:
    resolved = resolver.map(_stage_dep_fallbacks(resolver))
    resolved["_stage_failure_is_recoverable"] = _stage_failure_is_recoverable_override(resolver)
    return OrchestrationStageDeps(**resolved)


def _build_advance_deps(resolver: OrchestrationOverrideResolver) -> OrchestrationAdvanceDeps:
    from . import _orchestration_advance

    return OrchestrationAdvanceDeps(
        _cancel_active_workflow_stages=resolver.get(
            "_cancel_active_workflow_stages",
            _orchestration_advance._cancel_active_workflow_stages,
        ),
        _cancel_stage_activity=resolver.get(
            "_cancel_stage_activity",
            _orchestration_advance._cancel_stage_activity,
        ),
    )


def orchestration_deps(
    overrides: Mapping[str, Any] | None = None,
    *,
    facade_module_name: str = _ORCHESTRATION_FACADE_MODULE,
) -> OrchestrationDeps:
    resolver = _override_resolver(overrides, facade_module_name=facade_module_name)
    return OrchestrationDeps(
        contracts=_build_contract_deps(resolver),
        persistence=_build_persistence_deps(resolver),
        engines=_build_engine_deps(resolver),
        stages=_build_stage_deps(resolver),
        advance=_build_advance_deps(resolver),
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
    "OrchestrationOverrideResolver",
    "OrchestrationPersistenceDeps",
    "OrchestrationStageDeps",
    "call_engine_aware",
    "orchestration_deps",
]
