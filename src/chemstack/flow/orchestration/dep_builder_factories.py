from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .dep_builder_core import (
    _LazyOrchestrationDeps,
    _StageDepFallbackGroup,
    _StageDepFallbackRegistry,
    _StageDepFallbackSpec,
    _bind_with_deps,
    _build_dep_dataclass,
    _deps_provider,
)
from .dep_builder_stage_fallbacks import (
    _stage_builder_fallbacks,
    _stage_builder_fallbacks_for_context,
    _stage_materialization_fallbacks,
    _stage_materialization_fallbacks_for_context,
    _stage_runtime_fallbacks,
    _stage_runtime_fallbacks_for_context,
    _stage_support_fallbacks,
    _stage_support_fallbacks_for_context,
    _stage_workflow_fallbacks,
    _stage_workflow_fallbacks_for_context,
)
from .dep_types import (
    _ORCHESTRATION_STAGE_BUILDER_GROUP,
    _ORCHESTRATION_STAGE_MATERIALIZATION_GROUP,
    _ORCHESTRATION_STAGE_RUNTIME_GROUP,
    _ORCHESTRATION_STAGE_SUPPORT_GROUP,
    _ORCHESTRATION_STAGE_WORKFLOW_GROUP,
    OrchestrationAdvanceDeps,
    OrchestrationContractDeps,
    OrchestrationEngineDeps,
    OrchestrationPersistenceDeps,
    OrchestrationStageDeps,
)


def _build_contract_deps(overrides: Mapping[str, Any] | None) -> OrchestrationContractDeps:
    from chemstack.flow.contracts import (
        CrestDownstreamPolicy,
        WorkflowStageInput,
        XtbDownstreamPolicy,
    )
    from chemstack.flow.endpoint_pairing import EndpointPairingPolicy

    return _build_dep_dataclass(
        OrchestrationContractDeps,
        overrides,
        {
            "CrestDownstreamPolicy": CrestDownstreamPolicy,
            "EndpointPairingPolicy": EndpointPairingPolicy,
            "WorkflowStageInput": WorkflowStageInput,
            "XtbDownstreamPolicy": XtbDownstreamPolicy,
        },
    )


def _build_persistence_deps(
    overrides: Mapping[str, Any] | None,
) -> OrchestrationPersistenceDeps:
    from chemstack.core.utils import now_utc_iso

    from chemstack.flow.registry import sync_workflow_registry
    from chemstack.flow.state import acquire_workflow_lock, load_workflow_payload
    from chemstack.flow.state import resolve_workflow_workspace, write_workflow_payload

    return _build_dep_dataclass(
        OrchestrationPersistenceDeps,
        overrides,
        {
            "acquire_workflow_lock": acquire_workflow_lock,
            "load_workflow_payload": load_workflow_payload,
            "now_utc_iso": now_utc_iso,
            "resolve_workflow_workspace": resolve_workflow_workspace,
            "sync_workflow_registry": sync_workflow_registry,
            "write_workflow_payload": write_workflow_payload,
        },
    )


def _build_engine_deps(overrides: Mapping[str, Any] | None) -> OrchestrationEngineDeps:
    from chemstack.flow._orca_stage_materialization import build_materialized_orca_stage, safe_name
    from chemstack.flow.adapters.crest import (
        load_crest_artifact_contract,
        select_crest_downstream_inputs,
    )
    from chemstack.flow.adapters.orca import load_orca_artifact_contract
    from chemstack.flow.adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
    from chemstack.flow.endpoint_pairing import select_endpoint_pairs
    from chemstack.flow.engine_runtime import engine_runtime_paths
    from chemstack.flow.submitters.crest import (
        cancel_target as crest_cancel_target,
        submit_job_dir as submit_crest_job_dir,
    )
    from chemstack.flow.submitters.orca import cancel_target as orca_cancel_target
    from chemstack.flow.submitters.orca import submit_reaction_dir
    from chemstack.flow.submitters.xtb import (
        cancel_target as xtb_cancel_target,
        submit_job_dir as submit_xtb_job_dir,
    )
    from chemstack.flow.xyz_utils import choose_orca_geometry_frame

    return _build_dep_dataclass(
        OrchestrationEngineDeps,
        overrides,
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
            "engine_runtime_paths": engine_runtime_paths,
            "submit_crest_job_dir": submit_crest_job_dir,
            "submit_reaction_dir": submit_reaction_dir,
            "submit_xtb_job_dir": submit_xtb_job_dir,
            "xtb_cancel_target": xtb_cancel_target,
        },
    )


def _stage_dep_fallback_registry() -> _StageDepFallbackRegistry:
    return _StageDepFallbackRegistry(
        (
            _StageDepFallbackSpec(
                _ORCHESTRATION_STAGE_BUILDER_GROUP,
                _stage_builder_fallbacks_for_context,
            ),
            _StageDepFallbackSpec(
                _ORCHESTRATION_STAGE_MATERIALIZATION_GROUP,
                _stage_materialization_fallbacks_for_context,
            ),
            _StageDepFallbackSpec(
                _ORCHESTRATION_STAGE_RUNTIME_GROUP,
                _stage_runtime_fallbacks_for_context,
            ),
            _StageDepFallbackSpec(
                _ORCHESTRATION_STAGE_SUPPORT_GROUP,
                _stage_support_fallbacks_for_context,
            ),
            _StageDepFallbackSpec(
                _ORCHESTRATION_STAGE_WORKFLOW_GROUP,
                _stage_workflow_fallbacks_for_context,
            ),
        )
    )


def _stage_dep_fallbacks(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    return _stage_dep_fallback_registry().flat_fallbacks(overrides, deps_provider)


def _stage_dep_fallback_groups(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> tuple[_StageDepFallbackGroup, ...]:
    return _stage_dep_fallback_registry().build_groups(overrides, deps_provider)


def _build_stage_deps(
    overrides: Mapping[str, Any] | None,
    *,
    deps_provider: _LazyOrchestrationDeps | None = None,
) -> OrchestrationStageDeps:
    provider = _deps_provider(overrides, deps_provider)
    return _stage_dep_fallback_registry().build_deps(
        OrchestrationStageDeps,
        overrides,
        provider,
    )


def _build_advance_deps(
    overrides: Mapping[str, Any] | None,
    *,
    deps_provider: _LazyOrchestrationDeps | None = None,
) -> OrchestrationAdvanceDeps:
    from chemstack.flow.orchestration.advance import (
        _cancel_active_workflow_stages,
        _cancel_stage_activity,
    )

    provider = _deps_provider(overrides, deps_provider)
    return _build_dep_dataclass(
        OrchestrationAdvanceDeps,
        overrides,
        {
            "_cancel_active_workflow_stages": _bind_with_deps(
                provider,
                _cancel_active_workflow_stages,
            ),
            "_cancel_stage_activity": _bind_with_deps(
                provider,
                _cancel_stage_activity,
            ),
        },
    )


__all__ = [
    "_build_advance_deps",
    "_build_contract_deps",
    "_build_engine_deps",
    "_build_persistence_deps",
    "_build_stage_deps",
    "_stage_builder_fallbacks",
    "_stage_dep_fallback_registry",
    "_stage_dep_fallback_groups",
    "_stage_dep_fallbacks",
    "_stage_materialization_fallbacks",
    "_stage_runtime_fallbacks",
    "_stage_support_fallbacks",
    "_stage_workflow_fallbacks",
]
