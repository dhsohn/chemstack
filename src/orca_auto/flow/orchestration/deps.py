from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import orca_auto.flow.orchestration.dep_builders as _dep_builders
from orca_auto.flow.orchestration.dep_types import (
    _ORCHESTRATION_STAGE_BUILDER_GROUP,
    _ORCHESTRATION_STAGE_DEP_GROUPS,
    _ORCHESTRATION_STAGE_DEP_REGISTRY,
    _ORCHESTRATION_STAGE_DEP_TARGETS,
    _ORCHESTRATION_STAGE_MATERIALIZATION_GROUP,
    _ORCHESTRATION_STAGE_RUNTIME_GROUP,
    _ORCHESTRATION_STAGE_SUPPORT_GROUP,
    _ORCHESTRATION_STAGE_WORKFLOW_GROUP,
    AnyCallable,
    MappingCoercer,
    OrchestrationAdvanceDeps,
    OrchestrationContractDeps,
    OrchestrationDeps,
    OrchestrationEngineDeps,
    OrchestrationPersistenceDeps,
    OrchestrationStageBuilderDeps,
    OrchestrationStageDeps,
    OrchestrationStageMaterializationDeps,
    OrchestrationStageRuntimeDeps,
    OrchestrationStageSupportDeps,
    OrchestrationStageWorkflowDeps,
    StageMetadataResolver,
    StagePredicate,
    TextNormalizer,
    WorkflowPayload,
    WorkflowPayloadLoader,
    WorkflowPayloadWriter,
    WorkflowPredicate,
    WorkflowRegistrySyncer,
    WorkflowStagePayload,
    WorkflowStatusComputer,
    WorkflowWorkspace,
    WorkflowWorkspaceResolver,
    _OrchestrationStageDepGroup,
    _stage_dep_group,
)


def orchestration_deps(overrides: Mapping[str, Any] | None = None) -> OrchestrationDeps:
    deps_provider = _dep_builders._LazyOrchestrationDeps(overrides, factory=orchestration_deps)
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
    "AnyCallable",
    "MappingCoercer",
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
    "StageMetadataResolver",
    "StagePredicate",
    "TextNormalizer",
    "WorkflowPayload",
    "WorkflowPayloadLoader",
    "WorkflowPayloadWriter",
    "WorkflowPredicate",
    "WorkflowRegistrySyncer",
    "WorkflowStagePayload",
    "WorkflowStatusComputer",
    "WorkflowWorkspace",
    "WorkflowWorkspaceResolver",
    "_ORCHESTRATION_STAGE_BUILDER_GROUP",
    "_ORCHESTRATION_STAGE_DEP_GROUPS",
    "_ORCHESTRATION_STAGE_DEP_REGISTRY",
    "_ORCHESTRATION_STAGE_DEP_TARGETS",
    "_ORCHESTRATION_STAGE_MATERIALIZATION_GROUP",
    "_ORCHESTRATION_STAGE_RUNTIME_GROUP",
    "_ORCHESTRATION_STAGE_SUPPORT_GROUP",
    "_ORCHESTRATION_STAGE_WORKFLOW_GROUP",
    "_OrchestrationStageDepGroup",
    "_stage_dep_group",
    "orchestration_deps",
]
