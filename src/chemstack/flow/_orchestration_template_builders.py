from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from ._orchestration_requests import (
    ConformerScreeningWorkflowRequest,
    ReactionTsSearchWorkflowRequest,
    WorkflowCreationContext,
)
from ._orchestration_workflow_builders import (
    _ConformerWorkflowInput,
    _ReactionWorkflowInputs,
    _WorkflowWorkspace,
    _optional_mapping_parameter,
)
from .contracts import (
    WorkflowArtifactRef,
    WorkflowStagePayload,
    WorkflowTemplateRequest,
)


@dataclass(frozen=True)
class _WorkflowTemplateBuild:
    request: WorkflowTemplateRequest
    stages: list[WorkflowStagePayload]


def _crest_stage_payload(
    context: WorkflowCreationContext,
    *,
    workflow_id: str,
    template_name: str,
    stage_id: str,
    source_path: str,
    input_role: str,
    mode: str,
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    manifest_overrides: dict[str, Any] | None,
) -> WorkflowStagePayload:
    return cast(
        WorkflowStagePayload,
        context.new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name=template_name,
            stage_id=stage_id,
            source_path=source_path,
            input_role=input_role,
            mode=mode,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            manifest_overrides=manifest_overrides,
        ),
    )


def _reaction_crest_stages(
    request: ReactionTsSearchWorkflowRequest,
    workspace: _WorkflowWorkspace,
    inputs: _ReactionWorkflowInputs,
    context: WorkflowCreationContext,
    *,
    manifest_overrides: dict[str, Any],
) -> list[WorkflowStagePayload]:
    return [
        _crest_stage_payload(
            context,
            workflow_id=workspace.workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_reactant_01",
            source_path=inputs.reactant_xyz,
            input_role="reactant",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=manifest_overrides,
        ),
        _crest_stage_payload(
            context,
            workflow_id=workspace.workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_product_01",
            source_path=inputs.product_xyz,
            input_role="product",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=manifest_overrides,
        ),
    ]


def _conformer_crest_stages(
    request: ConformerScreeningWorkflowRequest,
    workspace: _WorkflowWorkspace,
    copied_input: _ConformerWorkflowInput,
    context: WorkflowCreationContext,
) -> list[WorkflowStagePayload]:
    return [
        _crest_stage_payload(
            context,
            workflow_id=workspace.workflow_id,
            template_name="conformer_screening",
            stage_id="crest_conformer_01",
            source_path=copied_input.input_xyz,
            input_role="molecule",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=request.crest_job_manifest,
        ),
    ]


def _reaction_template_request(
    request: ReactionTsSearchWorkflowRequest,
    workspace: _WorkflowWorkspace,
    inputs: _ReactionWorkflowInputs,
    *,
    resolved_crest_job_manifest: dict[str, Any],
) -> WorkflowTemplateRequest:
    return WorkflowTemplateRequest(
        workflow_id=workspace.workflow_id,
        template_name="reaction_ts_search",
        source_job_id=request.source_job_id,
        source_job_type=request.source_job_type or "raw_xyz",
        reaction_key=inputs.reaction_key,
        status="planned",
        requested_at=workspace.requested_at,
        parameters={
            "crest_mode": request.crest_mode,
            "priority": int(request.priority),
            "max_cores": int(request.max_cores),
            "max_memory_gb": int(request.max_memory_gb),
            "max_crest_candidates": int(request.max_crest_candidates),
            "max_xtb_stages": int(request.max_xtb_stages),
            "max_xtb_handoff_retries": int(request.max_xtb_handoff_retries),
            "max_orca_stages": int(request.max_orca_stages),
            "orca_route_line": str(request.orca_route_line),
            "charge": int(request.charge),
            "multiplicity": int(request.multiplicity),
            **_optional_mapping_parameter("crest_job_manifest", resolved_crest_job_manifest),
            **_optional_mapping_parameter("xtb_job_manifest", request.xtb_job_manifest),
            **_optional_mapping_parameter("endpoint_pairing", request.endpoint_pairing),
        },
        source_artifacts=(
            WorkflowArtifactRef(kind="reactant_xyz", path=inputs.reactant_xyz, selected=True),
            WorkflowArtifactRef(kind="product_xyz", path=inputs.product_xyz, selected=True),
        ),
    )


def _conformer_template_request(
    request: ConformerScreeningWorkflowRequest,
    workspace: _WorkflowWorkspace,
    copied_input: _ConformerWorkflowInput,
) -> WorkflowTemplateRequest:
    return WorkflowTemplateRequest(
        workflow_id=workspace.workflow_id,
        template_name="conformer_screening",
        source_job_id="",
        source_job_type="raw_xyz",
        reaction_key=copied_input.reaction_key,
        status="planned",
        requested_at=workspace.requested_at,
        parameters={
            "crest_mode": request.crest_mode,
            "priority": int(request.priority),
            "max_cores": int(request.max_cores),
            "max_memory_gb": int(request.max_memory_gb),
            "max_orca_stages": int(request.max_orca_stages),
            "orca_route_line": str(request.orca_route_line),
            "charge": int(request.charge),
            "multiplicity": int(request.multiplicity),
            **_optional_mapping_parameter("crest_job_manifest", request.crest_job_manifest),
        },
        source_artifacts=(
            WorkflowArtifactRef(kind="input_xyz", path=copied_input.input_xyz, selected=True),
        ),
    )


def _reaction_template_build(
    request: ReactionTsSearchWorkflowRequest,
    workspace: _WorkflowWorkspace,
    inputs: _ReactionWorkflowInputs,
    context: WorkflowCreationContext,
    *,
    resolved_crest_job_manifest: dict[str, Any],
) -> _WorkflowTemplateBuild:
    return _WorkflowTemplateBuild(
        request=_reaction_template_request(
            request,
            workspace,
            inputs,
            resolved_crest_job_manifest=resolved_crest_job_manifest,
        ),
        stages=_reaction_crest_stages(
            request,
            workspace,
            inputs,
            context,
            manifest_overrides=resolved_crest_job_manifest,
        ),
    )


def _conformer_template_build(
    request: ConformerScreeningWorkflowRequest,
    workspace: _WorkflowWorkspace,
    copied_input: _ConformerWorkflowInput,
    context: WorkflowCreationContext,
) -> _WorkflowTemplateBuild:
    return _WorkflowTemplateBuild(
        request=_conformer_template_request(request, workspace, copied_input),
        stages=_conformer_crest_stages(request, workspace, copied_input, context),
    )


__all__ = [
    "_WorkflowTemplateBuild",
    "_conformer_template_build",
    "_conformer_template_request",
    "_reaction_template_build",
    "_reaction_template_request",
]
