from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from chemstack.core.app_ids import (
    CHEMSTACK_CREST_COMMAND,
    CHEMSTACK_CREST_MODULE,
    CHEMSTACK_XTB_COMMAND,
    CHEMSTACK_XTB_MODULE,
)

from ._orchestration_requests import (
    ConformerScreeningWorkflowRequest,
    ReactionTsSearchWorkflowCreationContext,
    ReactionTsSearchWorkflowRequest,
    WorkflowCreationContext,
    WorkflowPersistenceContext,
)
from .contracts import (
    WorkflowArtifactRef,
    WorkflowPlan,
    WorkflowPlanPayload,
    WorkflowStage,
    WorkflowStagePayload,
    WorkflowStageWithTaskPayload,
    WorkflowTask,
    WorkflowTemplateRequest,
)

_REACTION_TS_SEARCH_CREST_MANIFEST_DEFAULTS: dict[str, Any] = {"rthr": 0.3}


@dataclass(frozen=True)
class _StagePayloadSections:
    task_payload: dict[str, Any]
    task_metadata: dict[str, Any]
    stage_metadata: dict[str, Any]


def _merge_manifest_defaults(
    defaults: dict[str, Any],
    overrides: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(defaults)
    for raw_key, value in dict(overrides or {}).items():
        key = str(raw_key).strip()
        if not key:
            continue
        if value is None or (isinstance(value, str) and not value.strip()):
            merged.pop(key, None)
            continue
        merged[key] = value
    return merged


def _resource_request(max_cores: int, max_memory_gb: int) -> dict[str, int]:
    return {"max_cores": int(max_cores), "max_memory_gb": int(max_memory_gb)}


def _optional_mapping_parameter(name: str, value: dict[str, Any] | None) -> dict[str, Any]:
    return {name: dict(value)} if value else {}


def _stage_payload_sections(
    *,
    task_payload: dict[str, Any],
    task_metadata: dict[str, Any],
    stage_metadata: dict[str, Any],
    manifest_overrides: dict[str, Any] | None,
) -> _StagePayloadSections:
    resolved_overrides = dict(manifest_overrides or {})
    if resolved_overrides:
        task_payload["job_manifest_overrides"] = resolved_overrides
        task_metadata["job_manifest_overrides"] = resolved_overrides
        stage_metadata["job_manifest_overrides"] = resolved_overrides
    return _StagePayloadSections(
        task_payload=task_payload,
        task_metadata=task_metadata,
        stage_metadata=stage_metadata,
    )


def _engine_enqueue_payload(
    *,
    submitter: str,
    app_name: str,
    command_name: str,
    module_name: str,
    config_placeholder: str,
    priority: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "submitter": submitter,
        "app_name": app_name,
        "command": f"{command_name} --config {config_placeholder} run-dir '<job_dir>' --priority {int(priority)}",
        "command_argv": [
            "python",
            "-m",
            module_name,
            "--config",
            config_placeholder,
            "run-dir",
            "<job_dir>",
            "--priority",
            str(int(priority)),
        ],
        "requires_config": True,
        "config_argument_placeholder": config_placeholder,
        "job_dir": "",
        "priority": int(priority),
    }
    payload.update(extra or {})
    return payload


def new_crest_stage_impl(
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
    manifest_overrides: dict[str, Any] | None = None,
) -> WorkflowStageWithTaskPayload:
    config_placeholder = "<crest_auto_config>"
    sections = _stage_payload_sections(
        task_payload={
            "workflow_id": workflow_id,
            "template_name": template_name,
            "source_input_xyz": source_path,
            "selected_input_xyz": "",
            "job_dir": "",
            "mode": mode,
            "input_role": input_role,
        },
        task_metadata={
            "input_role": input_role,
            "mode": mode,
        },
        stage_metadata={"input_role": input_role, "mode": mode},
        manifest_overrides=manifest_overrides,
    )
    task = WorkflowTask.from_raw(
        task_id=f"{workflow_id}:{stage_id}",
        engine="crest",
        task_kind="conformer_search",
        resource_request=_resource_request(max_cores, max_memory_gb),
        payload=sections.task_payload,
        enqueue_payload=_engine_enqueue_payload(
            submitter="crest_auto_cli",
            app_name="crest_auto",
            command_name=CHEMSTACK_CREST_COMMAND,
            module_name=CHEMSTACK_CREST_MODULE,
            config_placeholder=config_placeholder,
            priority=priority,
        ),
        metadata=sections.task_metadata,
    )
    stage = WorkflowStage(
        stage_id=stage_id,
        stage_kind="crest_stage",
        status="planned",
        input_artifacts=(
            WorkflowArtifactRef(
                kind="input_xyz",
                path=source_path,
                selected=True,
                metadata={"input_role": input_role},
            ),
        ),
        output_artifacts=(),
        task=task,
        metadata=sections.stage_metadata,
    )
    return cast(WorkflowStageWithTaskPayload, stage.to_dict())


def new_xtb_stage_impl(
    *,
    workflow_id: str,
    stage_id: str,
    reaction_key: str,
    reactant_input: dict[str, Any],
    product_input: dict[str, Any],
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    max_handoff_retries: int = 2,
    manifest_overrides: dict[str, Any] | None = None,
) -> WorkflowStageWithTaskPayload:
    config_placeholder = "<xtb_auto_config>"
    retry_limit = max(0, int(max_handoff_retries))
    sections = _stage_payload_sections(
        task_payload={
            "workflow_id": workflow_id,
            "job_dir": "",
            "reaction_key": reaction_key,
            "reactant_source": dict(reactant_input),
            "product_source": dict(product_input),
            "selected_input_xyz": "",
            "secondary_input_xyz": "",
            "max_handoff_retries": retry_limit,
        },
        task_metadata={
            "reaction_key": reaction_key,
            "max_handoff_retries": retry_limit,
        },
        stage_metadata={
            "reaction_key": reaction_key,
            "max_handoff_retries": retry_limit,
        },
        manifest_overrides=manifest_overrides,
    )
    task = WorkflowTask.from_raw(
        task_id=f"{workflow_id}:{stage_id}",
        engine="xtb",
        task_kind="path_search",
        resource_request=_resource_request(max_cores, max_memory_gb),
        payload=sections.task_payload,
        enqueue_payload=_engine_enqueue_payload(
            submitter="xtb_auto_cli",
            app_name="xtb_auto",
            command_name=CHEMSTACK_XTB_COMMAND,
            module_name=CHEMSTACK_XTB_MODULE,
            config_placeholder=config_placeholder,
            priority=priority,
            extra={"reaction_key": reaction_key},
        ),
        metadata=sections.task_metadata,
    )
    stage = WorkflowStage(
        stage_id=stage_id,
        stage_kind="xtb_stage",
        status="planned",
        input_artifacts=(
            WorkflowArtifactRef(
                kind="crest_conformer",
                path=str(reactant_input["artifact_path"]),
                selected=True,
                metadata={"role": "reactant", "source_job_id": reactant_input["source_job_id"]},
            ),
            WorkflowArtifactRef(
                kind="crest_conformer",
                path=str(product_input["artifact_path"]),
                selected=True,
                metadata={"role": "product", "source_job_id": product_input["source_job_id"]},
            ),
        ),
        output_artifacts=(),
        task=task,
        metadata=sections.stage_metadata,
    )
    return cast(WorkflowStageWithTaskPayload, stage.to_dict())


def _copy_input_impl(source: str, target: Path) -> str:
    src = Path(source).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Input XYZ not found: {src}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)
    return str(target.resolve())


def _persist_workflow(
    *,
    persistence_context: WorkflowPersistenceContext,
    request: WorkflowTemplateRequest,
    stages: list[dict[str, Any]],
    creation_context: WorkflowCreationContext,
) -> WorkflowPlanPayload:
    plan = WorkflowPlan(
        workflow_id=persistence_context.workflow_id,
        template_name=persistence_context.template_name,
        status="planned",
        source_job_id=persistence_context.source_job_id,
        source_job_type=persistence_context.source_job_type,
        reaction_key=persistence_context.reaction_key,
        requested_at=persistence_context.requested_at,
        stages=(),
        metadata={
            "request": request.to_dict(),
            "workspace_dir": str(persistence_context.workspace_dir),
        },
    )
    payload = plan.to_dict()
    payload["stages"] = cast(list[WorkflowStagePayload], list(stages))
    callback_payload = cast(dict[str, Any], payload)
    creation_context.write_workflow_payload_fn(
        persistence_context.workspace_dir, callback_payload
    )
    creation_context.sync_workflow_registry_fn(
        persistence_context.workflow_root_path,
        persistence_context.workspace_dir,
        callback_payload,
    )
    return payload


def create_reaction_ts_search_workflow_impl(
    *,
    request: ReactionTsSearchWorkflowRequest,
    context: ReactionTsSearchWorkflowCreationContext,
) -> WorkflowPlanPayload:
    workflow_id = str(request.workflow_id or "").strip() or context.workflow_id_factory(
        "wf_reaction_ts"
    )
    workflow_root_path = Path(request.workflow_root).expanduser().resolve()
    workspace_dir = workflow_root_path / workflow_id
    resolved_crest_job_manifest = _merge_manifest_defaults(
        _REACTION_TS_SEARCH_CREST_MANIFEST_DEFAULTS,
        request.crest_job_manifest,
    )
    reactant_sequence = context.load_xyz_atom_sequence_fn(request.reactant_xyz)
    product_sequence = context.load_xyz_atom_sequence_fn(request.product_xyz)
    if reactant_sequence != product_sequence:
        raise ValueError(
            "reaction_ts_search requires identical reactant/product atom order for xTB path search; "
            f"reactant sequence={list(reactant_sequence)}, product sequence={list(product_sequence)}"
        )
    input_reactant = context.copy_input_fn(
        request.reactant_xyz,
        workspace_dir / "inputs" / "reactants" / Path(request.reactant_xyz).name,
    )
    input_product = context.copy_input_fn(
        request.product_xyz,
        workspace_dir / "inputs" / "products" / Path(request.product_xyz).name,
    )
    requested_at = context.now_utc_iso_fn()
    stages: list[dict[str, Any]] = [
        context.new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_reactant_01",
            source_path=input_reactant,
            input_role="reactant",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=resolved_crest_job_manifest,
        ),
        context.new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="reaction_ts_search",
            stage_id="crest_product_01",
            source_path=input_product,
            input_role="product",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=resolved_crest_job_manifest,
        ),
    ]
    resolved_source_job_type = request.source_job_type or "raw_xyz"
    reaction_key = f"{Path(input_reactant).stem}_to_{Path(input_product).stem}"
    template_request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        source_job_id=request.source_job_id,
        source_job_type=resolved_source_job_type,
        reaction_key=reaction_key,
        status="planned",
        requested_at=requested_at,
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
            WorkflowArtifactRef(kind="reactant_xyz", path=input_reactant, selected=True),
            WorkflowArtifactRef(kind="product_xyz", path=input_product, selected=True),
        ),
    )
    return _persist_workflow(
        persistence_context=WorkflowPersistenceContext(
            workflow_root_path=workflow_root_path,
            workspace_dir=workspace_dir,
            workflow_id=workflow_id,
            template_name="reaction_ts_search",
            source_job_id=request.source_job_id,
            source_job_type=resolved_source_job_type,
            reaction_key=reaction_key,
            requested_at=requested_at,
        ),
        request=template_request,
        stages=stages,
        creation_context=context,
    )


def create_conformer_screening_workflow_impl(
    *,
    request: ConformerScreeningWorkflowRequest,
    context: WorkflowCreationContext,
) -> WorkflowPlanPayload:
    workflow_id = str(request.workflow_id or "").strip() or context.workflow_id_factory(
        "wf_conformer_screening"
    )
    workflow_root_path = Path(request.workflow_root).expanduser().resolve()
    workspace_dir = workflow_root_path / workflow_id
    copied_input = context.copy_input_fn(
        request.input_xyz,
        workspace_dir / "inputs" / Path(request.input_xyz).name,
    )
    requested_at = context.now_utc_iso_fn()
    stages = [
        context.new_crest_stage_fn(
            workflow_id=workflow_id,
            template_name="conformer_screening",
            stage_id="crest_conformer_01",
            source_path=copied_input,
            input_role="molecule",
            mode=request.crest_mode,
            priority=request.priority,
            max_cores=request.max_cores,
            max_memory_gb=request.max_memory_gb,
            manifest_overrides=request.crest_job_manifest,
        ),
    ]
    template_request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="conformer_screening",
        source_job_id="",
        source_job_type="raw_xyz",
        reaction_key=Path(copied_input).stem,
        status="planned",
        requested_at=requested_at,
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
        source_artifacts=(WorkflowArtifactRef(kind="input_xyz", path=copied_input, selected=True),),
    )
    return _persist_workflow(
        persistence_context=WorkflowPersistenceContext(
            workflow_root_path=workflow_root_path,
            workspace_dir=workspace_dir,
            workflow_id=workflow_id,
            template_name="conformer_screening",
            source_job_id="",
            source_job_type="raw_xyz",
            reaction_key=template_request.reaction_key,
            requested_at=requested_at,
        ),
        request=template_request,
        stages=stages,
        creation_context=context,
    )


__all__ = [
    "_copy_input_impl",
    "create_conformer_screening_workflow_impl",
    "create_reaction_ts_search_workflow_impl",
    "new_crest_stage_impl",
    "new_xtb_stage_impl",
]
