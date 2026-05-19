from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from chemstack.core.utils import atomic_write_json, now_utc_iso, timestamped_token

from ..adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
from ..contracts import (
    WorkflowArtifactRef,
    WorkflowPlan,
    WorkflowStage,
    WorkflowTask,
    WorkflowTemplateRequest,
    XtbArtifactContract,
    XtbDownstreamPolicy,
)
from ..registry import sync_workflow_registry
from ..xyz_utils import choose_orca_geometry_frame
from .orca_stage_utils import (
    build_orca_enqueue_payload as _shared_build_orca_enqueue_payload,
    ensure_route_line as _shared_ensure_route_line,
    materialize_orca_stage as _shared_materialize_orca_stage,
    maxcore_mb_per_core as _shared_maxcore_mb_per_core,
    render_orca_input as _shared_render_orca_input,
    safe_name as _shared_safe_name,
)
from . import reaction_ts_orca_stage as _reaction_ts_orca_stage
from .reaction_ts_models import (
    BuiltReactionOrcaStage,
    OrcaStageBuildContext,
    OrcaStagePayload,
    ReactionTsPlanBuildContext,
    ReactionTsSearchPlanRequest,
)


@dataclass(frozen=True)
class _ReactionTsStageDeps:
    BuiltReactionOrcaStage: Any
    OrcaStagePayload: Any
    WorkflowArtifactRef: Any
    WorkflowStage: Any
    WorkflowTask: Any
    atomic_write_json: Any
    _build_orca_enqueue_payload: Any
    _build_reaction_orca_stage: Any
    _materialized_orca_payload: Any
    _orca_payload_from_candidate: Any
    _safe_name: Any
    _shared_materialize_orca_stage: Any
    _workflow_stage_for_orca_payload: Any
    _workflow_task_for_orca_stage: Any
    _write_stage_enqueue_payload: Any


def _reaction_ts_stage_deps() -> _ReactionTsStageDeps:
    return _ReactionTsStageDeps(
        BuiltReactionOrcaStage=BuiltReactionOrcaStage,
        OrcaStagePayload=OrcaStagePayload,
        WorkflowArtifactRef=WorkflowArtifactRef,
        WorkflowStage=WorkflowStage,
        WorkflowTask=WorkflowTask,
        atomic_write_json=atomic_write_json,
        _build_orca_enqueue_payload=_build_orca_enqueue_payload,
        _build_reaction_orca_stage=_build_reaction_orca_stage,
        _materialized_orca_payload=_materialized_orca_payload,
        _orca_payload_from_candidate=_orca_payload_from_candidate,
        _safe_name=_safe_name,
        _shared_materialize_orca_stage=_shared_materialize_orca_stage,
        _workflow_stage_for_orca_payload=_workflow_stage_for_orca_payload,
        _workflow_task_for_orca_stage=_workflow_task_for_orca_stage,
        _write_stage_enqueue_payload=_write_stage_enqueue_payload,
    )


def _workflow_id(_: XtbArtifactContract) -> str:
    return timestamped_token("wf_reaction_ts")


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _safe_name(value: str, *, fallback: str) -> str:
    return _shared_safe_name(value, fallback=fallback)


def _selected_input_label(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""


def _ensure_route_line(route_line: str) -> str:
    return _shared_ensure_route_line(route_line, default="r2scan-3c OptTS Freq TightSCF")


def _maxcore_mb_per_core(*, max_memory_gb: int, max_cores: int) -> int:
    return _shared_maxcore_mb_per_core(max_memory_gb=max_memory_gb, max_cores=max_cores)


def _render_orca_input(
    *,
    route_line: str,
    charge: int,
    multiplicity: int,
    max_cores: int,
    max_memory_gb: int,
    xyz_filename: str,
) -> str:
    return _shared_render_orca_input(
        route_line=route_line,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        xyz_filename=xyz_filename,
        default_route_line="r2scan-3c OptTS Freq TightSCF",
    )


def _build_orca_enqueue_payload(
    *,
    workflow_id: str,
    stage_id: str,
    reaction_dir: str,
    selected_inp: str,
    priority: int,
    resource_request: dict[str, int],
    source_job_id: str,
    reaction_key: str,
) -> dict[str, Any]:
    return _shared_build_orca_enqueue_payload(
        workflow_id=workflow_id,
        stage_id=stage_id,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        priority=priority,
        resource_request=resource_request,
        source_job_id=source_job_id,
        reaction_key=reaction_key,
    )


def _orca_payload_from_candidate(
    *,
    contract: XtbArtifactContract,
    workflow_id: str,
    candidate_index: int,
    candidate: Any,
    resource_request: dict[str, int],
) -> OrcaStagePayload:
    stage_id = f"orca_optts_freq_{candidate_index:02d}"
    return OrcaStagePayload(
        stage_id=stage_id,
        engine="orca",
        task_kind="optts_freq",
        selected_input_xyz=candidate.artifact_path,
        selected_input_label=_selected_input_label(candidate.artifact_path),
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        resource_request=resource_request,
        metadata={
            "candidate_rank": candidate.rank,
            "candidate_kind": candidate.kind,
            "candidate_score": candidate.score,
            "candidate_selected": candidate.selected,
            "candidate_metadata": dict(candidate.metadata),
            "source_selected_input_xyz": contract.selected_input_xyz,
            "source_selected_candidate_paths": list(contract.selected_candidate_paths),
        },
    )


def _materialize_orca_stage(
    *,
    workspace_dir: Path,
    index: int,
    candidate: Any,
    contract: XtbArtifactContract,
    orca_payload: OrcaStagePayload,
    route_line: str,
    charge: int,
    multiplicity: int,
    max_cores: int,
    max_memory_gb: int,
) -> OrcaStagePayload:
    return _materialize_orca_stage_from_context(
        OrcaStageBuildContext(
            workspace_dir=workspace_dir,
            index=index,
            candidate=candidate,
            contract=contract,
            orca_payload=orca_payload,
            route_line=route_line,
            charge=charge,
            multiplicity=multiplicity,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
        )
    )


def _materialize_orca_stage_from_context(ctx: OrcaStageBuildContext) -> OrcaStagePayload:
    return _reaction_ts_orca_stage.materialize_orca_stage_from_context(
        ctx,
        deps=_reaction_ts_stage_deps(),
    )


def _reaction_ts_guess_error(contract: XtbArtifactContract) -> str:
    candidates = sorted(
        [
            item
            for item in contract.candidate_details
            if _normalize_text(item.kind) == "ts_guess" and _normalize_text(item.path)
        ],
        key=lambda item: item.rank if item.rank > 0 else 10_000,
    )
    if not candidates:
        return "xTB path_search did not produce a ts_guess candidate (xtbpath_ts.xyz); refusing ORCA handoff."
    _, metadata = choose_orca_geometry_frame(candidates[0].path, candidate_kind="ts_guess")
    selection_reason = _normalize_text(metadata.get("selection_reason"))
    if selection_reason == "ts_guess_requires_single_frame":
        return f"xTB produced xtbpath_ts.xyz but it is not a single-geometry XYZ file: {candidates[0].path}"
    return f"xTB produced xtbpath_ts.xyz but it is empty or not a valid XYZ geometry: {candidates[0].path}"


def _select_ts_guess_candidates(request: ReactionTsSearchPlanRequest) -> tuple[Any, ...]:
    policy = XtbDownstreamPolicy.build(
        preferred_kinds=("ts_guess",),
        allowed_kinds=("ts_guess",),
        max_candidates=request.max_orca_stages,
        selected_only=request.selected_only,
        fallback_to_selected_paths=False,
    )
    candidates = select_xtb_downstream_inputs(
        request.contract,
        policy=policy,
        require_geometry=True,
    )
    if not candidates:
        raise ValueError(_reaction_ts_guess_error(request.contract))
    return tuple(candidates)


def _workspace_dir_for_request(
    request: ReactionTsSearchPlanRequest,
    *,
    workflow_id: str,
) -> Path | None:
    if request.workspace_root is None:
        return None
    workspace_dir = Path(request.workspace_root).expanduser().resolve() / workflow_id
    workspace_dir.mkdir(parents=True, exist_ok=True)
    return workspace_dir


def _reaction_ts_plan_context(request: ReactionTsSearchPlanRequest) -> ReactionTsPlanBuildContext:
    workflow_id = _workflow_id(request.contract)
    return ReactionTsPlanBuildContext(
        request=request,
        workflow_id=workflow_id,
        requested_at=now_utc_iso(),
        resource_request=request.resource_request,
        workspace_dir=_workspace_dir_for_request(request, workflow_id=workflow_id),
    )


def _materialized_orca_payload(
    ctx: ReactionTsPlanBuildContext,
    *,
    index: int,
    candidate: Any,
    orca_payload: OrcaStagePayload,
) -> OrcaStagePayload:
    if ctx.workspace_dir is None:
        return orca_payload
    return _materialize_orca_stage(
        workspace_dir=ctx.workspace_dir,
        index=index,
        candidate=candidate,
        contract=ctx.request.contract,
        orca_payload=orca_payload,
        route_line=ctx.request.orca_route_line,
        charge=ctx.request.charge,
        multiplicity=ctx.request.multiplicity,
        max_cores=ctx.resource_request["max_cores"],
        max_memory_gb=ctx.resource_request["max_memory_gb"],
    )


def _workflow_task_for_orca_stage(
    ctx: ReactionTsPlanBuildContext,
    *,
    candidate: Any,
    orca_payload: OrcaStagePayload,
    enqueue_payload: dict[str, Any],
) -> WorkflowTask:
    return _reaction_ts_orca_stage.workflow_task_for_orca_stage(
        ctx,
        candidate=candidate,
        orca_payload=orca_payload,
        enqueue_payload=enqueue_payload,
        deps=_reaction_ts_stage_deps(),
    )


def _workflow_stage_for_orca_payload(
    *,
    candidate: Any,
    orca_payload: OrcaStagePayload,
    stage_task: WorkflowTask,
) -> WorkflowStage:
    return _reaction_ts_orca_stage.workflow_stage_for_orca_payload(
        candidate=candidate,
        orca_payload=orca_payload,
        stage_task=stage_task,
        deps=_reaction_ts_stage_deps(),
    )


def _build_reaction_orca_stage(
    ctx: ReactionTsPlanBuildContext,
    *,
    index: int,
    candidate: Any,
) -> BuiltReactionOrcaStage:
    return _reaction_ts_orca_stage.build_reaction_orca_stage(
        ctx,
        index=index,
        candidate=candidate,
        deps=_reaction_ts_stage_deps(),
    )


def _write_stage_enqueue_payload(
    ctx: ReactionTsPlanBuildContext, stage: BuiltReactionOrcaStage
) -> None:
    _reaction_ts_orca_stage.write_stage_enqueue_payload(
        ctx,
        stage,
        deps=_reaction_ts_stage_deps(),
    )


def _build_reaction_orca_stages(
    ctx: ReactionTsPlanBuildContext,
    candidates: tuple[Any, ...],
) -> list[BuiltReactionOrcaStage]:
    return _reaction_ts_orca_stage.build_reaction_orca_stages(
        ctx,
        candidates,
        deps=_reaction_ts_stage_deps(),
    )


def _template_request_for_plan(ctx: ReactionTsPlanBuildContext) -> WorkflowTemplateRequest:
    request = ctx.request
    contract = request.contract
    return WorkflowTemplateRequest(
        workflow_id=ctx.workflow_id,
        template_name="reaction_ts_search",
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        status="planned",
        requested_at=ctx.requested_at,
        parameters={
            "max_orca_stages": request.max_orca_stages,
            "selected_only": request.selected_only,
            "charge": int(request.charge),
            "multiplicity": int(request.multiplicity),
            "max_cores": ctx.resource_request["max_cores"],
            "max_memory_gb": ctx.resource_request["max_memory_gb"],
            "orca_route_line": _ensure_route_line(request.orca_route_line),
            "priority": int(request.priority),
        },
        source_artifacts=tuple(
            WorkflowArtifactRef(
                kind="xtb_selected_candidate",
                path=path,
                selected=True,
            )
            for path in contract.selected_candidate_paths
        ),
    )


def _reaction_ts_plan_payload(
    ctx: ReactionTsPlanBuildContext,
    built_stages: list[BuiltReactionOrcaStage],
) -> dict[str, Any]:
    contract = ctx.request.contract
    stages = [built.stage for built in built_stages]
    plan = WorkflowPlan(
        workflow_id=ctx.workflow_id,
        template_name="reaction_ts_search",
        status="planned",
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        requested_at=ctx.requested_at,
        stages=tuple(stages),
        metadata={
            "request": _template_request_for_plan(ctx).to_dict(),
            "source_contract": contract.to_dict(),
            "orca_stage_payloads": [built.payload.to_dict() for built in built_stages],
            "orca_stage_enqueue_payloads": [dict(built.enqueue_payload) for built in built_stages],
            "workspace_dir": str(ctx.workspace_dir) if ctx.workspace_dir is not None else "",
        },
    )
    return cast(dict[str, Any], plan.to_dict())


def _persist_reaction_ts_plan(ctx: ReactionTsPlanBuildContext, payload: dict[str, Any]) -> None:
    if ctx.workspace_dir is None:
        return
    atomic_write_json(ctx.workspace_dir / "workflow.json", payload, ensure_ascii=True, indent=2)
    workspace_root_path = (
        Path(ctx.request.workspace_root).expanduser().resolve()
        if ctx.request.workspace_root is not None
        else ctx.workspace_dir.parent.parent
    )
    sync_workflow_registry(workspace_root_path, ctx.workspace_dir, payload)


def build_reaction_ts_search_plan(
    contract: XtbArtifactContract,
    *,
    max_orca_stages: int = 3,
    selected_only: bool = True,
    workspace_root: str | Path | None = None,
    charge: int = 0,
    multiplicity: int = 1,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF",
    priority: int = 10,
) -> dict[str, Any]:
    request = ReactionTsSearchPlanRequest(
        contract=contract,
        max_orca_stages=max_orca_stages,
        selected_only=selected_only,
        workspace_root=workspace_root,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        orca_route_line=orca_route_line,
        priority=priority,
    )
    return _build_reaction_ts_search_plan(request)


def _build_reaction_ts_search_plan(request: ReactionTsSearchPlanRequest) -> dict[str, Any]:
    context = _reaction_ts_plan_context(request)
    candidates = _select_ts_guess_candidates(request)
    built_stages = _build_reaction_orca_stages(context, candidates)
    payload = _reaction_ts_plan_payload(context, built_stages)
    _persist_reaction_ts_plan(context, payload)
    return payload


def build_reaction_ts_search_plan_from_target(
    *,
    xtb_index_root: str | Any,
    target: str,
    max_orca_stages: int = 3,
    selected_only: bool = True,
    workspace_root: str | Path | None = None,
    charge: int = 0,
    multiplicity: int = 1,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF",
    priority: int = 10,
) -> dict[str, Any]:
    contract = load_xtb_artifact_contract(xtb_index_root=xtb_index_root, target=target)
    return build_reaction_ts_search_plan(
        contract,
        max_orca_stages=max_orca_stages,
        selected_only=selected_only,
        workspace_root=workspace_root,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        orca_route_line=orca_route_line,
        priority=priority,
    )


__all__ = [
    "build_reaction_ts_search_plan",
    "build_reaction_ts_search_plan_from_target",
]
