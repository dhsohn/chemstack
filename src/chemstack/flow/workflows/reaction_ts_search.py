from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CLI_COMMAND
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
    render_orca_input as _shared_render_orca_input,
)


@dataclass(frozen=True)
class OrcaStagePayload:
    stage_id: str
    engine: str
    task_kind: str
    selected_input_xyz: str
    selected_input_label: str
    source_job_id: str
    source_job_type: str
    reaction_key: str
    workflow_id: str
    template_name: str
    resource_request: dict[str, int]
    reaction_dir: str = ""
    selected_inp: str = ""
    suggested_command: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "engine": self.engine,
            "task_kind": self.task_kind,
            "selected_input_xyz": self.selected_input_xyz,
            "selected_input_label": self.selected_input_label,
            "source_job_id": self.source_job_id,
            "source_job_type": self.source_job_type,
            "reaction_key": self.reaction_key,
            "workflow_id": self.workflow_id,
            "template_name": self.template_name,
            "resource_request": dict(self.resource_request),
            "reaction_dir": self.reaction_dir,
            "selected_inp": self.selected_inp,
            "suggested_command": self.suggested_command,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class ReactionTsSearchPlanRequest:
    contract: XtbArtifactContract
    max_orca_stages: int = 3
    selected_only: bool = True
    workspace_root: str | Path | None = None
    charge: int = 0
    multiplicity: int = 1
    max_cores: int = 8
    max_memory_gb: int = 32
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF"
    priority: int = 10

    @property
    def resource_request(self) -> dict[str, int]:
        return {
            "max_cores": max(1, int(self.max_cores)),
            "max_memory_gb": max(1, int(self.max_memory_gb)),
        }


@dataclass(frozen=True)
class OrcaStageBuildContext:
    workspace_dir: Path
    index: int
    candidate: Any
    contract: XtbArtifactContract
    orca_payload: OrcaStagePayload
    route_line: str
    charge: int
    multiplicity: int
    max_cores: int
    max_memory_gb: int


@dataclass(frozen=True)
class ReactionTsPlanBuildContext:
    request: ReactionTsSearchPlanRequest
    workflow_id: str
    requested_at: str
    resource_request: dict[str, int]
    workspace_dir: Path | None


@dataclass(frozen=True)
class BuiltReactionOrcaStage:
    stage: WorkflowStage
    payload: OrcaStagePayload
    enqueue_payload: dict[str, Any]
    candidate_index: int
    candidate_kind: str


def _workflow_id(_: XtbArtifactContract) -> str:
    return timestamped_token("wf_reaction_ts")


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _safe_name(value: str, *, fallback: str) -> str:
    cleaned = "".join(
        ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in _normalize_text(value)
    )
    cleaned = cleaned.strip("._-").lower()
    return cleaned or fallback


def _selected_input_label(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""


def _ensure_route_line(route_line: str) -> str:
    return _shared_ensure_route_line(route_line, default="r2scan-3c OptTS Freq TightSCF")


def _maxcore_mb_per_core(*, max_memory_gb: int, max_cores: int) -> int:
    total_mb = max(1, int(max_memory_gb)) * 1024
    return max(1, total_mb // max(1, int(max_cores)))


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
    source_xyz = Path(ctx.candidate.artifact_path).expanduser().resolve()
    if not source_xyz.exists():
        raise FileNotFoundError(f"xTB candidate artifact not found: {source_xyz}")

    materialized = _shared_materialize_orca_stage(
        workspace_dir=ctx.workspace_dir,
        stage_root_name="03_orca",
        stage_key=f"{ctx.index:02d}_{_safe_name(ctx.candidate.kind, fallback='candidate')}",
        source_artifact_path=str(source_xyz),
        candidate_kind=str(ctx.candidate.kind),
        route_line=ctx.route_line,
        charge=ctx.charge,
        multiplicity=ctx.multiplicity,
        max_cores=ctx.max_cores,
        max_memory_gb=ctx.max_memory_gb,
        xyz_filename="ts_guess.xyz",
        inp_filename="ts_guess.inp",
        extra_source_payload={
            "source_job_id": ctx.contract.job_id,
            "source_job_type": ctx.contract.job_type,
            "source_candidate_path": str(source_xyz),
            "reaction_key": ctx.contract.reaction_key,
        },
    )

    return OrcaStagePayload(
        stage_id=ctx.orca_payload.stage_id,
        engine=ctx.orca_payload.engine,
        task_kind=ctx.orca_payload.task_kind,
        selected_input_xyz=ctx.orca_payload.selected_input_xyz,
        selected_input_label=ctx.orca_payload.selected_input_label,
        source_job_id=ctx.orca_payload.source_job_id,
        source_job_type=ctx.orca_payload.source_job_type,
        reaction_key=ctx.orca_payload.reaction_key,
        workflow_id=ctx.orca_payload.workflow_id,
        template_name=ctx.orca_payload.template_name,
        resource_request=dict(ctx.orca_payload.resource_request),
        reaction_dir=materialized.reaction_dir,
        selected_inp=materialized.selected_inp,
        suggested_command=f"{CHEMSTACK_CLI_COMMAND} run-dir '{materialized.reaction_dir}'",
        metadata=dict(ctx.orca_payload.metadata),
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
    return WorkflowTask.from_raw(
        task_id=f"{ctx.workflow_id}:{orca_payload.stage_id}",
        engine=orca_payload.engine,
        task_kind=orca_payload.task_kind,
        resource_request=ctx.resource_request,
        payload=orca_payload.to_dict(),
        enqueue_payload=enqueue_payload,
        depends_on=(),
        metadata={
            "workflow_id": ctx.workflow_id,
            "template_name": "reaction_ts_search",
            "source_candidate_path": candidate.artifact_path,
            "queue_priority": int(ctx.request.priority),
            "reaction_dir": orca_payload.reaction_dir,
            "selected_inp": orca_payload.selected_inp,
        },
    )


def _workflow_stage_for_orca_payload(
    *,
    candidate: Any,
    orca_payload: OrcaStagePayload,
    stage_task: WorkflowTask,
) -> WorkflowStage:
    return WorkflowStage(
        stage_id=orca_payload.stage_id,
        stage_kind="orca_stage",
        status="planned",
        input_artifacts=(
            WorkflowArtifactRef(
                kind="xtb_candidate",
                path=candidate.artifact_path,
                selected=candidate.selected,
                metadata={
                    "rank": candidate.rank,
                    "kind": candidate.kind,
                    "score": candidate.score,
                    **dict(candidate.metadata),
                },
            ),
        ),
        output_artifacts=(
            WorkflowArtifactRef(
                kind="orca_input",
                path=orca_payload.selected_inp
                or f"{orca_payload.workflow_id}/{orca_payload.stage_id}/orca.inp",
                metadata={
                    "engine": "orca",
                    "task_kind": "optts_freq",
                    "reaction_dir": orca_payload.reaction_dir,
                    "suggested_command": orca_payload.suggested_command,
                },
            ),
        ),
        task=stage_task,
        metadata={
            "candidate_rank": candidate.rank,
            "candidate_kind": candidate.kind,
            "candidate_score": candidate.score,
            "selected_input_label": orca_payload.selected_input_label,
            "reaction_dir": orca_payload.reaction_dir,
        },
    )


def _build_reaction_orca_stage(
    ctx: ReactionTsPlanBuildContext,
    *,
    index: int,
    candidate: Any,
) -> BuiltReactionOrcaStage:
    contract = ctx.request.contract
    orca_payload = _orca_payload_from_candidate(
        contract=contract,
        workflow_id=ctx.workflow_id,
        candidate_index=index,
        candidate=candidate,
        resource_request=ctx.resource_request,
    )
    orca_payload = _materialized_orca_payload(
        ctx,
        index=index,
        candidate=candidate,
        orca_payload=orca_payload,
    )
    enqueue_payload = _build_orca_enqueue_payload(
        workflow_id=ctx.workflow_id,
        stage_id=orca_payload.stage_id,
        reaction_dir=orca_payload.reaction_dir,
        selected_inp=orca_payload.selected_inp,
        priority=ctx.request.priority,
        resource_request=ctx.resource_request,
        source_job_id=contract.job_id,
        reaction_key=contract.reaction_key,
    )
    stage_task = _workflow_task_for_orca_stage(
        ctx,
        candidate=candidate,
        orca_payload=orca_payload,
        enqueue_payload=enqueue_payload,
    )
    return BuiltReactionOrcaStage(
        stage=_workflow_stage_for_orca_payload(
            candidate=candidate,
            orca_payload=orca_payload,
            stage_task=stage_task,
        ),
        payload=orca_payload,
        enqueue_payload=enqueue_payload,
        candidate_index=index,
        candidate_kind=str(candidate.kind),
    )


def _write_stage_enqueue_payload(ctx: ReactionTsPlanBuildContext, stage: BuiltReactionOrcaStage) -> None:
    if ctx.workspace_dir is None:
        return
    stage_key = f"{stage.candidate_index:02d}_{_safe_name(stage.candidate_kind, fallback='candidate')}"
    atomic_write_json(
        ctx.workspace_dir / "03_orca" / stage_key / "enqueue_payload.json",
        dict(stage.enqueue_payload),
        ensure_ascii=True,
        indent=2,
    )


def _build_reaction_orca_stages(
    ctx: ReactionTsPlanBuildContext,
    candidates: tuple[Any, ...],
) -> list[BuiltReactionOrcaStage]:
    built: list[BuiltReactionOrcaStage] = []
    for index, candidate in enumerate(candidates, start=1):
        stage = _build_reaction_orca_stage(ctx, index=index, candidate=candidate)
        _write_stage_enqueue_payload(ctx, stage)
        built.append(stage)
    return built


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
    return plan.to_dict()


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
