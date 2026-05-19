from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from chemstack.core.utils import atomic_write_json, now_utc_iso, timestamped_token

from ..adapters import load_crest_artifact_contract, select_crest_downstream_inputs
from ..contracts import (
    CrestArtifactContract,
    CrestDownstreamPolicy,
    WorkflowPlan,
    WorkflowStage,
    WorkflowStageInput,
    WorkflowTemplateRequest,
)
from .orca_stage_utils import build_materialized_orca_stage, safe_name


def _workflow_id(_: CrestArtifactContract) -> str:
    return timestamped_token("wf_conformer_screening")


@dataclass(frozen=True)
class ConformerScreeningPlanContext:
    contract: CrestArtifactContract
    workflow_id: str
    requested_at: str
    max_orca_stages: int
    workspace_dir: Path | None
    orca_workspace_dir: Path | None
    charge: int
    multiplicity: int
    max_cores: int
    max_memory_gb: int
    orca_route_line: str
    priority: int

    @property
    def source_job_type(self) -> str:
        return f"crest_{self.contract.mode}"

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "max_orca_stages": int(self.max_orca_stages),
            "charge": int(self.charge),
            "multiplicity": int(self.multiplicity),
            "max_cores": int(self.max_cores),
            "max_memory_gb": int(self.max_memory_gb),
            "orca_route_line": str(self.orca_route_line),
            "priority": int(self.priority),
        }


def _workflow_dirs(
    *,
    workspace_root: str | Path | None,
    workflow_id: str,
) -> tuple[Path | None, Path | None]:
    if workspace_root is None:
        return None, None
    workspace_dir = Path(workspace_root).expanduser().resolve() / workflow_id
    workspace_dir.mkdir(parents=True, exist_ok=True)
    orca_workspace_dir = workspace_dir / "02_orca"
    orca_workspace_dir.mkdir(parents=True, exist_ok=True)
    return workspace_dir, orca_workspace_dir


def _conformer_plan_context(
    contract: CrestArtifactContract,
    *,
    max_orca_stages: int,
    workspace_root: str | Path | None,
    charge: int,
    multiplicity: int,
    max_cores: int,
    max_memory_gb: int,
    orca_route_line: str,
    priority: int,
) -> ConformerScreeningPlanContext:
    workflow_id = _workflow_id(contract)
    workspace_dir, orca_workspace_dir = _workflow_dirs(
        workspace_root=workspace_root,
        workflow_id=workflow_id,
    )
    return ConformerScreeningPlanContext(
        contract=contract,
        workflow_id=workflow_id,
        requested_at=now_utc_iso(),
        max_orca_stages=max_orca_stages,
        workspace_dir=workspace_dir,
        orca_workspace_dir=orca_workspace_dir,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        orca_route_line=orca_route_line,
        priority=priority,
    )


def _conformer_candidates(
    contract: CrestArtifactContract,
    *,
    max_orca_stages: int,
) -> tuple[WorkflowStageInput, ...]:
    return select_crest_downstream_inputs(
        contract,
        policy=CrestDownstreamPolicy.build(max_candidates=max_orca_stages),
    )


def _build_conformer_orca_stage(
    ctx: ConformerScreeningPlanContext,
    *,
    index: int,
    candidate: WorkflowStageInput,
) -> WorkflowStage:
    if ctx.orca_workspace_dir is None:
        raise ValueError("ORCA workspace directory is required to materialize a stage.")
    return build_materialized_orca_stage(
        workflow_id=ctx.workflow_id,
        template_name="conformer_screening",
        stage_id=f"orca_conformer_{index:02d}",
        stage_key=f"{index:02d}_{safe_name(candidate.kind, fallback='conformer')}",
        stage_root_name="",
        workspace_dir=ctx.orca_workspace_dir,
        input_artifact_kind="crest_conformer",
        candidate=candidate,
        task_kind="opt",
        route_line=ctx.orca_route_line,
        charge=ctx.charge,
        multiplicity=ctx.multiplicity,
        max_cores=ctx.max_cores,
        max_memory_gb=ctx.max_memory_gb,
        priority=ctx.priority,
        xyz_filename="conformer_guess.xyz",
        inp_filename="conformer_opt.inp",
        input_label=Path(candidate.artifact_path).name,
    )


def _build_conformer_orca_stages(
    ctx: ConformerScreeningPlanContext,
    candidates: tuple[WorkflowStageInput, ...],
) -> tuple[WorkflowStage, ...]:
    if ctx.workspace_dir is None or ctx.orca_workspace_dir is None:
        return ()
    return tuple(
        _build_conformer_orca_stage(ctx, index=index, candidate=candidate)
        for index, candidate in enumerate(candidates, start=1)
    )


def _conformer_template_request(
    ctx: ConformerScreeningPlanContext,
    stages: tuple[WorkflowStage, ...],
) -> WorkflowTemplateRequest:
    return WorkflowTemplateRequest(
        workflow_id=ctx.workflow_id,
        template_name="conformer_screening",
        source_job_id=ctx.contract.job_id,
        source_job_type=ctx.source_job_type,
        reaction_key=ctx.contract.molecule_key,
        status="planned",
        requested_at=ctx.requested_at,
        parameters=ctx.parameters,
        source_artifacts=tuple(stage.input_artifacts[0] for stage in stages if stage.input_artifacts),
    )


def _conformer_plan_payload(
    ctx: ConformerScreeningPlanContext,
    *,
    request: WorkflowTemplateRequest,
    stages: tuple[WorkflowStage, ...],
) -> dict[str, Any]:
    plan = WorkflowPlan(
        workflow_id=ctx.workflow_id,
        template_name="conformer_screening",
        status="planned",
        source_job_id=ctx.contract.job_id,
        source_job_type=ctx.source_job_type,
        reaction_key=ctx.contract.molecule_key,
        requested_at=ctx.requested_at,
        stages=stages,
        metadata={
            "request": request.to_dict(),
            "source_contract": ctx.contract.to_dict(),
            "workspace_dir": str(ctx.workspace_dir) if ctx.workspace_dir is not None else "",
        },
    )
    return cast(dict[str, Any], plan.to_dict())


def _persist_conformer_plan_payload(
    ctx: ConformerScreeningPlanContext,
    payload: dict[str, Any],
) -> None:
    if ctx.workspace_dir is not None:
        atomic_write_json(ctx.workspace_dir / "workflow.json", payload, ensure_ascii=True, indent=2)


def build_conformer_screening_plan(
    contract: CrestArtifactContract,
    *,
    max_orca_stages: int = 20,
    workspace_root: str | Path | None = None,
    charge: int = 0,
    multiplicity: int = 1,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    priority: int = 10,
) -> dict[str, Any]:
    ctx = _conformer_plan_context(
        contract,
        max_orca_stages=max_orca_stages,
        workspace_root=workspace_root,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        orca_route_line=orca_route_line,
        priority=priority,
    )
    candidates = _conformer_candidates(contract, max_orca_stages=max_orca_stages)
    stages = _build_conformer_orca_stages(ctx, candidates)
    request = _conformer_template_request(ctx, stages)
    payload = _conformer_plan_payload(ctx, request=request, stages=stages)
    _persist_conformer_plan_payload(ctx, payload)
    return payload


def build_conformer_screening_plan_from_target(
    *,
    crest_index_root: str | Path,
    target: str,
    max_orca_stages: int = 20,
    workspace_root: str | Path | None = None,
    charge: int = 0,
    multiplicity: int = 1,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    priority: int = 10,
) -> dict[str, Any]:
    contract = load_crest_artifact_contract(crest_index_root=crest_index_root, target=target)
    payload = build_conformer_screening_plan(
        contract,
        max_orca_stages=max_orca_stages,
        workspace_root=workspace_root,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        orca_route_line=orca_route_line,
        priority=priority,
    )
    if workspace_root is not None:
        from ..registry import sync_workflow_registry

        workspace_dir = Path(payload["metadata"]["workspace_dir"]).expanduser().resolve()
        sync_workflow_registry(Path(workspace_root).expanduser().resolve(), workspace_dir, payload)
    return payload


__all__ = [
    "build_conformer_screening_plan",
    "build_conformer_screening_plan_from_target",
]
