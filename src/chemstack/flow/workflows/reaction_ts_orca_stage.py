from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CLI_COMMAND


def materialize_orca_stage_from_context(ctx: Any, *, deps: Any) -> Any:
    source_xyz = Path(ctx.candidate.artifact_path).expanduser().resolve()
    if not source_xyz.exists():
        raise FileNotFoundError(f"xTB candidate artifact not found: {source_xyz}")

    materialized = deps._shared_materialize_orca_stage(
        workspace_dir=ctx.workspace_dir,
        stage_root_name="03_orca",
        stage_key=f"{ctx.index:02d}_{deps._safe_name(ctx.candidate.kind, fallback='candidate')}",
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

    return deps.OrcaStagePayload(
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


def workflow_task_for_orca_stage(
    ctx: Any,
    *,
    candidate: Any,
    orca_payload: Any,
    enqueue_payload: dict[str, Any],
    deps: Any,
) -> Any:
    return deps.WorkflowTask.from_raw(
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


def workflow_stage_for_orca_payload(
    *,
    candidate: Any,
    orca_payload: Any,
    stage_task: Any,
    deps: Any,
) -> Any:
    return deps.WorkflowStage(
        stage_id=orca_payload.stage_id,
        stage_kind="orca_stage",
        status="planned",
        input_artifacts=(
            deps.WorkflowArtifactRef(
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
            deps.WorkflowArtifactRef(
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


def build_reaction_orca_stage(
    ctx: Any,
    *,
    index: int,
    candidate: Any,
    deps: Any,
) -> Any:
    contract = ctx.request.contract
    orca_payload = deps._orca_payload_from_candidate(
        contract=contract,
        workflow_id=ctx.workflow_id,
        candidate_index=index,
        candidate=candidate,
        resource_request=ctx.resource_request,
    )
    orca_payload = deps._materialized_orca_payload(
        ctx,
        index=index,
        candidate=candidate,
        orca_payload=orca_payload,
    )
    enqueue_payload = deps._build_orca_enqueue_payload(
        workflow_id=ctx.workflow_id,
        stage_id=orca_payload.stage_id,
        reaction_dir=orca_payload.reaction_dir,
        selected_inp=orca_payload.selected_inp,
        priority=ctx.request.priority,
        resource_request=ctx.resource_request,
        source_job_id=contract.job_id,
        reaction_key=contract.reaction_key,
    )
    stage_task = deps._workflow_task_for_orca_stage(
        ctx,
        candidate=candidate,
        orca_payload=orca_payload,
        enqueue_payload=enqueue_payload,
    )
    return deps.BuiltReactionOrcaStage(
        stage=deps._workflow_stage_for_orca_payload(
            candidate=candidate,
            orca_payload=orca_payload,
            stage_task=stage_task,
        ),
        payload=orca_payload,
        enqueue_payload=enqueue_payload,
        candidate_index=index,
        candidate_kind=str(candidate.kind),
    )


def write_stage_enqueue_payload(ctx: Any, stage: Any, *, deps: Any) -> None:
    if ctx.workspace_dir is None:
        return
    stage_key = f"{stage.candidate_index:02d}_{deps._safe_name(stage.candidate_kind, fallback='candidate')}"
    deps.atomic_write_json(
        ctx.workspace_dir / "03_orca" / stage_key / "enqueue_payload.json",
        dict(stage.enqueue_payload),
        ensure_ascii=True,
        indent=2,
    )


def build_reaction_orca_stages(
    ctx: Any,
    candidates: tuple[Any, ...],
    *,
    deps: Any,
) -> list[Any]:
    built: list[Any] = []
    for index, candidate in enumerate(candidates, start=1):
        stage = deps._build_reaction_orca_stage(ctx, index=index, candidate=candidate)
        deps._write_stage_enqueue_payload(ctx, stage)
        built.append(stage)
    return built
