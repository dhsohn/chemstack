from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.utils import atomic_write_json, now_utc_iso, timestamped_token

from ..adapters import load_crest_artifact_contract, select_crest_downstream_inputs
from ..contracts import CrestArtifactContract, CrestDownstreamPolicy, WorkflowPlan, WorkflowTemplateRequest
from .orca_stage_utils import build_materialized_orca_stage, safe_name


def _workflow_id(_: CrestArtifactContract) -> str:
    return timestamped_token("wf_conformer_screening")


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
    workflow_id = _workflow_id(contract)
    requested_at = now_utc_iso()
    candidates = select_crest_downstream_inputs(
        contract,
        policy=CrestDownstreamPolicy.build(max_candidates=max_orca_stages),
    )
    workspace_dir: Path | None = None
    orca_workspace_dir: Path | None = None
    if workspace_root is not None:
        workspace_dir = Path(workspace_root).expanduser().resolve() / workflow_id
        workspace_dir.mkdir(parents=True, exist_ok=True)
        orca_workspace_dir = workspace_dir / "02_orca"
        orca_workspace_dir.mkdir(parents=True, exist_ok=True)

    stages = []
    for index, candidate in enumerate(candidates, start=1):
        if workspace_dir is None or orca_workspace_dir is None:
            continue
        stage = build_materialized_orca_stage(
            workflow_id=workflow_id,
            template_name="conformer_screening",
            stage_id=f"orca_conformer_{index:02d}",
            stage_key=f"{index:02d}_{safe_name(candidate.kind, fallback='conformer')}",
            stage_root_name="",
            workspace_dir=orca_workspace_dir,
            input_artifact_kind="crest_conformer",
            candidate=candidate,
            task_kind="opt",
            route_line=orca_route_line,
            charge=charge,
            multiplicity=multiplicity,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            priority=priority,
            xyz_filename="conformer_guess.xyz",
            inp_filename="conformer_opt.inp",
            input_label=Path(candidate.artifact_path).name,
        )
        stages.append(stage)

    request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="conformer_screening",
        source_job_id=contract.job_id,
        source_job_type=f"crest_{contract.mode}",
        reaction_key=contract.molecule_key,
        status="planned",
        requested_at=requested_at,
        parameters={
            "max_orca_stages": int(max_orca_stages),
            "charge": int(charge),
            "multiplicity": int(multiplicity),
            "max_cores": int(max_cores),
            "max_memory_gb": int(max_memory_gb),
            "orca_route_line": str(orca_route_line),
            "priority": int(priority),
        },
        source_artifacts=tuple(stage.input_artifacts[0] for stage in stages if stage.input_artifacts),
    )
    plan = WorkflowPlan(
        workflow_id=workflow_id,
        template_name="conformer_screening",
        status="planned",
        source_job_id=contract.job_id,
        source_job_type=f"crest_{contract.mode}",
        reaction_key=contract.molecule_key,
        requested_at=requested_at,
        stages=tuple(stages),
        metadata={
            "request": request.to_dict(),
            "source_contract": contract.to_dict(),
            "workspace_dir": str(workspace_dir) if workspace_dir is not None else "",
        },
    )
    payload = plan.to_dict()
    if workspace_dir is not None:
        atomic_write_json(workspace_dir / "workflow.json", payload, ensure_ascii=True, indent=2)
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
