from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CLI_COMMAND, CHEMSTACK_CLI_MODULE, CHEMSTACK_ORCA_SUBMITTER
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
from ..xyz_utils import choose_orca_geometry_frame, write_orca_ready_xyz


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


def _workflow_id(_: XtbArtifactContract) -> str:
    return timestamped_token("wf_reaction_ts")


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _safe_name(value: str, *, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in _normalize_text(value))
    cleaned = cleaned.strip("._-").lower()
    return cleaned or fallback


def _selected_input_label(path: str) -> str:
    return path.rsplit("/", 1)[-1] if path else ""


def _ensure_route_line(route_line: str) -> str:
    normalized = _normalize_text(route_line) or "r2scan-3c OptTS Freq TightSCF"
    return normalized if normalized.startswith("!") else f"! {normalized}"


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
    return "\n".join(
        [
            _ensure_route_line(route_line),
            "",
            "%pal",
            f"  nprocs {max(1, int(max_cores))}",
            "end",
            f"%maxcore {_maxcore_mb_per_core(max_memory_gb=max_memory_gb, max_cores=max_cores)}",
            "",
            f"* xyzfile {int(charge)} {int(multiplicity)} {xyz_filename}",
            "",
        ]
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
    config_placeholder = "<chemstack_config>"
    max_cores = int(resource_request.get("max_cores", 0) or 0)
    max_memory_gb = int(resource_request.get("max_memory_gb", 0) or 0)
    command_parts = [
        f"{CHEMSTACK_CLI_COMMAND} --config {config_placeholder}",
        "run-dir",
        "orca",
        f"'{reaction_dir}'",
        f"--priority {int(priority)}",
    ]
    command_argv = [
        "python",
        "-m",
        CHEMSTACK_CLI_MODULE,
        "--config",
        config_placeholder,
        "run-dir",
        "orca",
        reaction_dir,
        "--priority",
        str(int(priority)),
    ]
    if max_cores > 0:
        command_parts.append(f"--max-cores {max_cores}")
        command_argv.extend(["--max-cores", str(max_cores)])
    if max_memory_gb > 0:
        command_parts.append(f"--max-memory-gb {max_memory_gb}")
        command_argv.extend(["--max-memory-gb", str(max_memory_gb)])
    return {
        "submitter": CHEMSTACK_ORCA_SUBMITTER,
        "command": " ".join(command_parts),
        "command_argv": command_argv,
        "requires_config": True,
        "config_argument_placeholder": config_placeholder,
        "reaction_dir": reaction_dir,
        "selected_inp": selected_inp,
        "priority": int(priority),
        "force": False,
        "max_cores": max_cores,
        "max_memory_gb": max_memory_gb,
        "workflow_id": workflow_id,
        "workflow_stage_id": stage_id,
        "source_job_id": source_job_id,
        "reaction_key": reaction_key,
        "resource_request": dict(resource_request),
    }


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
    source_xyz = Path(candidate.artifact_path).expanduser().resolve()
    if not source_xyz.exists():
        raise FileNotFoundError(f"xTB candidate artifact not found: {source_xyz}")

    stage_dir = workspace_dir / "stage_03_orca" / f"{index:02d}_{_safe_name(candidate.kind, fallback='candidate')}"
    reaction_dir = stage_dir / "reaction_dir"
    reaction_dir.mkdir(parents=True, exist_ok=True)

    target_xyz = reaction_dir / "ts_guess.xyz"
    target_inp = reaction_dir / "ts_guess.inp"
    geometry_metadata = write_orca_ready_xyz(
        source_path=source_xyz,
        target_path=target_xyz,
        candidate_kind=str(candidate.kind),
    )
    target_inp.write_text(
        _render_orca_input(
            route_line=route_line,
            charge=charge,
            multiplicity=multiplicity,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
            xyz_filename=target_xyz.name,
        ),
        encoding="utf-8",
    )

    source_payload = {
        "source_job_id": contract.job_id,
        "source_job_type": contract.job_type,
        "source_candidate_path": str(source_xyz),
        "reaction_key": contract.reaction_key,
        "geometry_materialization": dict(geometry_metadata),
    }
    atomic_write_json(stage_dir / "source_candidate.json", source_payload, ensure_ascii=True, indent=2)

    return OrcaStagePayload(
        stage_id=orca_payload.stage_id,
        engine=orca_payload.engine,
        task_kind=orca_payload.task_kind,
        selected_input_xyz=orca_payload.selected_input_xyz,
        selected_input_label=orca_payload.selected_input_label,
        source_job_id=orca_payload.source_job_id,
        source_job_type=orca_payload.source_job_type,
        reaction_key=orca_payload.reaction_key,
        workflow_id=orca_payload.workflow_id,
        template_name=orca_payload.template_name,
        resource_request=dict(orca_payload.resource_request),
        reaction_dir=str(reaction_dir),
        selected_inp=str(target_inp),
        suggested_command=f"{CHEMSTACK_CLI_COMMAND} run-dir '{reaction_dir}'",
        metadata=dict(orca_payload.metadata),
    )


def _reaction_ts_guess_error(contract: XtbArtifactContract) -> str:
    candidates = sorted(
        [
            item for item in contract.candidate_details
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
    policy = XtbDownstreamPolicy.build(
        preferred_kinds=("ts_guess",),
        allowed_kinds=("ts_guess",),
        max_candidates=max_orca_stages,
        selected_only=selected_only,
        fallback_to_selected_paths=False,
    )
    candidates = select_xtb_downstream_inputs(contract, policy=policy, require_geometry=True)
    if not candidates:
        raise ValueError(_reaction_ts_guess_error(contract))
    workflow_id = _workflow_id(contract)
    requested_at = now_utc_iso()
    resource_request = {
        "max_cores": max(1, int(max_cores)),
        "max_memory_gb": max(1, int(max_memory_gb)),
    }

    workspace_dir: Path | None = None
    if workspace_root is not None:
        workspace_dir = Path(workspace_root).expanduser().resolve() / workflow_id
        workspace_dir.mkdir(parents=True, exist_ok=True)
        (workspace_dir / "stage_03_orca").mkdir(parents=True, exist_ok=True)

    stages: list[WorkflowStage] = []
    stage_payloads: list[dict[str, Any]] = []
    for index, candidate in enumerate(candidates, start=1):
        orca_payload = _orca_payload_from_candidate(
            contract=contract,
            workflow_id=workflow_id,
            candidate_index=index,
            candidate=candidate,
            resource_request=resource_request,
        )
        if workspace_dir is not None:
            orca_payload = _materialize_orca_stage(
                workspace_dir=workspace_dir,
                index=index,
                candidate=candidate,
                contract=contract,
                orca_payload=orca_payload,
                route_line=orca_route_line,
                charge=charge,
                multiplicity=multiplicity,
                max_cores=resource_request["max_cores"],
                max_memory_gb=resource_request["max_memory_gb"],
            )

        enqueue_payload = _build_orca_enqueue_payload(
            workflow_id=workflow_id,
            stage_id=orca_payload.stage_id,
            reaction_dir=orca_payload.reaction_dir,
            selected_inp=orca_payload.selected_inp,
            priority=priority,
            resource_request=resource_request,
            source_job_id=contract.job_id,
            reaction_key=contract.reaction_key,
        )
        stage_task = WorkflowTask.from_raw(
            task_id=f"{workflow_id}:{orca_payload.stage_id}",
            engine=orca_payload.engine,
            task_kind=orca_payload.task_kind,
            resource_request=resource_request,
            payload=orca_payload.to_dict(),
            enqueue_payload=enqueue_payload,
            depends_on=(),
            metadata={
                "workflow_id": workflow_id,
                "template_name": "reaction_ts_search",
                "source_candidate_path": candidate.artifact_path,
                "queue_priority": int(priority),
                "reaction_dir": orca_payload.reaction_dir,
                "selected_inp": orca_payload.selected_inp,
            },
        )
        stage = WorkflowStage(
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
                    path=orca_payload.selected_inp or f"{workflow_id}/{orca_payload.stage_id}/orca.inp",
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
        stages.append(stage)
        stage_payloads.append(orca_payload.to_dict())

        if workspace_dir is not None:
            stage_key = f"{index:02d}_{_safe_name(candidate.kind, fallback='candidate')}"
            atomic_write_json(
                workspace_dir / "stage_03_orca" / stage_key / "enqueue_payload.json",
                dict(enqueue_payload),
                ensure_ascii=True,
                indent=2,
            )

    request = WorkflowTemplateRequest(
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        status="planned",
        requested_at=requested_at,
        parameters={
            "max_orca_stages": max_orca_stages,
            "selected_only": selected_only,
            "charge": int(charge),
            "multiplicity": int(multiplicity),
            "max_cores": resource_request["max_cores"],
            "max_memory_gb": resource_request["max_memory_gb"],
            "orca_route_line": _ensure_route_line(orca_route_line),
            "priority": int(priority),
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
    plan = WorkflowPlan(
        workflow_id=workflow_id,
        template_name="reaction_ts_search",
        status="planned",
        source_job_id=contract.job_id,
        source_job_type=contract.job_type,
        reaction_key=contract.reaction_key,
        requested_at=requested_at,
        stages=tuple(stages),
        metadata={
            "request": request.to_dict(),
            "source_contract": contract.to_dict(),
            "orca_stage_payloads": stage_payloads,
            "orca_stage_enqueue_payloads": [dict(stage.task.enqueue_payload) for stage in stages if stage.task is not None],
            "workspace_dir": str(workspace_dir) if workspace_dir is not None else "",
        },
    )
    payload = plan.to_dict()
    if workspace_dir is not None:
        atomic_write_json(workspace_dir / "workflow.json", payload, ensure_ascii=True, indent=2)
        workspace_root_path = Path(workspace_root).expanduser().resolve() if workspace_root is not None else workspace_dir.parent.parent
        sync_workflow_registry(workspace_root_path, workspace_dir, payload)
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
