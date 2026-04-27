from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_CLI_COMMAND, CHEMSTACK_CLI_MODULE, CHEMSTACK_ORCA_SUBMITTER
from chemstack.core.utils import atomic_write_json

from ..contracts import WorkflowArtifactRef, WorkflowStage, WorkflowStageInput, WorkflowTask
from ..xyz_utils import write_orca_ready_xyz


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def safe_name(value: str, *, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in normalize_text(value))
    cleaned = cleaned.strip("._-").lower()
    return cleaned or fallback


def ensure_route_line(route_line: str) -> str:
    normalized = normalize_text(route_line) or "r2scan-3c TightSCF"
    return normalized if normalized.startswith("!") else f"! {normalized}"


def maxcore_mb_per_core(*, max_memory_gb: int, max_cores: int) -> int:
    total_mb = max(1, int(max_memory_gb)) * 1024
    return max(1, total_mb // max(1, int(max_cores)))


def render_orca_input(
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
            ensure_route_line(route_line),
            "",
            "%pal",
            f"  nprocs {max(1, int(max_cores))}",
            "end",
            f"%maxcore {maxcore_mb_per_core(max_memory_gb=max_memory_gb, max_cores=max_cores)}",
            "",
            f"* xyzfile {int(charge)} {int(multiplicity)} {xyz_filename}",
            "",
        ]
    )


def build_orca_enqueue_payload(
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
    command_argv = [
        "python",
        "-m",
        CHEMSTACK_CLI_MODULE,
        "--config",
        config_placeholder,
        "run-dir",
        reaction_dir,
        "--priority",
        str(int(priority)),
    ]
    if max_cores > 0:
        command_argv.extend(["--max-cores", str(max_cores)])
    if max_memory_gb > 0:
        command_argv.extend(["--max-memory-gb", str(max_memory_gb)])
    command_parts = [
        f"{CHEMSTACK_CLI_COMMAND} --config {config_placeholder}",
        "run-dir",
        f"'{reaction_dir}'",
        f"--priority {int(priority)}",
    ]
    if max_cores > 0:
        command_parts.append(f"--max-cores {max_cores}")
    if max_memory_gb > 0:
        command_parts.append(f"--max-memory-gb {max_memory_gb}")
    command = " ".join(command_parts)
    return {
        "submitter": CHEMSTACK_ORCA_SUBMITTER,
        "command": command,
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


@dataclass(frozen=True)
class OrcaStageMaterialization:
    reaction_dir: str
    selected_inp: str
    selected_xyz: str
    stage_workspace_dir: str


def materialize_orca_stage(
    *,
    workspace_dir: Path,
    stage_root_name: str,
    stage_key: str,
    source_artifact_path: str,
    candidate_kind: str,
    route_line: str,
    charge: int,
    multiplicity: int,
    max_cores: int,
    max_memory_gb: int,
    xyz_filename: str = "input.xyz",
    inp_filename: str = "input.inp",
    extra_source_payload: dict[str, Any] | None = None,
) -> OrcaStageMaterialization:
    source_xyz = Path(source_artifact_path).expanduser().resolve()
    if not source_xyz.exists():
        raise FileNotFoundError(f"ORCA stage source artifact not found: {source_xyz}")

    root_name = normalize_text(stage_root_name)
    stage_root = workspace_dir / root_name if root_name else workspace_dir
    stage_dir = stage_root / stage_key
    reaction_dir = stage_dir
    reaction_dir.mkdir(parents=True, exist_ok=True)

    target_xyz = reaction_dir / xyz_filename
    geometry_metadata = write_orca_ready_xyz(
        source_path=source_xyz,
        target_path=target_xyz,
        candidate_kind=candidate_kind,
    )
    target_inp = reaction_dir / inp_filename
    target_inp.write_text(
        render_orca_input(
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
        "source_artifact_path": str(source_xyz),
        "geometry_materialization": dict(geometry_metadata),
        **dict(extra_source_payload or {}),
    }
    atomic_write_json(stage_dir / "source_candidate.json", source_payload, ensure_ascii=True, indent=2)
    return OrcaStageMaterialization(
        reaction_dir=str(reaction_dir),
        selected_inp=str(target_inp),
        selected_xyz=str(target_xyz),
        stage_workspace_dir=str(stage_dir),
    )


def build_materialized_orca_stage(
    *,
    workflow_id: str,
    template_name: str,
    stage_id: str,
    stage_key: str,
    stage_root_name: str,
    workspace_dir: Path,
    input_artifact_kind: str,
    candidate: WorkflowStageInput,
    task_kind: str,
    route_line: str,
    charge: int,
    multiplicity: int,
    max_cores: int,
    max_memory_gb: int,
    priority: int,
    xyz_filename: str,
    inp_filename: str,
    input_label: str | None = None,
) -> WorkflowStage:
    resource_request = {
        "max_cores": max(1, int(max_cores)),
        "max_memory_gb": max(1, int(max_memory_gb)),
    }
    materialized = materialize_orca_stage(
        workspace_dir=workspace_dir,
        stage_root_name=stage_root_name,
        stage_key=stage_key,
        source_artifact_path=candidate.artifact_path,
        candidate_kind=candidate.kind,
        route_line=route_line,
        charge=charge,
        multiplicity=multiplicity,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        xyz_filename=xyz_filename,
        inp_filename=inp_filename,
        extra_source_payload={
            "source_job_id": candidate.source_job_id,
            "source_job_type": candidate.source_job_type,
            "reaction_key": candidate.reaction_key,
            "rank": candidate.rank,
            "kind": candidate.kind,
        },
    )
    enqueue_payload = build_orca_enqueue_payload(
        workflow_id=workflow_id,
        stage_id=stage_id,
        reaction_dir=materialized.reaction_dir,
        selected_inp=materialized.selected_inp,
        priority=priority,
        resource_request=resource_request,
        source_job_id=candidate.source_job_id,
        reaction_key=candidate.reaction_key,
    )
    task_payload = {
        "stage_id": stage_id,
        "engine": "orca",
        "task_kind": task_kind,
        "selected_input_xyz": materialized.selected_xyz,
        "selected_input_label": input_label or Path(candidate.artifact_path).name,
        "source_job_id": candidate.source_job_id,
        "source_job_type": candidate.source_job_type,
        "reaction_key": candidate.reaction_key,
        "workflow_id": workflow_id,
        "template_name": template_name,
        "resource_request": dict(resource_request),
        "reaction_dir": materialized.reaction_dir,
        "selected_inp": materialized.selected_inp,
        "suggested_command": f"{CHEMSTACK_CLI_COMMAND} run-dir '{materialized.reaction_dir}'",
        "metadata": {
            "candidate_rank": candidate.rank,
            "candidate_kind": candidate.kind,
            "candidate_score": candidate.score,
            "candidate_selected": candidate.selected,
            "candidate_metadata": dict(candidate.metadata),
            "source_selected_input_xyz": candidate.selected_input_xyz,
        },
    }
    task = WorkflowTask.from_raw(
        task_id=f"{workflow_id}:{stage_id}",
        engine="orca",
        task_kind=task_kind,
        resource_request=resource_request,
        payload=task_payload,
        enqueue_payload=enqueue_payload,
        metadata={
            "workflow_id": workflow_id,
            "template_name": template_name,
            "source_candidate_path": candidate.artifact_path,
            "queue_priority": int(priority),
            "reaction_dir": materialized.reaction_dir,
            "selected_inp": materialized.selected_inp,
        },
    )
    atomic_write_json(Path(materialized.stage_workspace_dir) / "enqueue_payload.json", enqueue_payload, ensure_ascii=True, indent=2)
    return WorkflowStage(
        stage_id=stage_id,
        stage_kind="orca_stage",
        status="planned",
        input_artifacts=(
            WorkflowArtifactRef(
                kind=input_artifact_kind,
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
                path=materialized.selected_inp,
                selected=True,
                metadata={
                    "engine": "orca",
                    "task_kind": task_kind,
                    "reaction_dir": materialized.reaction_dir,
                },
            ),
        ),
        task=task,
        metadata={
            "candidate_rank": candidate.rank,
            "candidate_kind": candidate.kind,
            "candidate_score": candidate.score,
            "selected_input_label": input_label or Path(candidate.artifact_path).name,
            "reaction_dir": materialized.reaction_dir,
        },
    )


__all__ = [
    "build_materialized_orca_stage",
    "build_orca_enqueue_payload",
    "ensure_route_line",
    "materialize_orca_stage",
    "normalize_text",
    "render_orca_input",
    "safe_name",
]
