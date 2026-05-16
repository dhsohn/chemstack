from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..contracts import WorkflowStage, XtbArtifactContract


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


__all__ = [
    "BuiltReactionOrcaStage",
    "OrcaStageBuildContext",
    "OrcaStagePayload",
    "ReactionTsPlanBuildContext",
    "ReactionTsSearchPlanRequest",
]
