from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _normalize_text(value: Any) -> str:
    return str(value).strip()


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


@dataclass(frozen=True)
class WorkflowArtifactRef:
    kind: str
    path: str
    selected: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not self.metadata:
            payload["metadata"] = {}
        return payload


@dataclass(frozen=True)
class WorkflowTask:
    task_id: str
    engine: str
    task_kind: str
    resource_request: dict[str, int]
    status: str = "planned"
    payload: dict[str, Any] = field(default_factory=dict)
    enqueue_payload: dict[str, Any] = field(default_factory=dict)
    submission_result: dict[str, Any] = field(default_factory=dict)
    depends_on: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(
        cls,
        *,
        task_id: str,
        engine: str,
        task_kind: str,
        status: str = "planned",
        resource_request: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        enqueue_payload: dict[str, Any] | None = None,
        submission_result: dict[str, Any] | None = None,
        depends_on: list[str] | tuple[str, ...] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "WorkflowTask":
        request = resource_request or {}
        return cls(
            task_id=_normalize_text(task_id),
            engine=_normalize_text(engine) or "unknown",
            task_kind=_normalize_text(task_kind) or "task",
            status=_normalize_text(status) or "planned",
            resource_request={
                str(key): _safe_int(value, default=0)
                for key, value in request.items()
                if _normalize_text(key)
            },
            payload=_coerce_mapping(payload),
            enqueue_payload=_coerce_mapping(enqueue_payload),
            submission_result=_coerce_mapping(submission_result),
            depends_on=tuple(_normalize_text(item) for item in (depends_on or ()) if _normalize_text(item)),
            metadata=_coerce_mapping(metadata),
        )

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not self.payload:
            payload["payload"] = {}
        if not self.enqueue_payload:
            payload["enqueue_payload"] = {}
        if not self.submission_result:
            payload["submission_result"] = {}
        if not self.metadata:
            payload["metadata"] = {}
        return payload


@dataclass(frozen=True)
class WorkflowStage:
    stage_id: str
    stage_kind: str
    status: str
    input_artifacts: tuple[WorkflowArtifactRef, ...] = ()
    output_artifacts: tuple[WorkflowArtifactRef, ...] = ()
    task: WorkflowTask | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_id": self.stage_id,
            "stage_kind": self.stage_kind,
            "status": self.status,
            "input_artifacts": [item.to_dict() for item in self.input_artifacts],
            "output_artifacts": [item.to_dict() for item in self.output_artifacts],
            "task": self.task.to_dict() if self.task is not None else None,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class WorkflowTemplateRequest:
    workflow_id: str
    template_name: str
    source_job_id: str
    source_job_type: str
    reaction_key: str
    status: str
    requested_at: str
    parameters: dict[str, Any] = field(default_factory=dict)
    source_artifacts: tuple[WorkflowArtifactRef, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "template_name": self.template_name,
            "source_job_id": self.source_job_id,
            "source_job_type": self.source_job_type,
            "reaction_key": self.reaction_key,
            "status": self.status,
            "requested_at": self.requested_at,
            "parameters": dict(self.parameters),
            "source_artifacts": [item.to_dict() for item in self.source_artifacts],
        }


@dataclass(frozen=True)
class WorkflowPlan:
    workflow_id: str
    template_name: str
    status: str
    source_job_id: str
    source_job_type: str
    reaction_key: str
    requested_at: str
    stages: tuple[WorkflowStage, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "template_name": self.template_name,
            "status": self.status,
            "source_job_id": self.source_job_id,
            "source_job_type": self.source_job_type,
            "reaction_key": self.reaction_key,
            "requested_at": self.requested_at,
            "stages": [item.to_dict() for item in self.stages],
            "metadata": dict(self.metadata),
        }


__all__ = [
    "WorkflowArtifactRef",
    "WorkflowPlan",
    "WorkflowStage",
    "WorkflowTask",
    "WorkflowTemplateRequest",
]
