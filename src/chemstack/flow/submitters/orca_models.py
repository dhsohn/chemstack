from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SiblingSubmitterConfig:
    config_path: str
    executable: str
    repo_root: str | None


@dataclass
class WorkflowStageOutcome:
    bucket: str
    detail: dict[str, Any]
    stage_result: dict[str, Any]


@dataclass(frozen=True)
class TaskStageMutation:
    task_status: str | None = None
    stage_status: str | None = None
    task_record_key: str | None = None
    metadata_updates: dict[str, Any] = field(default_factory=dict)
    metadata_removals: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecordedStageTransition:
    bucket: str
    detail: dict[str, Any]
    stage_result: dict[str, Any]
    mutation: TaskStageMutation


@dataclass
class WorkflowBuckets:
    submitted: list[dict[str, Any]] = field(default_factory=list)
    cancelled: list[dict[str, Any]] = field(default_factory=list)
    requested: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    stage_results: list[dict[str, Any]] = field(default_factory=list)

    def record(self, outcome: WorkflowStageOutcome) -> None:
        bucket = getattr(self, outcome.bucket)
        bucket.append(outcome.detail)
        self.stage_results.append(outcome.stage_result)


@dataclass
class CancelStageContext:
    stage: dict[str, Any]
    task: dict[str, Any]
    stage_metadata: dict[str, Any]
    enqueue_payload: dict[str, Any]
    stage_id: str
    task_status: str
    stage_status: str
    queue_id: str
    reaction_dir: str


def apply_task_stage_mutation(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    mutation: TaskStageMutation,
    task_record: Any = None,
) -> None:
    if mutation.task_status is not None:
        task["status"] = mutation.task_status
    if mutation.stage_status is not None:
        stage["status"] = mutation.stage_status
    if mutation.task_record_key is not None:
        task[mutation.task_record_key] = task_record
    stage_metadata.update(mutation.metadata_updates)
    for key in mutation.metadata_removals:
        stage_metadata.pop(key, None)


def mapping_payload(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def ensure_submission_metadata(stage: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    metadata = task.get("metadata")
    if not isinstance(metadata, dict):
        task["metadata"] = {}
    stage_metadata = stage.get("metadata")
    if not isinstance(stage_metadata, dict):
        stage_metadata = {}
        stage["metadata"] = stage_metadata
    return stage_metadata


def workflow_metadata(payload: dict[str, Any]) -> dict[str, Any] | None:
    payload.setdefault("metadata", {})
    return payload["metadata"] if isinstance(payload["metadata"], dict) else None
