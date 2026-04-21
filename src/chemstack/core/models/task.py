from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    CREATED = "created"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class ResourceRequest:
    max_cores: int = 8
    max_memory_gb: int = 32


@dataclass(frozen=True)
class ResourcePolicy:
    max_cores_per_task: int = 8
    max_memory_gb_per_task: int = 32
    max_concurrent_tasks: int = 4


@dataclass(frozen=True)
class TaskRecord:
    task_id: str
    app_name: str
    task_kind: str
    engine: str
    status: TaskStatus = TaskStatus.CREATED
    workflow_id: str | None = None
    job_id: str | None = None
    input_paths: tuple[str, ...] = ()
    output_paths: tuple[str, ...] = ()
    resource_request: ResourceRequest = field(default_factory=ResourceRequest)
