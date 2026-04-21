from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class WorkflowStatus(str, Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class WorkflowRecord:
    workflow_id: str
    workflow_type: str
    status: WorkflowStatus = WorkflowStatus.CREATED
    task_ids: tuple[str, ...] = field(default_factory=tuple)
