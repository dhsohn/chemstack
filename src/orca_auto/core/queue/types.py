from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class QueueStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class QueueEntry:
    queue_id: str
    app_name: str
    task_id: str
    task_kind: str
    engine: str
    status: QueueStatus = QueueStatus.PENDING
    priority: int = 10
    enqueued_at: str = ""
    started_at: str = ""
    finished_at: str = ""
    cancel_requested: bool = False
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
