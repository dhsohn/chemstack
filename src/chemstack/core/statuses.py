from __future__ import annotations

STATUS_CANCEL_REQUESTED = "cancel_requested"
STATUS_CANCEL_FAILED = "cancel_failed"
STATUS_CANCELLED = "cancelled"
STATUS_COMPLETED = "completed"
STATUS_CREATED = "created"
STATUS_FAILED = "failed"
STATUS_PENDING = "pending"
STATUS_PLANNED = "planned"
STATUS_QUEUED = "queued"
STATUS_RETRYING = "retrying"
STATUS_RUNNING = "running"
STATUS_SUBMISSION_FAILED = "submission_failed"
STATUS_SUBMITTED = "submitted"
STATUS_UNKNOWN = "unknown"

TERMINAL_STATUSES = frozenset({STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED})
FAILED_STATUSES = frozenset({STATUS_FAILED, STATUS_CANCEL_FAILED, STATUS_SUBMISSION_FAILED})
WORKFLOW_ACTIVE_STATUSES = frozenset(
    {
        STATUS_CREATED,
        STATUS_PLANNED,
        STATUS_PENDING,
        STATUS_QUEUED,
        STATUS_SUBMITTED,
        STATUS_RUNNING,
        STATUS_RETRYING,
        STATUS_CANCEL_REQUESTED,
    }
)
WORKFLOW_FAILED_STATUSES = FAILED_STATUSES
WORKFLOW_ATTENTION_STATUSES = WORKFLOW_FAILED_STATUSES
WORKFLOW_TERMINAL_STATUSES = frozenset(
    {STATUS_COMPLETED, STATUS_CANCELLED, *WORKFLOW_FAILED_STATUSES}
)
WORKFLOW_STATUS_ORDER = (
    STATUS_RUNNING,
    STATUS_QUEUED,
    STATUS_SUBMITTED,
    STATUS_PLANNED,
    STATUS_RETRYING,
    STATUS_CANCEL_REQUESTED,
    STATUS_FAILED,
    STATUS_CANCEL_FAILED,
    STATUS_SUBMISSION_FAILED,
)
QUEUE_ACTIVE_STATUSES = frozenset(
    {
        STATUS_PLANNED,
        STATUS_PENDING,
        STATUS_QUEUED,
        STATUS_SUBMITTED,
        STATUS_RUNNING,
        STATUS_RETRYING,
        STATUS_CANCEL_REQUESTED,
    }
)
STAGE_TERMINAL_STATUSES = WORKFLOW_TERMINAL_STATUSES
STAGE_CANCELLABLE_STATUSES = frozenset(
    {
        STATUS_PLANNED,
        STATUS_QUEUED,
        STATUS_RUNNING,
        STATUS_SUBMITTED,
    }
)
CANCEL_ACK_STATUSES = frozenset({STATUS_CANCELLED, STATUS_CANCEL_REQUESTED})
SYNC_ONLY_WORKFLOW_STATUSES = frozenset(
    {
        STATUS_COMPLETED,
        STATUS_CANCEL_REQUESTED,
        STATUS_CANCELLED,
        STATUS_CANCEL_FAILED,
    }
)


def normalize_status(value: object) -> str:
    return str(value or "").strip().lower()


def status_in(value: object, statuses: frozenset[str] | set[str] | tuple[str, ...]) -> bool:
    return normalize_status(value) in statuses


def is_terminal_status(value: object) -> bool:
    return status_in(value, TERMINAL_STATUSES)


def is_failed_status(value: object) -> bool:
    return status_in(value, FAILED_STATUSES)


def is_workflow_active_status(value: object) -> bool:
    return status_in(value, WORKFLOW_ACTIVE_STATUSES)


def is_workflow_terminal_status(value: object) -> bool:
    return status_in(value, WORKFLOW_TERMINAL_STATUSES)


def is_queue_active_status(value: object) -> bool:
    return status_in(value, QUEUE_ACTIVE_STATUSES)


def is_stage_terminal_status(value: object) -> bool:
    return status_in(value, STAGE_TERMINAL_STATUSES)


def is_stage_cancellable_status(value: object) -> bool:
    return status_in(value, STAGE_CANCELLABLE_STATUSES)


def is_cancel_ack_status(value: object) -> bool:
    return status_in(value, CANCEL_ACK_STATUSES)


def is_sync_only_workflow_status(value: object) -> bool:
    return status_in(value, SYNC_ONLY_WORKFLOW_STATUSES)

__all__ = [
    "CANCEL_ACK_STATUSES",
    "FAILED_STATUSES",
    "QUEUE_ACTIVE_STATUSES",
    "STAGE_CANCELLABLE_STATUSES",
    "STAGE_TERMINAL_STATUSES",
    "STATUS_CANCEL_FAILED",
    "STATUS_CANCEL_REQUESTED",
    "STATUS_CANCELLED",
    "STATUS_COMPLETED",
    "STATUS_CREATED",
    "STATUS_FAILED",
    "STATUS_PENDING",
    "STATUS_PLANNED",
    "STATUS_QUEUED",
    "STATUS_RETRYING",
    "STATUS_RUNNING",
    "STATUS_SUBMISSION_FAILED",
    "STATUS_SUBMITTED",
    "STATUS_UNKNOWN",
    "TERMINAL_STATUSES",
    "WORKFLOW_ACTIVE_STATUSES",
    "WORKFLOW_ATTENTION_STATUSES",
    "WORKFLOW_FAILED_STATUSES",
    "WORKFLOW_STATUS_ORDER",
    "WORKFLOW_TERMINAL_STATUSES",
    "SYNC_ONLY_WORKFLOW_STATUSES",
    "is_cancel_ack_status",
    "is_failed_status",
    "is_queue_active_status",
    "is_stage_cancellable_status",
    "is_stage_terminal_status",
    "is_sync_only_workflow_status",
    "is_terminal_status",
    "is_workflow_active_status",
    "is_workflow_terminal_status",
    "normalize_status",
    "status_in",
]
