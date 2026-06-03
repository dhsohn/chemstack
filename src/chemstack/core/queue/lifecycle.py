from __future__ import annotations

import logging

from .child_process import (
    reconcile_orphaned_child_queue_entries as reconcile_orphaned_child_queue_entries,
)
from .child_process import (
    shutdown_child_process_with_grace as shutdown_child_process_with_grace,
)
from .child_process import (
    status_matches as status_matches,
)
from .lifecycle_hooks import (
    ChildExitPolicy,
    EngineQueueProcessLifecycleHooks,
    EngineQueueProcessReconcileHooks,
    EngineQueueProcessShutdownHooks,
    EngineQueueTerminalSideEffectHooks,
    OrphanedRunningPolicy,
)
from .lifecycle_reconcile import (
    live_worker_pid_slots,
    reconcile_orphaned_process_entries,
    reconcile_orphaned_running,
    reconcile_orphaned_running_with_policy,
)
from .lifecycle_shutdown import (
    cancel_running_process_job,
    finalize_child_exit_with_policy,
    finalize_child_worker_exit,
    request_pending_cancellations,
    shutdown_running_job,
    shutdown_running_process_job,
)
from .lifecycle_terminal import (
    attach_started_process_metadata,
    entry_status_is,
    entry_status_is_running,
    finalize_process_finished_job,
    job_queue_root,
    mark_terminal_process_queue_entry,
    record_terminal_process_side_effects,
    resolved_job_queue_root,
    run_terminal_process_side_effects,
    sync_terminal_running_entries,
)
from .types import QueueStatus as QueueStatus

LOGGER = logging.getLogger(__name__)

__all__ = [
    "ChildExitPolicy",
    "EngineQueueProcessLifecycleHooks",
    "EngineQueueProcessReconcileHooks",
    "EngineQueueProcessShutdownHooks",
    "EngineQueueTerminalSideEffectHooks",
    "OrphanedRunningPolicy",
    "attach_started_process_metadata",
    "cancel_running_process_job",
    "entry_status_is",
    "entry_status_is_running",
    "finalize_child_exit_with_policy",
    "finalize_child_worker_exit",
    "finalize_process_finished_job",
    "job_queue_root",
    "live_worker_pid_slots",
    "mark_terminal_process_queue_entry",
    "reconcile_orphaned_process_entries",
    "reconcile_orphaned_running",
    "reconcile_orphaned_running_with_policy",
    "record_terminal_process_side_effects",
    "request_pending_cancellations",
    "resolved_job_queue_root",
    "run_terminal_process_side_effects",
    "shutdown_running_job",
    "shutdown_running_process_job",
    "sync_terminal_running_entries",
]
