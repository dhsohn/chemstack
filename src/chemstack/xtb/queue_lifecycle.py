from __future__ import annotations

from chemstack.core.queue import lifecycle as _queue_lifecycle
from chemstack.core.queue.internal_engine import InternalEngineSpec


entry_status_is_running = _queue_lifecycle.entry_status_is_running
live_worker_pid_slots = _queue_lifecycle.live_worker_pid_slots
shutdown_running_job = _queue_lifecycle.shutdown_running_job
sync_terminal_running_entries = _queue_lifecycle.sync_terminal_running_entries

_ENGINE_LIFECYCLE = InternalEngineSpec(engine="xtb").lifecycle()
finalize_child_exit = _ENGINE_LIFECYCLE.finalize_child_exit
reconcile_orphaned_running = _ENGINE_LIFECYCLE.reconcile_orphaned_running


__all__ = [
    "entry_status_is_running",
    "finalize_child_exit",
    "live_worker_pid_slots",
    "reconcile_orphaned_running",
    "shutdown_running_job",
    "sync_terminal_running_entries",
]
