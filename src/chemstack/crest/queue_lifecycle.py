from __future__ import annotations

from chemstack.core.queue import lifecycle as _queue_lifecycle
from chemstack.core.queue.internal_engine import InternalEngineSpec


shutdown_running_job = _queue_lifecycle.shutdown_running_job
_ENGINE_LIFECYCLE = InternalEngineSpec(engine="crest").lifecycle()
finalize_child_exit = _ENGINE_LIFECYCLE.finalize_child_exit
reconcile_orphaned_running = _ENGINE_LIFECYCLE.reconcile_orphaned_running


__all__ = [
    "finalize_child_exit",
    "reconcile_orphaned_running",
    "shutdown_running_job",
]
