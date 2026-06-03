from __future__ import annotations

from chemstack.core.queue.internal_engine import InternalEngineSpec


_ENGINE_LIFECYCLE_EXPORTS = InternalEngineSpec(engine="crest").lifecycle_module_exports()
shutdown_running_job = _ENGINE_LIFECYCLE_EXPORTS.shutdown_running_job
finalize_child_exit = _ENGINE_LIFECYCLE_EXPORTS.finalize_child_exit
reconcile_orphaned_running = _ENGINE_LIFECYCLE_EXPORTS.reconcile_orphaned_running


__all__ = [
    "finalize_child_exit",
    "reconcile_orphaned_running",
    "shutdown_running_job",
]
