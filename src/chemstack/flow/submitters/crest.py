from __future__ import annotations

from chemstack.core import queue as _queue_store
from chemstack.core.commands import queue as _queue_commands
from chemstack.crest import queue_runtime as _queue_runtime
from chemstack.crest import submission as _submission

from . import internal_engine as _internal_engine

display_status = _queue_commands.display_status
enqueue = _queue_store.enqueue
request_cancel = _queue_store.request_cancel
build_submission = _submission._build_submission
load_config = _submission.load_config
load_job_manifest = _submission.load_job_manifest
load_queue_config = _queue_runtime.load_config
queue_entries_with_roots = _queue_runtime.queue_entries_with_roots
record_queued = _submission._record_queued
resolve_job_dir = _submission.resolve_job_dir


submit_job_dir, cancel_target = _internal_engine.build_internal_engine_module_submitter(
    engine="crest",
    namespace=globals(),
)


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
