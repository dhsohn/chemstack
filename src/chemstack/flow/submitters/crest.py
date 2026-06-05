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


def _submitter_deps() -> _internal_engine.InternalEngineSubmitterDeps:
    return _internal_engine.InternalEngineSubmitterDeps(
        load_config_fn=lambda config_path: load_config(config_path),
        resolve_job_dir_fn=lambda cfg, job_dir: resolve_job_dir(cfg, job_dir),
        load_manifest_fn=lambda job_dir: load_job_manifest(job_dir),
        build_submission_fn=lambda cfg, job_dir, manifest, args: build_submission(
            cfg,
            job_dir,
            manifest,
            args,
        ),
        record_queued_fn=lambda cfg, submission, entry: record_queued(
            cfg,
            submission,
            entry,
        ),
        enqueue_fn=lambda *args, **kwargs: enqueue(*args, **kwargs),
        load_queue_config_fn=lambda config_path: load_queue_config(config_path),
        queue_entries_with_roots_fn=lambda cfg: queue_entries_with_roots(cfg),
        request_cancel_fn=lambda queue_root, queue_id: request_cancel(queue_root, queue_id),
        display_status_fn=lambda entry: display_status(entry),
    )


submit_job_dir, cancel_target = _internal_engine.build_internal_engine_module_submitter(
    engine="crest",
    deps_factory=_submitter_deps,
)


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
