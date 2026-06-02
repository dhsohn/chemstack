from __future__ import annotations

from typing import Any

from chemstack.core import queue as _queue_store
from chemstack.core.commands import queue as _queue_commands
from chemstack.xtb import queue_runtime as _queue_runtime
from chemstack.xtb import submission as _submission

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


def _extra_fields(submission: Any | None, _entry: Any | None) -> dict[str, Any]:
    metadata = getattr(submission, "metadata", {}) if submission is not None else {}
    return {
        "job_type": _internal_engine.normalize_text(metadata.get("job_type")),
        "reaction_key": _internal_engine.normalize_text(metadata.get("reaction_key")),
    }


def _submitter_deps() -> _internal_engine.InternalEngineSubmitterDeps:
    return _internal_engine.InternalEngineSubmitterDeps(
        load_config_fn=load_config,
        resolve_job_dir_fn=resolve_job_dir,
        load_manifest_fn=load_job_manifest,
        build_submission_fn=build_submission,
        record_queued_fn=record_queued,
        enqueue_fn=enqueue,
        load_queue_config_fn=load_queue_config,
        queue_entries_with_roots_fn=queue_entries_with_roots,
        request_cancel_fn=request_cancel,
        display_status_fn=display_status,
    )


submit_job_dir, cancel_target = _internal_engine.build_internal_engine_submitter(
    run_dir_api_name="chemstack.xtb.submission.direct_enqueue",
    cancel_api_name="chemstack.xtb.queue_runtime.direct_cancel",
    deps_factory=_submitter_deps,
    extra_fields_fn=_extra_fields,
)


__all__ = [
    "cancel_target",
    "submit_job_dir",
]
