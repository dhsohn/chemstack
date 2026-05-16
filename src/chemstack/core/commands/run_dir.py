from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.queue import DuplicateQueueEntryError, enqueue


@dataclass(frozen=True)
class EngineRunDirSubmission:
    queue_root: Path
    app_name: str
    task_id: str
    task_kind: str
    engine: str
    priority: int
    metadata: dict[str, Any]
    context: dict[str, Any]


def cmd_engine_run_dir(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    resolve_job_dir_fn: Callable[[Any, str], Path],
    load_manifest_fn: Callable[[Path], dict[str, Any]],
    build_submission_fn: Callable[[Any, Path, dict[str, Any], Any], EngineRunDirSubmission],
    record_queued_fn: Callable[[Any, EngineRunDirSubmission, Any], None],
    print_queued_fn: Callable[[EngineRunDirSubmission, Any], None],
    enqueue_fn: Callable[..., Any] = enqueue,
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    raw_job_dir = getattr(args, "path", None)
    if not isinstance(raw_job_dir, str) or not raw_job_dir.strip():
        raise ValueError("job directory path is required")

    job_dir = resolve_job_dir_fn(cfg, raw_job_dir)
    manifest = load_manifest_fn(job_dir)
    submission = build_submission_fn(cfg, job_dir, manifest, args)
    try:
        entry = enqueue_fn(
            submission.queue_root,
            app_name=submission.app_name,
            task_id=submission.task_id,
            task_kind=submission.task_kind,
            engine=submission.engine,
            priority=submission.priority,
            metadata=dict(submission.metadata),
        )
    except DuplicateQueueEntryError as exc:
        print(f"error: {exc}")
        return 1

    record_queued_fn(cfg, submission, entry)
    print_queued_fn(submission, entry)
    return 0
