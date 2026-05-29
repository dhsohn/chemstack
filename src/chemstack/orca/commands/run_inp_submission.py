from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from chemstack.core.queue.types import QueueEntry, QueueStatus

if TYPE_CHECKING:
    from ..types import QueueEnqueuedNotification
    from .run_inp_context import WorkerStatusInfo


@dataclass(frozen=True)
class QueuedSubmissionResult:
    entry: Any
    reaction_dir: Path
    selected_inp: Path | None
    queue_metadata: dict[str, Any]
    worker_info: WorkerStatusInfo


@dataclass(frozen=True)
class DirectQueueSubmission:
    status: str
    reason: str = ""
    stderr: str = ""
    context: Any | None = None
    queued_result: Any | None = None


def active_queue_entry(allowed_root: Path, reaction_dir: Path, *, deps: Any) -> QueueEntry | None:
    queue_adapter = deps.submission._queue_adapter
    helper = getattr(queue_adapter, "get_active_entry_for_reaction_dir", None)
    if callable(helper):
        return helper(allowed_root, str(reaction_dir))

    resolved = str(reaction_dir.expanduser().resolve())
    for entry in queue_adapter.list_queue(allowed_root):
        if queue_adapter.queue_entry_reaction_dir(entry) != resolved:
            continue
        if queue_adapter.queue_entry_status(entry) in {
            QueueStatus.PENDING.value,
            QueueStatus.RUNNING.value,
        }:
            return entry
    return None


def find_submission_conflict(
    allowed_root: Path,
    reaction_dir: Path,
    *,
    deps: Any,
) -> str | None:
    active_entry = active_queue_entry(allowed_root, reaction_dir, deps=deps)
    queue_adapter = deps.submission._queue_adapter
    if active_entry is not None:
        return (
            "Job directory already queued: "
            f"{reaction_dir} (queue_id={queue_adapter.queue_entry_id(active_entry)}, "
            f"status={queue_adapter.queue_entry_status(active_entry)})"
        )
    return deps.submission._active_direct_run_error(reaction_dir)


def emit_queued_submission(
    reaction_dir: Path,
    entry: QueueEntry,
    *,
    worker_status: str | None,
    worker_pid: int | None,
    worker_log: str | Path | None,
    worker_detail: str | None = None,
    deps: Any,
) -> None:
    queue_adapter = deps.submission._queue_adapter
    print("status: queued")
    print(f"job_dir: {reaction_dir}")
    print(f"queue_id: {queue_adapter.queue_entry_id(entry)}")
    task_id = queue_adapter.queue_entry_task_id(entry)
    if task_id:
        print(f"job_id: {task_id}")
    print(f"priority: {queue_adapter.queue_entry_priority(entry)}")
    if queue_adapter.queue_entry_force(entry):
        print("force: true")
    if worker_status:
        print(f"worker: {worker_status}")
    if worker_pid is not None:
        print(f"worker_pid: {worker_pid}")
    if worker_log:
        print(f"worker_log: {worker_log}")
    if worker_detail:
        print(f"worker_detail: {worker_detail}")


def worker_status_for_submission(allowed_root: Path) -> WorkerStatusInfo:
    from ..queue_worker import read_worker_pid
    from .run_inp_context import WorkerStatusInfo

    pid = read_worker_pid(allowed_root)
    if pid is None:
        return WorkerStatusInfo(status="inactive")
    return WorkerStatusInfo(status="running", pid=pid)


def build_queue_enqueued_notification(entry: Any, *, deps: Any) -> QueueEnqueuedNotification:
    submission = deps.submission
    return {
        "queue_id": submission._queue_adapter.queue_entry_id(entry),
        "reaction_dir": submission._queue_adapter.queue_entry_reaction_dir(entry),
        "priority": submission._queue_adapter.queue_entry_priority(entry),
        "force": submission._queue_adapter.queue_entry_force(entry),
        "enqueued_at": getattr(entry, "enqueued_at", ""),
    }


def resource_request_from_selected_inp(
    cfg: Any,
    selected_inp: Path | None,
    *,
    deps: Any,
    logger: logging.Logger,
) -> dict[str, int]:
    if selected_inp is None:
        raise ValueError("No .inp file selected for ORCA queue submission.")
    resource_request, actions = deps.submission.ensure_submission_resource_request(
        selected_inp,
        default_max_cores=int(cfg.resources.max_cores_per_task),
        default_max_memory_gb=int(cfg.resources.max_memory_gb_per_task),
    )
    if actions:
        logger.info(
            "Updated ORCA input resource directives in %s: %s",
            selected_inp,
            ", ".join(actions),
        )
    return resource_request


def warn_ignored_resource_override_flags(args: Any, *, logger: logging.Logger) -> None:
    if getattr(args, "max_cores", None) is None and getattr(args, "max_memory_gb", None) is None:
        return
    logger.warning(
        "Standalone ORCA queue submission ignores --max-cores/--max-memory-gb; "
        "resource metadata is read from the input file."
    )


def build_queue_metadata(
    cfg: Any,
    *,
    reaction_dir: Path,
    selected_inp: Path | None,
    args: Any | None = None,
    deps: Any,
) -> dict[str, Any]:
    del args
    from ..job_locations import resolve_job_metadata

    selected_input = str(selected_inp) if selected_inp is not None else ""
    job_type, molecule_key = resolve_job_metadata(selected_input, reaction_dir)
    requested = deps.submission._resource_request_from_selected_inp(cfg, selected_inp)
    metadata: dict[str, Any] = {
        "submitted_via": "run_inp",
        "max_retries": max(0, int(cfg.runtime.default_max_retries)),
        "job_type": job_type,
        "molecule_key": molecule_key,
        "resource_request": requested,
        "resource_actual": dict(requested),
    }
    if selected_inp is not None:
        metadata["selected_inp"] = str(selected_inp)
        metadata["selected_input_xyz"] = str(selected_inp)
    return metadata


def upsert_queued_job_record(
    cfg: Any,
    *,
    reaction_dir: Path,
    selected_inp: Path | None,
    job_id: str,
    queue_metadata: dict[str, Any] | None = None,
    deps: Any,
) -> None:
    from ..job_locations import resolve_job_metadata, upsert_job_record

    selected_input = str(selected_inp) if selected_inp is not None else ""
    metadata = dict(queue_metadata or {})
    job_type = str(metadata.get("job_type") or "").strip()
    molecule_key = str(metadata.get("molecule_key") or "").strip()
    if not job_type or not molecule_key:
        derived_job_type, derived_molecule_key = resolve_job_metadata(selected_input, reaction_dir)
        job_type = job_type or derived_job_type
        molecule_key = molecule_key or derived_molecule_key
    requested = metadata.get("resource_request")
    if not isinstance(requested, dict):
        requested = {}
    if not requested and selected_inp is not None and selected_inp.exists():
        requested = deps.submission.read_resource_request_from_input(selected_inp)
    if not requested and selected_inp is not None and selected_inp.exists():
        requested = deps.submission._resource_request_from_selected_inp(cfg, selected_inp)
    actual = metadata.get("resource_actual")
    if not isinstance(actual, dict):
        actual = dict(requested)
    upsert_job_record(
        cfg,
        job_id=job_id,
        status="queued",
        job_dir=reaction_dir,
        job_type=job_type,
        selected_input_xyz=selected_input,
        molecule_key=molecule_key,
        resource_request=requested,
        resource_actual=actual,
    )


def create_queued_submission(
    cfg: Any,
    args: Any,
    reaction_dir: Path,
    *,
    selected_inp: Path | None = None,
    deps: Any,
) -> QueuedSubmissionResult:
    from ..queue_adapter import enqueue

    submission = deps.submission
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    if selected_inp is None:
        try:
            selected_inp = submission._select_latest_inp(reaction_dir)
        except ValueError:
            selected_inp = None
    submission._warn_ignored_resource_override_flags(args)
    queue_metadata = submission._build_queue_metadata(
        cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        args=args,
    )
    entry = enqueue(
        allowed_root,
        str(reaction_dir),
        priority=int(getattr(args, "priority", 10)),
        force=bool(getattr(args, "force", False)),
        metadata=queue_metadata,
    )

    task_id = submission._queue_adapter.queue_entry_task_id(entry)
    if task_id:
        submission._upsert_queued_job_record(
            cfg,
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            job_id=task_id,
            queue_metadata=queue_metadata,
        )

    worker_info = submission._worker_status_for_submission(allowed_root)
    return QueuedSubmissionResult(
        entry=entry,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        queue_metadata=queue_metadata,
        worker_info=worker_info,
    )


def notify_queued_submission(
    cfg: Any,
    result: QueuedSubmissionResult,
    *,
    deps: Any,
) -> None:
    notification = deps.submission._build_queue_enqueued_notification(result.entry)
    deps.notifications.notify_queue_enqueued_event(cfg.telegram, notification)


def submit_reaction_dir_to_queue(
    args: Any,
    *,
    deps: Any,
) -> DirectQueueSubmission:
    context = deps.submission._resolve_submission_context(args)
    if context is None:
        return DirectQueueSubmission(
            status="failed",
            reason="invalid_submission_target",
            stderr="failed to resolve ORCA submission target",
        )

    conflict_error = deps.submission._find_submission_conflict(
        context.allowed_root,
        context.reaction_dir,
    )
    if conflict_error is not None:
        return DirectQueueSubmission(
            status="failed",
            reason="submission_conflict",
            stderr=conflict_error,
            context=context,
        )

    try:
        from ..queue_adapter import DuplicateEntryError

        queued = create_queued_submission(
            context.cfg,
            args,
            context.reaction_dir,
            selected_inp=context.selected_inp,
            deps=deps,
        )
        notify_queued_submission(context.cfg, queued, deps=deps)
    except DuplicateEntryError as exc:
        return DirectQueueSubmission(
            status="failed",
            reason="submission_conflict",
            stderr=str(exc),
            context=context,
        )
    return DirectQueueSubmission(status="submitted", context=context, queued_result=queued)


def cmd_run_inp_submit(
    args: Any,
    *,
    runner_cls: type[Any],
    deps: Any,
    logger: logging.Logger,
) -> int:
    del runner_cls
    submission = deps.submission._submit_reaction_dir_to_queue(args)
    if submission.status != "submitted":
        if submission.stderr:
            logger.error("%s", submission.stderr.rstrip())
        return 1

    result = submission.queued_result
    context = submission.context
    if result is None or context is None:
        logger.error("ORCA queue submission did not return a queued result.")
        return 1
    worker_info = result.worker_info
    deps.submission._emit_queued_submission(
        context.reaction_dir,
        result.entry,
        worker_status=worker_info.status,
        worker_pid=worker_info.pid,
        worker_log=worker_info.log_file,
        worker_detail=worker_info.detail,
    )
    return 0
