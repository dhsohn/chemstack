from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..types import QueueEnqueuedNotification


def build_queue_enqueued_notification(entry: Any, *, deps: Any) -> QueueEnqueuedNotification:
    return {
        "queue_id": deps._queue_store.queue_entry_id(entry),
        "reaction_dir": deps._queue_store.queue_entry_reaction_dir(entry),
        "priority": deps._queue_store.queue_entry_priority(entry),
        "force": deps._queue_store.queue_entry_force(entry),
        "enqueued_at": entry.get("enqueued_at", ""),
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
    resource_request, actions = deps.ensure_submission_resource_request(
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
    requested = deps._resource_request_from_selected_inp(cfg, selected_inp)
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
        requested = deps.read_resource_request_from_input(selected_inp)
    if not requested and selected_inp is not None and selected_inp.exists():
        requested = deps._resource_request_from_selected_inp(cfg, selected_inp)
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


def submit_as_queued(
    cfg: Any,
    args: Any,
    reaction_dir: Path,
    *,
    selected_inp: Path | None = None,
    deps: Any,
    logger: logging.Logger,
) -> int:
    from ..queue_store import DuplicateEntryError, enqueue

    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    if selected_inp is None:
        try:
            selected_inp = deps._select_latest_inp(reaction_dir)
        except ValueError:
            selected_inp = None
    deps._warn_ignored_resource_override_flags(args)
    queue_metadata = deps._build_queue_metadata(
        cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        args=args,
    )
    try:
        entry = enqueue(
            allowed_root,
            str(reaction_dir),
            priority=int(getattr(args, "priority", 10)),
            force=bool(getattr(args, "force", False)),
            metadata=queue_metadata,
        )
    except DuplicateEntryError as exc:
        logger.error("%s", exc)
        return 1

    task_id = deps._queue_store.queue_entry_task_id(entry)
    if task_id:
        deps._upsert_queued_job_record(
            cfg,
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            job_id=task_id,
            queue_metadata=queue_metadata,
        )

    worker_info = deps._worker_status_for_submission(allowed_root)
    deps._emit_queued_submission(
        reaction_dir,
        entry,
        worker_status=worker_info.status,
        worker_pid=worker_info.pid,
        worker_log=worker_info.log_file,
        worker_detail=worker_info.detail,
    )
    notification = deps._build_queue_enqueued_notification(entry)
    deps.notify_queue_enqueued_event(cfg.telegram, notification)
    return 0
