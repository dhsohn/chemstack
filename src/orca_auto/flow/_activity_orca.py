from __future__ import annotations

from pathlib import Path
from typing import Any

from orca_auto.core.app_ids import ORCA_AUTO_ORCA_SOURCE
from orca_auto.core.utils import normalize_text

from ._activity_model import ActivityRecord

_ORCA_ACTIVE_QUEUE_STATUSES = frozenset({"pending", "running"})


def snapshot_matches_entry(
    queue_adapter: Any,
    entry: Any,
    snapshot_by_run_id: dict[str, Any],
    snapshot_by_dir: dict[str, Any],
) -> Any | None:
    run_id = normalize_text(queue_adapter.queue_entry_run_id(entry))
    if run_id:
        return snapshot_by_run_id.get(run_id)
    if normalize_text(queue_adapter.queue_entry_status(entry)) not in _ORCA_ACTIVE_QUEUE_STATUSES:
        return None
    reaction_dir = normalize_text(queue_adapter.queue_entry_reaction_dir(entry))
    if not reaction_dir:
        return None
    try:
        resolved = str(Path(reaction_dir).expanduser().resolve())
    except OSError:
        resolved = reaction_dir
    return snapshot_by_dir.get(resolved)


def queue_represents_snapshot(queue_adapter: Any, entry: Any, snapshot: Any) -> bool:
    if snapshot is None:
        return False
    run_id = normalize_text(queue_adapter.queue_entry_run_id(entry))
    if run_id and run_id == normalize_text(getattr(snapshot, "run_id", "")):
        return True
    if normalize_text(queue_adapter.queue_entry_status(entry)) not in _ORCA_ACTIVE_QUEUE_STATUSES:
        return False
    reaction_dir = normalize_text(queue_adapter.queue_entry_reaction_dir(entry))
    try:
        resolved = str(Path(reaction_dir).expanduser().resolve())
    except OSError:
        resolved = reaction_dir
    return resolved == normalize_text(
        getattr(
            getattr(snapshot, "reaction_dir", None),
            "resolve",
            lambda: getattr(snapshot, "reaction_dir", ""),
        )()
    )


def snapshot_indexes(snapshots: list[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    snapshot_by_run_id = {
        normalize_text(getattr(snapshot, "run_id", "")): snapshot
        for snapshot in snapshots
        if normalize_text(getattr(snapshot, "run_id", ""))
    }
    snapshot_by_dir: dict[str, Any] = {}
    for snapshot in snapshots:
        try:
            snapshot_by_dir[str(Path(snapshot.reaction_dir).expanduser().resolve())] = (
                snapshot
            )
        except OSError:
            continue
    return snapshot_by_run_id, snapshot_by_dir


def queue_entry_status(queue_adapter: Any, entry: Any, snapshot: Any) -> str:
    status = normalize_text(queue_adapter.queue_entry_status(entry)) or "unknown"
    if bool(getattr(entry, "cancel_requested", False)) and status == "running":
        return "cancel_requested"
    if snapshot is None or status != "running":
        return status
    snapshot_status = normalize_text(getattr(snapshot, "status", ""))
    return snapshot_status if snapshot_status and snapshot_status != "running" else status


def queue_record(
    queue_adapter: Any,
    entry: Any,
    snapshot: Any,
    *,
    allowed_root: Path,
    deps: Any,
) -> ActivityRecord:
    entry_metadata_loader = getattr(queue_adapter, "queue_entry_metadata", None)
    entry_metadata = dict(entry_metadata_loader(entry)) if callable(entry_metadata_loader) else {}
    queue_id = normalize_text(queue_adapter.queue_entry_id(entry))
    task_id = normalize_text(queue_adapter.queue_entry_task_id(entry))
    run_id = normalize_text(queue_adapter.queue_entry_run_id(entry))
    reaction_dir = normalize_text(queue_adapter.queue_entry_reaction_dir(entry))
    label = (
        normalize_text(getattr(snapshot, "name", ""))
        or normalize_text(Path(reaction_dir).name if reaction_dir else "")
        or queue_id
        or task_id
    )
    submitted_at = normalize_text(getattr(entry, "enqueued_at", ""))
    started_at = normalize_text(getattr(entry, "started_at", ""))
    finished_at = normalize_text(getattr(entry, "finished_at", ""))
    updated_at = (
        normalize_text(getattr(snapshot, "completed_at", ""))
        or normalize_text(getattr(snapshot, "updated_at", ""))
        or finished_at
        or started_at
        or submitted_at
    )
    return ActivityRecord(
        activity_id=queue_id or run_id or task_id or label,
        kind="job",
        engine="orca",
        status=queue_entry_status(queue_adapter, entry, snapshot),
        label=label,
        source=ORCA_AUTO_ORCA_SOURCE,
        submitted_at=submitted_at,
        updated_at=updated_at,
        cancel_target=queue_id or run_id or reaction_dir,
        aliases=deps._unique_texts(
            [queue_id, task_id, run_id, *list(deps._path_aliases(reaction_dir, root=allowed_root))]
        ),
        metadata={
            "queue_id": queue_id,
            "task_id": task_id,
            "task_kind": normalize_text(getattr(entry, "task_kind", "")),
            "run_id": run_id,
            "job_type": normalize_text(entry_metadata.get("job_type")),
            "selected_inp": normalize_text(entry_metadata.get("selected_inp")),
            "workflow_id": normalize_text(entry_metadata.get("workflow_id")),
            "reaction_dir": reaction_dir,
            "allowed_root": str(allowed_root),
            "priority": queue_adapter.queue_entry_priority(entry),
            **deps._timestamp_metadata(
                enqueued_at=submitted_at, started_at=started_at, finished_at=finished_at
            ),
        },
    )


def snapshot_reaction_dir(snapshot: Any) -> str:
    reaction_dir_obj = getattr(snapshot, "reaction_dir", None)
    if reaction_dir_obj is None:
        return ""
    try:
        return str(Path(reaction_dir_obj).expanduser().resolve())
    except OSError:
        return str(reaction_dir_obj)


def snapshot_record(snapshot: Any, *, allowed_root: Path, deps: Any) -> ActivityRecord:
    reaction_dir = snapshot_reaction_dir(snapshot)
    run_id = normalize_text(getattr(snapshot, "run_id", ""))
    label = (
        normalize_text(getattr(snapshot, "name", ""))
        or normalize_text(Path(reaction_dir).name if reaction_dir else "")
        or run_id
    )
    started_at = normalize_text(getattr(snapshot, "started_at", ""))
    completed_at = normalize_text(getattr(snapshot, "completed_at", ""))
    return ActivityRecord(
        activity_id=run_id or label,
        kind="job",
        engine="orca",
        status=normalize_text(getattr(snapshot, "status", "")) or "unknown",
        label=label,
        source=ORCA_AUTO_ORCA_SOURCE,
        submitted_at=started_at,
        updated_at=completed_at
        or normalize_text(getattr(snapshot, "updated_at", ""))
        or started_at,
        cancel_target=run_id or reaction_dir,
        aliases=deps._unique_texts(
            [
                run_id,
                *list(deps._path_aliases(reaction_dir, root=allowed_root)),
                normalize_text(getattr(snapshot, "name", "")),
            ]
        ),
        metadata={
            "run_id": run_id,
            "reaction_dir": reaction_dir,
            "allowed_root": str(allowed_root),
            "attempts": getattr(snapshot, "attempts", 0),
            "selected_inp_name": normalize_text(getattr(snapshot, "selected_inp_name", "")),
            "job_type": normalize_text(getattr(snapshot, "job_type", "")),
            **deps._timestamp_metadata(started_at=started_at, finished_at=completed_at),
        },
    )


def orca_records(
    *,
    config_path: str,
    deps: Any,
) -> list[ActivityRecord]:
    from orca_auto.orca import queue_adapter, run_snapshot

    runtime_paths = deps.engine_runtime_paths(config_path, engine="orca")
    allowed_root = runtime_paths["allowed_root"]
    reconcile = getattr(queue_adapter, "reconcile_orphaned_running_entries", None)
    if callable(reconcile):
        reconcile(allowed_root)

    queue_entries = list(queue_adapter.list_queue(allowed_root))
    snapshots = list(run_snapshot.collect_run_snapshots(allowed_root))
    snapshot_by_run_id, snapshot_by_dir = snapshot_indexes(snapshots)
    represented_snapshot_keys: set[str] = set()
    rows: list[ActivityRecord] = []

    for entry in queue_entries:
        snapshot = snapshot_matches_entry(
            queue_adapter, entry, snapshot_by_run_id, snapshot_by_dir
        )
        rows.append(queue_record(queue_adapter, entry, snapshot, allowed_root=allowed_root, deps=deps))
        if snapshot is not None and queue_represents_snapshot(queue_adapter, entry, snapshot):
            represented_snapshot_keys.add(normalize_text(getattr(snapshot, "key", "")))

    for snapshot in snapshots:
        snapshot_key = normalize_text(getattr(snapshot, "key", ""))
        if not snapshot_key or snapshot_key not in represented_snapshot_keys:
            rows.append(snapshot_record(snapshot, allowed_root=allowed_root, deps=deps))

    return rows
