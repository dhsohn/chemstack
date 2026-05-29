from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Any

from chemstack.core.utils import file_lock, now_utc_iso, timestamped_token

from . import registry_store as _store
from . import worker_state_store as _worker_state
from . import workflow_journal as _journal
from .registry_store import (
    WORKFLOW_REGISTRY_CLEARED_FILE_NAME,
    WORKFLOW_REGISTRY_FILE_NAME,
    WORKFLOW_REGISTRY_LOCK_NAME,
    WorkflowRegistryCorruptError,
    WorkflowRegistryRecord,
    _cleared_path,
    _coerce_counts,
    _filter_cleared_terminal_records,
    _load_cleared_markers,
    _read_existing_json,
    _record_from_dict,
    _record_is_clearable_terminal,
    _record_to_dict,
    _registry_lock_path,
    _registry_path,
    _save_cleared_markers,
)
from .state import iter_workflow_workspaces, load_workflow_payload, workflow_summary
from .worker_state_store import WORKFLOW_WORKER_STATE_FILE_NAME, workflow_worker_state_path
from .workflow_journal import WORKFLOW_JOURNAL_FILE_NAME, workflow_journal_path

_TERMINAL_WORKFLOW_STATUSES = _store._TERMINAL_WORKFLOW_STATUSES
_ORIGINAL_MAYBE_NOTIFY_JOURNAL_EVENT = _journal._maybe_notify_journal_event


def _sync_store_dependencies() -> None:
    _store.file_lock = file_lock
    _store.now_utc_iso = now_utc_iso
    _store.iter_workflow_workspaces = iter_workflow_workspaces
    _store.load_workflow_payload = load_workflow_payload
    _store.workflow_summary = workflow_summary


def _sync_journal_dependencies() -> None:
    _journal.file_lock = file_lock
    _journal.now_utc_iso = now_utc_iso
    _journal.timestamped_token = timestamped_token
    _journal._maybe_notify_journal_event = _maybe_notify_journal_event


def _sync_worker_state_dependencies() -> None:
    _worker_state.file_lock = file_lock
    _worker_state.now_utc_iso = now_utc_iso
    _worker_state.os = os
    _worker_state.socket = socket


def _record_from_summary(summary: dict[str, Any]) -> WorkflowRegistryRecord:
    _sync_store_dependencies()
    return _store._record_from_summary(summary)


def _load_records(workflow_root: str | Path) -> list[WorkflowRegistryRecord]:
    _sync_store_dependencies()
    return _store._load_records(workflow_root)


def _save_records(workflow_root: str | Path, records: list[WorkflowRegistryRecord]) -> None:
    _sync_store_dependencies()
    _store._save_records(workflow_root, records)


def _maybe_notify_journal_event(event: dict[str, Any], workflow_root: str | Path) -> None:
    return _ORIGINAL_MAYBE_NOTIFY_JOURNAL_EVENT(event, workflow_root)


def append_workflow_journal_event(
    workflow_root: str | Path,
    *,
    event_type: str,
    workflow_id: str = "",
    template_name: str = "",
    status: str = "",
    previous_status: str = "",
    reason: str = "",
    worker_session_id: str = "",
    stage_id: str = "",
    engine: str = "",
    task_kind: str = "",
    stage_status: str = "",
    previous_stage_status: str = "",
    reaction_handoff_status: str = "",
    previous_reaction_handoff_status: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _sync_journal_dependencies()
    return _journal.append_workflow_journal_event(
        workflow_root,
        event_type=event_type,
        workflow_id=workflow_id,
        template_name=template_name,
        status=status,
        previous_status=previous_status,
        reason=reason,
        worker_session_id=worker_session_id,
        stage_id=stage_id,
        engine=engine,
        task_kind=task_kind,
        stage_status=stage_status,
        previous_stage_status=previous_stage_status,
        reaction_handoff_status=reaction_handoff_status,
        previous_reaction_handoff_status=previous_reaction_handoff_status,
        metadata=metadata,
    )


def list_workflow_journal(workflow_root: str | Path, *, limit: int = 50) -> list[dict[str, Any]]:
    _sync_journal_dependencies()
    return _journal.list_workflow_journal(workflow_root, limit=limit)


def load_workflow_worker_state(workflow_root: str | Path) -> dict[str, Any]:
    _sync_worker_state_dependencies()
    return _worker_state.load_workflow_worker_state(workflow_root)


def write_workflow_worker_state(
    workflow_root: str | Path,
    *,
    worker_session_id: str,
    status: str,
    workflow_root_path: str | Path | None = None,
    last_cycle_started_at: str = "",
    last_cycle_finished_at: str = "",
    last_heartbeat_at: str = "",
    lease_expires_at: str = "",
    interval_seconds: float | None = None,
    submit_ready: bool | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    _sync_worker_state_dependencies()
    return _worker_state.write_workflow_worker_state(
        workflow_root,
        worker_session_id=worker_session_id,
        status=status,
        workflow_root_path=workflow_root_path,
        last_cycle_started_at=last_cycle_started_at,
        last_cycle_finished_at=last_cycle_finished_at,
        last_heartbeat_at=last_heartbeat_at,
        lease_expires_at=lease_expires_at,
        interval_seconds=interval_seconds,
        submit_ready=submit_ready,
        metadata=metadata,
    )


def upsert_workflow_registry_record(
    workflow_root: str | Path, record: WorkflowRegistryRecord
) -> WorkflowRegistryRecord:
    _sync_store_dependencies()
    return _store.upsert_workflow_registry_record(workflow_root, record)


def sync_workflow_registry(
    workflow_root: str | Path, workspace_dir: str | Path, payload: dict[str, Any] | None = None
) -> WorkflowRegistryRecord:
    _sync_store_dependencies()
    summary = workflow_summary(workspace_dir, payload)
    record = _record_from_summary(summary)
    return upsert_workflow_registry_record(workflow_root, record)


def reindex_workflow_registry(workflow_root: str | Path) -> list[WorkflowRegistryRecord]:
    _sync_store_dependencies()
    return _store.reindex_workflow_registry(workflow_root)


def list_workflow_registry(
    workflow_root: str | Path, *, reindex_if_missing: bool = True
) -> list[WorkflowRegistryRecord]:
    _sync_store_dependencies()
    return _store.list_workflow_registry(
        workflow_root,
        reindex_if_missing=reindex_if_missing,
        reindex_fn=reindex_workflow_registry,
    )


def clear_terminal_workflow_registry(
    workflow_root: str | Path,
    *,
    statuses: set[str] | frozenset[str] | None = None,
) -> int:
    _sync_store_dependencies()
    return _store.clear_terminal_workflow_registry(
        workflow_root,
        statuses=statuses,
        reindex_fn=reindex_workflow_registry,
    )


def get_workflow_registry_record(
    workflow_root: str | Path, workflow_id: str
) -> WorkflowRegistryRecord | None:
    target = _store._normalize_text(workflow_id)
    if not target:
        return None
    for record in list_workflow_registry(workflow_root):
        if record.workflow_id == target:
            return record
    return None


def resolve_workflow_registry_record(
    workflow_root: str | Path, target: str
) -> WorkflowRegistryRecord | None:
    normalized = _store._normalize_text(target)
    if not normalized:
        return None

    direct: Path | None
    try:
        direct = Path(normalized).expanduser().resolve()
    except OSError:
        direct = None

    for record in list_workflow_registry(workflow_root):
        if record.workflow_id == normalized:
            return record
        if direct is None:
            continue
        if direct == Path(record.workspace_dir).expanduser().resolve():
            return record
        if record.workflow_file and direct == Path(record.workflow_file).expanduser().resolve():
            return record
    return None


__all__ = [
    "WORKFLOW_REGISTRY_CLEARED_FILE_NAME",
    "WORKFLOW_REGISTRY_FILE_NAME",
    "WORKFLOW_JOURNAL_FILE_NAME",
    "WORKFLOW_REGISTRY_LOCK_NAME",
    "WORKFLOW_WORKER_STATE_FILE_NAME",
    "WorkflowRegistryCorruptError",
    "WorkflowRegistryRecord",
    "append_workflow_journal_event",
    "clear_terminal_workflow_registry",
    "get_workflow_registry_record",
    "list_workflow_journal",
    "list_workflow_registry",
    "load_workflow_worker_state",
    "reindex_workflow_registry",
    "resolve_workflow_registry_record",
    "sync_workflow_registry",
    "upsert_workflow_registry_record",
    "workflow_journal_path",
    "workflow_worker_state_path",
    "write_workflow_worker_state",
    "_cleared_path",
    "_coerce_counts",
    "_filter_cleared_terminal_records",
    "_load_cleared_markers",
    "_read_existing_json",
    "_record_from_dict",
    "_record_is_clearable_terminal",
    "_record_to_dict",
    "_registry_lock_path",
    "_registry_path",
    "_save_cleared_markers",
]
