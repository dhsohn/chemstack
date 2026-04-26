from __future__ import annotations

import json
import os
import socket
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.config.schema import TelegramConfig
from chemstack.core.notifications import build_telegram_transport
from chemstack.core.utils import atomic_write_json, file_lock, now_utc_iso, timestamped_token

from ._workflow_phases import SUPPRESSED_STAGE_NOTIFICATION_ENGINES, WORKFLOW_PHASE_FINISHED_EVENT
from .state import iter_workflow_workspaces, load_workflow_payload, workflow_summary

WORKFLOW_REGISTRY_FILE_NAME = "workflow_registry.json"
WORKFLOW_REGISTRY_LOCK_NAME = "workflow_registry.lock"
WORKFLOW_JOURNAL_FILE_NAME = "workflow_registry.journal.jsonl"
WORKFLOW_WORKER_STATE_FILE_NAME = "workflow_worker_state.json"
WORKFLOW_REGISTRY_CLEARED_FILE_NAME = "workflow_registry_cleared.json"
_TERMINAL_WORKFLOW_STATUSES = frozenset({"completed", "failed", "cancelled", "cancel_failed", "submission_failed"})
DEFAULT_NOTIFICATION_EVENT_TYPES = frozenset(
    {
        "workflow_status_changed",
        "workflow_advance_failed",
        "workflow_stage_submitted",
        "workflow_stage_completed",
        "workflow_stage_failed",
        "workflow_stage_cancelled",
        "workflow_stage_handoff_ready",
        "workflow_stage_handoff_retrying",
        "workflow_stage_handoff_failed",
        "workflow_stage_status_changed",
        "workflow_stage_reaction_handoff_status_changed",
        WORKFLOW_PHASE_FINISHED_EVENT,
        "worker_started",
        "worker_stopped",
        "worker_interrupted",
        "worker_lock_error",
    }
)
STAGE_STATUS_EVENT_TYPES = frozenset(
    {
        "workflow_stage_submitted",
        "workflow_stage_completed",
        "workflow_stage_failed",
        "workflow_stage_cancelled",
        "workflow_stage_status_changed",
    }
)
STAGE_HANDOFF_EVENT_TYPES = frozenset(
    {
        "workflow_stage_handoff_ready",
        "workflow_stage_handoff_retrying",
        "workflow_stage_handoff_failed",
        "workflow_stage_reaction_handoff_status_changed",
    }
)


@dataclass(frozen=True)
class WorkflowRegistryRecord:
    workflow_id: str
    template_name: str
    status: str
    source_job_id: str
    source_job_type: str
    reaction_key: str
    requested_at: str
    workspace_dir: str
    workflow_file: str
    stage_count: int = 0
    updated_at: str = ""
    stage_status_counts: dict[str, int] = field(default_factory=dict)
    task_status_counts: dict[str, int] = field(default_factory=dict)
    submission_summary: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _event_text(event: dict[str, Any], metadata: dict[str, Any], *keys: str) -> str:
    for key in keys:
        text = _normalize_text(event.get(key))
        if text:
            return text
        text = _normalize_text(metadata.get(key))
        if text:
            return text
    return ""


def _transition_text(previous: str, current: str) -> str:
    previous_text = _normalize_text(previous)
    current_text = _normalize_text(current)
    if previous_text and current_text:
        return f"{previous_text} -> {current_text}"
    if current_text:
        return current_text
    if previous_text:
        return previous_text
    return "-"


def _format_count_mapping(value: Any) -> str:
    mapping = _coerce_mapping(value)
    if not mapping:
        return "-"
    parts: list[str] = []
    for key in sorted(mapping):
        try:
            count = int(mapping[key])
        except (TypeError, ValueError):
            continue
        parts.append(f"{_normalize_text(key)}:{count}")
    return ",".join(parts) if parts else "-"


def _registry_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_REGISTRY_FILE_NAME


def _registry_lock_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_REGISTRY_LOCK_NAME


def workflow_journal_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_JOURNAL_FILE_NAME


def workflow_worker_state_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_WORKER_STATE_FILE_NAME


def _cleared_path(workflow_root: str | Path) -> Path:
    root = Path(workflow_root).expanduser().resolve()
    return root / WORKFLOW_REGISTRY_CLEARED_FILE_NAME


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items()}


def _coerce_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    counts: dict[str, int] = {}
    for key, item in value.items():
        text = _normalize_text(key)
        if not text:
            continue
        try:
            counts[text] = int(item)
        except (TypeError, ValueError):
            continue
    return counts


def _notification_event_types_from_env() -> set[str]:
    raw = os.environ.get("CHEM_FLOW_NOTIFY_EVENT_TYPES", "")
    if not raw.strip():
        return set(DEFAULT_NOTIFICATION_EVENT_TYPES)
    return {item.strip() for item in raw.split(",") if item.strip()}


def _journal_notification_enabled(event_type: str) -> bool:
    disabled = os.environ.get("CHEM_FLOW_NOTIFY_DISABLED", "").strip().lower()
    if disabled in {"1", "true", "yes", "on"}:
        return False
    return event_type in _notification_event_types_from_env()


def _telegram_transport_from_env():
    token = os.environ.get("CHEM_FLOW_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("CHEM_FLOW_TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return None
    return build_telegram_transport(TelegramConfig(bot_token=token, chat_id=chat_id))


def _journal_event_message(event: dict[str, Any], workflow_root: str | Path) -> str:
    event_type = _normalize_text(event.get("event_type"))
    metadata = _coerce_mapping(event.get("metadata"))
    workflow_id = _normalize_text(event.get("workflow_id")) or "-"
    template_name = _normalize_text(event.get("template_name")) or "-"
    status = _normalize_text(event.get("status")) or "-"
    previous_status = _normalize_text(event.get("previous_status")) or "-"
    reason = _normalize_text(event.get("reason")) or "-"
    session = _normalize_text(event.get("worker_session_id")) or "-"
    stage_id = _event_text(event, metadata, "stage_id") or "-"
    engine = _event_text(event, metadata, "engine") or "-"
    task_kind = _event_text(event, metadata, "task_kind") or "-"
    stage_status = _event_text(event, metadata, "stage_status", "status")
    previous_stage_status = _event_text(event, metadata, "previous_stage_status", "previous_status")
    reaction_handoff_status = _event_text(event, metadata, "reaction_handoff_status")
    previous_reaction_handoff_status = _event_text(event, metadata, "previous_reaction_handoff_status")
    root_text = str(Path(workflow_root).expanduser().resolve())

    if event_type == "workflow_status_changed":
        return (
            "[chem_flow]\n"
            f"workflow={workflow_id}\n"
            f"template={template_name}\n"
            f"status={previous_status} -> {status}\n"
            f"worker_session={session}"
        )
    if event_type == "workflow_advance_failed":
        return (
            "[chem_flow]\n"
            f"workflow={workflow_id}\n"
            f"template={template_name}\n"
            f"advance_failed={reason}\n"
            f"worker_session={session}"
        )
    if event_type in STAGE_STATUS_EVENT_TYPES:
        lines = [
            "[chem_flow]",
            f"workflow={workflow_id}",
            f"template={template_name}",
            f"event={event_type}",
            f"stage={stage_id}",
            f"task={engine}/{task_kind}",
            f"stage_status={_transition_text(previous_stage_status, stage_status)}",
            f"worker_session={session}",
        ]
        if reason:
            lines.append(f"reason={reason}")
        return "\n".join(lines)
    if event_type in STAGE_HANDOFF_EVENT_TYPES:
        lines = [
            "[chem_flow]",
            f"workflow={workflow_id}",
            f"template={template_name}",
            f"event={event_type}",
            f"stage={stage_id}",
            f"task={engine}/{task_kind}",
            f"stage_status={_transition_text(previous_stage_status, stage_status)}",
            f"reaction_handoff_status={_transition_text(previous_reaction_handoff_status, reaction_handoff_status)}",
            f"worker_session={session}",
        ]
        if reason:
            lines.append(f"reason={reason}")
        return "\n".join(lines)
    if event_type == WORKFLOW_PHASE_FINISHED_EVENT:
        phase = _event_text(event, metadata, "phase_label", "phase") or "-"
        lines = [
            "[chem_flow]",
            f"workflow={workflow_id}",
            f"template={template_name}",
            f"event={event_type}",
            f"phase={phase}",
            f"phase_outcome={_event_text(event, metadata, 'phase_outcome', 'status') or '-'}",
            f"stage_count={_event_text(event, metadata, 'stage_count') or '0'}",
            f"stage_status_counts={_format_count_mapping(metadata.get('stage_status_counts'))}",
            f"worker_session={session}",
        ]
        handoff_counts = _format_count_mapping(metadata.get("reaction_handoff_status_counts"))
        if handoff_counts != "-":
            lines.append(f"reaction_handoff_status_counts={handoff_counts}")
        failure_reasons = metadata.get("failure_reasons")
        if isinstance(failure_reasons, list):
            joined = ",".join(_normalize_text(item) for item in failure_reasons if _normalize_text(item))
            if joined:
                lines.append(f"failure_reasons={joined}")
        return "\n".join(lines)
    if event_type in {"worker_started", "worker_stopped", "worker_interrupted", "worker_lock_error"}:
        return (
            "[chem_flow]\n"
            f"event={event_type}\n"
            f"workflow_root={root_text}\n"
            f"worker_session={session}\n"
            f"reason={reason}"
        )
    return (
        "[chem_flow]\n"
        f"event={event_type}\n"
        f"workflow={workflow_id}\n"
        f"status={status}\n"
        f"worker_session={session}"
    )


def _maybe_notify_journal_event(event: dict[str, Any], workflow_root: str | Path) -> None:
    event_type = _normalize_text(event.get("event_type"))
    if not _journal_notification_enabled(event_type):
        return
    engine = _event_text(event, _coerce_mapping(event.get("metadata")), "engine")
    if (
        event_type in STAGE_STATUS_EVENT_TYPES | STAGE_HANDOFF_EVENT_TYPES
        and engine.lower() in SUPPRESSED_STAGE_NOTIFICATION_ENGINES
    ):
        return
    transport = _telegram_transport_from_env()
    if transport is None:
        return
    try:
        transport.send_text(_journal_event_message(event, workflow_root))
    except Exception:
        return


def _record_from_summary(summary: dict[str, Any]) -> WorkflowRegistryRecord:
    workspace_dir = _normalize_text(summary.get("workspace_dir"))
    updated_at = _normalize_text(_coerce_mapping(summary.get("submission_summary")).get("updated_at")) or now_utc_iso()
    return WorkflowRegistryRecord(
        workflow_id=_normalize_text(summary.get("workflow_id")),
        template_name=_normalize_text(summary.get("template_name")),
        status=_normalize_text(summary.get("status")),
        source_job_id=_normalize_text(summary.get("source_job_id")),
        source_job_type=_normalize_text(summary.get("source_job_type")),
        reaction_key=_normalize_text(summary.get("reaction_key")),
        requested_at=_normalize_text(summary.get("requested_at")),
        workspace_dir=workspace_dir,
        workflow_file=str(Path(workspace_dir).expanduser().resolve() / "workflow.json") if workspace_dir else "",
        stage_count=int(summary.get("stage_count", 0) or 0),
        updated_at=updated_at,
        stage_status_counts=_coerce_counts(summary.get("stage_status_counts")),
        task_status_counts=_coerce_counts(summary.get("task_status_counts")),
        submission_summary=_coerce_mapping(summary.get("submission_summary")),
        metadata={
            "downstream_reaction_workflow": _coerce_mapping(summary.get("downstream_reaction_workflow")),
            "precomplex_handoff": _coerce_mapping(summary.get("precomplex_handoff")),
            "parent_workflow": _coerce_mapping(summary.get("parent_workflow")),
            "final_child_sync_pending": bool(summary.get("final_child_sync_pending")),
        },
    )


def _record_to_dict(record: WorkflowRegistryRecord) -> dict[str, Any]:
    return asdict(record)


def _record_from_dict(raw: dict[str, Any]) -> WorkflowRegistryRecord:
    return WorkflowRegistryRecord(
        workflow_id=_normalize_text(raw.get("workflow_id")),
        template_name=_normalize_text(raw.get("template_name")),
        status=_normalize_text(raw.get("status")),
        source_job_id=_normalize_text(raw.get("source_job_id")),
        source_job_type=_normalize_text(raw.get("source_job_type")),
        reaction_key=_normalize_text(raw.get("reaction_key")),
        requested_at=_normalize_text(raw.get("requested_at")),
        workspace_dir=_normalize_text(raw.get("workspace_dir")),
        workflow_file=_normalize_text(raw.get("workflow_file")),
        stage_count=int(raw.get("stage_count", 0) or 0),
        updated_at=_normalize_text(raw.get("updated_at")),
        stage_status_counts=_coerce_counts(raw.get("stage_status_counts")),
        task_status_counts=_coerce_counts(raw.get("task_status_counts")),
        submission_summary=_coerce_mapping(raw.get("submission_summary")),
        metadata=_coerce_mapping(raw.get("metadata")),
    )


def _load_records(workflow_root: str | Path) -> list[WorkflowRegistryRecord]:
    path = _registry_path(workflow_root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [_record_from_dict(item) for item in raw if isinstance(item, dict)]


def _save_records(workflow_root: str | Path, records: list[WorkflowRegistryRecord]) -> None:
    atomic_write_json(
        _registry_path(workflow_root),
        [_record_to_dict(record) for record in records],
        ensure_ascii=True,
        indent=2,
    )


def _normal_path_key(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def _cleared_record_keys(record: WorkflowRegistryRecord) -> set[str]:
    keys = {_normalize_text(record.workflow_id)}
    keys.add(_normal_path_key(record.workspace_dir))
    keys.add(_normal_path_key(record.workflow_file))
    return {key for key in keys if key}


def _load_cleared_markers(workflow_root: str | Path) -> list[dict[str, Any]]:
    path = _cleared_path(workflow_root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [_coerce_mapping(item) for item in raw if isinstance(item, dict)]


def _save_cleared_markers(workflow_root: str | Path, markers: list[dict[str, Any]]) -> None:
    atomic_write_json(_cleared_path(workflow_root), markers, ensure_ascii=True, indent=2)


def _marker_keys(marker: dict[str, Any]) -> set[str]:
    keys = {_normalize_text(marker.get("workflow_id"))}
    keys.add(_normal_path_key(_normalize_text(marker.get("workspace_dir"))))
    keys.add(_normal_path_key(_normalize_text(marker.get("workflow_file"))))
    return {key for key in keys if key}


def _cleared_marker_identity(marker: dict[str, Any]) -> str:
    return (
        _normalize_text(marker.get("workflow_id"))
        or _normal_path_key(_normalize_text(marker.get("workspace_dir")))
        or _normal_path_key(_normalize_text(marker.get("workflow_file")))
    )


def _record_clear_identity(record: WorkflowRegistryRecord) -> str:
    return (
        _normalize_text(record.workflow_id)
        or _normal_path_key(record.workspace_dir)
        or _normal_path_key(record.workflow_file)
    )


def _record_matches_cleared_marker(record: WorkflowRegistryRecord, markers: list[dict[str, Any]]) -> bool:
    keys = _cleared_record_keys(record)
    return any(keys & _marker_keys(marker) for marker in markers)


def _record_is_clearable_terminal(record: WorkflowRegistryRecord, statuses: set[str] | frozenset[str] | None = None) -> bool:
    target_statuses = {
        _normalize_text(status).lower()
        for status in (statuses or _TERMINAL_WORKFLOW_STATUSES)
        if _normalize_text(status)
    }
    return _normalize_text(record.status).lower() in target_statuses


def _remove_matching_cleared_markers(
    markers: list[dict[str, Any]],
    record: WorkflowRegistryRecord,
) -> tuple[list[dict[str, Any]], bool]:
    keys = _cleared_record_keys(record)
    kept = [marker for marker in markers if not keys & _marker_keys(marker)]
    return kept, len(kept) != len(markers)


def _add_cleared_markers(
    markers: list[dict[str, Any]],
    records: list[WorkflowRegistryRecord],
    *,
    cleared_at: str,
) -> tuple[list[dict[str, Any]], bool]:
    by_key: dict[str, dict[str, Any]] = {}
    for marker in markers:
        marker_key = _cleared_marker_identity(marker)
        if not marker_key:
            continue
        by_key[marker_key] = marker

    changed = False
    for record in records:
        marker = {
            "workflow_id": _normalize_text(record.workflow_id),
            "status": _normalize_text(record.status).lower(),
            "workspace_dir": _normal_path_key(record.workspace_dir),
            "workflow_file": _normal_path_key(record.workflow_file),
            "cleared_at": cleared_at,
        }
        key = _record_clear_identity(record)
        if not key:
            continue
        if by_key.get(key) != marker:
            changed = True
        by_key[key] = marker
    return list(by_key.values()), changed


def _filter_cleared_terminal_records(
    records: list[WorkflowRegistryRecord],
    markers: list[dict[str, Any]],
) -> list[WorkflowRegistryRecord]:
    return [
        record
        for record in records
        if not (_record_is_clearable_terminal(record) and _record_matches_cleared_marker(record, markers))
    ]


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
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    event = {
        "event_id": timestamped_token("wf_evt"),
        "occurred_at": now_utc_iso(),
        "event_type": _normalize_text(event_type),
        "workflow_id": _normalize_text(workflow_id),
        "template_name": _normalize_text(template_name),
        "status": _normalize_text(status),
        "previous_status": _normalize_text(previous_status),
        "reason": _normalize_text(reason),
        "worker_session_id": _normalize_text(worker_session_id),
        "stage_id": _normalize_text(stage_id),
        "engine": _normalize_text(engine),
        "task_kind": _normalize_text(task_kind),
        "stage_status": _normalize_text(stage_status),
        "previous_stage_status": _normalize_text(previous_stage_status),
        "reaction_handoff_status": _normalize_text(reaction_handoff_status),
        "previous_reaction_handoff_status": _normalize_text(previous_reaction_handoff_status),
        "metadata": _coerce_mapping(metadata),
    }
    with file_lock(_registry_lock_path(resolved_root)):
        path = workflow_journal_path(resolved_root)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=True))
            handle.write("\n")
    _maybe_notify_journal_event(event, resolved_root)
    return event


def list_workflow_journal(workflow_root: str | Path, *, limit: int = 50) -> list[dict[str, Any]]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    path = workflow_journal_path(resolved_root)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with file_lock(_registry_lock_path(resolved_root)):
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                raw = json.loads(text)
            except Exception:
                continue
            if isinstance(raw, dict):
                rows.append({str(key): value for key, value in raw.items()})
    rows.sort(key=lambda item: _normalize_text(item.get("occurred_at")), reverse=True)
    if limit > 0:
        return rows[:limit]
    return rows


def load_workflow_worker_state(workflow_root: str | Path) -> dict[str, Any]:
    path = workflow_worker_state_path(workflow_root)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items()}


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
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "worker_session_id": _normalize_text(worker_session_id),
        "status": _normalize_text(status),
        "workflow_root": str((Path(workflow_root_path).expanduser().resolve() if workflow_root_path else resolved_root)),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "last_heartbeat_at": _normalize_text(last_heartbeat_at) or now_utc_iso(),
        "last_cycle_started_at": _normalize_text(last_cycle_started_at),
        "last_cycle_finished_at": _normalize_text(last_cycle_finished_at),
        "lease_expires_at": _normalize_text(lease_expires_at),
        "interval_seconds": interval_seconds,
        "submit_ready": submit_ready,
        "metadata": _coerce_mapping(metadata),
    }
    with file_lock(_registry_lock_path(resolved_root)):
        atomic_write_json(workflow_worker_state_path(resolved_root), payload, ensure_ascii=True, indent=2)
    return payload


def upsert_workflow_registry_record(workflow_root: str | Path, record: WorkflowRegistryRecord) -> WorkflowRegistryRecord:
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    with file_lock(_registry_lock_path(resolved_root)):
        cleared_markers = _load_cleared_markers(resolved_root)
        is_clearable_terminal = _record_is_clearable_terminal(record)
        matches_cleared_marker = _record_matches_cleared_marker(record, cleared_markers)
        if is_clearable_terminal and matches_cleared_marker:
            return record
        if not is_clearable_terminal and matches_cleared_marker:
            cleared_markers, removed_marker = _remove_matching_cleared_markers(cleared_markers, record)
            if removed_marker:
                _save_cleared_markers(resolved_root, cleared_markers)

        records = _load_records(resolved_root)
        updated = False
        for index, existing in enumerate(records):
            if existing.workflow_id != record.workflow_id:
                continue
            records[index] = record
            updated = True
            break
        if not updated:
            records.append(record)
        records.sort(key=lambda item: (item.requested_at, item.workflow_id), reverse=True)
        _save_records(resolved_root, records)
    return record


def sync_workflow_registry(workflow_root: str | Path, workspace_dir: str | Path, payload: dict[str, Any] | None = None) -> WorkflowRegistryRecord:
    summary = workflow_summary(workspace_dir, payload)
    record = _record_from_summary(summary)
    return upsert_workflow_registry_record(workflow_root, record)


def reindex_workflow_registry(workflow_root: str | Path) -> list[WorkflowRegistryRecord]:
    root = Path(workflow_root).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    records: list[WorkflowRegistryRecord] = []
    for workspace_dir in iter_workflow_workspaces(root):
        try:
            payload = load_workflow_payload(workspace_dir)
            summary = workflow_summary(workspace_dir, payload)
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            continue
        records.append(_record_from_summary(summary))
    records.sort(key=lambda item: (item.requested_at, item.workflow_id), reverse=True)
    with file_lock(_registry_lock_path(root)):
        cleared_markers = _load_cleared_markers(root)
        markers_changed = False
        for record in records:
            if _record_is_clearable_terminal(record) or not _record_matches_cleared_marker(record, cleared_markers):
                continue
            cleared_markers, removed_marker = _remove_matching_cleared_markers(cleared_markers, record)
            markers_changed = markers_changed or removed_marker
        records = _filter_cleared_terminal_records(records, cleared_markers)
        if markers_changed:
            _save_cleared_markers(root, cleared_markers)
        _save_records(root, records)
    return records


def list_workflow_registry(workflow_root: str | Path, *, reindex_if_missing: bool = True) -> list[WorkflowRegistryRecord]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    path = _registry_path(resolved_root)
    with file_lock(_registry_lock_path(resolved_root)):
        if not path.exists():
            records: list[WorkflowRegistryRecord] = []
            loaded_valid_list = False
        else:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                raw = None
            if isinstance(raw, list):
                records = [_record_from_dict(item) for item in raw if isinstance(item, dict)]
                loaded_valid_list = True
            else:
                records = []
                loaded_valid_list = False
    if loaded_valid_list or not reindex_if_missing:
        return records
    return reindex_workflow_registry(resolved_root)


def clear_terminal_workflow_registry(
    workflow_root: str | Path,
    *,
    statuses: set[str] | frozenset[str] | None = None,
) -> int:
    resolved_root = Path(workflow_root).expanduser().resolve()
    resolved_root.mkdir(parents=True, exist_ok=True)
    if not _registry_path(resolved_root).exists():
        reindex_workflow_registry(resolved_root)

    target_statuses = {
        _normalize_text(status).lower()
        for status in (statuses or _TERMINAL_WORKFLOW_STATUSES)
        if _normalize_text(status)
    }
    if not target_statuses:
        return 0

    with file_lock(_registry_lock_path(resolved_root)):
        records = _load_records(resolved_root)
        removed_records = [
            record
            for record in records
            if _normalize_text(record.status).lower() in target_statuses
        ]
        kept_records = [
            record
            for record in records
            if _normalize_text(record.status).lower() not in target_statuses
        ]
        removed_count = len(records) - len(kept_records)
        if removed_count > 0:
            markers, markers_changed = _add_cleared_markers(
                _load_cleared_markers(resolved_root),
                removed_records,
                cleared_at=now_utc_iso(),
            )
            if markers_changed:
                _save_cleared_markers(resolved_root, markers)
            _save_records(resolved_root, kept_records)
        return removed_count


def get_workflow_registry_record(workflow_root: str | Path, workflow_id: str) -> WorkflowRegistryRecord | None:
    target = _normalize_text(workflow_id)
    if not target:
        return None
    for record in list_workflow_registry(workflow_root):
        if record.workflow_id == target:
            return record
    return None


def resolve_workflow_registry_record(workflow_root: str | Path, target: str) -> WorkflowRegistryRecord | None:
    normalized = _normalize_text(target)
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
    "WORKFLOW_REGISTRY_FILE_NAME",
    "WORKFLOW_JOURNAL_FILE_NAME",
    "WORKFLOW_REGISTRY_LOCK_NAME",
    "WORKFLOW_WORKER_STATE_FILE_NAME",
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
]
