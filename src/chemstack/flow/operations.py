from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE

from .activity import (
    cancel_activity as cancel_activity_impl,
    clear_activities as clear_activities_impl,
    list_activities as list_activities_impl,
)
from .orchestration import (
    advance_workflow,
    cancel_materialized_workflow,
    create_conformer_screening_workflow as create_conformer_screening_workflow_impl,
    create_reaction_ts_search_workflow,
)
from .registry import (
    list_workflow_journal,
    load_workflow_worker_state,
    list_workflow_registry,
    reindex_workflow_registry,
    resolve_workflow_registry_record,
    sync_workflow_registry,
    workflow_journal_path,
    workflow_worker_state_path,
)
from .state import load_workflow_payload, resolve_workflow_workspace, workflow_artifacts, workflow_summary


def list_workflows(*, workflow_root: str | Path, limit: int = 0, refresh: bool = False) -> dict[str, Any]:
    records = reindex_workflow_registry(workflow_root) if refresh else list_workflow_registry(workflow_root)
    resolved_root = Path(workflow_root).expanduser().resolve()
    payloads = [
        {
            "workflow_id": record.workflow_id,
            "template_name": record.template_name,
            "status": record.status,
            "source_job_id": record.source_job_id,
            "source_job_type": record.source_job_type,
            "reaction_key": record.reaction_key,
            "requested_at": record.requested_at,
            "workspace_dir": record.workspace_dir,
            "workflow_file": record.workflow_file,
            "stage_count": record.stage_count,
            "updated_at": record.updated_at,
            "stage_status_counts": dict(record.stage_status_counts),
            "task_status_counts": dict(record.task_status_counts),
            "submission_summary": dict(record.submission_summary),
            "metadata": dict(record.metadata),
        }
        for record in records
    ]
    if limit > 0:
        payloads = payloads[:limit]
    return {
        "workflow_root": str(resolved_root),
        "worker_state_file": str(workflow_worker_state_path(resolved_root)),
        "journal_file": str(workflow_journal_path(resolved_root)),
        "worker_state": load_workflow_worker_state(resolved_root),
        "count": len(payloads),
        "workflows": payloads,
    }


def get_workflow(*, target: str, workflow_root: str | Path | None = None, sync_registry: bool = True) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    summary = workflow_summary(workspace_dir, payload)

    record_payload: dict[str, Any] = {}
    if workflow_root is not None and sync_registry:
        synced_record = sync_workflow_registry(workflow_root, workspace_dir, payload)
        record_payload = {
            "workflow_id": synced_record.workflow_id,
            "template_name": synced_record.template_name,
            "status": synced_record.status,
            "workspace_dir": synced_record.workspace_dir,
            "workflow_file": synced_record.workflow_file,
            "stage_count": synced_record.stage_count,
            "updated_at": synced_record.updated_at,
            "stage_status_counts": dict(synced_record.stage_status_counts),
            "task_status_counts": dict(synced_record.task_status_counts),
            "submission_summary": dict(synced_record.submission_summary),
            "metadata": dict(synced_record.metadata),
        }
    elif workflow_root is not None:
        resolved_record = resolve_workflow_registry_record(workflow_root, target)
        if resolved_record is not None:
            record_payload = {
                "workflow_id": resolved_record.workflow_id,
                "template_name": resolved_record.template_name,
                "status": resolved_record.status,
                "workspace_dir": resolved_record.workspace_dir,
                "workflow_file": resolved_record.workflow_file,
                "stage_count": resolved_record.stage_count,
                "updated_at": resolved_record.updated_at,
                "stage_status_counts": dict(resolved_record.stage_status_counts),
                "task_status_counts": dict(resolved_record.task_status_counts),
                "submission_summary": dict(resolved_record.submission_summary),
                "metadata": dict(resolved_record.metadata),
            }

    return {
        "summary": summary,
        "registry_record": record_payload,
        "worker_state": load_workflow_worker_state(workflow_root) if workflow_root is not None else {},
        "workflow": payload,
    }


def get_workflow_artifacts(*, target: str, workflow_root: str | Path | None = None, sync_registry: bool = True) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    summary = workflow_summary(workspace_dir, payload)
    if workflow_root is not None and sync_registry:
        sync_workflow_registry(workflow_root, workspace_dir, payload)
    artifacts = workflow_artifacts(workspace_dir, payload)
    return {
        "workflow_id": summary.get("workflow_id", ""),
        "workspace_dir": summary.get("workspace_dir", ""),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
    }


def get_workflow_runtime_status(*, workflow_root: str | Path) -> dict[str, Any]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    return {
        "workflow_root": str(resolved_root),
        "worker_state_file": str(workflow_worker_state_path(resolved_root)),
        "journal_file": str(workflow_journal_path(resolved_root)),
        "worker_state": load_workflow_worker_state(resolved_root),
    }


def get_workflow_journal(*, workflow_root: str | Path, limit: int = 50) -> dict[str, Any]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    events = list_workflow_journal(resolved_root, limit=int(limit or 0))
    return {
        "workflow_root": str(resolved_root),
        "journal_file": str(workflow_journal_path(resolved_root)),
        "count": len(events),
        "events": events,
    }


def get_workflow_telemetry(*, workflow_root: str | Path, limit: int = 200) -> dict[str, Any]:
    resolved_root = Path(workflow_root).expanduser().resolve()
    records = list_workflow_registry(resolved_root)
    worker_state = load_workflow_worker_state(resolved_root)
    events = list_workflow_journal(resolved_root, limit=int(limit or 0))

    workflow_status_counts: dict[str, int] = {}
    template_counts: dict[str, int] = {}
    for record in records:
        workflow_status_counts[record.status] = workflow_status_counts.get(record.status, 0) + 1
        template_counts[record.template_name] = template_counts.get(record.template_name, 0) + 1

    event_type_counts: dict[str, int] = {}
    recent_failures: list[dict[str, Any]] = []
    recent_status_changes: list[dict[str, Any]] = []
    for event in events:
        event_type = str(event.get("event_type", "")).strip()
        if event_type:
            event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
        if event_type == "workflow_advance_failed" and len(recent_failures) < 5:
            recent_failures.append(event)
        if event_type == "workflow_status_changed" and len(recent_status_changes) < 5:
            recent_status_changes.append(event)

    return {
        "workflow_root": str(resolved_root),
        "registry_count": len(records),
        "worker_state": worker_state,
        "workflow_status_counts": workflow_status_counts,
        "template_counts": template_counts,
        "journal_event_count": len(events),
        "journal_event_type_counts": event_type_counts,
        "recent_failures": recent_failures,
        "recent_status_changes": recent_status_changes,
        "journal_file": str(workflow_journal_path(resolved_root)),
        "worker_state_file": str(workflow_worker_state_path(resolved_root)),
    }


def cancel_workflow(
    *,
    target: str,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    crest_auto_executable: str = "crest_auto",
    crest_auto_repo_root: str | None = None,
    xtb_auto_config: str | None = None,
    xtb_auto_executable: str = "xtb_auto",
    xtb_auto_repo_root: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    return cancel_materialized_workflow(
        target=target,
        workflow_root=workflow_root or "",
        crest_auto_config=crest_auto_config,
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        xtb_auto_config=xtb_auto_config,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )


def list_activities(
    *,
    workflow_root: str | Path | None = None,
    limit: int = 0,
    refresh: bool = False,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    return list_activities_impl(
        workflow_root=workflow_root,
        limit=limit,
        refresh=refresh,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
    )


def clear_activities(
    *,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    return clear_activities_impl(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
    )


def cancel_activity(
    *,
    target: str,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    crest_auto_executable: str = "crest_auto",
    crest_auto_repo_root: str | None = None,
    xtb_auto_config: str | None = None,
    xtb_auto_executable: str = "xtb_auto",
    xtb_auto_repo_root: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    return cancel_activity_impl(
        target=target,
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        xtb_auto_config=xtb_auto_config,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )


def create_reaction_workflow(*, reactant_xyz: str, product_xyz: str, workflow_root: str | Path, **kwargs: Any) -> dict[str, Any]:
    return create_reaction_ts_search_workflow(
        reactant_xyz=reactant_xyz,
        product_xyz=product_xyz,
        workflow_root=workflow_root,
        **kwargs,
    )


def create_conformer_screening_workflow(*, input_xyz: str, workflow_root: str | Path, **kwargs: Any) -> dict[str, Any]:
    return create_conformer_screening_workflow_impl(
        input_xyz=input_xyz,
        workflow_root=workflow_root,
        **kwargs,
    )

def advance_materialized_workflow(
    *,
    target: str,
    workflow_root: str | Path,
    **kwargs: Any,
) -> dict[str, Any]:
    return advance_workflow(target=target, workflow_root=workflow_root, **kwargs)


__all__ = [
    "advance_materialized_workflow",
    "cancel_activity",
    "cancel_workflow",
    "create_conformer_screening_workflow",
    "create_reaction_workflow",
    "get_workflow_journal",
    "get_workflow_runtime_status",
    "get_workflow_telemetry",
    "get_workflow",
    "get_workflow_artifacts",
    "list_activities",
    "list_workflows",
]
