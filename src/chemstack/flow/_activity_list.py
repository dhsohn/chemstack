from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import CHEMSTACK_ORCA_SOURCE
from chemstack.core.queue.types import QueueEntry

from ._activity_deps import ActivityListProvider
from ._activity_model import (
    ActivityListRequest,
    ActivityRecord,
    ActivitySourceRequest,
    ResolvedActivitySources,
)
from .submitters.common import normalize_text


@dataclass(frozen=True)
class ActivityListDeps:
    list_workflow_registry: Callable[..., Any]
    reindex_workflow_registry: Callable[..., Any]
    list_workflow_summaries: Callable[..., Any]
    select_current_stage: Callable[..., Any]
    list_queue: Callable[..., Any]
    shared_workflow_root_from_config: Callable[..., Any]
    iter_workflow_runtime_workspaces: Callable[..., Any]
    workflow_workspace_internal_engine_paths: Callable[..., Any]
    sibling_runtime_paths: Callable[..., Any]
    _coerce_mapping: Callable[..., dict[str, Any]]
    _mapping_text: Callable[..., str]
    _path_aliases: Callable[..., tuple[str, ...]]
    _sort_key: Callable[[ActivityRecord], Any]
    _timestamp_metadata: Callable[..., dict[str, str]]
    _unique_texts: Callable[..., tuple[str, ...]]
    _orca_records: Callable[..., list[ActivityRecord]]
    _resolved_activity_sources_for_request: Callable[
        [ActivitySourceRequest], ResolvedActivitySources
    ]
    _collect_activity_records: Callable[..., list[ActivityRecord]]


def workflow_elapsed_metadata(
    *,
    record_metadata: dict[str, Any],
    summary: dict[str, Any],
    deps: ActivityListDeps,
) -> dict[str, Any]:
    restart_summary = deps._coerce_mapping(record_metadata.get("restart_summary")) or deps._coerce_mapping(
        summary.get("restart_summary")
    )
    last_restarted_at = (
        normalize_text(record_metadata.get("last_restarted_at"))
        or normalize_text(summary.get("last_restarted_at"))
        or normalize_text(restart_summary.get("restarted_at"))
    )
    metadata: dict[str, Any] = {}
    if last_restarted_at:
        metadata["last_restarted_at"] = last_restarted_at
        metadata["elapsed_started_at"] = last_restarted_at
    if restart_summary:
        metadata["restart_summary"] = restart_summary
    return metadata


def workflow_records(
    *,
    workflow_root: str | Path,
    refresh: bool,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    root = Path(workflow_root).expanduser().resolve()
    registry_records = deps.reindex_workflow_registry(root) if refresh else deps.list_workflow_registry(root)
    summary_by_id = {
        normalize_text(summary.get("workflow_id")): summary
        for summary in deps.list_workflow_summaries(root)
        if normalize_text(summary.get("workflow_id"))
    }

    rows: list[ActivityRecord] = []
    for record in registry_records:
        workflow_id = normalize_text(record.workflow_id)
        summary = summary_by_id.get(workflow_id, {})
        record_metadata = deps._coerce_mapping(getattr(record, "metadata", {}))
        current_stage = deps.select_current_stage(summary.get("stage_summaries") or [])
        current_engine = deps._mapping_text(current_stage, "engine") or "workflow"
        current_stage_id = deps._mapping_text(current_stage, "stage_id")
        label = (
            deps._mapping_text(current_stage, "reaction_dir")
            or normalize_text(record.reaction_key)
            or normalize_text(record.source_job_id)
            or normalize_text(record.template_name)
            or workflow_id
        )
        aliases = deps._unique_texts(
            [
                workflow_id,
                normalize_text(record.workspace_dir),
                normalize_text(record.workflow_file),
                Path(normalize_text(record.workspace_dir)).name
                if normalize_text(record.workspace_dir)
                else "",
            ]
        )
        rows.append(
            ActivityRecord(
                activity_id=workflow_id,
                kind="workflow",
                engine="workflow",
                status=normalize_text(record.status) or "unknown",
                label=label,
                source="chemstack_flow",
                submitted_at=normalize_text(record.requested_at),
                updated_at=normalize_text(record.updated_at) or normalize_text(record.requested_at),
                cancel_target=workflow_id,
                aliases=aliases,
                metadata={
                    "template_name": normalize_text(record.template_name),
                    "request_parameters": deps._coerce_mapping(
                        summary.get("request_parameters")
                    ),
                    "workspace_dir": normalize_text(record.workspace_dir),
                    "workflow_file": normalize_text(record.workflow_file),
                    "stage_count": int(record.stage_count),
                    "reaction_key": normalize_text(record.reaction_key),
                    "source_job_id": normalize_text(record.source_job_id),
                    "source_job_type": normalize_text(record.source_job_type),
                    "current_engine": current_engine,
                    "current_stage_id": current_stage_id,
                    "current_stage_status": deps._mapping_text(current_stage, "status"),
                    "current_task_status": deps._mapping_text(current_stage, "task_status"),
                    **workflow_elapsed_metadata(
                        record_metadata=record_metadata,
                        summary=summary,
                        deps=deps,
                    ),
                },
            )
        )
    return rows


def queue_entry_status(entry: QueueEntry) -> str:
    status = normalize_text(
        getattr(getattr(entry, "status", None), "value", None)
    ) or normalize_text(getattr(entry, "status", None))
    status = status or "unknown"
    if getattr(entry, "cancel_requested", False) and status == "running":
        return "cancel_requested"
    return status


def runtime_paths_for_engine(
    config_path: str,
    *,
    engine: str,
    deps: ActivityListDeps,
) -> dict[str, Path]:
    return deps.sibling_runtime_paths(config_path, engine=engine)


def engine_queue_roots(
    config_path: str,
    *,
    engine: str,
    deps: ActivityListDeps,
) -> tuple[Path, ...]:
    runtime_paths = runtime_paths_for_engine(config_path, engine=engine, deps=deps)
    if engine not in {"xtb", "crest"}:
        engine_roots: list[Path] = [runtime_paths["allowed_root"]]
        return tuple(engine_roots)

    workflow_root = deps.shared_workflow_root_from_config(config_path)
    if not workflow_root:
        return (runtime_paths["allowed_root"],)

    roots: list[Path] = []

    for workspace_dir in deps.iter_workflow_runtime_workspaces(workflow_root, engine=engine):
        runtime_root = deps.workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)[
            "allowed_root"
        ]
        if runtime_root not in roots:
            roots.append(runtime_root)
    return tuple(roots)


def standalone_queue_records(
    *,
    app_name: str,
    engine: str,
    config_path: str,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    rows: list[ActivityRecord] = []
    for allowed_root in engine_queue_roots(config_path, engine=engine, deps=deps):
        for entry in deps.list_queue(allowed_root):
            metadata = dict(entry.metadata)
            workflow_id = normalize_text(metadata.get("workflow_id"))
            path_text = normalize_text(metadata.get("job_dir")) or normalize_text(
                metadata.get("reaction_dir")
            )
            label = (
                normalize_text(metadata.get("reaction_key"))
                or normalize_text(metadata.get("molecule_key"))
                or normalize_text(Path(path_text).name if path_text else "")
                or normalize_text(entry.task_id)
                or normalize_text(entry.queue_id)
            )
            aliases = deps._unique_texts(
                [
                    normalize_text(entry.queue_id),
                    normalize_text(entry.task_id),
                    *list(deps._path_aliases(path_text, root=allowed_root)),
                ]
            )
            enqueued_at = normalize_text(entry.enqueued_at)
            started_at = normalize_text(entry.started_at)
            finished_at = normalize_text(entry.finished_at)
            updated_at = finished_at or started_at or enqueued_at
            rows.append(
                ActivityRecord(
                    activity_id=normalize_text(entry.queue_id) or normalize_text(entry.task_id),
                    kind="job",
                    engine=engine,
                    status=queue_entry_status(entry),
                    label=label,
                    source=app_name,
                    submitted_at=enqueued_at,
                    updated_at=updated_at,
                    cancel_target=normalize_text(entry.queue_id),
                    aliases=aliases,
                    metadata={
                        "queue_id": normalize_text(entry.queue_id),
                        "task_id": normalize_text(entry.task_id),
                        "task_kind": normalize_text(entry.task_kind),
                        "mode": normalize_text(metadata.get("mode")),
                        "job_type": normalize_text(metadata.get("job_type")),
                        "workflow_id": workflow_id,
                        "job_dir": path_text,
                        "allowed_root": str(allowed_root),
                        "priority": int(entry.priority),
                        **deps._timestamp_metadata(
                            enqueued_at=enqueued_at,
                            started_at=started_at,
                            finished_at=finished_at,
                        ),
                    },
                )
            )
    return rows


def requested_child_engines(request: ActivityListRequest) -> tuple[bool, set[str]]:
    include_children = {
        normalize_text(engine).lower()
        for engine in (request.child_job_engines or ())
        if normalize_text(engine)
    }
    return request.child_job_engines is None, include_children


def collect_workflow_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    if not normalize_text(resolved.workflow_root):
        return []
    return workflow_records(
        workflow_root=str(resolved.workflow_root),
        refresh=request.refresh,
        deps=deps,
    )


def collect_child_queue_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    app_name: str,
    engine: str,
    config_path: str | None,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    include_all_children, include_children = requested_child_engines(request)
    if (
        not normalize_text(resolved.workflow_root)
        or not normalize_text(config_path)
        or (not include_all_children and engine not in include_children)
    ):
        return []
    return standalone_queue_records(
        app_name=app_name,
        engine=engine,
        config_path=str(config_path),
        deps=deps,
    )


def collect_crest_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    return collect_child_queue_activity(
        resolved,
        request,
        app_name="chemstack_crest",
        engine="crest",
        config_path=resolved.crest_config,
        deps=deps,
    )


def collect_xtb_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    return collect_child_queue_activity(
        resolved,
        request,
        app_name="chemstack_xtb",
        engine="xtb",
        config_path=resolved.xtb_config,
        deps=deps,
    )


def collect_orca_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    if not normalize_text(resolved.orca_config):
        return []
    return deps._orca_records(
        config_path=str(resolved.orca_config),
        repo_root=request.sources.orca_repo_root,
    )


def activity_list_providers(deps: ActivityListDeps) -> tuple[ActivityListProvider, ...]:
    return (
        ActivityListProvider(
            "chemstack_flow",
            lambda resolved, request: collect_workflow_activity(
                resolved,
                request,
                deps=deps,
            ),
        ),
        ActivityListProvider(
            "chemstack_crest",
            lambda resolved, request: collect_crest_activity(
                resolved,
                request,
                deps=deps,
            ),
        ),
        ActivityListProvider(
            "chemstack_xtb",
            lambda resolved, request: collect_xtb_activity(
                resolved,
                request,
                deps=deps,
            ),
        ),
        ActivityListProvider(
            CHEMSTACK_ORCA_SOURCE,
            lambda resolved, request: collect_orca_activity(
                resolved,
                request,
                deps=deps,
            ),
        ),
    )


def collect_activity_records_from_request(
    request: ActivityListRequest,
    *,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    resolved = deps._resolved_activity_sources_for_request(request.sources)
    rows: list[ActivityRecord] = []
    for provider in activity_list_providers(deps):
        rows.extend(provider.collect(resolved, request))
    return sorted(rows, key=deps._sort_key, reverse=True)


def collect_activity_records(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    child_job_engines: tuple[str, ...] | None = None,
    deps: ActivityListDeps,
) -> list[ActivityRecord]:
    return collect_activity_records_from_request(
        ActivityListRequest(
            sources=ActivitySourceRequest(
                workflow_root=workflow_root,
                crest_config=crest_config,
                xtb_config=xtb_config,
                orca_config=orca_config,
                orca_repo_root=orca_repo_root,
            ),
            refresh=refresh,
            child_job_engines=child_job_engines,
        ),
        deps=deps,
    )


def list_activities(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    limit: int = 0,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    child_job_engines: tuple[str, ...] | None = None,
    deps: ActivityListDeps,
) -> dict[str, Any]:
    request = ActivityListRequest(
        sources=ActivitySourceRequest(
            workflow_root=workflow_root,
            crest_config=crest_config,
            xtb_config=xtb_config,
            orca_config=orca_config,
            orca_repo_root=orca_repo_root,
        ),
        refresh=refresh,
        limit=limit,
        child_job_engines=child_job_engines,
    )
    resolved = deps._resolved_activity_sources_for_request(request.sources)
    records = deps._collect_activity_records(
        workflow_root=resolved.workflow_root,
        refresh=request.refresh,
        crest_config=resolved.crest_config,
        xtb_config=resolved.xtb_config,
        orca_config=resolved.orca_config,
        orca_repo_root=request.sources.orca_repo_root,
        child_job_engines=request.child_job_engines,
    )
    if request.limit > 0:
        records = records[: request.limit]
    workflow_root_text = normalize_text(resolved.workflow_root)
    return {
        "count": len(records),
        "activities": [record.to_dict() for record in records],
        "sources": {
            "workflow_root": str(Path(workflow_root_text).expanduser().resolve())
            if workflow_root_text
            else "",
            "crest_config": normalize_text(resolved.crest_config),
            "xtb_config": normalize_text(resolved.xtb_config),
            "orca_config": normalize_text(resolved.orca_config),
        },
    }
