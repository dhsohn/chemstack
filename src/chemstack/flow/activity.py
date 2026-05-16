from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_ORCA_SOURCE,
    CHEMSTACK_REPO_ROOT_ENV_VAR,
)
from chemstack.core.config.files import (
    shared_workflow_root_from_config,
)
from chemstack.core.queue import clear_terminal as clear_queue_terminal, list_queue
from chemstack.core.queue.types import QueueEntry

from .registry import (
    clear_terminal_workflow_registry,
    list_workflow_registry,
    reindex_workflow_registry,
)
from ._activity_model import (
    ActivityCancelRequest,
    ActivityListRequest,
    ActivityRecord,
    ActivitySourceRequest,
    ResolvedActivitySources,
    mapping_text as _mapping_text,
    parse_iso as _parse_iso,
    path_aliases as _path_aliases,
    sort_key as _sort_key,
    timestamp_metadata as _timestamp_metadata,
    unique_texts as _unique_texts,
)
from .state import (
    iter_workflow_runtime_workspaces,
    list_workflow_summaries,
    workflow_workspace_internal_engine_paths,
)
from .submitters.common import normalize_text, sibling_runtime_paths
from .submitters.crest_auto import cancel_target as cancel_crest_target
from .submitters.orca_auto import cancel_target as cancel_orca_target
from .submitters.xtb_auto import cancel_target as cancel_xtb_target
from .workflow_status import WORKFLOW_TERMINAL_STATUSES, select_current_stage
from . import _activity_orca
from . import _activity_sources
from . import _activity_cancel

_ACTIVITY_CLEARABLE_TERMINAL_STATUSES = WORKFLOW_TERMINAL_STATUSES
_ACTIVITY_MODEL_COMPAT = (
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_REPO_ROOT_ENV_VAR,
    cancel_crest_target,
    cancel_orca_target,
    cancel_xtb_target,
    _parse_iso,
)


def _this_module() -> Any:
    return sys.modules[__name__]


@dataclass(frozen=True)
class _ActivityListProvider:
    source: str
    collect: Callable[[ResolvedActivitySources, ActivityListRequest], list[ActivityRecord]]


_ActivityCancelProvider = _activity_cancel.ActivityCancelProvider


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _activity_sources.coerce_mapping(value)


def _project_root() -> Path:
    return _activity_sources.project_root()


def _resolve_existing_path(path_text: str) -> Path | None:
    return _activity_sources.resolve_existing_path(path_text)


def _discover_workflow_root(explicit: str | Path | None) -> str | None:
    return _activity_sources.discover_workflow_root(explicit, deps=_this_module())


def _discover_sibling_config(explicit: str | None, *, app_name: str) -> str | None:
    return _activity_sources.discover_sibling_config(
        explicit,
        app_name=app_name,
        deps=_this_module(),
    )


def _discover_orca_config(explicit: str | None) -> str | None:
    return _activity_sources.discover_orca_config(explicit, deps=_this_module())


def _shared_config_hint(*configs: str | None) -> str | None:
    return _activity_sources.shared_config_hint(*configs)


def _resolve_activity_source_request(request: ActivitySourceRequest) -> ResolvedActivitySources:
    return _activity_sources.resolve_activity_source_request(
        request,
        deps=_this_module(),
    )


def _resolve_activity_sources(
    *,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
) -> tuple[str | None, str | None, str | None, str | None]:
    return _resolve_activity_source_request(
        ActivitySourceRequest(
            workflow_root=workflow_root,
            crest_auto_config=crest_auto_config,
            xtb_auto_config=xtb_auto_config,
            orca_auto_config=orca_auto_config,
        )
    ).as_tuple()


def _resolved_activity_sources_for_request(
    request: ActivitySourceRequest,
) -> ResolvedActivitySources:
    return ResolvedActivitySources.from_tuple(
        _resolve_activity_sources(
            workflow_root=request.workflow_root,
            crest_auto_config=request.crest_auto_config,
            xtb_auto_config=request.xtb_auto_config,
            orca_auto_config=request.orca_auto_config,
        )
    )


def _discover_orca_repo_root(explicit: str | None) -> str | None:
    return _activity_sources.discover_orca_repo_root(explicit)


def _workflow_elapsed_metadata(
    *,
    record_metadata: dict[str, Any],
    summary: dict[str, Any],
) -> dict[str, Any]:
    restart_summary = _coerce_mapping(record_metadata.get("restart_summary")) or _coerce_mapping(
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


def _workflow_records(*, workflow_root: str | Path, refresh: bool) -> list[ActivityRecord]:
    root = Path(workflow_root).expanduser().resolve()
    registry_records = reindex_workflow_registry(root) if refresh else list_workflow_registry(root)
    summary_by_id = {
        normalize_text(summary.get("workflow_id")): summary
        for summary in list_workflow_summaries(root)
        if normalize_text(summary.get("workflow_id"))
    }

    rows: list[ActivityRecord] = []
    for record in registry_records:
        workflow_id = normalize_text(record.workflow_id)
        summary = summary_by_id.get(workflow_id, {})
        record_metadata = _coerce_mapping(getattr(record, "metadata", {}))
        current_stage = select_current_stage(summary.get("stage_summaries") or [])
        current_engine = _mapping_text(current_stage, "engine") or "workflow"
        current_stage_id = _mapping_text(current_stage, "stage_id")
        label = (
            _mapping_text(current_stage, "reaction_dir")
            or normalize_text(record.reaction_key)
            or normalize_text(record.source_job_id)
            or normalize_text(record.template_name)
            or workflow_id
        )
        aliases = _unique_texts(
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
                source="chem_flow",
                submitted_at=normalize_text(record.requested_at),
                updated_at=normalize_text(record.updated_at) or normalize_text(record.requested_at),
                cancel_target=workflow_id,
                aliases=aliases,
                metadata={
                    "template_name": normalize_text(record.template_name),
                    "request_parameters": _coerce_mapping(summary.get("request_parameters")),
                    "workspace_dir": normalize_text(record.workspace_dir),
                    "workflow_file": normalize_text(record.workflow_file),
                    "stage_count": int(record.stage_count),
                    "reaction_key": normalize_text(record.reaction_key),
                    "source_job_id": normalize_text(record.source_job_id),
                    "source_job_type": normalize_text(record.source_job_type),
                    "current_engine": current_engine,
                    "current_stage_id": current_stage_id,
                    "current_stage_status": _mapping_text(current_stage, "status"),
                    "current_task_status": _mapping_text(current_stage, "task_status"),
                    **_workflow_elapsed_metadata(record_metadata=record_metadata, summary=summary),
                },
            )
        )
    return rows


def _queue_entry_status(entry: QueueEntry) -> str:
    status = normalize_text(
        getattr(getattr(entry, "status", None), "value", None)
    ) or normalize_text(getattr(entry, "status", None))
    status = status or "unknown"
    if getattr(entry, "cancel_requested", False) and status == "running":
        return "cancel_requested"
    return status


def _runtime_paths_for_engine(config_path: str, *, engine: str) -> dict[str, Path]:
    try:
        return sibling_runtime_paths(config_path, engine=engine)
    except TypeError as exc:
        if "engine" not in str(exc):
            raise
        return sibling_runtime_paths(config_path)


def _engine_queue_roots(config_path: str, *, engine: str) -> tuple[Path, ...]:
    runtime_paths = _runtime_paths_for_engine(config_path, engine=engine)
    if engine not in {"xtb", "crest"}:
        engine_roots: list[Path] = [runtime_paths["allowed_root"]]
        return tuple(engine_roots)

    workflow_root = shared_workflow_root_from_config(config_path)
    if not workflow_root:
        return (runtime_paths["allowed_root"],)

    roots: list[Path] = []

    for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine=engine):
        runtime_root = workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)[
            "allowed_root"
        ]
        if runtime_root not in roots:
            roots.append(runtime_root)
    return tuple(roots)


def _standalone_queue_records(
    *,
    app_name: str,
    engine: str,
    config_path: str,
) -> list[ActivityRecord]:
    rows: list[ActivityRecord] = []
    for allowed_root in _engine_queue_roots(config_path, engine=engine):
        for entry in list_queue(allowed_root):
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
            aliases = _unique_texts(
                [
                    normalize_text(entry.queue_id),
                    normalize_text(entry.task_id),
                    *list(_path_aliases(path_text, root=allowed_root)),
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
                    status=_queue_entry_status(entry),
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
                        **_timestamp_metadata(
                            enqueued_at=enqueued_at,
                            started_at=started_at,
                            finished_at=finished_at,
                        ),
                    },
                )
            )
    return rows


def _orca_snapshot_matches_entry(
    queue_store: Any,
    entry: Any,
    snapshot_by_run_id: dict[str, Any],
    snapshot_by_dir: dict[str, Any],
) -> Any | None:
    return _activity_orca.snapshot_matches_entry(
        queue_store,
        entry,
        snapshot_by_run_id,
        snapshot_by_dir,
    )


def _orca_queue_represents_snapshot(queue_store: Any, entry: Any, snapshot: Any) -> bool:
    return _activity_orca.queue_represents_snapshot(queue_store, entry, snapshot)


def _orca_snapshot_indexes(snapshots: list[Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    return _activity_orca.snapshot_indexes(snapshots)


def _orca_queue_entry_status(queue_store: Any, entry: Any, snapshot: Any) -> str:
    return _activity_orca.queue_entry_status(queue_store, entry, snapshot)


def _orca_queue_record(
    queue_store: Any, entry: Any, snapshot: Any, *, allowed_root: Path
) -> ActivityRecord:
    return _activity_orca.queue_record(
        queue_store,
        entry,
        snapshot,
        allowed_root=allowed_root,
        deps=_this_module(),
    )


def _orca_snapshot_reaction_dir(snapshot: Any) -> str:
    return _activity_orca.snapshot_reaction_dir(snapshot)


def _orca_snapshot_record(snapshot: Any, *, allowed_root: Path) -> ActivityRecord:
    return _activity_orca.snapshot_record(
        snapshot,
        allowed_root=allowed_root,
        deps=_this_module(),
    )


def _orca_records(*, config_path: str, repo_root: str | None = None) -> list[ActivityRecord]:
    return _activity_orca.orca_records(
        config_path=config_path,
        repo_root=repo_root,
        deps=_this_module(),
    )


def _requested_child_engines(request: ActivityListRequest) -> tuple[bool, set[str]]:
    include_children = {
        normalize_text(engine).lower()
        for engine in (request.child_job_engines or ())
        if normalize_text(engine)
    }
    return request.child_job_engines is None, include_children


def _collect_workflow_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    if not normalize_text(resolved.workflow_root):
        return []
    return _workflow_records(
        workflow_root=str(resolved.workflow_root),
        refresh=request.refresh,
    )


def _collect_child_queue_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    app_name: str,
    engine: str,
    config_path: str | None,
) -> list[ActivityRecord]:
    include_all_children, include_children = _requested_child_engines(request)
    if (
        not normalize_text(resolved.workflow_root)
        or not normalize_text(config_path)
        or (not include_all_children and engine not in include_children)
    ):
        return []
    return _standalone_queue_records(
        app_name=app_name,
        engine=engine,
        config_path=str(config_path),
    )


def _collect_crest_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _collect_child_queue_activity(
        resolved,
        request,
        app_name="crest_auto",
        engine="crest",
        config_path=resolved.crest_auto_config,
    )


def _collect_xtb_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _collect_child_queue_activity(
        resolved,
        request,
        app_name="xtb_auto",
        engine="xtb",
        config_path=resolved.xtb_auto_config,
    )


def _collect_orca_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    if not normalize_text(resolved.orca_auto_config):
        return []
    return _orca_records(
        config_path=str(resolved.orca_auto_config),
        repo_root=request.sources.orca_auto_repo_root,
    )


def _activity_list_providers() -> tuple[_ActivityListProvider, ...]:
    return (
        _ActivityListProvider("chem_flow", _collect_workflow_activity),
        _ActivityListProvider("crest_auto", _collect_crest_activity),
        _ActivityListProvider("xtb_auto", _collect_xtb_activity),
        _ActivityListProvider(CHEMSTACK_ORCA_SOURCE, _collect_orca_activity),
    )


def _collect_activity_records_from_request(request: ActivityListRequest) -> list[ActivityRecord]:
    resolved = _resolved_activity_sources_for_request(request.sources)
    rows: list[ActivityRecord] = []
    for provider in _activity_list_providers():
        rows.extend(provider.collect(resolved, request))
    return sorted(rows, key=_sort_key, reverse=True)


def _collect_activity_records(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
    child_job_engines: tuple[str, ...] | None = None,
) -> list[ActivityRecord]:
    return _collect_activity_records_from_request(
        ActivityListRequest(
            sources=ActivitySourceRequest(
                workflow_root=workflow_root,
                crest_auto_config=crest_auto_config,
                xtb_auto_config=xtb_auto_config,
                orca_auto_config=orca_auto_config,
                orca_auto_repo_root=orca_auto_repo_root,
            ),
            refresh=refresh,
            child_job_engines=child_job_engines,
        )
    )


def list_activities(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    limit: int = 0,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
    child_job_engines: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    request = ActivityListRequest(
        sources=ActivitySourceRequest(
            workflow_root=workflow_root,
            crest_auto_config=crest_auto_config,
            xtb_auto_config=xtb_auto_config,
            orca_auto_config=orca_auto_config,
            orca_auto_repo_root=orca_auto_repo_root,
        ),
        refresh=refresh,
        limit=limit,
        child_job_engines=child_job_engines,
    )
    resolved = _resolved_activity_sources_for_request(request.sources)
    records = _collect_activity_records(
        workflow_root=resolved.workflow_root,
        refresh=request.refresh,
        crest_auto_config=resolved.crest_auto_config,
        xtb_auto_config=resolved.xtb_auto_config,
        orca_auto_config=resolved.orca_auto_config,
        orca_auto_repo_root=request.sources.orca_auto_repo_root,
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
            "crest_auto_config": normalize_text(resolved.crest_auto_config),
            "xtb_auto_config": normalize_text(resolved.xtb_auto_config),
            "orca_auto_config": normalize_text(resolved.orca_auto_config),
        },
    }


def clear_activities(
    *,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    del orca_auto_repo_root
    source_request = ActivitySourceRequest(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )
    resolved = _resolved_activity_sources_for_request(source_request)

    cleared = {
        "workflows": 0,
        "xtb_queue_entries": 0,
        "crest_queue_entries": 0,
        "orca_queue_entries": 0,
        "orca_run_states": 0,
    }

    if normalize_text(resolved.workflow_root):
        cleared["workflows"] = clear_terminal_workflow_registry(
            str(resolved.workflow_root),
            statuses=_ACTIVITY_CLEARABLE_TERMINAL_STATUSES,
        )
    if normalize_text(resolved.xtb_auto_config):
        for allowed_root in _engine_queue_roots(str(resolved.xtb_auto_config), engine="xtb"):
            cleared["xtb_queue_entries"] += clear_queue_terminal(allowed_root)
    if normalize_text(resolved.crest_auto_config):
        for allowed_root in _engine_queue_roots(str(resolved.crest_auto_config), engine="crest"):
            cleared["crest_queue_entries"] += clear_queue_terminal(allowed_root)
    if normalize_text(resolved.orca_auto_config):
        from chemstack.orca.commands.list_runs import (
            clear_terminal_entries as clear_orca_terminal_entries,
        )

        allowed_root = sibling_runtime_paths(str(resolved.orca_auto_config), engine="orca")[
            "allowed_root"
        ]
        queue_count, run_count = clear_orca_terminal_entries(allowed_root)
        cleared["orca_queue_entries"] += queue_count
        cleared["orca_run_states"] += run_count

    workflow_root_text = normalize_text(resolved.workflow_root)
    return {
        "total_cleared": sum(int(value) for value in cleared.values()),
        "cleared": cleared,
        "sources": {
            "workflow_root": str(Path(workflow_root_text).expanduser().resolve())
            if workflow_root_text
            else "",
            "crest_auto_config": normalize_text(resolved.crest_auto_config),
            "xtb_auto_config": normalize_text(resolved.xtb_auto_config),
            "orca_auto_config": normalize_text(resolved.orca_auto_config),
        },
    }


def _match_activity_record(records: list[ActivityRecord], target: str) -> ActivityRecord:
    return _activity_cancel.match_activity_record(records, target)


def _cancel_activity_payload(
    record: ActivityRecord,
    result: dict[str, Any],
    *,
    fallback_status: str,
) -> dict[str, Any]:
    return _activity_cancel.cancel_activity_payload(
        record,
        result,
        fallback_status=fallback_status,
    )


def _cancel_workflow_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    return _activity_cancel.cancel_workflow_activity(record, resolved, request)


def _cancel_crest_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    return _activity_cancel.cancel_crest_activity(
        record,
        resolved,
        request,
        deps=_this_module(),
    )


def _cancel_xtb_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    return _activity_cancel.cancel_xtb_activity(
        record,
        resolved,
        request,
        deps=_this_module(),
    )


def _cancel_orca_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    return _activity_cancel.cancel_orca_activity(
        record,
        resolved,
        request,
        deps=_this_module(),
    )


def _activity_cancel_providers() -> tuple[_ActivityCancelProvider, ...]:
    return (
        _ActivityCancelProvider("crest_auto", _cancel_crest_activity),
        _ActivityCancelProvider("xtb_auto", _cancel_xtb_activity),
        _ActivityCancelProvider(CHEMSTACK_ORCA_SOURCE, _cancel_orca_activity),
    )


def _cancel_non_workflow_activity(
    record: ActivityRecord,
    resolved: ResolvedActivitySources,
    request: ActivityCancelRequest,
) -> dict[str, Any]:
    for provider in _activity_cancel_providers():
        if record.source == provider.source:
            return provider.cancel(record, resolved, request)
    raise ValueError(f"Unsupported activity source: {record.source}")


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
    request = ActivityCancelRequest(
        target=target,
        sources=ActivitySourceRequest(
            workflow_root=workflow_root,
            crest_auto_config=crest_auto_config,
            xtb_auto_config=xtb_auto_config,
            orca_auto_config=orca_auto_config,
            orca_auto_repo_root=orca_auto_repo_root,
        ),
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
    )
    resolved = _resolved_activity_sources_for_request(request.sources)
    record = _match_activity_record(
        _collect_activity_records(
            workflow_root=resolved.workflow_root,
            refresh=False,
            crest_auto_config=resolved.crest_auto_config,
            xtb_auto_config=resolved.xtb_auto_config,
            orca_auto_config=resolved.orca_auto_config,
            orca_auto_repo_root=request.sources.orca_auto_repo_root,
        ),
        request.target,
    )

    if record.kind == "workflow":
        result = _cancel_workflow_activity(record, resolved, request)
        return _cancel_activity_payload(record, result, fallback_status="cancelled")

    result = _cancel_non_workflow_activity(record, resolved, request)
    return _cancel_activity_payload(record, result, fallback_status="failed")


__all__ = [
    "ActivityCancelRequest",
    "ActivityListRequest",
    "ActivityRecord",
    "ActivitySourceRequest",
    "ResolvedActivitySources",
    "cancel_activity",
    "clear_activities",
    "list_activities",
]
