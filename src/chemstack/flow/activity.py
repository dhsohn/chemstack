from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_ORCA_SOURCE,
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
    path_aliases as _path_aliases,
    sort_key as _sort_key,
    timestamp_metadata as _timestamp_metadata,
    unique_texts as _unique_texts,
)
from ._activity_deps import (
    ActivityCancelDeps as _ActivityCancelDeps,
    ActivityListProvider as _ActivityListProvider,
    ActivitySourceDeps as _ActivitySourceDeps,
    OrcaActivityDeps as _OrcaActivityDeps,
)
from .state import (
    iter_workflow_runtime_workspaces,
    list_workflow_summaries,
    workflow_workspace_internal_engine_paths,
)
from .submitters.common import sibling_runtime_paths
from .submitters.crest_auto import cancel_target as cancel_crest_target
from .submitters.orca_auto import cancel_target as cancel_orca_target
from .submitters.xtb_auto import cancel_target as cancel_xtb_target
from .workflow_status import WORKFLOW_TERMINAL_STATUSES, select_current_stage
from . import _activity_clear
from . import _activity_list
from . import _activity_orca
from . import _activity_sources
from . import _activity_cancel

_ACTIVITY_CLEARABLE_TERMINAL_STATUSES = WORKFLOW_TERMINAL_STATUSES
_ActivityClearDeps = _activity_clear.ActivityClearDeps
_ActivityListDeps = _activity_list.ActivityListDeps
_ActivityCancelProvider = _activity_cancel.ActivityCancelProvider
_EngineQueueClearProvider = _activity_clear.EngineQueueClearProvider


def _activity_source_deps() -> _ActivitySourceDeps:
    return _ActivitySourceDeps(
        _project_root=_project_root,
        _resolve_existing_path=_resolve_existing_path,
        _discover_workflow_root=_discover_workflow_root,
        _discover_sibling_config=_discover_sibling_config,
        _discover_orca_config=_discover_orca_config,
        _shared_config_hint=_shared_config_hint,
    )


def _orca_activity_deps() -> _OrcaActivityDeps:
    return _OrcaActivityDeps(
        sibling_runtime_paths=sibling_runtime_paths,
        _unique_texts=_unique_texts,
        _path_aliases=_path_aliases,
        _timestamp_metadata=_timestamp_metadata,
    )


def _activity_cancel_deps() -> _ActivityCancelDeps:
    return _ActivityCancelDeps(
        cancel_crest_target=cancel_crest_target,
        cancel_xtb_target=cancel_xtb_target,
        cancel_orca_target=cancel_orca_target,
        _discover_orca_repo_root=_discover_orca_repo_root,
    )


def _activity_list_deps() -> _ActivityListDeps:
    return _ActivityListDeps(
        list_workflow_registry=list_workflow_registry,
        reindex_workflow_registry=reindex_workflow_registry,
        list_workflow_summaries=list_workflow_summaries,
        select_current_stage=select_current_stage,
        list_queue=list_queue,
        shared_workflow_root_from_config=shared_workflow_root_from_config,
        iter_workflow_runtime_workspaces=iter_workflow_runtime_workspaces,
        workflow_workspace_internal_engine_paths=workflow_workspace_internal_engine_paths,
        sibling_runtime_paths=sibling_runtime_paths,
        _coerce_mapping=_coerce_mapping,
        _mapping_text=_mapping_text,
        _path_aliases=_path_aliases,
        _sort_key=_sort_key,
        _timestamp_metadata=_timestamp_metadata,
        _unique_texts=_unique_texts,
        _orca_records=_orca_records,
        _resolved_activity_sources_for_request=_resolved_activity_sources_for_request,
        _collect_activity_records=_collect_activity_records,
    )


def _activity_clear_deps() -> _ActivityClearDeps:
    return _ActivityClearDeps(
        _resolved_activity_sources_for_request=_resolved_activity_sources_for_request,
        clear_terminal_workflow_registry=clear_terminal_workflow_registry,
        clear_queue_terminal=clear_queue_terminal,
        _engine_queue_roots=_engine_queue_roots,
        sibling_runtime_paths=sibling_runtime_paths,
    )


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _activity_sources.coerce_mapping(value)


def _project_root() -> Path:
    return _activity_sources.project_root()


def _resolve_existing_path(path_text: str) -> Path | None:
    return _activity_sources.resolve_existing_path(path_text)


def _discover_workflow_root(explicit: str | Path | None) -> str | None:
    return _activity_sources.discover_workflow_root(explicit, deps=_activity_source_deps())


def _discover_sibling_config(explicit: str | None, *, app_name: str) -> str | None:
    return _activity_sources.discover_sibling_config(
        explicit,
        app_name=app_name,
        deps=_activity_source_deps(),
    )


def _discover_orca_config(explicit: str | None) -> str | None:
    return _activity_sources.discover_orca_config(explicit, deps=_activity_source_deps())


def _shared_config_hint(*configs: str | None) -> str | None:
    return _activity_sources.shared_config_hint(*configs)


def _resolve_activity_source_request(request: ActivitySourceRequest) -> ResolvedActivitySources:
    return _activity_sources.resolve_activity_source_request(
        request,
        deps=_activity_source_deps(),
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
    return _activity_list.workflow_elapsed_metadata(
        record_metadata=record_metadata,
        summary=summary,
        deps=_activity_list_deps(),
    )


def _workflow_records(*, workflow_root: str | Path, refresh: bool) -> list[ActivityRecord]:
    return _activity_list.workflow_records(
        workflow_root=workflow_root,
        refresh=refresh,
        deps=_activity_list_deps(),
    )


def _queue_entry_status(entry: QueueEntry) -> str:
    return _activity_list.queue_entry_status(entry)


def _runtime_paths_for_engine(config_path: str, *, engine: str) -> dict[str, Path]:
    return _activity_list.runtime_paths_for_engine(
        config_path,
        engine=engine,
        deps=_activity_list_deps(),
    )


def _engine_queue_roots(config_path: str, *, engine: str) -> tuple[Path, ...]:
    return _activity_list.engine_queue_roots(
        config_path,
        engine=engine,
        deps=_activity_list_deps(),
    )


def _standalone_queue_records(
    *,
    app_name: str,
    engine: str,
    config_path: str,
) -> list[ActivityRecord]:
    return _activity_list.standalone_queue_records(
        app_name=app_name,
        engine=engine,
        config_path=config_path,
        deps=_activity_list_deps(),
    )


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
        deps=_orca_activity_deps(),
    )


def _orca_snapshot_reaction_dir(snapshot: Any) -> str:
    return _activity_orca.snapshot_reaction_dir(snapshot)


def _orca_snapshot_record(snapshot: Any, *, allowed_root: Path) -> ActivityRecord:
    return _activity_orca.snapshot_record(
        snapshot,
        allowed_root=allowed_root,
        deps=_orca_activity_deps(),
    )


def _orca_records(*, config_path: str, repo_root: str | None = None) -> list[ActivityRecord]:
    return _activity_orca.orca_records(
        config_path=config_path,
        repo_root=repo_root,
        deps=_orca_activity_deps(),
    )


def _requested_child_engines(request: ActivityListRequest) -> tuple[bool, set[str]]:
    return _activity_list.requested_child_engines(request)


def _collect_workflow_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _activity_list.collect_workflow_activity(
        resolved,
        request,
        deps=_activity_list_deps(),
    )


def _collect_child_queue_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
    *,
    app_name: str,
    engine: str,
    config_path: str | None,
) -> list[ActivityRecord]:
    return _activity_list.collect_child_queue_activity(
        resolved,
        request,
        app_name=app_name,
        engine=engine,
        config_path=config_path,
        deps=_activity_list_deps(),
    )


def _collect_crest_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _activity_list.collect_crest_activity(
        resolved,
        request,
        deps=_activity_list_deps(),
    )


def _collect_xtb_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _activity_list.collect_xtb_activity(
        resolved,
        request,
        deps=_activity_list_deps(),
    )


def _collect_orca_activity(
    resolved: ResolvedActivitySources,
    request: ActivityListRequest,
) -> list[ActivityRecord]:
    return _activity_list.collect_orca_activity(
        resolved,
        request,
        deps=_activity_list_deps(),
    )


def _activity_list_providers() -> tuple[_ActivityListProvider, ...]:
    return _activity_list.activity_list_providers(_activity_list_deps())


def _engine_queue_clear_providers() -> tuple[_EngineQueueClearProvider, ...]:
    return _activity_clear.engine_queue_clear_providers(_activity_clear_deps())


def _collect_activity_records_from_request(request: ActivityListRequest) -> list[ActivityRecord]:
    return _activity_list.collect_activity_records_from_request(
        request,
        deps=_activity_list_deps(),
    )


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
    return _activity_list.collect_activity_records(
        workflow_root=workflow_root,
        refresh=refresh,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
        child_job_engines=child_job_engines,
        deps=_activity_list_deps(),
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
    return _activity_list.list_activities(
        workflow_root=workflow_root,
        refresh=refresh,
        limit=limit,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
        child_job_engines=child_job_engines,
        deps=_activity_list_deps(),
    )


def clear_activities(
    *,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    return _activity_clear.clear_activities(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
        clearable_terminal_statuses=_ACTIVITY_CLEARABLE_TERMINAL_STATUSES,
        deps=_activity_clear_deps(),
    )


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
        deps=_activity_cancel_deps(),
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
        deps=_activity_cancel_deps(),
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
        deps=_activity_cancel_deps(),
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
