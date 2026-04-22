from __future__ import annotations

import importlib
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_CONFIG_ENV_VAR,
    CHEMSTACK_ORCA_SOURCE,
    CHEMSTACK_REPO_ROOT_ENV_VAR,
    LEGACY_ORCA_REPO_ROOT_ENV_VAR,
    LEGACY_ORCA_SOURCE,
)
from chemstack.core.config.files import default_config_path_from_repo_root, shared_workflow_root_from_config
from chemstack.core.queue import clear_terminal as clear_queue_terminal, list_queue
from chemstack.core.queue.types import QueueEntry

from .registry import clear_terminal_workflow_registry, list_workflow_registry, reindex_workflow_registry
from .state import iter_workflow_runtime_workspaces, iter_workflow_workspaces, list_workflow_summaries, workflow_workspace_internal_engine_paths
from .submitters.common import normalize_text, sibling_runtime_paths
from .submitters.crest_auto import cancel_target as cancel_crest_target
from .submitters.orca_auto import cancel_target as cancel_orca_target
from .submitters.xtb_auto import cancel_target as cancel_xtb_target

_WORKFLOW_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled"})
_ACTIVITY_CLEARABLE_TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "cancel_failed"})
_ORCA_ACTIVE_QUEUE_STATUSES = frozenset({"pending", "running"})
def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_existing_path(path_text: str) -> Path | None:
    text = normalize_text(path_text)
    if not text:
        return None
    try:
        candidate = Path(text).expanduser().resolve()
    except OSError:
        return None
    return candidate if candidate.exists() else None


def _discover_workflow_root(explicit: str | Path | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    return shared_workflow_root_from_config(default_config_path_from_repo_root(_project_root()))


def _discover_sibling_config(explicit: str | None, *, app_name: str) -> str | None:
    del app_name
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())

    env_text = normalize_text(os.getenv(CHEMSTACK_CONFIG_ENV_VAR))
    if env_text:
        return str(Path(env_text).expanduser().resolve())

    project_root = _project_root()
    candidates = [
        project_root / "config" / "chemstack.yaml",
        Path.home() / "chemstack" / "config" / "chemstack.yaml",
    ]
    for candidate in candidates:
        resolved = _resolve_existing_path(str(candidate))
        if resolved is not None:
            return str(resolved)
    return None


def _discover_orca_config(explicit: str | None) -> str | None:
    return _discover_sibling_config(
        explicit,
        app_name="chemstack",
    )


def _shared_config_hint(*configs: str | None) -> str | None:
    for config in configs:
        text = normalize_text(config)
        if text:
            return text
    return None


def _resolve_activity_sources(
    *,
    workflow_root: str | Path | None = None,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
) -> tuple[str | None, str | None, str | None, str | None]:
    shared_config_hint = _shared_config_hint(orca_auto_config, crest_auto_config, xtb_auto_config)
    explicit_workflow_root = normalize_text(workflow_root)
    resolved_workflow_root: str | None
    if explicit_workflow_root:
        resolved_workflow_root = str(Path(explicit_workflow_root).expanduser().resolve())
    elif shared_config_hint:
        resolved_workflow_root = shared_workflow_root_from_config(shared_config_hint)
    else:
        resolved_workflow_root = _discover_workflow_root(None)
    resolved_crest_auto_config = _discover_sibling_config(
        crest_auto_config or shared_config_hint,
        app_name="crest_auto",
    )
    resolved_xtb_auto_config = _discover_sibling_config(
        xtb_auto_config or shared_config_hint,
        app_name="xtb_auto",
    )
    resolved_orca_auto_config = _discover_orca_config(orca_auto_config or shared_config_hint)
    return (
        resolved_workflow_root,
        resolved_crest_auto_config,
        resolved_xtb_auto_config,
        resolved_orca_auto_config,
    )


def _discover_orca_repo_root(explicit: str | None) -> str | None:
    explicit_text = normalize_text(explicit)
    if explicit_text:
        return str(Path(explicit_text).expanduser().resolve())
    for env_var in (CHEMSTACK_REPO_ROOT_ENV_VAR, LEGACY_ORCA_REPO_ROOT_ENV_VAR):
        env_text = normalize_text(os.getenv(env_var))
        if env_text:
            return str(Path(env_text).expanduser().resolve())
    return None


@dataclass(frozen=True)
class ActivityRecord:
    activity_id: str
    kind: str
    engine: str
    status: str
    label: str
    source: str
    submitted_at: str
    updated_at: str
    cancel_target: str
    aliases: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "activity_id": self.activity_id,
            "kind": self.kind,
            "engine": self.engine,
            "status": self.status,
            "label": self.label,
            "source": self.source,
            "submitted_at": self.submitted_at,
            "updated_at": self.updated_at,
            "cancel_target": self.cancel_target,
            "aliases": list(self.aliases),
            "metadata": dict(self.metadata),
        }


def _parse_iso(value: str) -> datetime:
    text = normalize_text(value)
    if not text:
        return datetime.min.replace(tzinfo=timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _sort_key(record: ActivityRecord) -> tuple[datetime, datetime, str]:
    return (
        _parse_iso(record.updated_at),
        _parse_iso(record.submitted_at),
        record.activity_id,
    )


def _unique_texts(values: list[str]) -> tuple[str, ...]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return tuple(ordered)


def _mapping_text(mapping: dict[str, Any], key: str) -> str:
    return normalize_text(mapping.get(key))


def _path_aliases(path_text: str, *, root: Path | None = None) -> tuple[str, ...]:
    text = normalize_text(path_text)
    if not text:
        return ()
    try:
        path = Path(text).expanduser().resolve()
    except OSError:
        return (text,)

    aliases = [str(path), path.name]
    if root is not None:
        try:
            relative = path.relative_to(root)
        except ValueError:
            relative = None
        if relative is not None:
            aliases.extend([str(relative), relative.as_posix()])
    return _unique_texts(aliases)


def _select_current_stage(stage_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    if not stage_summaries:
        return {}
    for stage in stage_summaries:
        stage_status = _mapping_text(stage, "status").lower()
        task_status = _mapping_text(stage, "task_status").lower()
        if stage_status not in _WORKFLOW_TERMINAL_STATUSES or task_status not in _WORKFLOW_TERMINAL_STATUSES:
            return dict(stage)
    return dict(stage_summaries[-1])


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
        current_stage = _select_current_stage(list(summary.get("stage_summaries", [])))
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
                Path(normalize_text(record.workspace_dir)).name if normalize_text(record.workspace_dir) else "",
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
                },
            )
        )
    return rows


def _queue_entry_status(entry: QueueEntry) -> str:
    status = normalize_text(getattr(getattr(entry, "status", None), "value", None)) or normalize_text(getattr(entry, "status", None))
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
        roots: list[Path] = [runtime_paths["allowed_root"]]
        return tuple(roots)

    workflow_root = shared_workflow_root_from_config(config_path)
    if not workflow_root:
        return (runtime_paths["allowed_root"],)

    roots: list[Path] = []

    for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine=engine):
        runtime_root = workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)["allowed_root"]
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
            path_text = normalize_text(metadata.get("job_dir")) or normalize_text(metadata.get("reaction_dir"))
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
            updated_at = normalize_text(entry.finished_at) or normalize_text(entry.started_at) or normalize_text(entry.enqueued_at)
            rows.append(
                ActivityRecord(
                    activity_id=normalize_text(entry.queue_id) or normalize_text(entry.task_id),
                    kind="job",
                    engine=engine,
                    status=_queue_entry_status(entry),
                    label=label,
                    source=app_name,
                    submitted_at=normalize_text(entry.enqueued_at),
                    updated_at=updated_at,
                    cancel_target=normalize_text(entry.queue_id),
                    aliases=aliases,
                    metadata={
                        "queue_id": normalize_text(entry.queue_id),
                        "task_id": normalize_text(entry.task_id),
                        "task_kind": normalize_text(entry.task_kind),
                        "job_dir": path_text,
                        "allowed_root": str(allowed_root),
                        "priority": int(entry.priority),
                    },
                )
            )
    return rows


def _default_repo_root(name: str) -> Path | None:
    project_root = _project_root()
    if name in {"chemstack", "orca_auto", "xtb_auto", "crest_auto", "chem_flow"}:
        return project_root
    candidate = project_root / name
    return candidate if candidate.is_dir() else None


def _ensure_repo_on_syspath(repo_root: str | None, *, fallback_name: str) -> Path | None:
    repo_root_text = normalize_text(repo_root)
    root = Path(repo_root_text).expanduser().resolve() if repo_root_text else _default_repo_root(fallback_name)
    if root is None or not root.is_dir():
        return None
    for candidate in (root, root / "src"):
        if not candidate.is_dir():
            continue
        candidate_text = str(candidate)
        if candidate_text not in sys.path:
            sys.path.insert(0, candidate_text)
    return root


def _import_orca_runtime_modules(repo_root: str | None) -> tuple[Any, Any] | None:
    root = _ensure_repo_on_syspath(repo_root, fallback_name="chemstack")
    if root is None:
        return None
    try:
        queue_store = importlib.import_module("chemstack.orca.queue_store")
        run_snapshot = importlib.import_module("chemstack.orca.run_snapshot")
    except ModuleNotFoundError:
        return None
    return queue_store, run_snapshot


def _orca_snapshot_matches_entry(queue_store: Any, entry: Any, snapshot_by_run_id: dict[str, Any], snapshot_by_dir: dict[str, Any]) -> Any | None:
    run_id = normalize_text(queue_store.queue_entry_run_id(entry))
    if run_id:
        return snapshot_by_run_id.get(run_id)
    if normalize_text(queue_store.queue_entry_status(entry)) not in _ORCA_ACTIVE_QUEUE_STATUSES:
        return None
    reaction_dir = normalize_text(queue_store.queue_entry_reaction_dir(entry))
    if not reaction_dir:
        return None
    try:
        resolved = str(Path(reaction_dir).expanduser().resolve())
    except OSError:
        resolved = reaction_dir
    return snapshot_by_dir.get(resolved)


def _orca_queue_represents_snapshot(queue_store: Any, entry: Any, snapshot: Any) -> bool:
    if snapshot is None:
        return False
    run_id = normalize_text(queue_store.queue_entry_run_id(entry))
    if run_id and run_id == normalize_text(getattr(snapshot, "run_id", "")):
        return True
    if normalize_text(queue_store.queue_entry_status(entry)) not in _ORCA_ACTIVE_QUEUE_STATUSES:
        return False
    reaction_dir = normalize_text(queue_store.queue_entry_reaction_dir(entry))
    try:
        resolved = str(Path(reaction_dir).expanduser().resolve())
    except OSError:
        resolved = reaction_dir
    return resolved == normalize_text(getattr(getattr(snapshot, "reaction_dir", None), "resolve", lambda: getattr(snapshot, "reaction_dir", ""))())


def _fallback_orca_queue_records(*, config_path: str) -> list[ActivityRecord]:
    runtime_paths = sibling_runtime_paths(config_path, engine="orca")
    allowed_root = runtime_paths["allowed_root"]
    queue_path = allowed_root / "queue.json"
    if not queue_path.exists():
        return []

    try:
        raw_entries = json.loads(queue_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw_entries, list):
        return []

    rows: list[ActivityRecord] = []
    for raw in raw_entries:
        if not isinstance(raw, dict):
            continue
        metadata = raw.get("metadata")
        metadata = dict(metadata) if isinstance(metadata, dict) else {}
        reaction_dir = normalize_text(metadata.get("reaction_dir")) or normalize_text(raw.get("reaction_dir"))
        queue_id = normalize_text(raw.get("queue_id"))
        run_id = normalize_text(metadata.get("run_id")) or normalize_text(raw.get("run_id"))
        label = normalize_text(Path(reaction_dir).name if reaction_dir else "") or queue_id or run_id
        aliases = _unique_texts([queue_id, run_id, *list(_path_aliases(reaction_dir, root=allowed_root))])
        status = normalize_text(raw.get("status")).lower() or "unknown"
        if bool(raw.get("cancel_requested")) and status == "running":
            status = "cancel_requested"
        rows.append(
            ActivityRecord(
                activity_id=queue_id or run_id or label,
                kind="job",
                engine="orca",
                status=status,
                label=label,
                source=CHEMSTACK_ORCA_SOURCE,
                submitted_at=normalize_text(raw.get("enqueued_at")),
                updated_at=normalize_text(raw.get("finished_at")) or normalize_text(raw.get("started_at")) or normalize_text(raw.get("enqueued_at")),
                cancel_target=queue_id or run_id or reaction_dir,
                aliases=aliases,
                metadata={
                    "queue_id": queue_id,
                    "run_id": run_id,
                    "reaction_dir": reaction_dir,
                    "allowed_root": str(allowed_root),
                    "priority": raw.get("priority"),
                },
            )
        )
    return rows


def _orca_records(*, config_path: str, repo_root: str | None = None) -> list[ActivityRecord]:
    runtime_paths = sibling_runtime_paths(config_path, engine="orca")
    allowed_root = runtime_paths["allowed_root"]
    modules = _import_orca_runtime_modules(repo_root)
    if modules is None:
        return _fallback_orca_queue_records(config_path=config_path)

    queue_store, run_snapshot = modules
    reconcile = getattr(queue_store, "reconcile_orphaned_running_entries", None)
    if callable(reconcile):
        reconcile(allowed_root)

    queue_entries = list(getattr(queue_store, "list_queue")(allowed_root))
    snapshots = list(getattr(run_snapshot, "collect_run_snapshots")(allowed_root))
    snapshot_by_run_id = {
        normalize_text(getattr(snapshot, "run_id", "")): snapshot
        for snapshot in snapshots
        if normalize_text(getattr(snapshot, "run_id", ""))
    }
    snapshot_by_dir: dict[str, Any] = {}
    for snapshot in snapshots:
        try:
            snapshot_by_dir[str(Path(getattr(snapshot, "reaction_dir")).expanduser().resolve())] = snapshot
        except OSError:
            continue

    represented_snapshot_keys: set[str] = set()
    rows: list[ActivityRecord] = []

    for entry in queue_entries:
        snapshot = _orca_snapshot_matches_entry(queue_store, entry, snapshot_by_run_id, snapshot_by_dir)
        queue_id = normalize_text(queue_store.queue_entry_id(entry))
        task_id = normalize_text(queue_store.queue_entry_task_id(entry))
        run_id = normalize_text(queue_store.queue_entry_run_id(entry))
        reaction_dir = normalize_text(queue_store.queue_entry_reaction_dir(entry))
        status = normalize_text(queue_store.queue_entry_status(entry)) or "unknown"
        if bool(entry.get("cancel_requested")) and status == "running":
            status = "cancel_requested"
        if snapshot is not None and status == "running":
            snapshot_status = normalize_text(getattr(snapshot, "status", ""))
            if snapshot_status and snapshot_status != "running":
                status = snapshot_status
        label = normalize_text(getattr(snapshot, "name", "")) or normalize_text(Path(reaction_dir).name if reaction_dir else "") or queue_id or task_id
        aliases = _unique_texts([queue_id, task_id, run_id, *list(_path_aliases(reaction_dir, root=allowed_root))])
        submitted_at = normalize_text(entry.get("enqueued_at"))
        updated_at = (
            normalize_text(getattr(snapshot, "completed_at", ""))
            or normalize_text(getattr(snapshot, "updated_at", ""))
            or normalize_text(entry.get("finished_at"))
            or normalize_text(entry.get("started_at"))
            or submitted_at
        )
        rows.append(
            ActivityRecord(
                activity_id=queue_id or run_id or task_id or label,
                kind="job",
                engine="orca",
                status=status,
                label=label,
                source=CHEMSTACK_ORCA_SOURCE,
                submitted_at=submitted_at,
                updated_at=updated_at,
                cancel_target=queue_id or run_id or reaction_dir,
                aliases=aliases,
                metadata={
                    "queue_id": queue_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "reaction_dir": reaction_dir,
                    "allowed_root": str(allowed_root),
                    "priority": getattr(queue_store, "queue_entry_priority")(entry),
                },
            )
        )
        if snapshot is not None and _orca_queue_represents_snapshot(queue_store, entry, snapshot):
            represented_snapshot_keys.add(normalize_text(getattr(snapshot, "key", "")))

    for snapshot in snapshots:
        snapshot_key = normalize_text(getattr(snapshot, "key", ""))
        if snapshot_key and snapshot_key in represented_snapshot_keys:
            continue
        reaction_dir_obj = getattr(snapshot, "reaction_dir", None)
        reaction_dir = ""
        if reaction_dir_obj is not None:
            try:
                reaction_dir = str(Path(reaction_dir_obj).expanduser().resolve())
            except OSError:
                reaction_dir = str(reaction_dir_obj)
        run_id = normalize_text(getattr(snapshot, "run_id", ""))
        label = normalize_text(getattr(snapshot, "name", "")) or normalize_text(Path(reaction_dir).name if reaction_dir else "") or run_id
        aliases = _unique_texts([run_id, *list(_path_aliases(reaction_dir, root=allowed_root)), normalize_text(getattr(snapshot, "name", ""))])
        rows.append(
            ActivityRecord(
                activity_id=run_id or label,
                kind="job",
                engine="orca",
                status=normalize_text(getattr(snapshot, "status", "")) or "unknown",
                label=label,
                source=CHEMSTACK_ORCA_SOURCE,
                submitted_at=normalize_text(getattr(snapshot, "started_at", "")),
                updated_at=normalize_text(getattr(snapshot, "completed_at", "")) or normalize_text(getattr(snapshot, "updated_at", "")) or normalize_text(getattr(snapshot, "started_at", "")),
                cancel_target=run_id or reaction_dir,
                aliases=aliases,
                metadata={
                    "run_id": run_id,
                    "reaction_dir": reaction_dir,
                    "allowed_root": str(allowed_root),
                    "attempts": getattr(snapshot, "attempts", 0),
                    "selected_inp_name": normalize_text(getattr(snapshot, "selected_inp_name", "")),
                },
            )
        )

    return rows


def _collect_activity_records(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> list[ActivityRecord]:
    (
        resolved_workflow_root,
        resolved_crest_auto_config,
        resolved_xtb_auto_config,
        resolved_orca_auto_config,
    ) = _resolve_activity_sources(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )

    rows: list[ActivityRecord] = []
    if normalize_text(resolved_workflow_root):
        rows.extend(_workflow_records(workflow_root=str(resolved_workflow_root), refresh=refresh))
    if normalize_text(resolved_workflow_root) and normalize_text(resolved_crest_auto_config):
        rows.extend(_standalone_queue_records(app_name="crest_auto", engine="crest", config_path=str(resolved_crest_auto_config)))
    if normalize_text(resolved_workflow_root) and normalize_text(resolved_xtb_auto_config):
        rows.extend(_standalone_queue_records(app_name="xtb_auto", engine="xtb", config_path=str(resolved_xtb_auto_config)))
    if normalize_text(resolved_orca_auto_config):
        rows.extend(_orca_records(config_path=str(resolved_orca_auto_config), repo_root=_discover_orca_repo_root(orca_auto_repo_root)))
    return sorted(rows, key=_sort_key, reverse=True)


def list_activities(
    *,
    workflow_root: str | Path | None = None,
    refresh: bool = False,
    limit: int = 0,
    crest_auto_config: str | None = None,
    xtb_auto_config: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    (
        resolved_workflow_root,
        resolved_crest_auto_config,
        resolved_xtb_auto_config,
        resolved_orca_auto_config,
    ) = _resolve_activity_sources(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )
    records = _collect_activity_records(
        workflow_root=resolved_workflow_root,
        refresh=refresh,
        crest_auto_config=resolved_crest_auto_config,
        xtb_auto_config=resolved_xtb_auto_config,
        orca_auto_config=resolved_orca_auto_config,
        orca_auto_repo_root=orca_auto_repo_root,
    )
    if limit > 0:
        records = records[:limit]
    workflow_root_text = normalize_text(resolved_workflow_root)
    return {
        "count": len(records),
        "activities": [record.to_dict() for record in records],
        "sources": {
            "workflow_root": str(Path(workflow_root_text).expanduser().resolve()) if workflow_root_text else "",
            "crest_auto_config": normalize_text(resolved_crest_auto_config),
            "xtb_auto_config": normalize_text(resolved_xtb_auto_config),
            "orca_auto_config": normalize_text(resolved_orca_auto_config),
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
    (
        resolved_workflow_root,
        resolved_crest_auto_config,
        resolved_xtb_auto_config,
        resolved_orca_auto_config,
    ) = _resolve_activity_sources(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )

    cleared = {
        "workflows": 0,
        "xtb_queue_entries": 0,
        "crest_queue_entries": 0,
        "orca_queue_entries": 0,
        "orca_run_states": 0,
    }

    if normalize_text(resolved_workflow_root):
        cleared["workflows"] = clear_terminal_workflow_registry(
            str(resolved_workflow_root),
            statuses=_ACTIVITY_CLEARABLE_TERMINAL_STATUSES,
        )
    if normalize_text(resolved_xtb_auto_config):
        for allowed_root in _engine_queue_roots(str(resolved_xtb_auto_config), engine="xtb"):
            cleared["xtb_queue_entries"] += clear_queue_terminal(allowed_root)
    if normalize_text(resolved_crest_auto_config):
        for allowed_root in _engine_queue_roots(str(resolved_crest_auto_config), engine="crest"):
            cleared["crest_queue_entries"] += clear_queue_terminal(allowed_root)
    if normalize_text(resolved_orca_auto_config):
        from chemstack.orca.commands.list_runs import clear_terminal_entries as clear_orca_terminal_entries

        allowed_root = sibling_runtime_paths(str(resolved_orca_auto_config), engine="orca")["allowed_root"]
        queue_count, run_count = clear_orca_terminal_entries(allowed_root)
        cleared["orca_queue_entries"] += queue_count
        cleared["orca_run_states"] += run_count

    workflow_root_text = normalize_text(resolved_workflow_root)
    return {
        "total_cleared": sum(int(value) for value in cleared.values()),
        "cleared": cleared,
        "sources": {
            "workflow_root": str(Path(workflow_root_text).expanduser().resolve()) if workflow_root_text else "",
            "crest_auto_config": normalize_text(resolved_crest_auto_config),
            "xtb_auto_config": normalize_text(resolved_xtb_auto_config),
            "orca_auto_config": normalize_text(resolved_orca_auto_config),
        },
    }


def _match_activity_record(records: list[ActivityRecord], target: str) -> ActivityRecord:
    normalized_target = normalize_text(target)
    if not normalized_target:
        raise ValueError("Cancel target is empty.")

    exact_matches = [
        record
        for record in records
        if normalized_target in {record.activity_id, record.cancel_target}
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise ValueError(
            f"Ambiguous activity target: {normalized_target}. Matches: "
            + ", ".join(sorted(record.activity_id for record in exact_matches))
        )

    alias_matches = [
        record
        for record in records
        if normalized_target in set(record.aliases)
    ]
    if len(alias_matches) == 1:
        return alias_matches[0]
    if len(alias_matches) > 1:
        raise ValueError(
            f"Ambiguous activity target: {normalized_target}. Matches: "
            + ", ".join(sorted(record.activity_id for record in alias_matches))
        )
    raise LookupError(f"Activity target not found: {normalized_target}")


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
    (
        resolved_workflow_root,
        resolved_crest_auto_config,
        resolved_xtb_auto_config,
        resolved_orca_auto_config,
    ) = _resolve_activity_sources(
        workflow_root=workflow_root,
        crest_auto_config=crest_auto_config,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )
    record = _match_activity_record(
        _collect_activity_records(
            workflow_root=resolved_workflow_root,
            refresh=False,
            crest_auto_config=resolved_crest_auto_config,
            xtb_auto_config=resolved_xtb_auto_config,
            orca_auto_config=resolved_orca_auto_config,
            orca_auto_repo_root=orca_auto_repo_root,
        ),
        target,
    )

    if record.kind == "workflow":
        from .operations import cancel_workflow

        result = cancel_workflow(
            target=record.cancel_target,
            workflow_root=resolved_workflow_root,
            crest_auto_config=resolved_crest_auto_config,
            crest_auto_executable=crest_auto_executable,
            crest_auto_repo_root=crest_auto_repo_root,
            xtb_auto_config=resolved_xtb_auto_config,
            xtb_auto_executable=xtb_auto_executable,
            xtb_auto_repo_root=xtb_auto_repo_root,
            orca_auto_config=resolved_orca_auto_config,
            orca_auto_executable=orca_auto_executable,
            orca_auto_repo_root=orca_auto_repo_root,
        )
        return {
            "activity_id": record.activity_id,
            "kind": record.kind,
            "engine": record.engine,
            "source": record.source,
            "label": record.label,
            "status": normalize_text(result.get("status")) or "cancelled",
            "cancel_target": record.cancel_target,
            "result": result,
        }

    if record.source == "crest_auto":
        if not normalize_text(resolved_crest_auto_config):
            raise ValueError("crest_auto_config is required to cancel crest_auto activities.")
        result = cancel_crest_target(
            target=record.cancel_target,
            config_path=str(resolved_crest_auto_config),
            executable=crest_auto_executable,
            repo_root=crest_auto_repo_root,
        )
    elif record.source == "xtb_auto":
        if not normalize_text(resolved_xtb_auto_config):
            raise ValueError("xtb_auto_config is required to cancel xtb_auto activities.")
        result = cancel_xtb_target(
            target=record.cancel_target,
            config_path=str(resolved_xtb_auto_config),
            executable=xtb_auto_executable,
            repo_root=xtb_auto_repo_root,
        )
    elif record.source in {CHEMSTACK_ORCA_SOURCE, LEGACY_ORCA_SOURCE}:
        if not normalize_text(resolved_orca_auto_config):
            raise ValueError("chemstack_config is required to cancel chemstack ORCA activities.")
        result = cancel_orca_target(
            target=record.cancel_target,
            config_path=str(resolved_orca_auto_config),
            executable=orca_auto_executable,
            repo_root=_discover_orca_repo_root(orca_auto_repo_root),
        )
    else:
        raise ValueError(f"Unsupported activity source: {record.source}")

    return {
        "activity_id": record.activity_id,
        "kind": record.kind,
        "engine": record.engine,
        "source": record.source,
        "label": record.label,
        "status": normalize_text(result.get("status")) or "failed",
        "cancel_target": record.cancel_target,
        "result": result,
    }


__all__ = [
    "ActivityRecord",
    "cancel_activity",
    "clear_activities",
    "list_activities",
]
