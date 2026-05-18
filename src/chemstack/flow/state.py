from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
from typing import Any

from chemstack.core.paths.workflow import (
    WORKFLOW_ENGINE_STAGE_ALIASES,
    WORKFLOW_FILE_NAME,
    WORKFLOW_STAGE_DIRNAMES,
    iter_workflow_runtime_workspaces,
    workflow_root_dir,
    workflow_stage_dirnames_for_engine,
    workflow_workspace_internal_engine_paths,
    workflow_workspace_internal_engine_paths_from_path,
)
from chemstack.core.utils import (
    atomic_write_json,
    coerce_list as _shared_coerce_list,
    coerce_mapping as _shared_coerce_mapping,
    file_lock,
    normalize_text as _shared_normalize_text,
)
from chemstack.core.utils.coercion import normalize_bool as _shared_normalize_bool

WORKFLOW_LOCK_NAME = "workflow.lock"


def _normalize_text(value: Any) -> str:
    return _shared_normalize_text(value)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _shared_coerce_mapping(value)


def _coerce_sequence(value: Any) -> list[Any]:
    return _shared_coerce_list(value)


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, (bool, str)) or value is None:
        return _shared_normalize_bool(value)
    return bool(value)


def _workflow_parent_dir(path: Path) -> Path:
    if path.is_file() and path.name == WORKFLOW_FILE_NAME:
        return path.parent
    return path


def resolve_workflow_workspace(*, target: str, workflow_root: str | Path | None = None) -> Path:
    raw_target = _normalize_text(target)
    if not raw_target:
        raise ValueError("workflow target is required")

    try:
        direct = Path(raw_target).expanduser().resolve()
    except OSError:
        direct = None
    if direct is not None and direct.exists():
        parent = _workflow_parent_dir(direct)
        if parent.is_dir():
            return parent

    if workflow_root is None:
        raise FileNotFoundError(f"workflow not found: {target}")

    root = workflow_root_dir(workflow_root)
    candidate = root / raw_target
    if candidate.exists():
        parent = _workflow_parent_dir(candidate)
        if parent.is_dir():
            return parent
    raise FileNotFoundError(f"workflow not found: {target}")


def workflow_file_path(workspace_dir: str | Path) -> Path:
    return Path(workspace_dir).expanduser().resolve() / WORKFLOW_FILE_NAME


def workflow_lock_path(workspace_dir: str | Path) -> Path:
    return Path(workspace_dir).expanduser().resolve() / WORKFLOW_LOCK_NAME


@contextmanager
def acquire_workflow_lock(workspace_dir: str | Path, *, timeout_seconds: float = 10.0):
    with file_lock(workflow_lock_path(workspace_dir), timeout_seconds=timeout_seconds):
        yield


def load_workflow_payload(workspace_dir: str | Path) -> dict[str, Any]:
    path = workflow_file_path(workspace_dir)
    if not path.exists():
        raise FileNotFoundError(f"workflow file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"workflow file is not a JSON object: {path}")
    return raw


def write_workflow_payload(workspace_dir: str | Path, payload: dict[str, Any]) -> Path:
    path = workflow_file_path(workspace_dir)
    atomic_write_json(path, payload, ensure_ascii=True, indent=2)
    return path


def iter_workflow_workspaces(workflow_root: str | Path) -> list[Path]:
    root = workflow_root_dir(workflow_root)
    if not root.exists():
        return []
    candidates = [item for item in root.iterdir() if item.is_dir() and (item / WORKFLOW_FILE_NAME).exists()]
    return sorted(candidates, key=lambda item: item.name, reverse=True)


def workflow_has_active_downstream(payload: dict[str, Any]) -> bool:
    metadata = _coerce_mapping(payload.get("metadata"))
    downstream = _coerce_mapping(metadata.get("downstream_reaction_workflow"))
    status = _normalize_text(downstream.get("status")).lower()
    if status in {"planned", "queued", "running", "submitted", "cancel_requested"}:
        return True
    if _coerce_bool(downstream.get("final_child_sync_pending")):
        return True
    latest_stage = _coerce_mapping(downstream.get("latest_stage"))
    if _normalize_text(latest_stage.get("status")).lower() in {"planned", "queued", "running", "submitted", "cancel_requested"}:
        return True
    if _normalize_text(latest_stage.get("task_status")).lower() in {"planned", "queued", "running", "submitted", "cancel_requested"}:
        return True
    return False


def _workflow_stage_summary(stage: dict[str, Any]) -> dict[str, Any]:
    stage_status = _normalize_text(stage.get("status")) or "unknown"
    task = _coerce_mapping(stage.get("task"))
    task_status = _normalize_text(task.get("status")) or "unknown"
    task_payload = _coerce_mapping(task.get("payload"))
    enqueue_payload = _coerce_mapping(task.get("enqueue_payload"))
    submission_result = _coerce_mapping(task.get("submission_result"))
    stage_metadata = _coerce_mapping(stage.get("metadata"))
    return {
        "stage_id": _normalize_text(stage.get("stage_id")),
        "stage_kind": _normalize_text(stage.get("stage_kind")),
        "status": stage_status,
        "task_status": task_status,
        "engine": _normalize_text(task.get("engine")),
        "task_kind": _normalize_text(task.get("task_kind")),
        "input_role": _normalize_text(stage_metadata.get("input_role") or task_payload.get("input_role")),
        "reaction_key": _normalize_text(
            task_payload.get("reaction_key") or enqueue_payload.get("reaction_key")
        ),
        "queue_id": _normalize_text(stage_metadata.get("queue_id")),
        "reaction_dir": _normalize_text(
            task_payload.get("reaction_dir") or enqueue_payload.get("reaction_dir")
        ),
        "selected_input_xyz": _normalize_text(task_payload.get("selected_input_xyz")),
        "selected_inp": _normalize_text(
            task_payload.get("selected_inp") or enqueue_payload.get("selected_inp")
        ),
        "submission_status": _normalize_text(submission_result.get("status")),
        "run_id": _normalize_text(stage_metadata.get("run_id")),
        "latest_known_path": _normalize_text(stage_metadata.get("latest_known_path")),
        "organized_output_dir": _normalize_text(stage_metadata.get("organized_output_dir")),
        "optimized_xyz_path": _normalize_text(
            stage_metadata.get("optimized_xyz_path") or task_payload.get("optimized_xyz_path")
        ),
        "analyzer_status": _normalize_text(stage_metadata.get("analyzer_status")),
        "reason": _normalize_text(stage_metadata.get("reason")),
        "reaction_handoff_status": _normalize_text(stage_metadata.get("reaction_handoff_status")),
        "reaction_handoff_reason": _normalize_text(stage_metadata.get("reaction_handoff_reason")),
        "xtb_handoff_retries_used": stage_metadata.get("xtb_handoff_retries_used"),
        "xtb_handoff_retry_limit": stage_metadata.get("xtb_handoff_retry_limit"),
        "orca_attempt_count": stage_metadata.get("attempt_count"),
        "orca_max_retries": stage_metadata.get("max_retries"),
        "completed_at": _normalize_text(stage_metadata.get("completed_at")),
        "output_artifact_count": len(_coerce_sequence(stage.get("output_artifacts"))),
        "last_out_path": _normalize_text(task_payload.get("last_out_path")),
    }


def _workflow_stage_summary_rows(
    stages: list[Any],
) -> tuple[dict[str, int], dict[str, int], list[dict[str, Any]]]:
    status_counts: dict[str, int] = {}
    task_status_counts: dict[str, int] = {}
    stage_summaries: list[dict[str, Any]] = []

    for raw_stage in stages:
        stage = _coerce_mapping(raw_stage)
        summary = _workflow_stage_summary(stage)
        status_counts[summary["status"]] = status_counts.get(summary["status"], 0) + 1
        task_status = summary["task_status"]
        task_status_counts[task_status] = task_status_counts.get(task_status, 0) + 1
        stage_summaries.append(summary)
    return status_counts, task_status_counts, stage_summaries


def workflow_summary(workspace_dir: str | Path, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    workspace = Path(workspace_dir).expanduser().resolve()
    data = payload if payload is not None else load_workflow_payload(workspace)
    stages = _coerce_sequence(data.get("stages"))
    status_counts, task_status_counts, stage_summaries = _workflow_stage_summary_rows(stages)

    metadata = _coerce_mapping(data.get("metadata"))
    request = _coerce_mapping(metadata.get("request"))
    request_parameters = _coerce_mapping(request.get("parameters"))
    downstream = _coerce_mapping(metadata.get("downstream_reaction_workflow"))
    precomplex_handoff = _coerce_mapping(metadata.get("precomplex_handoff"))
    parent_workflow = _coerce_mapping(metadata.get("parent_workflow"))
    summary = {
        "workflow_id": _normalize_text(data.get("workflow_id")),
        "template_name": _normalize_text(data.get("template_name")),
        "status": _normalize_text(data.get("status")),
        "source_job_id": _normalize_text(data.get("source_job_id")),
        "source_job_type": _normalize_text(data.get("source_job_type")),
        "reaction_key": _normalize_text(data.get("reaction_key")),
        "requested_at": _normalize_text(data.get("requested_at")),
        "workspace_dir": str(workspace),
        "workflow_file": str(workflow_file_path(workspace)),
        "stage_count": len(stages),
        "stage_status_counts": status_counts,
        "task_status_counts": task_status_counts,
        "submission_summary": _coerce_mapping(metadata.get("submission_summary")),
        "request_parameters": request_parameters,
        "downstream_reaction_workflow": downstream,
        "precomplex_handoff": precomplex_handoff,
        "parent_workflow": parent_workflow,
        "final_child_sync_pending": _coerce_bool(metadata.get("final_child_sync_pending")),
        "stage_summaries": stage_summaries,
    }
    last_restarted_at = _normalize_text(metadata.get("last_restarted_at"))
    if last_restarted_at:
        summary["last_restarted_at"] = last_restarted_at
    restart_summary = _coerce_mapping(metadata.get("restart_summary"))
    if restart_summary:
        summary["restart_summary"] = restart_summary
    return summary


def list_workflow_summaries(workflow_root: str | Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for workspace in iter_workflow_workspaces(workflow_root):
        try:
            summaries.append(workflow_summary(workspace))
        except (FileNotFoundError, ValueError, json.JSONDecodeError):
            continue
    return summaries


class _WorkflowArtifactRows:
    def __init__(self, workspace: Path) -> None:
        self.workspace = workspace
        self.rows: list[dict[str, Any]] = []
        self.seen: set[tuple[str, str, str, str]] = set()

    def add(
        self,
        *,
        kind: str,
        path_value: Any,
        source: str,
        stage_id: str = "",
        selected: bool = False,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        path_text = _normalize_text(path_value)
        if not path_text:
            return
        candidate = Path(path_text).expanduser()
        resolved = (
            candidate.resolve()
            if candidate.is_absolute()
            else (self.workspace / candidate).resolve()
        )
        key = (_normalize_text(kind), path_text, _normalize_text(stage_id), _normalize_text(source))
        if key in self.seen:
            return
        self.seen.add(key)
        self.rows.append(
            {
                "kind": _normalize_text(kind) or "artifact",
                "path": path_text,
                "resolved_path": str(resolved),
                "exists": resolved.exists(),
                "is_dir": resolved.is_dir(),
                "selected": bool(selected),
                "stage_id": _normalize_text(stage_id),
                "source": _normalize_text(source),
                "metadata": dict(metadata or {}),
            }
        )


def _collect_request_artifacts(collector: _WorkflowArtifactRows, data: dict[str, Any]) -> None:
    request = _coerce_mapping(_coerce_mapping(data.get("metadata")).get("request"))
    for artifact in _coerce_sequence(request.get("source_artifacts")):
        item = _coerce_mapping(artifact)
        collector.add(
            kind=_normalize_text(item.get("kind")) or "source_artifact",
            path_value=item.get("path"),
            source="request.source_artifacts",
            selected=bool(item.get("selected", False)),
            metadata=_coerce_mapping(item.get("metadata")),
        )


def _collect_stage_declared_artifacts(
    collector: _WorkflowArtifactRows,
    *,
    stage: dict[str, Any],
    stage_id: str,
    field_name: str,
    fallback_kind: str,
    source: str,
) -> None:
    for artifact in _coerce_sequence(stage.get(field_name)):
        item = _coerce_mapping(artifact)
        collector.add(
            kind=_normalize_text(item.get("kind")) or fallback_kind,
            path_value=item.get("path"),
            stage_id=stage_id,
            source=source,
            selected=bool(item.get("selected", False)),
            metadata=_coerce_mapping(item.get("metadata")),
        )


def _collect_stage_runtime_artifacts(
    collector: _WorkflowArtifactRows,
    *,
    stage: dict[str, Any],
    stage_id: str,
) -> None:
    task = _coerce_mapping(stage.get("task"))
    task_payload = _coerce_mapping(task.get("payload"))
    enqueue_payload = _coerce_mapping(task.get("enqueue_payload"))
    stage_metadata = _coerce_mapping(stage.get("metadata"))
    collector.add(
        kind="selected_input_xyz",
        path_value=task_payload.get("selected_input_xyz"),
        stage_id=stage_id,
        source="task.payload",
    )
    collector.add(
        kind="selected_inp",
        path_value=task_payload.get("selected_inp") or enqueue_payload.get("selected_inp"),
        stage_id=stage_id,
        source="task.payload",
    )
    collector.add(
        kind="reaction_dir",
        path_value=task_payload.get("reaction_dir") or enqueue_payload.get("reaction_dir"),
        stage_id=stage_id,
        source="task.payload",
    )
    collector.add(
        kind="latest_known_path",
        path_value=stage_metadata.get("latest_known_path"),
        stage_id=stage_id,
        source="stage.metadata",
    )
    collector.add(
        kind="organized_output_dir",
        path_value=stage_metadata.get("organized_output_dir"),
        stage_id=stage_id,
        source="stage.metadata",
    )
    collector.add(
        kind="last_out_path",
        path_value=task_payload.get("last_out_path"),
        stage_id=stage_id,
        source="task.payload",
    )
    collector.add(
        kind="optimized_xyz_path",
        path_value=task_payload.get("optimized_xyz_path") or stage_metadata.get("optimized_xyz_path"),
        stage_id=stage_id,
        source="task.payload",
    )


def _collect_stage_artifacts(collector: _WorkflowArtifactRows, data: dict[str, Any]) -> None:
    for raw_stage in _coerce_sequence(data.get("stages")):
        stage = _coerce_mapping(raw_stage)
        stage_id = _normalize_text(stage.get("stage_id"))
        _collect_stage_declared_artifacts(
            collector,
            stage=stage,
            stage_id=stage_id,
            field_name="input_artifacts",
            fallback_kind="input_artifact",
            source="stage.input_artifacts",
        )
        _collect_stage_declared_artifacts(
            collector,
            stage=stage,
            stage_id=stage_id,
            field_name="output_artifacts",
            fallback_kind="output_artifact",
            source="stage.output_artifacts",
        )
        _collect_stage_runtime_artifacts(collector, stage=stage, stage_id=stage_id)


def _collect_metadata_artifacts(collector: _WorkflowArtifactRows, data: dict[str, Any]) -> None:
    metadata = _coerce_mapping(data.get("metadata"))
    precomplex_handoff = _coerce_mapping(metadata.get("precomplex_handoff"))
    collector.add(
        kind="precomplex_handoff_xyz",
        path_value=precomplex_handoff.get("reactant_xyz"),
        source="metadata.precomplex_handoff",
        selected=True,
        metadata={"role": "reactant"},
    )
    collector.add(
        kind="precomplex_handoff_xyz",
        path_value=precomplex_handoff.get("product_xyz"),
        source="metadata.precomplex_handoff",
        selected=True,
        metadata={"role": "product"},
    )
    downstream = _coerce_mapping(metadata.get("downstream_reaction_workflow"))
    downstream_workspace = _normalize_text(downstream.get("workspace_dir"))
    if downstream_workspace:
        collector.add(
            kind="downstream_workflow_workspace",
            path_value=downstream_workspace,
            source="metadata.downstream_reaction_workflow",
            metadata={"workflow_id": _normalize_text(downstream.get("workflow_id"))},
        )
        collector.add(
            kind="downstream_workflow_file",
            path_value=str(Path(downstream_workspace).expanduser() / WORKFLOW_FILE_NAME),
            source="metadata.downstream_reaction_workflow",
            metadata={"workflow_id": _normalize_text(downstream.get("workflow_id"))},
        )
    latest_stage = _coerce_mapping(downstream.get("latest_stage"))
    collector.add(
        kind="downstream_latest_known_path",
        path_value=latest_stage.get("latest_known_path"),
        source="metadata.downstream_reaction_workflow",
        metadata={"stage_id": _normalize_text(latest_stage.get("stage_id"))},
    )
    collector.add(
        kind="downstream_organized_output_dir",
        path_value=latest_stage.get("organized_output_dir"),
        source="metadata.downstream_reaction_workflow",
        metadata={"stage_id": _normalize_text(latest_stage.get("stage_id"))},
    )


def workflow_artifacts(
    workspace_dir: str | Path,
    payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    workspace = Path(workspace_dir).expanduser().resolve()
    data = payload if payload is not None else load_workflow_payload(workspace)
    collector = _WorkflowArtifactRows(workspace)
    _collect_request_artifacts(collector, data)
    _collect_stage_artifacts(collector, data)
    _collect_metadata_artifacts(collector, data)
    return collector.rows


__all__ = [
    "WORKFLOW_FILE_NAME",
    "WORKFLOW_ENGINE_STAGE_ALIASES",
    "WORKFLOW_STAGE_DIRNAMES",
    "WORKFLOW_LOCK_NAME",
    "acquire_workflow_lock",
    "iter_workflow_runtime_workspaces",
    "iter_workflow_workspaces",
    "list_workflow_summaries",
    "load_workflow_payload",
    "resolve_workflow_workspace",
    "workflow_has_active_downstream",
    "workflow_lock_path",
    "workflow_artifacts",
    "workflow_file_path",
    "workflow_root_dir",
    "workflow_stage_dirnames_for_engine",
    "workflow_summary",
    "workflow_workspace_internal_engine_paths",
    "workflow_workspace_internal_engine_paths_from_path",
    "write_workflow_payload",
]
