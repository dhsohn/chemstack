from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.utils import normalize_bool as _shared_normalize_bool

from ._orchestration_deps import (
    OrchestrationDeps,
    call_engine_aware,
    orchestration_deps,
)
from .state import workflow_stage_dirnames_for_engine, workflow_workspace_internal_engine_paths

_LOGGER = logging.getLogger("chemstack.flow._orchestration_stage_runtime")


def _orchestration_context() -> OrchestrationDeps:
    return orchestration_deps()


def _call_engine_aware(func: Any, config_path: str | None, *, engine: str) -> Any:
    return call_engine_aware(func, config_path, engine=engine)


def _stage_id_for_log(stage: dict[str, Any] | None) -> str:
    if not isinstance(stage, dict):
        return ""
    value = stage.get("stage_id")
    return "" if value is None else str(value).strip()


def _load_contract_or_none(
    load_contract: Callable[..., Any],
    *,
    engine: str,
    target: str,
    stage: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Any | None:
    try:
        return load_contract(target=target, **kwargs)
    except Exception:
        _LOGGER.debug(
            "Failed to load %s artifact contract for target %r; returning None (stage_id=%r)",
            engine,
            target,
            _stage_id_for_log(stage),
            exc_info=True,
        )
        return None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, (bool, str)) or value is None:
        return _shared_normalize_bool(value)
    return bool(value)


def _submission_status(submission: dict[str, Any]) -> str:
    return str(submission.get("status", "")).strip().lower()


def _submission_is_deferred(submission: dict[str, Any]) -> bool:
    return _submission_status(submission) in {
        "blocked",
        "waiting_for_slot",
        "admission_blocked",
        "admission_limit_reached",
        "deferred",
    }


def _submission_deferred_reason(submission: dict[str, Any]) -> str:
    reason = str(submission.get("reason", "")).strip()
    if reason:
        return reason
    return _submission_status(submission) or "waiting_for_slot"


def _mark_submission_deferred(
    *,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    submission: dict[str, Any],
) -> None:
    task["status"] = "planned"
    stage["status"] = "planned"
    stage_metadata["submission_status"] = "waiting_for_slot"
    stage_metadata["submission_deferred_reason"] = _submission_deferred_reason(submission)
    stage_metadata["last_submission_attempt_at"] = str(submission.get("submitted_at", "")).strip()
    stage_metadata.pop("submitted_at", None)
    if not str(submission.get("queue_id", "")).strip():
        stage_metadata.pop("queue_id", None)


def _clear_submission_deferred_metadata(stage_metadata: dict[str, Any]) -> None:
    stage_metadata.pop("submission_deferred_reason", None)
    stage_metadata.pop("last_submission_attempt_at", None)


def _workflow_internal_runs_root(path_text: str, *, engine: str) -> Path | None:
    text = str(path_text).strip()
    if not text:
        return None
    try:
        path = Path(text).expanduser().resolve()
    except OSError:
        return None

    engine_text = str(engine).strip().lower()
    stage_dirnames = workflow_stage_dirnames_for_engine(engine_text)
    for candidate in (path, *path.parents):
        if candidate.name in stage_dirnames:
            return candidate
    return None


def _workflow_internal_organized_root(path_text: str, *, engine: str) -> Path | None:
    runs_root = _workflow_internal_runs_root(path_text, engine=engine)
    if runs_root is None:
        return None
    try:
        if runs_root.name not in workflow_stage_dirnames_for_engine(engine):
            return None
        workspace_dir = runs_root.parent
        return workflow_workspace_internal_engine_paths(
            workspace_dir,
            engine=engine,
            stage_dirname=runs_root.name,
        )["organized_root"]
    except (IndexError, ValueError):
        return None


def _manifest_override_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if str(key).strip()}


def append_unique_artifact_impl(
    rows: list[dict[str, Any]],
    *,
    kind: str,
    path: str,
    selected: bool = False,
    metadata: dict[str, Any] | None = None,
) -> None:
    o = _orchestration_context()
    path_text = o._normalize_text(path)
    if not path_text:
        return
    key = (o._normalize_text(kind), path_text)
    seen = {
        (o._normalize_text(item.get("kind")), o._normalize_text(item.get("path")))
        for item in rows
        if isinstance(item, dict)
    }
    if key in seen:
        return
    rows.append(
        {
            "kind": o._normalize_text(kind) or "artifact",
            "path": path_text,
            "selected": bool(selected),
            "metadata": dict(metadata or {}),
        }
    )
