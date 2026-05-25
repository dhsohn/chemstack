from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from ._runtime_common import normalize_text

ACTIVE_TERMINAL_SYNC_STATUSES = frozenset(
    {"queued", "running", "submitted", "cancel_requested"}
)


def workflow_needs_terminal_sync(
    workspace_dir: str | Path,
    *,
    load_workflow_payload_fn: Callable[[str | Path], dict[str, Any]],
    workflow_has_active_downstream_fn: Callable[[dict[str, Any]], bool],
    normalize_text_fn: Callable[[Any], str] = normalize_text,
) -> bool:
    try:
        payload = load_workflow_payload_fn(workspace_dir)
    except (FileNotFoundError, ValueError):
        return False
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and bool(metadata.get("final_child_sync_pending")):
        return True
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        if normalize_text_fn(raw_stage.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES:
            return True
        task = raw_stage.get("task")
        if (
            isinstance(task, dict)
            and normalize_text_fn(task.get("status")).lower() in ACTIVE_TERMINAL_SYNC_STATUSES
        ):
            return True
    return workflow_has_active_downstream_fn(payload)
