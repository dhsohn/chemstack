from __future__ import annotations

from pathlib import Path
from typing import Any, Sequence

from chemstack.core.admission import active_slot_count
from chemstack.flow.submitters.common import normalize_text
from chemstack.flow.submitters.common import sibling_runtime_paths

ACTIVE_SIMULATION_STATUSES = frozenset({"running", "retrying", "cancel_requested"})
ActivityItem = dict[str, Any]
TopLevelToken = tuple[str, str | int]


def workflow_parent_id_from_activity(item: dict[str, Any]) -> str:
    metadata = item.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    explicit_parent = normalize_text(metadata.get("workflow_id"))
    if explicit_parent:
        return explicit_parent
    for key in ("job_dir", "reaction_dir"):
        path_text = normalize_text(metadata.get(key))
        if not path_text:
            continue
        parts = [part for part in path_text.replace("\\", "/").split("/") if part]
        for index, part in enumerate(parts[:-1]):
            if part == "workflow_jobs" and index + 1 < len(parts):
                return normalize_text(parts[index + 1])
    return ""


def activity_with_parent_hint(item: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(item)
    parent_workflow_id = workflow_parent_id_from_activity(enriched)
    if parent_workflow_id:
        enriched["parent_workflow_id"] = parent_workflow_id
    return enriched


def count_active_simulations(items: Sequence[dict[str, Any]]) -> int:
    total = 0
    for item in items:
        if normalize_text(item.get("kind")).lower() != "job":
            continue
        status = normalize_text(item.get("status")).lower()
        if status in ACTIVE_SIMULATION_STATUSES:
            total += 1
    return total


def count_global_active_simulations(
    items: Sequence[dict[str, Any]],
    *,
    config_path: str | None = None,
) -> int:
    config_text = normalize_text(config_path)
    if config_text:
        try:
            runtime_paths = sibling_runtime_paths(config_text)
        except Exception:
            runtime_paths = {}
        admission_root = runtime_paths.get("admission_root")
        if isinstance(admission_root, Path):
            try:
                return max(0, int(active_slot_count(admission_root)))
            except Exception:
                pass
    return count_active_simulations(items)


def queue_list_display_rows(
    *,
    all_items: Sequence[dict[str, Any]],
    visible_items: Sequence[dict[str, Any]],
    show_workflow_context: bool,
) -> list[tuple[int, dict[str, Any]]]:
    workflow_by_id: dict[str, ActivityItem] = {}
    for item in all_items:
        workflow_id = normalize_text(item.get("activity_id"))
        if workflow_id and normalize_text(item.get("kind")).lower() == "workflow":
            workflow_by_id[workflow_id] = dict(item)

    workflow_children: dict[str, list[ActivityItem]] = {}
    standalone_items: dict[tuple[str, int], ActivityItem] = {}
    top_level_tokens: list[TopLevelToken] = []
    seen_workflow_tokens: set[str] = set()

    for index, raw_item in enumerate(visible_items):
        item = activity_with_parent_hint(raw_item)
        kind = normalize_text(item.get("kind")).lower()
        if kind == "job":
            parent_workflow_id = normalize_text(item.get("parent_workflow_id"))
            if show_workflow_context and parent_workflow_id and parent_workflow_id in workflow_by_id:
                workflow_children.setdefault(parent_workflow_id, []).append(item)
                if parent_workflow_id not in seen_workflow_tokens:
                    seen_workflow_tokens.add(parent_workflow_id)
                    top_level_tokens.append(("workflow", parent_workflow_id))
                continue
            token = ("item", index)
            standalone_items[token] = item
            top_level_tokens.append(token)
            continue

        if kind == "workflow":
            workflow_id = normalize_text(item.get("activity_id"))
            if not workflow_id:
                token = ("item", index)
                standalone_items[token] = item
                top_level_tokens.append(token)
                continue
            if workflow_id not in seen_workflow_tokens:
                seen_workflow_tokens.add(workflow_id)
                top_level_tokens.append(("workflow", workflow_id))
            workflow_by_id.setdefault(workflow_id, item)
            continue

        token = ("item", index)
        standalone_items[token] = item
        top_level_tokens.append(token)

    rows: list[tuple[int, ActivityItem]] = []
    for row_token in top_level_tokens:
        token_kind, token_value = row_token
        if token_kind == "workflow":
            workflow_id = str(token_value)
            parent = workflow_by_id.get(workflow_id)
            children = workflow_children.get(workflow_id, [])
            if parent is not None:
                rows.append((0, dict(parent)))
                for child in children:
                    rows.append((1, dict(child)))
                continue
            for child in children:
                rows.append((0, dict(child)))
            continue
        if not isinstance(token_value, int):
            continue
        standalone_item = standalone_items.get((token_kind, token_value))
        if standalone_item is not None:
            rows.append((0, dict(standalone_item)))
    return rows
