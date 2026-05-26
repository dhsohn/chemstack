from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Sequence

from chemstack import activity_rendering as _activity_rendering
from chemstack.activity_view import (
    activity_with_parent_hint,
    count_global_active_simulations,
    queue_list_default_visible_items,
    queue_list_display_rows,
)
from chemstack.cli_common import (
    _dependency,
    _effective_shared_config_text,
    _workflow_root_for_args,
)
from chemstack.flow.operations import cancel_activity, clear_activities, list_activities
from chemstack.flow.submitters.common import normalize_text

_DEFAULT_QUEUE_TABLE_NOW = _activity_rendering._queue_table_now


@dataclass(frozen=True)
class _QueueListRequest:
    shared_config: str | None
    limit: int
    engine_values: tuple[str, ...]
    status_values: tuple[str, ...]
    kind_values: tuple[str, ...]
    json_output: bool

    @property
    def default_combined_text_view(self) -> bool:
        return (
            not self.json_output
            and not self.engine_values
            and not self.status_values
            and not self.kind_values
        )


def _normalize_filter_values(values: Sequence[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = normalize_text(value).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return tuple(normalized)


def _filter_activity_items(
    items: Sequence[dict[str, Any]],
    *,
    engines: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    kinds: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    engine_filter = set(_normalize_filter_values(engines))
    status_filter = set(_normalize_filter_values(statuses))
    kind_filter = set(_normalize_filter_values(kinds))

    filtered: list[dict[str, Any]] = []
    for item in items:
        engine = normalize_text(item.get("engine")).lower()
        status = normalize_text(item.get("status")).lower()
        kind = normalize_text(item.get("kind")).lower()
        if engine_filter and engine not in engine_filter:
            continue
        if status_filter and status not in status_filter:
            continue
        if kind_filter and kind not in kind_filter:
            continue
        filtered.append(dict(item))
    return filtered


def _activity_counter_config_path(
    *,
    payload: dict[str, Any],
    config_hint: str | None,
) -> str | None:
    config_text = normalize_text(config_hint)
    if config_text:
        return config_text
    sources = payload.get("sources")
    if not isinstance(sources, dict):
        return None
    for key in ("orca_config", "crest_config", "xtb_config"):
        source_text = normalize_text(sources.get(key))
        if source_text:
            return source_text
    return None


def _queue_table_now() -> Any:
    return _DEFAULT_QUEUE_TABLE_NOW()


def _queue_elapsed_text(item: dict[str, Any], *, now: Any | None = None) -> str:
    return _activity_rendering._queue_elapsed_text(item, now=now)


def _queue_display_width(value: str) -> int:
    return _activity_rendering._queue_display_width(value)


def _queue_table_lines(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    deps: Any | None = None,
) -> list[str]:
    queue_table_now = _dependency(deps, "_queue_table_now", _queue_table_now)
    original = _activity_rendering._queue_table_now
    _activity_rendering._queue_table_now = queue_table_now
    try:
        return _activity_rendering.queue_table_lines(rows)
    finally:
        _activity_rendering._queue_table_now = original


def _queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    return _activity_rendering.queue_clear_lines(payload)


def _queue_list_request(args: Any, *, deps: Any | None = None) -> _QueueListRequest:
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    return _QueueListRequest(
        shared_config=effective_shared_config_text(args) or None,
        limit=int(getattr(args, "limit", 0) or 0),
        engine_values=_normalize_filter_values(getattr(args, "engine", None)),
        status_values=_normalize_filter_values(getattr(args, "status", None)),
        kind_values=_normalize_filter_values(getattr(args, "kind", None)),
        json_output=bool(getattr(args, "json", False)),
    )


def _cmd_queue_list_clear(
    args: Any,
    request: _QueueListRequest,
    *,
    deps: Any | None = None,
) -> int:
    if (
        any(getattr(args, field, None) for field in ("engine", "status", "kind"))
        or request.limit > 0
    ):
        print(
            "error: `chemstack queue list clear` does not support --engine/--status/--kind/--limit filters."
        )
        return 1

    workflow_root_for_args = _dependency(deps, "_workflow_root_for_args", _workflow_root_for_args)
    clear = _dependency(deps, "clear_activities", clear_activities)
    payload = clear(
        workflow_root=workflow_root_for_args(args),
        crest_config=request.shared_config,
        xtb_config=request.shared_config,
        orca_config=request.shared_config,
    )
    if request.json_output:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    for line in _queue_clear_lines(payload):
        print(line)
    return 0


def _queue_list_payload(
    args: Any,
    request: _QueueListRequest,
    *,
    deps: Any | None = None,
) -> dict[str, Any]:
    workflow_root_for_args = _dependency(deps, "_workflow_root_for_args", _workflow_root_for_args)
    list_activity_items = _dependency(deps, "list_activities", list_activities)
    payload = list_activity_items(
        workflow_root=workflow_root_for_args(args),
        limit=0,
        refresh=bool(getattr(args, "refresh", False)),
        crest_config=request.shared_config,
        xtb_config=request.shared_config,
        orca_config=request.shared_config,
        child_job_engines=() if request.default_combined_text_view else None,
    )
    return payload


def _filtered_queue_payload(
    payload: dict[str, Any],
    request: _QueueListRequest,
    *,
    deps: Any | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    activities = _filter_activity_items(
        payload.get("activities", []),
        engines=request.engine_values,
        statuses=request.status_values,
        kinds=request.kind_values,
    )
    limited_activities = activities[: request.limit] if request.limit > 0 else list(activities)
    count_active = _dependency(
        deps, "count_global_active_simulations", count_global_active_simulations
    )
    active_simulations = count_active(
        payload.get("activities", []),
        config_path=_activity_counter_config_path(
            payload=payload, config_hint=request.shared_config
        ),
    )
    return {
        "count": len(limited_activities),
        "active_simulations": active_simulations,
        "activities": [activity_with_parent_hint(item) for item in limited_activities],
        "sources": dict(payload.get("sources", {})),
    }, activities


def _queue_list_display_rows(
    *,
    payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
) -> list[tuple[int, dict[str, Any]]]:
    display_items = list(filtered_activities)
    if request.default_combined_text_view:
        display_items = queue_list_default_visible_items(display_items)
    if request.limit > 0:
        display_items = display_items[: request.limit]
    show_workflow_context = set(request.kind_values) != {"job"}
    return queue_list_display_rows(
        all_items=payload.get("activities", []),
        visible_items=display_items,
        show_workflow_context=show_workflow_context,
        visible_workflow_child_engines=("orca",) if request.default_combined_text_view else None,
    )


def _print_queue_list_text(
    *,
    payload: dict[str, Any],
    filtered_payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
    deps: Any | None = None,
) -> int:
    table_lines = _dependency(deps, "_queue_table_lines", _queue_table_lines)
    display_rows = _queue_list_display_rows(
        payload=payload,
        filtered_activities=filtered_activities,
        request=request,
    )
    print(f"active_simulations: {filtered_payload['active_simulations']}")
    if not display_rows:
        print("No matching activities.")
        return 0
    for line in table_lines(display_rows):
        print(line)
    return 0


def cmd_queue_list(args: Any, *, deps: Any | None = None) -> int:
    request = _queue_list_request(args, deps=deps)
    if normalize_text(getattr(args, "action", None)).lower() == "clear":
        clear_cmd = _dependency(deps, "_cmd_queue_list_clear", _cmd_queue_list_clear)
        return clear_cmd(args, request)

    payload_fn = _dependency(deps, "_queue_list_payload", _queue_list_payload)
    filtered_fn = _dependency(deps, "_filtered_queue_payload", _filtered_queue_payload)
    payload = payload_fn(args, request)
    filtered_payload, filtered_activities = filtered_fn(payload, request)
    if request.json_output:
        print(json.dumps(filtered_payload, ensure_ascii=True, indent=2))
        return 0
    print_text = _dependency(deps, "_print_queue_list_text", _print_queue_list_text)
    return print_text(
        payload=payload,
        filtered_payload=filtered_payload,
        filtered_activities=filtered_activities,
        request=request,
    )


def cmd_queue_cancel(args: Any, *, deps: Any | None = None) -> int:
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    workflow_root_for_args = _dependency(deps, "_workflow_root_for_args", _workflow_root_for_args)
    cancel = _dependency(deps, "cancel_activity", cancel_activity)
    shared_config = effective_shared_config_text(args) or None
    try:
        payload = cancel(
            target=getattr(args, "target"),
            workflow_root=workflow_root_for_args(args),
            crest_config=shared_config,
            xtb_config=shared_config,
            orca_config=shared_config,
        )
    except (LookupError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}")
        return 1

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"activity_id: {payload.get('activity_id', '-')}")
    print(f"kind: {payload.get('kind', '-')}")
    print(f"engine: {payload.get('engine', '-')}")
    print(f"source: {payload.get('source', '-')}")
    print(f"label: {payload.get('label', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"cancel_target: {payload.get('cancel_target', '-')}")
    return 0
