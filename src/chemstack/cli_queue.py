from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Sequence

from chemstack import activity_rendering as _activity_rendering
from chemstack import cli_style
from chemstack.cli_errors import emit_error
from chemstack.activity_presenter import (
    QueueListPresentationDeps,
    QueueListPresentationRequest,
    queue_list_display_rows_for_request,
    queue_list_text_presentation,
)
from chemstack.activity_view import (
    activity_counter_config_path,
    activity_with_parent_hint,
    count_global_active_simulations,
    filter_activity_items,
    normalize_activity_filter_values,
)
from chemstack.cli_common import (
    _dependency,
    _effective_shared_config_text,
    _workflow_root_for_args,
)
from chemstack.flow.activity import cancel_activity, clear_activities, list_activities
from chemstack.core.utils import normalize_text


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


def _activity_counter_config_path(
    *,
    payload: dict[str, Any],
    config_hint: str | None,
) -> str | None:
    return activity_counter_config_path(
        payload,
        config_hints=(config_hint,),
        prefer_hints=True,
    )


def _queue_table_now() -> Any:
    return _activity_rendering._queue_table_now()


def _queue_elapsed_text(item: dict[str, Any], *, now: Any | None = None) -> str:
    return _activity_rendering._queue_elapsed_text(item, now=now)


def _queue_display_width(value: str) -> int:
    return _activity_rendering._queue_display_width(value)


def _queue_terminal_width() -> int | None:
    return _activity_rendering._terminal_max_width()


def _queue_table_lines(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    deps: Any | None = None,
) -> list[str]:
    queue_table_now = _dependency(deps, "_queue_table_now", _queue_table_now)
    terminal_width = _dependency(deps, "_queue_terminal_width", _queue_terminal_width)
    return _activity_rendering.queue_table_lines(
        rows, now=queue_table_now(), max_width=terminal_width()
    )


def _queue_list_text_lines(
    rows: Sequence[tuple[int, dict[str, Any]]],
    *,
    active_simulations: int,
    now: Any | None = None,
    max_width: int | None = None,
    include_id: bool = True,
    empty_message: str = "No matching activities.",
    deps: Any | None = None,
) -> list[str]:
    queue_table_now = _dependency(deps, "_queue_table_now", _queue_table_now)
    terminal_width = _dependency(deps, "_queue_terminal_width", _queue_terminal_width)
    return _activity_rendering.queue_list_text_lines(
        rows,
        active_simulations=active_simulations,
        now=now or queue_table_now(),
        max_width=max_width if max_width is not None else terminal_width(),
        include_id=include_id,
        empty_message=empty_message,
    )


def _queue_clear_lines(payload: dict[str, Any]) -> list[str]:
    return _activity_rendering.queue_clear_lines(payload)


def _queue_list_presentation_request(
    request: _QueueListRequest,
    *,
    visible_items: Sequence[dict[str, Any]],
    active_simulations: int | None = None,
    now: Any | None = None,
    max_width: int | None = None,
) -> QueueListPresentationRequest:
    return QueueListPresentationRequest(
        visible_items=visible_items,
        config_hints=(request.shared_config,),
        prefer_config_hints=True,
        default_visible_items=request.default_combined_text_view,
        limit=request.limit,
        show_workflow_context=set(request.kind_values) != {"job"},
        visible_workflow_child_engines=("orca",) if request.default_combined_text_view else None,
        active_simulations=active_simulations,
        now=now,
        max_width=max_width,
    )


def _queue_list_request(args: Any, *, deps: Any | None = None) -> _QueueListRequest:
    effective_shared_config_text = _dependency(
        deps, "_effective_shared_config_text", _effective_shared_config_text
    )
    return _QueueListRequest(
        shared_config=effective_shared_config_text(args) or None,
        limit=int(getattr(args, "limit", 0) or 0),
        engine_values=normalize_activity_filter_values(getattr(args, "engine", None)),
        status_values=normalize_activity_filter_values(getattr(args, "status", None)),
        kind_values=normalize_activity_filter_values(getattr(args, "kind", None)),
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
        emit_error(
            "`chemstack queue list clear` does not support "
            "--engine/--status/--kind/--limit filters."
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
    activities = filter_activity_items(
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
    return queue_list_display_rows_for_request(
        payload,
        request=_queue_list_presentation_request(
            request,
            visible_items=filtered_activities,
        ),
    )


def _print_queue_list_text(
    *,
    payload: dict[str, Any],
    filtered_payload: dict[str, Any],
    filtered_activities: Sequence[dict[str, Any]],
    request: _QueueListRequest,
    deps: Any | None = None,
) -> int:
    render_lines = _dependency(deps, "_queue_list_text_lines", _queue_list_text_lines)
    queue_table_now = _dependency(deps, "_queue_table_now", _queue_table_now)
    terminal_width = _dependency(deps, "_queue_terminal_width", _queue_terminal_width)
    presentation = queue_list_text_presentation(
        payload,
        request=_queue_list_presentation_request(
            request,
            visible_items=filtered_activities,
            active_simulations=filtered_payload["active_simulations"],
            now=queue_table_now(),
            max_width=terminal_width(),
        ),
        deps=QueueListPresentationDeps(
            queue_list_text_lines=render_lines,
        ),
    )
    display_rows = presentation.display_rows
    lines = presentation.lines
    print(lines[0])
    if not display_rows:
        print(lines[1])
        return 0
    # lines[1] is the header, lines[2] the divider, and the rest map one-to-one
    # onto display_rows so each data row can be tinted by its status. Colors are
    # a no-op when stdout is not a TTY, so piped/`--json` output is unaffected.
    print(cli_style.paint(lines[1], cli_style.BOLD))
    print(lines[2])
    for (_indent, item), line in zip(display_rows, lines[3:]):
        color = cli_style.status_color(item.get("status"))
        print(cli_style.paint(line, color) if color else line)
    return 0


def _emit_queue_list_once(args: Any, request: _QueueListRequest, *, deps: Any | None = None) -> int:
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


def _watch_queue_list(args: Any, request: _QueueListRequest, *, deps: Any | None = None) -> int:
    interval = max(0.5, float(getattr(args, "interval", 2.0) or 2.0))
    emit_once = _dependency(deps, "_emit_queue_list_once", _emit_queue_list_once)
    sleep = _dependency(deps, "sleep", time.sleep)
    banner = f"chemstack queue list — refresh every {interval:g}s · Ctrl-C to exit"
    try:
        while True:
            cli_style.clear_screen()
            print(cli_style.label(banner))
            emit_once(args, request, deps=deps)
            sleep(interval)
    except KeyboardInterrupt:
        print()
        return 0


def cmd_queue_list(args: Any, *, deps: Any | None = None) -> int:
    request = _queue_list_request(args, deps=deps)
    if normalize_text(getattr(args, "action", None)).lower() == "clear":
        clear_cmd = _dependency(deps, "_cmd_queue_list_clear", _cmd_queue_list_clear)
        return clear_cmd(args, request)

    if bool(getattr(args, "watch", False)) and not request.json_output:
        watch = _dependency(deps, "_watch_queue_list", _watch_queue_list)
        return watch(args, request, deps=deps)

    emit_once = _dependency(deps, "_emit_queue_list_once", _emit_queue_list_once)
    return emit_once(args, request, deps=deps)


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
        emit_error(exc, hint="Run `chemstack queue list` to see valid targets.")
        return 1

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"{cli_style.label('activity_id:')} {payload.get('activity_id', '-')}")
    print(f"{cli_style.label('kind:')} {payload.get('kind', '-')}")
    print(f"{cli_style.label('engine:')} {payload.get('engine', '-')}")
    print(f"{cli_style.label('source:')} {payload.get('source', '-')}")
    print(f"{cli_style.label('label:')} {payload.get('label', '-')}")
    print(f"{cli_style.label('status:')} {cli_style.status_text(payload.get('status', '-'))}")
    print(f"{cli_style.label('cancel_target:')} {payload.get('cancel_target', '-')}")
    return 0
