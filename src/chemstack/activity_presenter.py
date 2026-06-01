from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Sequence

from chemstack.activity_rendering import queue_list_text_lines
from chemstack.activity_view import (
    activity_counter_config_path,
    count_global_active_simulations,
    queue_list_default_visible_items,
    queue_list_display_rows,
)


@dataclass(frozen=True)
class QueueListPresentation:
    lines: list[str]
    display_rows: list[tuple[int, dict[str, Any]]]
    active_simulations: int
    counter_config_path: str | None


@dataclass(frozen=True)
class QueueListPresentationRequest:
    visible_items: Sequence[dict[str, Any]] | None = None
    config_hints: Sequence[str | None] = ()
    prefer_config_hints: bool = False
    default_visible_items: bool = False
    limit: int = 0
    show_workflow_context: bool = True
    visible_workflow_child_engines: Sequence[str] | None = None
    active_simulations: int | None = None
    now: datetime | None = None
    max_width: int | None = None
    include_id: bool = True
    empty_message: str = "No matching activities."


@dataclass(frozen=True)
class QueueListPresentationDeps:
    activity_counter_config_path: Callable[..., str | None] = activity_counter_config_path
    count_global_active_simulations: Callable[..., int] = count_global_active_simulations
    queue_list_default_visible_items: Callable[..., list[dict[str, Any]]] = (
        queue_list_default_visible_items
    )
    queue_list_display_rows: Callable[..., list[tuple[int, dict[str, Any]]]] = (
        queue_list_display_rows
    )
    queue_list_text_lines: Callable[..., list[str]] = queue_list_text_lines


def _queue_list_presentation_options(
    *,
    request: QueueListPresentationRequest | None,
    visible_items: Sequence[dict[str, Any]] | None,
    config_hints: Sequence[str | None],
    prefer_config_hints: bool,
    default_visible_items: bool,
    limit: int,
    show_workflow_context: bool,
    visible_workflow_child_engines: Sequence[str] | None,
    active_simulations: int | None,
    now: datetime | None,
    max_width: int | None,
    include_id: bool,
    empty_message: str,
) -> QueueListPresentationRequest:
    if request is not None:
        return request
    return QueueListPresentationRequest(
        visible_items=visible_items,
        config_hints=config_hints,
        prefer_config_hints=prefer_config_hints,
        default_visible_items=default_visible_items,
        limit=limit,
        show_workflow_context=show_workflow_context,
        visible_workflow_child_engines=visible_workflow_child_engines,
        active_simulations=active_simulations,
        now=now,
        max_width=max_width,
        include_id=include_id,
        empty_message=empty_message,
    )


def _queue_list_display_items(
    all_items: Sequence[dict[str, Any]],
    *,
    options: QueueListPresentationRequest,
    deps: QueueListPresentationDeps,
) -> list[dict[str, Any]]:
    display_items = list(all_items if options.visible_items is None else options.visible_items)
    if options.default_visible_items:
        display_items = deps.queue_list_default_visible_items(display_items)
    if options.limit > 0:
        display_items = display_items[: options.limit]
    return display_items


def queue_list_display_rows_for_request(
    payload: dict[str, Any],
    *,
    request: QueueListPresentationRequest | None = None,
    visible_items: Sequence[dict[str, Any]] | None = None,
    config_hints: Sequence[str | None] = (),
    prefer_config_hints: bool = False,
    default_visible_items: bool = False,
    limit: int = 0,
    show_workflow_context: bool = True,
    visible_workflow_child_engines: Sequence[str] | None = None,
    active_simulations: int | None = None,
    now: datetime | None = None,
    max_width: int | None = None,
    include_id: bool = True,
    empty_message: str = "No matching activities.",
    deps: QueueListPresentationDeps = QueueListPresentationDeps(),
) -> list[tuple[int, dict[str, Any]]]:
    options = _queue_list_presentation_options(
        request=request,
        visible_items=visible_items,
        config_hints=config_hints,
        prefer_config_hints=prefer_config_hints,
        default_visible_items=default_visible_items,
        limit=limit,
        show_workflow_context=show_workflow_context,
        visible_workflow_child_engines=visible_workflow_child_engines,
        active_simulations=active_simulations,
        now=now,
        max_width=max_width,
        include_id=include_id,
        empty_message=empty_message,
    )
    all_items = list(payload.get("activities", []))
    display_items = _queue_list_display_items(all_items, options=options, deps=deps)
    return deps.queue_list_display_rows(
        all_items=all_items,
        visible_items=display_items,
        show_workflow_context=options.show_workflow_context,
        visible_workflow_child_engines=options.visible_workflow_child_engines,
    )


def queue_list_text_presentation(
    payload: dict[str, Any],
    *,
    request: QueueListPresentationRequest | None = None,
    visible_items: Sequence[dict[str, Any]] | None = None,
    config_hints: Sequence[str | None] = (),
    prefer_config_hints: bool = False,
    default_visible_items: bool = False,
    limit: int = 0,
    show_workflow_context: bool = True,
    visible_workflow_child_engines: Sequence[str] | None = None,
    active_simulations: int | None = None,
    now: datetime | None = None,
    max_width: int | None = None,
    include_id: bool = True,
    empty_message: str = "No matching activities.",
    deps: QueueListPresentationDeps = QueueListPresentationDeps(),
) -> QueueListPresentation:
    options = _queue_list_presentation_options(
        request=request,
        visible_items=visible_items,
        config_hints=config_hints,
        prefer_config_hints=prefer_config_hints,
        default_visible_items=default_visible_items,
        limit=limit,
        show_workflow_context=show_workflow_context,
        visible_workflow_child_engines=visible_workflow_child_engines,
        active_simulations=active_simulations,
        now=now,
        max_width=max_width,
        include_id=include_id,
        empty_message=empty_message,
    )
    all_items = list(payload.get("activities", []))
    display_rows = queue_list_display_rows_for_request(
        payload,
        request=options,
        deps=deps,
    )
    counter_config_path = deps.activity_counter_config_path(
        payload,
        config_hints=options.config_hints,
        prefer_hints=options.prefer_config_hints,
    )
    resolved_active_simulations = (
        options.active_simulations
        if options.active_simulations is not None
        else deps.count_global_active_simulations(
            all_items,
            config_path=counter_config_path,
        )
    )
    lines = deps.queue_list_text_lines(
        display_rows,
        active_simulations=resolved_active_simulations,
        now=options.now,
        max_width=options.max_width,
        include_id=options.include_id,
        empty_message=options.empty_message,
    )
    return QueueListPresentation(
        lines=lines,
        display_rows=display_rows,
        active_simulations=resolved_active_simulations,
        counter_config_path=counter_config_path,
    )


__all__ = [
    "QueueListPresentation",
    "QueueListPresentationDeps",
    "QueueListPresentationRequest",
    "queue_list_display_rows_for_request",
    "queue_list_text_presentation",
]
