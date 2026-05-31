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
    options = request or QueueListPresentationRequest(
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
    display_items = list(all_items if options.visible_items is None else options.visible_items)
    if options.default_visible_items:
        display_items = deps.queue_list_default_visible_items(display_items)
    if options.limit > 0:
        display_items = display_items[: options.limit]

    display_rows = deps.queue_list_display_rows(
        all_items=all_items,
        visible_items=display_items,
        show_workflow_context=options.show_workflow_context,
        visible_workflow_child_engines=options.visible_workflow_child_engines,
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
    "queue_list_text_presentation",
]
