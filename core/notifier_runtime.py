from __future__ import annotations

import logging
import queue
import threading
import time
from typing import Any, Callable, Dict, List, Set

from .notifier_events import (
    EVT_ATTEMPT_COMPLETED,
    EVT_HEARTBEAT,
    event_heartbeat,
    render_message,
)
from .notifier_state import (
    compact_dedup_state,
    is_duplicate,
    load_dedup_state,
    mark_sent,
    save_dedup_state,
)
from .telegram_client import TelegramConfig, send_with_retry

logger = logging.getLogger(__name__)

_PRIORITY_PRESERVE: Set[str] = {"run_started", "run_completed", "run_failed", "run_interrupted"}
_PRIORITY_DROP_FIRST: Set[str] = {EVT_HEARTBEAT}
_SENTINEL = None


def _overflow_drop(q: queue.Queue, new_event: Dict[str, Any]) -> bool:
    new_type = new_event.get("event_type", "")
    if new_type in _PRIORITY_DROP_FIRST:
        return False

    items: List[Dict[str, Any]] = []
    try:
        while True:
            items.append(q.get_nowait())
    except queue.Empty:
        pass

    drop_idx: int | None = None
    for i, item in enumerate(items):
        if item.get("event_type", "") in _PRIORITY_DROP_FIRST:
            drop_idx = i
            break
    if drop_idx is None and new_type in _PRIORITY_PRESERVE:
        for i, item in enumerate(items):
            if item.get("event_type") == EVT_ATTEMPT_COMPLETED:
                drop_idx = i
                break

    if drop_idx is not None:
        items.pop(drop_idx)

    for item in items:
        try:
            q.put_nowait(item)
        except queue.Full:
            break

    return drop_idx is not None


def _worker_loop(
    q: queue.Queue,
    tg_config: TelegramConfig,
    reaction_dir,
    dedup_ttl_sec: int,
    mask_paths: bool,
    alive_flag: threading.Event,
) -> None:
    alive_flag.set()
    dedup_state = load_dedup_state(reaction_dir)
    compact_counter = 0

    while True:
        try:
            event = q.get(timeout=1.0)
        except queue.Empty:
            continue

        if event is _SENTINEL:
            break

        event_id = event.get("event_id", "")
        if event_id and is_duplicate(dedup_state, event_id):
            logger.debug("Dedup: skipping %s", event_id)
            continue

        text = render_message(event, mask_paths=mask_paths)
        result = send_with_retry(tg_config, text)
        if result.success:
            if event_id:
                mark_sent(dedup_state, event_id)
                try:
                    save_dedup_state(reaction_dir, dedup_state)
                except Exception as exc:
                    logger.warning("Failed to save dedup state: %s", exc)
        else:
            logger.warning(
                "Telegram send failed for %s: status=%d error=%s",
                event_id, result.status_code, result.error,
            )

        compact_counter += 1
        if compact_counter >= 50:
            compact_dedup_state(dedup_state, dedup_ttl_sec)
            compact_counter = 0

    while True:
        try:
            event = q.get_nowait()
        except queue.Empty:
            break
        if event is _SENTINEL:
            break
        event_id = event.get("event_id", "")
        if event_id and is_duplicate(dedup_state, event_id):
            continue
        text = render_message(event, mask_paths=mask_paths)
        result = send_with_retry(tg_config, text)
        if result.success and event_id:
            mark_sent(dedup_state, event_id)

    try:
        save_dedup_state(reaction_dir, dedup_state)
    except Exception:
        pass

    alive_flag.clear()


def _heartbeat_loop(
    notify_fn: Callable[[Dict[str, Any]], None],
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
    state_getter: Callable[[], Dict[str, Any]],
    interval_sec: int,
    stop_event: threading.Event,
    start_time: float,
) -> None:
    while not stop_event.wait(timeout=interval_sec):
        try:
            state = state_getter()
            elapsed = time.time() - start_time
            evt = event_heartbeat(
                run_id,
                reaction_dir,
                selected_inp,
                status=state.get("status", "unknown"),
                attempt_count=len(state.get("attempts", [])),
                elapsed_sec=elapsed,
            )
            notify_fn(evt)
        except Exception as exc:
            logger.debug("Heartbeat emission error: %s", exc)
