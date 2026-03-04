from __future__ import annotations

import atexit
import logging
import os
import queue
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from .config import MonitoringConfig
from .notifier_events import (
    EVT_ATTEMPT_COMPLETED,
    EVT_RUN_COMPLETED,
    EVT_RUN_FAILED,
    EVT_RUN_INTERRUPTED,
    EVT_RUN_STARTED,
    event_attempt_completed,
    event_run_started,
    event_run_terminal,
    make_event_id,
    render_message,
)
from .notifier_runtime import _SENTINEL, _overflow_drop, _worker_loop
from .notifier_state import (
    compact_dedup_state,
    is_duplicate,
    load_dedup_state,
    mark_sent,
    save_dedup_state,
)
from .telegram_client import TelegramConfig, send_with_retry

logger = logging.getLogger(__name__)


class Notifier:
    def __init__(
        self,
        tg_config: TelegramConfig,
        mon_config: MonitoringConfig,
        reaction_dir: Path,
        run_id: str,
        reaction_dir_str: str,
        selected_inp_str: str,
        state_ref: Dict[str, Any],
    ) -> None:
        _ = (run_id, reaction_dir_str, selected_inp_str, state_ref)
        self._tg_config = tg_config
        self._mon_config = mon_config
        self._reaction_dir = reaction_dir
        self._async_enabled = mon_config.delivery.async_enabled
        self._disabled = False
        self._shutdown_called = False
        self._sync_lock = threading.Lock()
        self._sync_dedup_state: Dict[str, Any] = {"sent_event_ids": {}}
        self._sync_compact_counter = 0

        self._queue: queue.Queue | None = None
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_alive = threading.Event()

        if self._async_enabled:
            self._queue = queue.Queue(maxsize=mon_config.delivery.queue_size)
            self._worker_thread = threading.Thread(
                target=_worker_loop,
                args=(
                    self._queue,
                    tg_config,
                    reaction_dir,
                    mon_config.delivery.dedup_ttl_sec,
                    False,
                    self._worker_alive,
                ),
                daemon=True,
                name="orca_auto_notify_worker",
            )
            self._worker_thread.start()
        else:
            self._sync_dedup_state = load_dedup_state(reaction_dir)

        atexit.register(self.shutdown)

    def notify(self, event: Dict[str, Any]) -> None:
        if self._disabled or self._shutdown_called:
            return

        if self._async_enabled:
            if not self._worker_alive.is_set():
                logger.warning("Notification worker is dead; disabling notifier")
                self._disabled = True
                return
            if self._queue is None:
                self._disabled = True
                return
            try:
                self._queue.put_nowait(event)
            except queue.Full:
                if _overflow_drop(self._queue, event):
                    try:
                        self._queue.put_nowait(event)
                    except queue.Full:
                        logger.warning(
                            "Queue still full after drop attempt; event lost: %s",
                            event.get("event_id", "?"),
                        )
                else:
                    logger.debug("Dropped low-priority event: %s", event.get("event_id", "?"))
            return

        self._notify_sync(event)

    def _notify_sync(self, event: Dict[str, Any]) -> None:
        with self._sync_lock:
            event_id = event.get("event_id", "")
            if event_id and is_duplicate(self._sync_dedup_state, event_id):
                logger.debug("Dedup: skipping %s", event_id)
                return

            text = render_message(event, mask_paths=False)
            result = send_with_retry(self._tg_config, text)
            if result.success:
                if event_id:
                    mark_sent(self._sync_dedup_state, event_id)
                self._sync_compact_counter += 1
                if self._sync_compact_counter >= 50:
                    compact_dedup_state(self._sync_dedup_state, self._mon_config.delivery.dedup_ttl_sec)
                    self._sync_compact_counter = 0
                try:
                    save_dedup_state(self._reaction_dir, self._sync_dedup_state)
                except Exception as exc:
                    logger.warning("Failed to save dedup state: %s", exc)
            else:
                logger.warning(
                    "Telegram send failed for %s: status=%d error=%s",
                    event_id, result.status_code, result.error,
                )

    def shutdown(self) -> None:
        if self._shutdown_called:
            return
        self._shutdown_called = True
        self._disabled = True

        if not self._async_enabled:
            return
        if self._worker_thread is None or self._queue is None:
            return

        if self._worker_thread.is_alive():
            deadline = time.time() + self._mon_config.delivery.worker_flush_timeout_sec
            while time.time() < deadline:
                try:
                    self._queue.put(_SENTINEL, timeout=0.2)
                    break
                except queue.Full:
                    continue
            else:
                logger.warning("Failed to enqueue notifier shutdown sentinel before timeout")

        self._worker_thread.join(timeout=self._mon_config.delivery.worker_flush_timeout_sec)


def resolve_telegram_config(mon_config: MonitoringConfig) -> Optional[TelegramConfig]:
    if not mon_config.enabled:
        return None
    tg = mon_config.telegram
    bot_token = os.environ.get(tg.bot_token_env, "").strip()
    chat_id = os.environ.get(tg.chat_id_env, "").strip()
    if not bot_token or not chat_id:
        return None
    chat_id_stripped = chat_id.lstrip("-")
    if not chat_id_stripped.isdigit():
        return None
    return TelegramConfig(
        bot_token=bot_token,
        chat_id=chat_id,
        timeout_sec=tg.timeout_sec,
        retry_count=tg.retry_count,
        retry_backoff_sec=tg.retry_backoff_sec,
        retry_jitter_sec=tg.retry_jitter_sec,
    )


def send_batch_summary(mon_config: MonitoringConfig, text: str) -> None:
    tg_config = resolve_telegram_config(mon_config)
    if tg_config is None:
        return
    try:
        result = send_with_retry(tg_config, text)
    except Exception as exc:
        logger.warning("Telegram send failed (non-fatal): %s", exc)
        return

    if not result.success:
        logger.warning(
            "Telegram send failed for batch summary: status=%d error=%s",
            result.status_code,
            result.error,
        )


def create_notifier(
    mon_config: MonitoringConfig,
    reaction_dir: Path,
    run_id: str,
    selected_inp: str,
    state_ref: Dict[str, Any],
) -> Notifier | None:
    tg_config = resolve_telegram_config(mon_config)
    if tg_config is None:
        if mon_config.enabled:
            bot_token = os.environ.get(mon_config.telegram.bot_token_env, "").strip()
            chat_id = os.environ.get(mon_config.telegram.chat_id_env, "").strip()
            if not bot_token:
                logger.warning("Monitoring enabled but telegram bot token env var is empty; disabling")
            elif not chat_id:
                logger.warning("Monitoring enabled but telegram chat_id env var is empty; disabling")
            else:
                logger.warning("Invalid chat_id (must be numeric): disabling notifier")
        return None

    try:
        return Notifier(
            tg_config=tg_config,
            mon_config=mon_config,
            reaction_dir=reaction_dir,
            run_id=run_id,
            reaction_dir_str=str(reaction_dir),
            selected_inp_str=selected_inp,
            state_ref=state_ref,
        )
    except Exception as exc:
        logger.warning("Failed to create notifier: %s", exc)
        return None


def make_notify_callback(
    notifier: Notifier | None,
) -> Callable[[Dict[str, Any]], None] | None:
    if notifier is None:
        return None
    return notifier.notify
