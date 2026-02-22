from __future__ import annotations

import atexit
import fcntl
import json
import logging
import os
import queue
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .config import MonitoringConfig
from .state_store import _atomic_write_text, now_utc_iso
from .telegram_client import TelegramConfig, send_with_retry

logger = logging.getLogger(__name__)

EVT_RUN_STARTED = "run_started"
EVT_ATTEMPT_COMPLETED = "attempt_completed"
EVT_RUN_COMPLETED = "run_completed"
EVT_RUN_FAILED = "run_failed"
EVT_RUN_INTERRUPTED = "run_interrupted"
EVT_HEARTBEAT = "heartbeat"

_PRIORITY_PRESERVE: Set[str] = {EVT_RUN_STARTED, EVT_RUN_COMPLETED, EVT_RUN_FAILED, EVT_RUN_INTERRUPTED}
_PRIORITY_DROP_FIRST: Set[str] = {EVT_HEARTBEAT}

DEDUP_STATE_FILE = ".notify_state.json"
DEDUP_LOCK_FILE = ".notify_state.lock"

_SENTINEL = None


# -- Event Payload Construction -----------------------------------------------


def _make_common(
    event_type: str,
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "run_id": run_id,
        "reaction_dir": reaction_dir,
        "selected_inp": selected_inp,
        "timestamp": now_utc_iso(),
    }


def make_event_id(run_id: str, event_type: str, suffix: str = "") -> str:
    if suffix:
        return f"{run_id}:{event_type}:{suffix}"
    return f"{run_id}:{event_type}"


def event_run_started(
    run_id: str, reaction_dir: str, selected_inp: str,
) -> Dict[str, Any]:
    evt = _make_common(EVT_RUN_STARTED, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, EVT_RUN_STARTED)
    return evt


def event_attempt_completed(
    run_id: str, reaction_dir: str, selected_inp: str,
    *, attempt_index: int, analyzer_status: str, analyzer_reason: str,
) -> Dict[str, Any]:
    evt = _make_common(EVT_ATTEMPT_COMPLETED, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, EVT_ATTEMPT_COMPLETED, str(attempt_index))
    evt["attempt_index"] = attempt_index
    evt["analyzer_status"] = analyzer_status
    evt["analyzer_reason"] = analyzer_reason
    return evt


def event_run_terminal(
    event_type: str,
    run_id: str, reaction_dir: str, selected_inp: str,
    *, status: str, reason: str, attempt_count: int,
) -> Dict[str, Any]:
    evt = _make_common(event_type, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, event_type)
    evt["status"] = status
    evt["reason"] = reason
    evt["attempt_count"] = attempt_count
    return evt


def event_heartbeat(
    run_id: str, reaction_dir: str, selected_inp: str,
    *, status: str, attempt_count: int, elapsed_sec: float,
) -> Dict[str, Any]:
    bucket_ts = str(int(time.time()))
    evt = _make_common(EVT_HEARTBEAT, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, EVT_HEARTBEAT, bucket_ts)
    evt["status"] = status
    evt["attempt_count"] = attempt_count
    evt["elapsed_sec"] = round(elapsed_sec, 1)
    return evt


# -- Message Rendering --------------------------------------------------------


def render_message(event: Dict[str, Any], *, mask_paths: bool = False) -> str:
    etype = event.get("event_type", "unknown")
    run_id = event.get("run_id", "?")
    reaction_dir = event.get("reaction_dir", "?")
    if mask_paths:
        reaction_dir = Path(reaction_dir).name if reaction_dir != "?" else "?"

    if etype == EVT_RUN_STARTED:
        return f"[orca_auto] started | run_id={run_id} | dir={reaction_dir}"

    if etype == EVT_ATTEMPT_COMPLETED:
        idx = event.get("attempt_index", "?")
        astatus = event.get("analyzer_status", "?")
        return f"[orca_auto] attempt {idx} done | run_id={run_id} | status={astatus}"

    if etype == EVT_RUN_COMPLETED:
        count = event.get("attempt_count", "?")
        reason = event.get("reason", "?")
        return f"[orca_auto] completed | run_id={run_id} | attempts={count} | reason={reason}"

    if etype in (EVT_RUN_FAILED, EVT_RUN_INTERRUPTED):
        status = event.get("status", "?")
        reason = event.get("reason", "?")
        label = "failed" if etype == EVT_RUN_FAILED else "interrupted"
        return f"[orca_auto] {label} | run_id={run_id} | status={status} | reason={reason}"

    if etype == EVT_HEARTBEAT:
        status = event.get("status", "?")
        count = event.get("attempt_count", "?")
        elapsed = event.get("elapsed_sec", "?")
        return f"[orca_auto] heartbeat | run_id={run_id} | status={status} | attempts={count} | elapsed_sec={elapsed}"

    return f"[orca_auto] {etype} | run_id={run_id}"


# -- Dedup State --------------------------------------------------------------


def _dedup_state_path(reaction_dir: Path) -> Path:
    return reaction_dir / DEDUP_STATE_FILE


@contextmanager
def _dedup_file_lock(reaction_dir: Path):
    lock_path = reaction_dir / DEDUP_LOCK_FILE
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _load_dedup_state_unlocked(reaction_dir: Path) -> Dict[str, Any]:
    p = _dedup_state_path(reaction_dir)
    if not p.exists():
        return {"sent_event_ids": {}}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(raw, dict) and isinstance(raw.get("sent_event_ids"), dict):
            return raw
    except Exception:
        pass
    try:
        corrupt_name = f".notify_state.corrupt.{int(time.time())}"
        p.rename(p.with_name(corrupt_name))
        logger.warning("Corrupt dedup state backed up as %s", corrupt_name)
    except Exception:
        pass
    return {"sent_event_ids": {}}


def _save_dedup_state_unlocked(reaction_dir: Path, state: Dict[str, Any]) -> None:
    p = _dedup_state_path(reaction_dir)
    _atomic_write_text(p, json.dumps(state, ensure_ascii=True, indent=2))


def _merge_dedup_state(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {"sent_event_ids": {}}
    merged_sent = merged["sent_event_ids"]

    base_sent = base.get("sent_event_ids", {})
    if isinstance(base_sent, dict):
        merged_sent.update(base_sent)

    incoming_sent = incoming.get("sent_event_ids", {})
    if isinstance(incoming_sent, dict):
        merged_sent.update(incoming_sent)

    return merged


def load_dedup_state(reaction_dir: Path) -> Dict[str, Any]:
    with _dedup_file_lock(reaction_dir):
        return _load_dedup_state_unlocked(reaction_dir)


def save_dedup_state(reaction_dir: Path, state: Dict[str, Any]) -> None:
    with _dedup_file_lock(reaction_dir):
        current = _load_dedup_state_unlocked(reaction_dir)
        merged = _merge_dedup_state(current, state)
        _save_dedup_state_unlocked(reaction_dir, merged)
    state.clear()
    state.update(merged)


def is_duplicate(dedup_state: Dict[str, Any], event_id: str) -> bool:
    sent = dedup_state.get("sent_event_ids", {})
    return event_id in sent


def mark_sent(dedup_state: Dict[str, Any], event_id: str) -> None:
    sent = dedup_state.setdefault("sent_event_ids", {})
    sent[event_id] = now_utc_iso()


def compact_dedup_state(dedup_state: Dict[str, Any], ttl_sec: int) -> int:
    sent = dedup_state.get("sent_event_ids", {})
    cutoff = datetime.now(timezone.utc).timestamp() - ttl_sec
    to_remove: List[str] = []
    for eid, ts_str in sent.items():
        try:
            ts = datetime.fromisoformat(ts_str).timestamp()
            if ts < cutoff:
                to_remove.append(eid)
        except Exception:
            to_remove.append(eid)
    for eid in to_remove:
        del sent[eid]
    return len(to_remove)


# -- Queue Overflow Policy ----------------------------------------------------


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


# -- Worker Thread ------------------------------------------------------------


def _worker_loop(
    q: queue.Queue,
    tg_config: TelegramConfig,
    reaction_dir: Path,
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


# -- Heartbeat Daemon ---------------------------------------------------------


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
                run_id, reaction_dir, selected_inp,
                status=state.get("status", "unknown"),
                attempt_count=len(state.get("attempts", [])),
                elapsed_sec=elapsed,
            )
            notify_fn(evt)
        except Exception as exc:
            logger.debug("Heartbeat emission error: %s", exc)


# -- Notifier Facade ----------------------------------------------------------


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
        self._tg_config = tg_config
        self._mon_config = mon_config
        self._reaction_dir = reaction_dir
        self._run_id = run_id
        self._state_ref = state_ref
        self._async_enabled = mon_config.delivery.async_enabled
        self._disabled = False
        self._shutdown_called = False
        self._sync_lock = threading.Lock()
        self._sync_dedup_state: Dict[str, Any] = {"sent_event_ids": {}}
        self._sync_compact_counter = 0

        self._queue: queue.Queue | None = None
        self._worker_thread: Optional[threading.Thread] = None
        self._worker_alive = threading.Event()
        self._heartbeat_stop = threading.Event()
        self._start_time = time.time()

        if self._async_enabled:
            self._queue = queue.Queue(
                maxsize=mon_config.delivery.queue_size,
            )
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

        self._heartbeat_thread: Optional[threading.Thread] = None
        if mon_config.heartbeat.enabled:
            self._heartbeat_thread = threading.Thread(
                target=_heartbeat_loop,
                args=(
                    self.notify,
                    run_id,
                    reaction_dir_str,
                    selected_inp_str,
                    lambda: self._state_ref,
                    mon_config.heartbeat.interval_sec,
                    self._heartbeat_stop,
                    self._start_time,
                ),
                daemon=True,
                name="orca_auto_heartbeat",
            )
            self._heartbeat_thread.start()

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

        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2.0)

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

        self._worker_thread.join(
            timeout=self._mon_config.delivery.worker_flush_timeout_sec,
        )


# -- Factory ------------------------------------------------------------------


def create_notifier(
    mon_config: MonitoringConfig,
    reaction_dir: Path,
    run_id: str,
    selected_inp: str,
    state_ref: Dict[str, Any],
) -> Notifier | None:
    if not mon_config.enabled:
        return None

    tg = mon_config.telegram
    bot_token = os.environ.get(tg.bot_token_env, "").strip()
    chat_id = os.environ.get(tg.chat_id_env, "").strip()

    if not bot_token:
        logger.warning("Monitoring enabled but telegram bot token env var is empty; disabling")
        return None
    if not chat_id:
        logger.warning("Monitoring enabled but telegram chat_id env var is empty; disabling")
        return None

    chat_id_stripped = chat_id.lstrip("-")
    if not chat_id_stripped.isdigit():
        logger.warning("Invalid chat_id (must be numeric): disabling notifier")
        return None

    tg_config = TelegramConfig(
        bot_token=bot_token,
        chat_id=chat_id,
        timeout_sec=tg.timeout_sec,
        retry_count=tg.retry_count,
        retry_backoff_sec=tg.retry_backoff_sec,
        retry_jitter_sec=tg.retry_jitter_sec,
    )

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
