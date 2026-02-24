from __future__ import annotations

import fcntl
import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .state_store import _atomic_write_text, now_utc_iso

logger = logging.getLogger(__name__)

DEDUP_STATE_FILE = ".notify_state.json"
DEDUP_LOCK_FILE = ".notify_state.lock"


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
