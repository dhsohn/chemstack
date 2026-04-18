"""Shared admission slot store for enforcing a hard active-run cap."""

from __future__ import annotations

import json
import logging
import os
import fcntl
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List, TypedDict, cast

from .lock_utils import (
    current_process_start_ticks,
    is_process_alive,
    process_start_ticks,
)
from .persistence_utils import atomic_write_json, now_utc_iso, timestamped_token

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"
ADMISSION_TOKEN_ENV_VAR = "ORCA_AUTO_ADMISSION_TOKEN"

logger = logging.getLogger(__name__)


class AdmissionLimitReachedError(RuntimeError):
    """Raised when the global admission cap is already exhausted."""


class AdmissionSlot(TypedDict, total=False):
    token: str
    state: str
    work_dir: str | None
    reaction_dir: str | None
    queue_id: str | None
    owner_pid: int
    process_start_ticks: int | None
    source: str
    acquired_at: str
    app_name: str | None
    task_id: str | None
    workflow_id: str | None


def _admission_path(root: Path) -> Path:
    return root / ADMISSION_FILE_NAME


def _lock_path(root: Path) -> Path:
    return root / ADMISSION_LOCK_NAME


@contextmanager
def _acquire_admission_lock(root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    lock_path = _lock_path(root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    with lock_path.open("a+", encoding="utf-8") as handle:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out acquiring lock: {lock_path}")
                time.sleep(0.1)

        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\nacquired_at={now_utc_iso()}\n")
        handle.flush()
        os.fsync(handle.fileno())
        logger.debug("Admission lock acquired: %s", lock_path)
        try:
            yield
        finally:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            logger.debug("Admission lock released: %s", lock_path)


def _load_slots(root: Path) -> List[AdmissionSlot]:
    path = _admission_path(root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [cast(AdmissionSlot, slot) for slot in raw if isinstance(slot, dict)]


def _save_slots(root: Path, slots: List[AdmissionSlot]) -> None:
    atomic_write_json(_admission_path(root), slots, ensure_ascii=True, indent=2)


def _normalize_work_dir(value: str | Path | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def _normalize_slot(slot: AdmissionSlot) -> AdmissionSlot:
    normalized = dict(slot)
    work_dir = _normalize_work_dir(
        normalized.get("work_dir") or normalized.get("reaction_dir")
    )
    if work_dir is not None:
        normalized["work_dir"] = work_dir
        normalized["reaction_dir"] = work_dir
    return cast(AdmissionSlot, normalized)


def _slot_owner_alive(slot: AdmissionSlot) -> bool:
    pid = slot.get("owner_pid")
    if not isinstance(pid, int) or pid <= 0:
        return False
    if not is_process_alive(pid):
        return False

    expected_ticks = slot.get("process_start_ticks")
    if isinstance(expected_ticks, int) and expected_ticks > 0:
        observed_ticks = process_start_ticks(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            return False
    return True


def reconcile_stale_slots(root: Path) -> int:
    resolved_root = Path(root).expanduser().resolve()
    with _acquire_admission_lock(resolved_root):
        slots = [_normalize_slot(slot) for slot in _load_slots(resolved_root)]
        kept = [slot for slot in slots if _slot_owner_alive(slot)]
        removed = len(slots) - len(kept)
        if removed:
            _save_slots(resolved_root, kept)
    return removed


def list_slots(root: Path) -> List[AdmissionSlot]:
    resolved_root = Path(root).expanduser().resolve()
    with _acquire_admission_lock(resolved_root):
        slots = [_normalize_slot(slot) for slot in _load_slots(resolved_root)]
        kept = [slot for slot in slots if _slot_owner_alive(slot)]
        if len(kept) != len(slots):
            _save_slots(resolved_root, kept)
        return kept


def active_slot_count(root: Path) -> int:
    return len(list_slots(root))


def reserve_slot(
    root: Path,
    max_concurrent: int,
    *,
    reaction_dir: str | None = None,
    queue_id: str | None = None,
    source: str,
    owner_pid: int | None = None,
    exclude_reaction_dirs: set[str] | None = None,
    app_name: str | None = "orca_auto",
    task_id: str | None = None,
    workflow_id: str | None = None,
    state: str = "reserved",
) -> str | None:
    del exclude_reaction_dirs
    resolved_root = Path(root).expanduser().resolve()
    limit = max(1, int(max_concurrent))
    with _acquire_admission_lock(resolved_root):
        slots = [
            _normalize_slot(slot)
            for slot in _load_slots(resolved_root)
            if _slot_owner_alive(_normalize_slot(slot))
        ]
        if len(slots) >= limit:
            _save_slots(resolved_root, slots)
            return None

        token = timestamped_token("slot")
        resolved_work_dir = _normalize_work_dir(reaction_dir)
        resolved_owner_pid = owner_pid if owner_pid is not None else os.getpid()
        slot: AdmissionSlot = {
            "token": token,
            "state": state,
            "work_dir": resolved_work_dir,
            "reaction_dir": resolved_work_dir,
            "queue_id": queue_id,
            "owner_pid": resolved_owner_pid,
            "process_start_ticks": (
                process_start_ticks(resolved_owner_pid)
                if owner_pid is not None
                else current_process_start_ticks()
            ),
            "source": source,
            "acquired_at": now_utc_iso(),
            "app_name": app_name,
            "task_id": task_id,
            "workflow_id": workflow_id,
        }
        slots.append(slot)
        _save_slots(resolved_root, slots)
        return token


def activate_slot(
    root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    owner_pid: int | None = None,
    queue_id: str | None = None,
) -> bool:
    resolved_root = Path(root).expanduser().resolve()
    resolved_work_dir = _normalize_work_dir(reaction_dir)
    resolved_owner_pid = owner_pid if owner_pid is not None else os.getpid()
    with _acquire_admission_lock(resolved_root):
        slots = [
            _normalize_slot(slot)
            for slot in _load_slots(resolved_root)
            if _slot_owner_alive(_normalize_slot(slot))
        ]
        for slot in slots:
            if slot.get("token") != token:
                continue
            slot["state"] = "active"
            slot["work_dir"] = resolved_work_dir
            slot["reaction_dir"] = resolved_work_dir
            slot["queue_id"] = queue_id
            slot["owner_pid"] = resolved_owner_pid
            slot["process_start_ticks"] = process_start_ticks(resolved_owner_pid)
            slot["source"] = source
            _save_slots(resolved_root, slots)
            return True
        _save_slots(resolved_root, slots)
    return False


def release_slot(root: Path, token: str) -> bool:
    resolved_root = Path(root).expanduser().resolve()
    with _acquire_admission_lock(resolved_root):
        slots = [
            _normalize_slot(slot)
            for slot in _load_slots(resolved_root)
            if _slot_owner_alive(_normalize_slot(slot))
        ]
        kept = [slot for slot in slots if slot.get("token") != token]
        removed = len(kept) != len(slots)
        if removed or len(kept) != len(slots):
            _save_slots(resolved_root, kept)
        return removed


@contextmanager
def acquire_direct_slot(
    root: Path,
    max_concurrent: int,
    *,
    reaction_dir: str,
    source: str = "direct_run",
) -> Iterator[str]:
    resolved_reaction_dir = str(Path(reaction_dir).expanduser().resolve())
    token = reserve_slot(
        root,
        max_concurrent,
        reaction_dir=resolved_reaction_dir,
        source=source,
        state="reserved",
    )
    if token is None:
        raise AdmissionLimitReachedError(
            f"Global admission limit reached under {root} (max_concurrent={max(1, int(max_concurrent))})."
        )

    activated = activate_slot(
        root,
        token,
        reaction_dir=resolved_reaction_dir,
        source=source,
    )
    if not activated:
        release_slot(root, token)
        raise AdmissionLimitReachedError(
            f"Failed to activate admission slot for {resolved_reaction_dir}."
        )

    try:
        yield token
    finally:
        release_slot(root, token)


@contextmanager
def activate_reserved_slot(
    root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    queue_id: str | None = None,
) -> Iterator[str]:
    activated = activate_slot(
        root,
        token,
        reaction_dir=reaction_dir,
        source=source,
        queue_id=queue_id,
    )
    if not activated:
        release_slot(root, token)
        raise AdmissionLimitReachedError(
            f"Failed to activate reserved admission slot for {reaction_dir}."
        )

    try:
        yield token
    finally:
        release_slot(root, token)
