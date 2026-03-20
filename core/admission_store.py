"""Global admission slot store for enforcing a hard active-run cap."""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, TypedDict
from uuid import uuid4

from .lock_utils import (
    acquire_file_lock,
    current_process_start_ticks,
    is_process_alive,
    parse_lock_info,
    process_start_ticks,
)
from .state_store import LOCK_FILE_NAME, atomic_write_text

logger = logging.getLogger(__name__)

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"
ADMISSION_TOKEN_ENV_VAR = "ORCA_AUTO_ADMISSION_TOKEN"


class AdmissionLimitReachedError(RuntimeError):
    """Raised when the global admission cap is already exhausted."""


class AdmissionSlot(TypedDict, total=False):
    token: str
    state: str
    reaction_dir: str | None
    queue_id: str | None
    owner_pid: int
    process_start_ticks: int | None
    source: str
    acquired_at: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _admission_path(allowed_root: Path) -> Path:
    return allowed_root / ADMISSION_FILE_NAME


def _lock_path(allowed_root: Path) -> Path:
    return allowed_root / ADMISSION_LOCK_NAME


def _admission_lock_active_error(lock_pid: int, lock_info: dict, lock_path: Path) -> RuntimeError:
    return RuntimeError(
        f"Admission lock is held by active process (pid={lock_pid}). Lock: {lock_path}"
    )


def _admission_lock_unreadable_error(lock_path: Path) -> RuntimeError:
    return RuntimeError(f"Admission lock file unreadable. Remove manually: {lock_path}")


def _admission_lock_stale_remove_error(lock_pid: int, lock_path: Path, exc: OSError) -> RuntimeError:
    return RuntimeError(
        f"Failed to remove stale admission lock (pid={lock_pid}): {lock_path}. error={exc}"
    )


@contextmanager
def _acquire_admission_lock(allowed_root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    lp = _lock_path(allowed_root)
    payload = {"pid": os.getpid(), "started_at": _now_iso()}
    ticks = current_process_start_ticks()
    if ticks is not None:
        payload["process_start_ticks"] = ticks

    with acquire_file_lock(
        lock_path=lp,
        lock_payload_obj=payload,
        parse_lock_info_fn=parse_lock_info,
        is_process_alive_fn=is_process_alive,
        process_start_ticks_fn=process_start_ticks,
        logger=logger,
        acquired_log_template="Admission lock acquired: %s",
        released_log_template="Admission lock released: %s",
        stale_pid_reuse_log_template=(
            "Stale admission lock (PID reuse, pid=%d, expected=%d, observed=%d): %s"
        ),
        stale_lock_log_template="Stale admission lock (pid=%d), removing: %s",
        timeout_seconds=timeout_seconds,
        active_lock_error_builder=_admission_lock_active_error,
        unreadable_lock_error_builder=_admission_lock_unreadable_error,
        stale_remove_error_builder=_admission_lock_stale_remove_error,
    ):
        yield


def _load_slots(allowed_root: Path) -> List[AdmissionSlot]:
    path = _admission_path(allowed_root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Failed to parse admission slot file, starting fresh: %s", path)
        return []
    if not isinstance(raw, list):
        return []
    return [slot for slot in raw if isinstance(slot, dict)]


def _save_slots(allowed_root: Path, slots: List[AdmissionSlot]) -> None:
    atomic_write_text(_admission_path(allowed_root), json.dumps(slots, ensure_ascii=True, indent=2))


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
            logger.info(
                "Ignoring stale admission slot due to PID reuse: token=%s pid=%d expected=%d observed=%s",
                slot.get("token"),
                pid,
                expected_ticks,
                observed_ticks,
            )
            return False
    return True


def _active_lock_pid(reaction_dir: Path) -> int | None:
    lock_info = parse_lock_info(reaction_dir / LOCK_FILE_NAME)
    pid = lock_info.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return None
    if not is_process_alive(pid):
        return None

    expected_ticks = lock_info.get("process_start_ticks")
    if isinstance(expected_ticks, int) and expected_ticks > 0:
        observed_ticks = process_start_ticks(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            return None
    return pid


def _ticks_for_owner(owner_pid: int | None) -> int | None:
    if owner_pid is None or owner_pid == os.getpid():
        return current_process_start_ticks()
    return process_start_ticks(owner_pid)


def _reaction_dir_from_slot(slot: AdmissionSlot) -> str | None:
    reaction_dir = slot.get("reaction_dir")
    if not isinstance(reaction_dir, str) or not reaction_dir.strip():
        return None
    return str(Path(reaction_dir).expanduser().resolve())


def _sync_slots_with_run_locks(
    allowed_root: Path,
    slots: List[AdmissionSlot],
    *,
    exclude_reaction_dirs: set[str] | None = None,
) -> tuple[List[AdmissionSlot], int]:
    if not allowed_root.is_dir():
        return slots, 0

    excluded = {
        str(Path(reaction_dir).expanduser().resolve())
        for reaction_dir in (exclude_reaction_dirs or set())
    }
    active_reaction_dirs = {
        reaction_dir
        for reaction_dir in (_reaction_dir_from_slot(slot) for slot in slots)
        if reaction_dir is not None
    }
    changed = 0

    for lock_path in allowed_root.rglob(LOCK_FILE_NAME):
        reaction_dir = str(lock_path.parent.resolve())
        if reaction_dir in excluded:
            continue
        if reaction_dir in active_reaction_dirs:
            continue

        pid = _active_lock_pid(lock_path.parent)
        if pid is None:
            continue

        lock_info = parse_lock_info(lock_path)
        expected_ticks = lock_info.get("process_start_ticks")
        observed_ticks = process_start_ticks(pid)
        slot: AdmissionSlot = {
            "token": f"inferred_{uuid4().hex[:12]}",
            "state": "active",
            "reaction_dir": reaction_dir,
            "queue_id": None,
            "owner_pid": pid,
            "process_start_ticks": expected_ticks if isinstance(expected_ticks, int) and expected_ticks > 0 else observed_ticks,
            "source": "run_lock_inferred",
            "acquired_at": _now_iso(),
        }
        slots.append(slot)
        active_reaction_dirs.add(reaction_dir)
        changed += 1

    return slots, changed


def _dedupe_slots(slots: List[AdmissionSlot]) -> tuple[List[AdmissionSlot], int]:
    deduped: List[AdmissionSlot] = []
    reaction_dir_index: dict[str, int] = {}
    removed = 0

    for slot in slots:
        reaction_dir = _reaction_dir_from_slot(slot)
        if reaction_dir is None:
            deduped.append(slot)
            continue

        existing_idx = reaction_dir_index.get(reaction_dir)
        if existing_idx is None:
            reaction_dir_index[reaction_dir] = len(deduped)
            deduped.append(slot)
            continue

        existing = deduped[existing_idx]
        existing_inferred = existing.get("source") == "run_lock_inferred"
        current_inferred = slot.get("source") == "run_lock_inferred"
        if existing_inferred and not current_inferred:
            deduped[existing_idx] = slot
        removed += 1

    return deduped, removed


def _reconcile_slots(
    allowed_root: Path,
    slots: List[AdmissionSlot],
    *,
    exclude_reaction_dirs: set[str] | None = None,
) -> tuple[List[AdmissionSlot], int]:
    reconciled: List[AdmissionSlot] = []
    removed = 0
    for slot in slots:
        if slot.get("source") == "run_lock_inferred":
            reaction_dir = _reaction_dir_from_slot(slot)
            if reaction_dir is None or _active_lock_pid(Path(reaction_dir)) is None:
                removed += 1
                continue
        if _slot_owner_alive(slot):
            reconciled.append(slot)
        else:
            removed += 1
    reconciled, inferred = _sync_slots_with_run_locks(
        allowed_root,
        reconciled,
        exclude_reaction_dirs=exclude_reaction_dirs,
    )
    reconciled, deduped = _dedupe_slots(reconciled)
    return reconciled, removed + inferred + deduped


def reconcile_stale_slots(allowed_root: Path) -> int:
    with _acquire_admission_lock(allowed_root):
        slots, changed = _reconcile_slots(allowed_root, _load_slots(allowed_root))
        if changed:
            _save_slots(allowed_root, slots)
    return changed


def list_slots(allowed_root: Path) -> List[AdmissionSlot]:
    with _acquire_admission_lock(allowed_root):
        slots, changed = _reconcile_slots(allowed_root, _load_slots(allowed_root))
        if changed:
            _save_slots(allowed_root, slots)
    return slots


def active_slot_count(allowed_root: Path) -> int:
    return len(list_slots(allowed_root))


def reserve_slot(
    allowed_root: Path,
    max_concurrent: int,
    *,
    reaction_dir: str | None = None,
    queue_id: str | None = None,
    source: str,
    owner_pid: int | None = None,
    exclude_reaction_dirs: set[str] | None = None,
) -> str | None:
    limit = max(1, int(max_concurrent))
    with _acquire_admission_lock(allowed_root):
        slots, changed = _reconcile_slots(
            allowed_root,
            _load_slots(allowed_root),
            exclude_reaction_dirs=exclude_reaction_dirs,
        )
        if len(slots) >= limit:
            if changed:
                _save_slots(allowed_root, slots)
            return None

        token = f"slot_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
        slot: AdmissionSlot = {
            "token": token,
            "state": "reserved",
            "reaction_dir": str(Path(reaction_dir).expanduser().resolve()) if reaction_dir else None,
            "queue_id": queue_id,
            "owner_pid": owner_pid if owner_pid is not None else os.getpid(),
            "process_start_ticks": _ticks_for_owner(owner_pid),
            "source": source,
            "acquired_at": _now_iso(),
        }
        slots.append(slot)
        _save_slots(allowed_root, slots)
        logger.info("Admission slot reserved: token=%s source=%s queue_id=%s", token, source, queue_id)
        return token


def activate_slot(
    allowed_root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    owner_pid: int | None = None,
    queue_id: str | None = None,
) -> bool:
    resolved_reaction_dir = str(Path(reaction_dir).expanduser().resolve())
    resolved_owner_pid = owner_pid if owner_pid is not None else os.getpid()
    with _acquire_admission_lock(allowed_root):
        slots, changed = _reconcile_slots(allowed_root, _load_slots(allowed_root))
        for slot in slots:
            if slot.get("token") != token:
                continue
            slot["state"] = "active"
            slot["reaction_dir"] = resolved_reaction_dir
            slot["queue_id"] = queue_id
            slot["owner_pid"] = resolved_owner_pid
            slot["process_start_ticks"] = _ticks_for_owner(owner_pid)
            slot["source"] = source
            _save_slots(allowed_root, slots)
            logger.info("Admission slot activated: token=%s source=%s", token, source)
            return True

        if changed:
            _save_slots(allowed_root, slots)
    return False


def release_slot(allowed_root: Path, token: str) -> bool:
    with _acquire_admission_lock(allowed_root):
        slots, changed = _reconcile_slots(allowed_root, _load_slots(allowed_root))
        original_len = len(slots)
        slots = [slot for slot in slots if slot.get("token") != token]
        removed = len(slots) != original_len
        if removed or changed:
            _save_slots(allowed_root, slots)
        if removed:
            logger.info("Admission slot released: token=%s", token)
        return removed


@contextmanager
def acquire_direct_slot(
    allowed_root: Path,
    max_concurrent: int,
    *,
    reaction_dir: str,
    source: str = "direct_run",
) -> Iterator[str]:
    resolved_reaction_dir = str(Path(reaction_dir).expanduser().resolve())
    token = reserve_slot(
        allowed_root,
        max_concurrent,
        reaction_dir=resolved_reaction_dir,
        source=source,
        exclude_reaction_dirs={resolved_reaction_dir},
    )
    if token is None:
        raise AdmissionLimitReachedError(
            f"Global admission limit reached under {allowed_root} (max_concurrent={max(1, int(max_concurrent))})."
        )

    activated = activate_slot(
        allowed_root,
        token,
        reaction_dir=resolved_reaction_dir,
        source=source,
    )
    if not activated:
        release_slot(allowed_root, token)
        raise AdmissionLimitReachedError(
            f"Failed to activate admission slot for {resolved_reaction_dir}."
        )

    try:
        yield token
    finally:
        release_slot(allowed_root, token)


@contextmanager
def activate_reserved_slot(
    allowed_root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    queue_id: str | None = None,
) -> Iterator[str]:
    activated = activate_slot(
        allowed_root,
        token,
        reaction_dir=reaction_dir,
        source=source,
        queue_id=queue_id,
    )
    if not activated:
        release_slot(allowed_root, token)
        raise AdmissionLimitReachedError(
            f"Failed to activate reserved admission slot for {reaction_dir}."
        )

    try:
        yield token
    finally:
        release_slot(allowed_root, token)
