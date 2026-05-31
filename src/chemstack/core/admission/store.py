from __future__ import annotations

import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Callable

from ..utils import process as process_utils
from ..utils.lock import file_lock
from ..utils.persistence import (
    atomic_write_json,
    coerce_int,
    coerce_optional_int,
    load_json_list_file,
    now_utc_iso,
    resolve_root_path,
    timestamped_token,
)

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"


class AdmissionLimitReachedError(RuntimeError):
    """Raised when no additional admission slots are available."""


class AdmissionStoreCorruptError(RuntimeError):
    """Raised when the admission slot file cannot be safely loaded."""


@dataclass(frozen=True)
class AdmissionSlot:
    token: str
    owner_pid: int
    process_start_ticks: int | None
    source: str
    acquired_at: str
    app_name: str = ""
    task_id: str = ""
    workflow_id: str = ""
    state: str = "active"
    work_dir: str = ""
    queue_id: str = ""


@dataclass(frozen=True)
class AdmissionReservationRequest:
    limit: int
    source: str
    app_name: str = ""
    task_id: str = ""
    workflow_id: str = ""
    state: str = "active"
    work_dir: str | Path = ""
    queue_id: str = ""
    owner_pid: int | None = None
    exclude_work_dirs: set[str] | None = None
    extra_active_count_fn: Callable[[Path, set[str], set[str]], int] | None = None


@dataclass(frozen=True)
class AdmissionSlotActivation:
    state: str = "active"
    work_dir: str | Path | None = None
    queue_id: str | None = None
    owner_pid: int | None = None
    source: str | None = None
    app_name: str | None = None
    task_id: str | None = None
    workflow_id: str | None = None


@dataclass(frozen=True)
class AdmissionSlotMetadataUpdate:
    queue_id: str | None = None
    app_name: str | None = None
    task_id: str | None = None
    workflow_id: str | None = None


def _admission_path(root: Path) -> Path:
    return root / ADMISSION_FILE_NAME


def _lock_path(root: Path) -> Path:
    return root / ADMISSION_LOCK_NAME


def _process_start_ticks(pid: int) -> int | None:
    return process_utils.process_start_ticks(pid, proc_root=Path("/proc"))


def _normalize_work_dir(value: str | Path | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def _slot_to_dict(slot: AdmissionSlot) -> dict[str, object]:
    return asdict(slot)


def _slot_from_dict(raw: dict[str, object]) -> AdmissionSlot:
    return AdmissionSlot(
        token=str(raw.get("token", "")).strip(),
        owner_pid=coerce_int(raw.get("owner_pid", 0), default=0),
        process_start_ticks=coerce_optional_int(raw.get("process_start_ticks")),
        source=str(raw.get("source", "")).strip(),
        acquired_at=str(raw.get("acquired_at", "")).strip(),
        app_name=str(raw.get("app_name", "")).strip(),
        task_id=str(raw.get("task_id", "")).strip(),
        workflow_id=str(raw.get("workflow_id", "")).strip(),
        state=str(raw.get("state", "active")).strip() or "active",
        work_dir=str(raw.get("work_dir", "")).strip(),
        queue_id=str(raw.get("queue_id", "")).strip(),
    )


def _load_slots(root: Path) -> list[AdmissionSlot]:
    raw = load_json_list_file(
        _admission_path(root),
        corrupt_error=AdmissionStoreCorruptError,
        description="Admission slot file",
    )
    return [_slot_from_dict(item) for item in raw if isinstance(item, dict)]


def _save_slots(root: Path, slots: list[AdmissionSlot]) -> None:
    atomic_write_json(root / ADMISSION_FILE_NAME, [_slot_to_dict(slot) for slot in slots], ensure_ascii=True, indent=2)


def _slot_owner_alive(slot: AdmissionSlot) -> bool:
    if slot.owner_pid <= 0:
        return False
    permission_denied = False
    try:
        os.kill(slot.owner_pid, 0)
    except PermissionError:
        permission_denied = True
    except OSError:
        return False
    expected = slot.process_start_ticks
    if expected is None:
        return True
    observed = _process_start_ticks(slot.owner_pid)
    if observed is None:
        return permission_denied
    return observed == expected


def _live_slots(root: Path) -> list[AdmissionSlot]:
    return [slot for slot in _load_slots(root) if _slot_owner_alive(slot)]


def _normalize_work_dir_set(work_dirs: set[str] | None) -> set[str]:
    normalized: set[str] = set()
    for work_dir in work_dirs or set():
        resolved = _normalize_work_dir(work_dir)
        if resolved:
            normalized.add(resolved)
    return normalized


def _counted_slots(
    slots: list[AdmissionSlot],
    *,
    exclude_work_dirs: set[str],
) -> tuple[list[AdmissionSlot], set[str]]:
    counted: list[AdmissionSlot] = []
    represented_work_dirs: set[str] = set()
    for slot in slots:
        if slot.work_dir and slot.work_dir in exclude_work_dirs:
            continue
        counted.append(slot)
        if slot.work_dir:
            represented_work_dirs.add(slot.work_dir)
    return counted, represented_work_dirs


def _reservation_limit_reached(
    resolved_root: Path,
    slots: list[AdmissionSlot],
    request: AdmissionReservationRequest,
) -> bool:
    excluded = _normalize_work_dir_set(request.exclude_work_dirs)
    counted, represented_work_dirs = _counted_slots(slots, exclude_work_dirs=excluded)
    extra_active_count = (
        request.extra_active_count_fn(resolved_root, represented_work_dirs, excluded)
        if request.extra_active_count_fn is not None
        else 0
    )
    return len(counted) + extra_active_count >= max(1, int(request.limit))


def _slot_from_reservation_request(request: AdmissionReservationRequest) -> AdmissionSlot:
    resolved_owner_pid = request.owner_pid if request.owner_pid is not None else os.getpid()
    return AdmissionSlot(
        token=timestamped_token("slot"),
        owner_pid=resolved_owner_pid,
        process_start_ticks=_process_start_ticks(resolved_owner_pid),
        source=request.source.strip(),
        acquired_at=now_utc_iso(),
        app_name=request.app_name.strip(),
        task_id=request.task_id.strip(),
        workflow_id=request.workflow_id.strip(),
        state=request.state.strip() or "active",
        work_dir=_normalize_work_dir(request.work_dir),
        queue_id=request.queue_id.strip(),
    )


def _activated_slot(slot: AdmissionSlot, update: AdmissionSlotActivation) -> AdmissionSlot:
    resolved_owner_pid = update.owner_pid if update.owner_pid is not None else os.getpid()
    return replace(
        slot,
        state=update.state.strip() or slot.state or "active",
        work_dir=slot.work_dir if update.work_dir is None else _normalize_work_dir(update.work_dir),
        queue_id=slot.queue_id if update.queue_id is None else update.queue_id.strip(),
        owner_pid=resolved_owner_pid,
        process_start_ticks=_process_start_ticks(resolved_owner_pid),
        source=slot.source if update.source is None else update.source.strip(),
        app_name=slot.app_name if update.app_name is None else update.app_name.strip(),
        task_id=slot.task_id if update.task_id is None else update.task_id.strip(),
        workflow_id=slot.workflow_id if update.workflow_id is None else update.workflow_id.strip(),
    )


def _metadata_updated_slot(
    slot: AdmissionSlot,
    update: AdmissionSlotMetadataUpdate,
) -> AdmissionSlot:
    return replace(
        slot,
        queue_id=slot.queue_id if update.queue_id is None else update.queue_id.strip(),
        app_name=slot.app_name if update.app_name is None else update.app_name.strip(),
        task_id=slot.task_id if update.task_id is None else update.task_id.strip(),
        workflow_id=slot.workflow_id if update.workflow_id is None else update.workflow_id.strip(),
    )


def reconcile_stale_slots(root: str | Path) -> int:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = _load_slots(resolved_root)
        kept = [slot for slot in slots if _slot_owner_alive(slot)]
        removed = len(slots) - len(kept)
        if removed:
            _save_slots(resolved_root, kept)
        return removed


def list_slots(root: str | Path) -> list[AdmissionSlot]:
    resolved_root = resolve_root_path(root)
    reconcile_stale_slots(resolved_root)
    with file_lock(_lock_path(resolved_root)):
        slots = _load_slots(resolved_root)
        if _admission_path(resolved_root).exists():
            _save_slots(resolved_root, slots)
        return slots


def active_slot_count(root: str | Path) -> int:
    return len(list_slots(root))


def reserve_slot(
    root: str | Path,
    limit: int,
    *,
    source: str,
    app_name: str = "",
    task_id: str = "",
    workflow_id: str = "",
    state: str = "active",
    work_dir: str | Path = "",
    queue_id: str = "",
    owner_pid: int | None = None,
    exclude_work_dirs: set[str] | None = None,
    extra_active_count_fn: Callable[[Path, set[str], set[str]], int] | None = None,
) -> str | None:
    return reserve_slot_from_request(
        root,
        AdmissionReservationRequest(
            limit=limit,
            source=source,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
            state=state,
            work_dir=work_dir,
            queue_id=queue_id,
            owner_pid=owner_pid,
            exclude_work_dirs=exclude_work_dirs,
            extra_active_count_fn=extra_active_count_fn,
        ),
    )


def reserve_slot_from_request(
    root: str | Path,
    request: AdmissionReservationRequest,
) -> str | None:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = _live_slots(resolved_root)
        if _reservation_limit_reached(resolved_root, slots, request):
            _save_slots(resolved_root, slots)
            return None

        slot = _slot_from_reservation_request(request)
        slots.append(slot)
        _save_slots(resolved_root, slots)
        return slot.token


def reserve_slot_or_raise(
    root: str | Path,
    limit: int,
    *,
    source: str,
    app_name: str = "",
    task_id: str = "",
    workflow_id: str = "",
    state: str = "active",
    work_dir: str | Path = "",
    queue_id: str = "",
    owner_pid: int | None = None,
    exclude_work_dirs: set[str] | None = None,
    extra_active_count_fn: Callable[[Path, set[str], set[str]], int] | None = None,
) -> str:
    token = reserve_slot(
        root,
        limit,
        source=source,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
        state=state,
        work_dir=work_dir,
        queue_id=queue_id,
        owner_pid=owner_pid,
        exclude_work_dirs=exclude_work_dirs,
        extra_active_count_fn=extra_active_count_fn,
    )
    if token is None:
        raise AdmissionLimitReachedError(f"Admission limit reached (limit={max(1, int(limit))})")
    return token


def activate_reserved_slot(
    root: str | Path,
    token: str,
    *,
    state: str = "active",
    work_dir: str | Path | None = None,
    queue_id: str | None = None,
    owner_pid: int | None = None,
    source: str | None = None,
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> AdmissionSlot | None:
    return activate_reserved_slot_with_update(
        root,
        token,
        AdmissionSlotActivation(
            state=state,
            work_dir=work_dir,
            queue_id=queue_id,
            owner_pid=owner_pid,
            source=source,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
        ),
    )


def activate_reserved_slot_with_update(
    root: str | Path,
    token: str,
    update: AdmissionSlotActivation,
) -> AdmissionSlot | None:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = _live_slots(resolved_root)
        for index, slot in enumerate(slots):
            if slot.token != token:
                continue
            updated = _activated_slot(slot, update)
            slots[index] = updated
            _save_slots(resolved_root, slots)
            return updated
    return None


def release_slot(root: str | Path, token: str) -> bool:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = _live_slots(resolved_root)
        kept = [slot for slot in slots if slot.token != token]
        removed = len(kept) != len(slots)
        if removed:
            _save_slots(resolved_root, kept)
        return removed


def update_slot_metadata(
    root: str | Path,
    token: str,
    *,
    queue_id: str | None = None,
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> AdmissionSlot | None:
    return update_slot_metadata_with_update(
        root,
        token,
        AdmissionSlotMetadataUpdate(
            queue_id=queue_id,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
        ),
    )


def update_slot_metadata_with_update(
    root: str | Path,
    token: str,
    update: AdmissionSlotMetadataUpdate,
) -> AdmissionSlot | None:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = _live_slots(resolved_root)
        for index, slot in enumerate(slots):
            if slot.token != token:
                continue
            updated = _metadata_updated_slot(slot, update)
            slots[index] = updated
            _save_slots(resolved_root, slots)
            return updated
        _save_slots(resolved_root, slots)
    return None
