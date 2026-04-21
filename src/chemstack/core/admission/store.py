from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path

from ..utils.lock import file_lock
from ..utils.persistence import (
    atomic_write_json,
    coerce_int,
    coerce_optional_int,
    now_utc_iso,
    resolve_root_path,
    timestamped_token,
)

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"


class AdmissionLimitReachedError(RuntimeError):
    """Raised when no additional admission slots are available."""


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


def _admission_path(root: Path) -> Path:
    return root / ADMISSION_FILE_NAME


def _lock_path(root: Path) -> Path:
    return root / ADMISSION_LOCK_NAME


def _process_start_ticks(pid: int) -> int | None:
    stat_path = Path("/proc") / str(pid) / "stat"
    try:
        text = stat_path.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        return None
    if not text:
        return None
    right_paren = text.rfind(")")
    if right_paren < 0:
        return None
    fields_after_comm = text[right_paren + 2 :].split()
    if len(fields_after_comm) <= 19:
        return None
    try:
        value = int(fields_after_comm[19])
    except ValueError:
        return None
    return value if value > 0 else None


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
    path = _admission_path(root)
    if not path.exists():
        return []
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    return [_slot_from_dict(item) for item in raw if isinstance(item, dict)]


def _save_slots(root: Path, slots: list[AdmissionSlot]) -> None:
    atomic_write_json(root / ADMISSION_FILE_NAME, [_slot_to_dict(slot) for slot in slots], ensure_ascii=True, indent=2)


def _slot_owner_alive(slot: AdmissionSlot) -> bool:
    if slot.owner_pid <= 0:
        return False
    try:
        os.kill(slot.owner_pid, 0)
    except OSError:
        return False
    expected = slot.process_start_ticks
    if expected is None:
        return True
    observed = _process_start_ticks(slot.owner_pid)
    return observed is not None and observed == expected


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
        return _load_slots(resolved_root)


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
) -> str | None:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = [slot for slot in _load_slots(resolved_root) if _slot_owner_alive(slot)]
        if len(slots) >= max(1, int(limit)):
            _save_slots(resolved_root, slots)
            return None

        token = timestamped_token("slot")
        slots.append(
            AdmissionSlot(
                token=token,
                owner_pid=os.getpid(),
                process_start_ticks=_process_start_ticks(os.getpid()),
                source=source.strip(),
                acquired_at=now_utc_iso(),
                app_name=app_name.strip(),
                task_id=task_id.strip(),
                workflow_id=workflow_id.strip(),
                state=state.strip() or "active",
                work_dir=_normalize_work_dir(work_dir),
                queue_id=queue_id.strip(),
            )
        )
        _save_slots(resolved_root, slots)
        return token


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
) -> AdmissionSlot | None:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = [slot for slot in _load_slots(resolved_root) if _slot_owner_alive(slot)]
        for index, slot in enumerate(slots):
            if slot.token != token:
                continue
            resolved_owner_pid = owner_pid if owner_pid is not None else os.getpid()
            updated = replace(
                slot,
                state=state.strip() or slot.state or "active",
                work_dir=slot.work_dir if work_dir is None else _normalize_work_dir(work_dir),
                queue_id=slot.queue_id if queue_id is None else queue_id.strip(),
                owner_pid=resolved_owner_pid,
                process_start_ticks=_process_start_ticks(resolved_owner_pid),
                source=slot.source if source is None else source.strip(),
            )
            slots[index] = updated
            _save_slots(resolved_root, slots)
            return updated
    return None


def release_slot(root: str | Path, token: str) -> bool:
    resolved_root = resolve_root_path(root)
    with file_lock(_lock_path(resolved_root)):
        slots = [slot for slot in _load_slots(resolved_root) if _slot_owner_alive(slot)]
        kept = [slot for slot in slots if slot.token != token]
        removed = len(kept) != len(slots)
        if removed:
            _save_slots(resolved_root, kept)
        return removed
