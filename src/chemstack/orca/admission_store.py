"""ORCA admission helpers layered on the shared core admission store."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Iterator, TypedDict, cast

from chemstack.core.admission import store as _core_admission
from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME

from .process_tracking import RUN_LOCK_FILE_NAME, active_run_lock_pid

ADMISSION_FILE_NAME = _core_admission.ADMISSION_FILE_NAME
ADMISSION_LOCK_NAME = _core_admission.ADMISSION_LOCK_NAME
ADMISSION_TOKEN_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_TOKEN"
ADMISSION_APP_NAME_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_APP_NAME"
ADMISSION_TASK_ID_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_TASK_ID"

AdmissionLimitReachedError = _core_admission.AdmissionLimitReachedError
AdmissionStoreCorruptError = _core_admission.AdmissionStoreCorruptError

logger = logging.getLogger(__name__)


class AdmissionSlot(TypedDict, total=False):
    token: str
    state: str
    work_dir: str | None
    queue_id: str | None
    owner_pid: int
    process_start_ticks: int | None
    source: str
    acquired_at: str
    app_name: str | None
    task_id: str | None
    workflow_id: str | None


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


def _slot_payload(slot: _core_admission.AdmissionSlot) -> AdmissionSlot:
    payload = asdict(slot)
    payload.pop("reaction_dir", None)
    return cast(AdmissionSlot, payload)


def _slot_payloads(slots: list[_core_admission.AdmissionSlot]) -> list[AdmissionSlot]:
    return [_slot_payload(slot) for slot in slots]


def _count_external_active_runs(
    root: Path,
    represented_work_dirs: set[str],
    exclude_work_dirs: set[str],
) -> int:
    if not root.is_dir():
        return 0

    count = 0
    seen_work_dirs: set[str] = set()
    for lock_path in root.rglob(RUN_LOCK_FILE_NAME):
        work_dir = str(lock_path.parent.resolve())
        if work_dir in exclude_work_dirs:
            continue
        if work_dir in represented_work_dirs or work_dir in seen_work_dirs:
            continue
        if active_run_lock_pid(lock_path.parent, logger=logger) is None:
            continue
        seen_work_dirs.add(work_dir)
        count += 1
    return count


def reconcile_stale_slots(root: Path) -> int:
    return _core_admission.reconcile_stale_slots(root)


def list_slots(root: Path) -> list[AdmissionSlot]:
    return _slot_payloads(_core_admission.list_slots(root))


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
    app_name: str | None = CHEMSTACK_ORCA_APP_NAME,
    task_id: str | None = None,
    workflow_id: str | None = None,
    state: str = "reserved",
) -> str | None:
    return _core_admission.reserve_slot(
        root,
        max_concurrent,
        source=source,
        app_name=app_name or "",
        task_id=task_id or "",
        workflow_id=workflow_id or "",
        state=state,
        work_dir=reaction_dir or "",
        queue_id=queue_id or "",
        owner_pid=owner_pid,
        exclude_work_dirs=exclude_reaction_dirs,
        extra_active_count_fn=_count_external_active_runs,
    )


def activate_slot(
    root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    owner_pid: int | None = None,
    queue_id: str | None = None,
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> bool:
    work_dir = _normalize_work_dir(reaction_dir)
    if work_dir is None:
        raise ValueError("reaction_dir must not be blank.")
    return (
        _core_admission.activate_reserved_slot(
            root,
            token,
            state="active",
            work_dir=work_dir,
            queue_id=queue_id,
            owner_pid=owner_pid,
            source=source,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
        )
        is not None
    )


def release_slot(root: Path, token: str) -> bool:
    return _core_admission.release_slot(root, token)


def update_slot_metadata(
    root: Path,
    token: str,
    *,
    queue_id: str | None = None,
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> bool:
    return (
        _core_admission.update_slot_metadata(
            root,
            token,
            queue_id=queue_id,
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
        )
        is not None
    )


@contextmanager
def acquire_direct_slot(
    root: Path,
    max_concurrent: int,
    *,
    reaction_dir: str,
    source: str = "direct_run",
    app_name: str | None = CHEMSTACK_ORCA_APP_NAME,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> Iterator[str]:
    resolved_reaction_dir = str(Path(reaction_dir).expanduser().resolve())
    token = reserve_slot(
        root,
        max_concurrent,
        reaction_dir=resolved_reaction_dir,
        exclude_reaction_dirs={resolved_reaction_dir},
        source=source,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
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
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
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
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> Iterator[str]:
    activated = activate_slot(
        root,
        token,
        reaction_dir=reaction_dir,
        source=source,
        queue_id=queue_id,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
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
