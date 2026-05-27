"""ORCA admission helpers backed by the shared admission store."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from chemstack.core.admission import store as _core_admission
from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME

ADMISSION_FILE_NAME = _core_admission.ADMISSION_FILE_NAME
ADMISSION_LOCK_NAME = _core_admission.ADMISSION_LOCK_NAME
ADMISSION_TOKEN_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_TOKEN"
ADMISSION_APP_NAME_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_APP_NAME"
ADMISSION_TASK_ID_ENV_VAR = "CHEMSTACK_ORCA_ADMISSION_TASK_ID"

AdmissionLimitReachedError = _core_admission.AdmissionLimitReachedError
AdmissionStoreCorruptError = _core_admission.AdmissionStoreCorruptError

logger = logging.getLogger(__name__)


AdmissionSlot = _core_admission.AdmissionSlot


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


def reconcile_stale_slots(root: Path) -> int:
    return _core_admission.reconcile_stale_slots(root)


def list_slots(root: Path) -> list[AdmissionSlot]:
    return _core_admission.list_slots(root)


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


def release_slot(root: str | Path, token: str) -> bool:
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
