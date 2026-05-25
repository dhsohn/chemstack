"""Shared admission slot store for enforcing a hard active-run cap."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, List, TypedDict, cast

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME
from chemstack.core.admission import store as _core_admission_store
from chemstack.core.utils.lock import file_lock as _core_file_lock

from .lock_utils import (
    current_process_start_ticks,
    is_process_alive,
    process_start_ticks,
)
from .persistence_utils import atomic_write_json, now_utc_iso, timestamped_token
from .process_tracking import RUN_LOCK_FILE_NAME, active_run_lock_pid
from . import admission_backend_adapter as _admission_backend_adapter_module
from . import admission_slot_adapter as _admission_slot_adapter

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"
ADMISSION_TOKEN_ENV_VAR = "ORCA_AUTO_ADMISSION_TOKEN"
ADMISSION_APP_NAME_ENV_VAR = "ORCA_AUTO_ADMISSION_APP_NAME"
ADMISSION_TASK_ID_ENV_VAR = "ORCA_AUTO_ADMISSION_TASK_ID"

logger = logging.getLogger(__name__)


class AdmissionLimitReachedError(RuntimeError):
    """Raised when the global admission cap is already exhausted."""


class AdmissionStoreCorruptError(RuntimeError):
    """Raised when the admission slot file cannot be safely loaded."""


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


@dataclass(frozen=True)
class _AdmissionReservationRequest:
    root: Path
    max_concurrent: int
    reaction_dir: str | None
    queue_id: str | None
    source: str
    owner_pid: int | None
    exclude_reaction_dirs: set[str] | None
    app_name: str | None
    task_id: str | None
    workflow_id: str | None
    state: str

    @property
    def limit(self) -> int:
        return max(1, int(self.max_concurrent))

    @property
    def excluded_reaction_dirs(self) -> set[str]:
        return _normalize_reaction_dir_set(self.exclude_reaction_dirs)


@dataclass(frozen=True)
class _AdmissionActivationRequest:
    root: Path
    token: str
    work_dir: str
    owner_pid: int
    source: str
    queue_id: str | None
    app_name: str | None
    task_id: str | None
    workflow_id: str | None

    @property
    def has_metadata_update(self) -> bool:
        return self.app_name is not None or self.task_id is not None or self.workflow_id is not None


_AdmissionBackendDeps = _admission_backend_adapter_module.AdmissionBackendAdapter


def _admission_backend_adapter() -> _AdmissionBackendDeps:
    return _admission_backend_adapter_module.AdmissionBackendAdapter(
        AdmissionStoreCorruptError=AdmissionStoreCorruptError,
        atomic_write_json=atomic_write_json,
        _admission_path=_admission_path,
        _chem_core_admission_module=_chem_core_admission_module,
        _normalize_slot=_normalize_slot,
    )


def _admission_backend_deps() -> _AdmissionBackendDeps:
    return _admission_backend_adapter()


def _wrap_backend_corruption(exc: Exception) -> None:
    _admission_backend_adapter()._wrap_backend_corruption(exc)


def _admission_path(root: Path) -> Path:
    return root / ADMISSION_FILE_NAME


def _lock_path(root: Path) -> Path:
    return root / ADMISSION_LOCK_NAME


def _resolve_root(root: str | Path) -> Path:
    return Path(root).expanduser().resolve()


@contextmanager
def _acquire_admission_lock(root: Path, *, timeout_seconds: int = 10) -> Iterator[None]:
    lock_path = _lock_path(root)
    with _core_file_lock(lock_path, timeout_seconds=float(timeout_seconds)):
        logger.debug("Admission lock acquired: %s", lock_path)
        try:
            yield
        finally:
            logger.debug("Admission lock released: %s", lock_path)


def _load_slots(root: Path) -> List[AdmissionSlot]:
    return cast(List[AdmissionSlot], _admission_backend_adapter().load_slots(root))


def _save_slots(root: Path, slots: List[AdmissionSlot]) -> None:
    _admission_backend_adapter().save_slots(root, slots)


def _chem_core_admission_module() -> Any | None:
    return _core_admission_store


def _call_chem_core_backend(
    root: Path,
    function_name: str,
    *args: Any,
    convert: Any = None,
    **kwargs: Any,
) -> Any | None:
    return _admission_backend_adapter().call_backend(
        root,
        function_name,
        *args,
        convert=convert,
        **kwargs,
    )


def _backend_list_slots(root: Path, *, backend: Any) -> list[AdmissionSlot] | None:
    return cast(
        list[AdmissionSlot] | None,
        _admission_backend_adapter().backend_list_slots(root, backend=backend),
    )


def _backend_reconcile_stale_slots(root: Path, *, backend: Any) -> int | None:
    return _admission_backend_adapter().backend_reconcile_stale_slots(root, backend=backend)


def _backend_active_slot_count(root: Path, *, backend: Any) -> int | None:
    return _admission_backend_adapter().backend_active_slot_count(root, backend=backend)


def _text_field(value: object) -> str:
    return _admission_backend_adapter().text_field(value)


def _to_chem_core_slot(slot: AdmissionSlot, *, backend: Any) -> Any:
    return _admission_backend_adapter()._to_chem_core_slot(slot, backend=backend)


def _from_chem_core_slot(slot: object) -> AdmissionSlot:
    return cast(AdmissionSlot, _admission_backend_adapter()._from_chem_core_slot(slot))


def _normalize_work_dir(value: str | Path | None) -> str | None:
    return _admission_slot_adapter.normalize_work_dir(value)


def _normalize_slot(slot: AdmissionSlot) -> AdmissionSlot:
    return cast(AdmissionSlot, _admission_slot_adapter.normalize_slot(dict(slot)))


def _slot_reaction_dir(slot: AdmissionSlot) -> str | None:
    return _admission_slot_adapter.slot_reaction_dir(dict(slot))


def _normalize_reaction_dir_set(reaction_dirs: set[str] | None) -> set[str]:
    return _admission_slot_adapter.normalize_reaction_dir_set(reaction_dirs)


def _count_external_active_runs(
    root: Path,
    *,
    represented_reaction_dirs: set[str],
    exclude_reaction_dirs: set[str],
) -> int:
    if not root.is_dir():
        return 0

    count = 0
    seen_reaction_dirs: set[str] = set()
    for lock_path in root.rglob(RUN_LOCK_FILE_NAME):
        reaction_dir = str(lock_path.parent.resolve())
        if reaction_dir in exclude_reaction_dirs:
            continue
        if reaction_dir in represented_reaction_dirs or reaction_dir in seen_reaction_dirs:
            continue
        if active_run_lock_pid(lock_path.parent, logger=logger) is None:
            continue
        seen_reaction_dirs.add(reaction_dir)
        count += 1
    return count


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


def _load_live_slots(root: Path) -> list[AdmissionSlot]:
    live_slots: list[AdmissionSlot] = []
    for slot in _load_slots(root):
        normalized = _normalize_slot(slot)
        if _slot_owner_alive(normalized):
            live_slots.append(normalized)
    return live_slots


def _counted_slots(
    slots: list[AdmissionSlot],
    *,
    exclude_reaction_dirs: set[str],
) -> tuple[list[AdmissionSlot], set[str]]:
    counted: list[AdmissionSlot] = []
    represented_reaction_dirs: set[str] = set()
    for existing_slot in slots:
        slot_reaction_dir = _slot_reaction_dir(existing_slot)
        if slot_reaction_dir is not None and slot_reaction_dir in exclude_reaction_dirs:
            continue
        counted.append(existing_slot)
        if slot_reaction_dir is not None:
            represented_reaction_dirs.add(slot_reaction_dir)
    return counted, represented_reaction_dirs


def _slot_owner_identity(owner_pid: int | None) -> tuple[int, int | None]:
    resolved_owner_pid = owner_pid if owner_pid is not None else os.getpid()
    if owner_pid is not None:
        return resolved_owner_pid, process_start_ticks(resolved_owner_pid)
    return resolved_owner_pid, current_process_start_ticks()


def _build_reserved_slot(
    *,
    token: str,
    reaction_dir: str | None,
    queue_id: str | None,
    source: str,
    owner_pid: int | None,
    app_name: str | None,
    task_id: str | None,
    workflow_id: str | None,
    state: str,
) -> AdmissionSlot:
    resolved_work_dir = _normalize_work_dir(reaction_dir)
    resolved_owner_pid, resolved_start_ticks = _slot_owner_identity(owner_pid)
    return cast(
        AdmissionSlot,
        _admission_backend_adapter().build_reserved_slot(
            token=token,
            work_dir=resolved_work_dir,
            queue_id=queue_id,
            source=source,
            owner_pid=resolved_owner_pid,
            process_start_ticks=resolved_start_ticks,
            acquired_at=now_utc_iso(),
            app_name=app_name,
            task_id=task_id,
            workflow_id=workflow_id,
            state=state,
        ),
    )


def _active_count_with_external_runs(
    root: Path,
    *,
    slots: list[AdmissionSlot],
    exclude_reaction_dirs: set[str],
) -> int:
    counted, represented_reaction_dirs = _counted_slots(
        slots,
        exclude_reaction_dirs=exclude_reaction_dirs,
    )
    return len(counted) + _count_external_active_runs(
        root,
        represented_reaction_dirs=represented_reaction_dirs,
        exclude_reaction_dirs=exclude_reaction_dirs,
    )


def _reservation_has_capacity(
    request: _AdmissionReservationRequest,
    *,
    slots: list[AdmissionSlot],
) -> bool:
    active_count = _active_count_with_external_runs(
        request.root,
        slots=slots,
        exclude_reaction_dirs=request.excluded_reaction_dirs,
    )
    return active_count < request.limit


def _build_slot_for_reservation(
    token: str,
    request: _AdmissionReservationRequest,
) -> AdmissionSlot:
    return _build_reserved_slot(
        token=token,
        reaction_dir=request.reaction_dir,
        queue_id=request.queue_id,
        source=request.source,
        owner_pid=request.owner_pid,
        app_name=request.app_name,
        task_id=request.task_id,
        workflow_id=request.workflow_id,
        state=request.state,
    )


def _reserve_slot_from_request(request: _AdmissionReservationRequest) -> str | None:
    with _acquire_admission_lock(request.root):
        slots = _load_live_slots(request.root)
        if not _reservation_has_capacity(request, slots=slots):
            _save_slots(request.root, slots)
            return None

        token = timestamped_token("slot")
        slots.append(_build_slot_for_reservation(token, request))
        _save_slots(request.root, slots)
        return token


def reconcile_stale_slots(root: Path) -> int:
    resolved_root = _resolve_root(root)
    delegated = _call_chem_core_backend(resolved_root, "reconcile_stale_slots", convert=int)
    if delegated is not None:
        return delegated
    with _acquire_admission_lock(resolved_root):
        original_slots = [_normalize_slot(slot) for slot in _load_slots(resolved_root)]
        kept = _load_live_slots(resolved_root)
        removed = len(original_slots) - len(kept)
        if removed:
            _save_slots(resolved_root, kept)
    return removed


def list_slots(root: Path) -> List[AdmissionSlot]:
    resolved_root = _resolve_root(root)
    delegated = _call_chem_core_backend(
        resolved_root,
        "list_slots",
        convert=lambda slots: [_from_chem_core_slot(slot) for slot in slots],
    )
    if delegated is not None:
        return delegated
    with _acquire_admission_lock(resolved_root):
        original_slots = [_normalize_slot(slot) for slot in _load_slots(resolved_root)]
        kept = _load_live_slots(resolved_root)
        if len(kept) != len(original_slots):
            _save_slots(resolved_root, kept)
        return kept


def active_slot_count(root: Path) -> int:
    resolved_root = _resolve_root(root)
    delegated = _call_chem_core_backend(resolved_root, "active_slot_count", convert=int)
    if delegated is not None:
        return delegated
    return len(list_slots(resolved_root))


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
    resolved_root = _resolve_root(root)
    request = _AdmissionReservationRequest(
        root=resolved_root,
        max_concurrent=max_concurrent,
        reaction_dir=reaction_dir,
        queue_id=queue_id,
        source=source,
        owner_pid=owner_pid,
        exclude_reaction_dirs=exclude_reaction_dirs,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
        state=state,
    )
    return _reserve_slot_from_request(request)


def _activation_request(
    root: Path,
    token: str,
    *,
    reaction_dir: str,
    source: str,
    owner_pid: int | None,
    queue_id: str | None,
    app_name: str | None,
    task_id: str | None,
    workflow_id: str | None,
) -> _AdmissionActivationRequest:
    resolved_root = _resolve_root(root)
    resolved_work_dir = _normalize_work_dir(reaction_dir)
    if resolved_work_dir is None:
        raise ValueError("reaction_dir must not be blank.")
    return _AdmissionActivationRequest(
        root=resolved_root,
        token=token,
        work_dir=resolved_work_dir,
        owner_pid=owner_pid if owner_pid is not None else os.getpid(),
        source=source,
        queue_id=queue_id,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
    )


def _activate_slot_with_backend(
    backend: Any,
    request: _AdmissionActivationRequest,
) -> bool:
    return _admission_backend_adapter().activate_reserved_slot(
        backend,
        request,
        update_slot_metadata=update_slot_metadata,
    )


def _activate_live_slot(
    slot: AdmissionSlot,
    request: _AdmissionActivationRequest,
) -> None:
    slot["state"] = "active"
    slot["work_dir"] = request.work_dir
    slot["reaction_dir"] = request.work_dir
    if request.queue_id is not None:
        slot["queue_id"] = request.queue_id
    slot["owner_pid"] = request.owner_pid
    slot["process_start_ticks"] = process_start_ticks(request.owner_pid)
    slot["source"] = request.source
    if request.app_name is not None:
        slot["app_name"] = request.app_name
    if request.task_id is not None:
        slot["task_id"] = request.task_id
    if request.workflow_id is not None:
        slot["workflow_id"] = request.workflow_id


def _activate_slot_in_store(
    request: _AdmissionActivationRequest,
) -> bool:
    with _acquire_admission_lock(request.root):
        slots = _load_live_slots(request.root)
        for slot in slots:
            if slot.get("token") != request.token:
                continue
            _activate_live_slot(slot, request)
            _save_slots(request.root, slots)
            return True
        _save_slots(request.root, slots)
    return False


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
    request = _activation_request(
        root,
        token,
        reaction_dir=reaction_dir,
        source=source,
        owner_pid=owner_pid,
        queue_id=queue_id,
        app_name=app_name,
        task_id=task_id,
        workflow_id=workflow_id,
    )
    backend = _chem_core_admission_module()
    if backend is not None:
        return _activate_slot_with_backend(backend, request)

    return _activate_slot_in_store(request)


def release_slot(root: Path, token: str) -> bool:
    resolved_root = _resolve_root(root)
    delegated = _call_chem_core_backend(resolved_root, "release_slot", token, convert=bool)
    if delegated is not None:
        return delegated

    with _acquire_admission_lock(resolved_root):
        slots = _load_live_slots(resolved_root)
        kept = [slot for slot in slots if slot.get("token") != token]
        removed = len(kept) != len(slots)
        if removed:
            _save_slots(resolved_root, kept)
        return removed


def update_slot_metadata(
    root: Path,
    token: str,
    *,
    queue_id: str | None = None,
    app_name: str | None = None,
    task_id: str | None = None,
    workflow_id: str | None = None,
) -> bool:
    resolved_root = _resolve_root(root)
    with _acquire_admission_lock(resolved_root):
        slots = _load_live_slots(resolved_root)
        for slot in slots:
            if slot.get("token") != token:
                continue
            if queue_id is not None:
                slot["queue_id"] = queue_id
            if app_name is not None:
                slot["app_name"] = app_name
            if task_id is not None:
                slot["task_id"] = task_id
            if workflow_id is not None:
                slot["workflow_id"] = workflow_id
            _save_slots(resolved_root, slots)
            return True
        _save_slots(resolved_root, slots)
    return False


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
