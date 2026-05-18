from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from chemstack.core.admission.compat import (
    admission_slot_payload,
    int_field,
    optional_int_field,
    text_field,
)
from chemstack.core.utils.persistence import load_json_list_file


def load_slots(root: Path, *, deps: Any) -> list[Any]:
    backend = deps._chem_core_admission_module()
    backend_load_slots = getattr(backend, "_load_slots", None) if backend is not None else None
    if callable(backend_load_slots):
        try:
            return [deps._from_chem_core_slot(slot) for slot in backend_load_slots(root)]
        except Exception as exc:
            deps._wrap_backend_corruption(exc)
            raise

    raw = load_json_list_file(
        deps._admission_path(root),
        corrupt_error=deps.AdmissionStoreCorruptError,
        description="Admission slot file",
    )
    return [cast(Any, slot) for slot in raw if isinstance(slot, dict)]


def save_slots(root: Path, slots: list[Any], *, deps: Any) -> None:
    backend = deps._chem_core_admission_module()
    if backend is None:
        deps.atomic_write_json(deps._admission_path(root), slots, ensure_ascii=True, indent=2)
        return

    backend_slots = [deps._to_chem_core_slot(slot, backend=backend) for slot in slots]
    backend._save_slots(root, backend_slots)


def backend_list_slots(root: Path, *, backend: Any, deps: Any) -> list[Any] | None:
    list_slots_fn = getattr(backend, "list_slots", None)
    if not callable(list_slots_fn):
        return None
    try:
        return [deps._from_chem_core_slot(slot) for slot in list_slots_fn(root)]
    except Exception as exc:
        deps._wrap_backend_corruption(exc)
        raise


def backend_reconcile_stale_slots(root: Path, *, backend: Any, deps: Any) -> int | None:
    reconcile_fn = getattr(backend, "reconcile_stale_slots", None)
    if not callable(reconcile_fn):
        return None
    try:
        return int(reconcile_fn(root))
    except Exception as exc:
        deps._wrap_backend_corruption(exc)
        raise


def backend_active_slot_count(root: Path, *, backend: Any, deps: Any) -> int | None:
    count_fn = getattr(backend, "active_slot_count", None)
    if not callable(count_fn):
        return None
    try:
        return int(count_fn(root))
    except Exception as exc:
        deps._wrap_backend_corruption(exc)
        raise


def to_chem_core_slot(slot: Any, *, backend: Any, deps: Any) -> Any:
    normalized = deps._normalize_slot(slot)
    return backend.AdmissionSlot(
        token=text_field(normalized.get("token")),
        owner_pid=int_field(normalized.get("owner_pid")),
        process_start_ticks=optional_int_field(normalized.get("process_start_ticks")),
        source=text_field(normalized.get("source")),
        acquired_at=text_field(normalized.get("acquired_at")),
        app_name=text_field(normalized.get("app_name")),
        task_id=text_field(normalized.get("task_id")),
        workflow_id=text_field(normalized.get("workflow_id")),
        state=text_field(normalized.get("state")) or "active",
        work_dir=text_field(normalized.get("work_dir") or normalized.get("reaction_dir")),
        queue_id=text_field(normalized.get("queue_id")),
    )


def from_chem_core_slot(slot: object, *, deps: Any) -> Any:
    normalized = admission_slot_payload(slot, include_legacy_reaction_dir=True)
    return deps._normalize_slot(normalized)
