from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

from ..utils.persistence import atomic_write_json, load_json_list_file, resolve_root_path
from .records import AdmissionSlot, slot_from_dict, slot_to_dict

ADMISSION_FILE_NAME = "admission_slots.json"
ADMISSION_LOCK_NAME = "admission.lock"


class AdmissionStoreCorruptError(RuntimeError):
    """Raised when the admission slot file cannot be safely loaded."""


def admission_path(root: Path) -> Path:
    return root / ADMISSION_FILE_NAME


def admission_lock_path(root: Path) -> Path:
    return root / ADMISSION_LOCK_NAME


def load_slots(
    root: str | Path,
    *,
    slot_from_dict_fn: Callable[[dict[str, object]], AdmissionSlot] = slot_from_dict,
    corrupt_error: type[Exception] = AdmissionStoreCorruptError,
) -> list[AdmissionSlot]:
    resolved_root = resolve_root_path(root)
    raw = load_json_list_file(
        admission_path(resolved_root),
        corrupt_error=corrupt_error,
        description="Admission slot file",
    )
    return [slot_from_dict_fn(item) for item in raw if isinstance(item, dict)]


def save_slots(
    root: str | Path,
    slots: Sequence[AdmissionSlot],
    *,
    slot_to_dict_fn: Callable[[AdmissionSlot], dict[str, object]] = slot_to_dict,
) -> None:
    resolved_root = resolve_root_path(root)
    atomic_write_json(
        admission_path(resolved_root),
        [slot_to_dict_fn(slot) for slot in slots],
        ensure_ascii=True,
        indent=2,
    )
