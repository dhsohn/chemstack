from __future__ import annotations


def int_field(value: object) -> int:
    return value if isinstance(value, int) else 0


def optional_int_field(value: object) -> int | None:
    return value if isinstance(value, int) else None


def text_field(value: object) -> str:
    return str(value or "").strip()


def admission_slot_payload(
    slot: object,
    *,
    include_legacy_reaction_dir: bool = False,
) -> dict[str, object]:
    work_dir = text_field(getattr(slot, "work_dir", ""))
    payload: dict[str, object] = {
        "token": text_field(getattr(slot, "token", "")),
        "state": text_field(getattr(slot, "state", "")) or "active",
        "work_dir": work_dir or None,
        "queue_id": text_field(getattr(slot, "queue_id", "")) or None,
        "owner_pid": int_field(getattr(slot, "owner_pid", 0)),
        "process_start_ticks": optional_int_field(
            getattr(slot, "process_start_ticks", None)
        ),
        "source": text_field(getattr(slot, "source", "")),
        "acquired_at": text_field(getattr(slot, "acquired_at", "")),
        "app_name": text_field(getattr(slot, "app_name", "")) or None,
        "task_id": text_field(getattr(slot, "task_id", "")) or None,
        "workflow_id": text_field(getattr(slot, "workflow_id", "")) or None,
    }
    if include_legacy_reaction_dir:
        payload["reaction_dir"] = work_dir or None
    return payload


__all__ = [
    "admission_slot_payload",
    "int_field",
    "optional_int_field",
    "text_field",
]
