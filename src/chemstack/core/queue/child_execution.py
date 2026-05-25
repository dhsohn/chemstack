from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class ChildWorkerShutdownController:
    requested: bool = False

    def request(self) -> None:
        self.requested = True

    def is_requested(self) -> bool:
        return self.requested


def find_queue_entry_by_id(
    queue_root: str | Path,
    queue_id: str,
    *,
    list_queue_fn: Callable[[str | Path], Iterable[Any]],
) -> Any | None:
    for entry in list_queue_fn(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def activate_child_admission_token(
    admission_root: str | Path,
    admission_token: str | None,
    *,
    work_dir: str | Path,
    queue_id: str,
    source: str,
    activate_reserved_slot_fn: Callable[..., Any],
) -> bool:
    if not admission_token:
        return True
    activated = activate_reserved_slot_fn(
        admission_root,
        admission_token,
        work_dir=work_dir,
        queue_id=queue_id,
        source=source,
    )
    return activated is not None


def release_child_admission_token(
    admission_root: str | Path,
    admission_token: str | None,
    *,
    release_slot_fn: Callable[[str | Path, str], Any],
) -> None:
    if admission_token:
        release_slot_fn(admission_root, admission_token)


def install_shutdown_request_handlers(
    controller: ChildWorkerShutdownController,
    *,
    install_signal_handlers_fn: Callable[[Callable[[], None]], Any],
) -> None:
    install_signal_handlers_fn(controller.request)


__all__ = [
    "ChildWorkerShutdownController",
    "activate_child_admission_token",
    "find_queue_entry_by_id",
    "install_shutdown_request_handlers",
    "release_child_admission_token",
]
