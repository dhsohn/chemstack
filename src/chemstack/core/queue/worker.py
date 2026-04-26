from __future__ import annotations

import os
import signal
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, MutableMapping, Protocol, TypeVar

from chemstack.core.admission import reserve_slot

T = TypeVar("T")


@dataclass(frozen=True)
class SlotFillResult:
    status: str
    started: int


class ManagedProcess(Protocol):
    pid: int

    def kill(self) -> None: ...
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


def resolve_admission_root(cfg: Any) -> str:
    return str(
        getattr(cfg.runtime, "resolved_admission_root", None)
        or getattr(cfg.runtime, "admission_root", "")
        or cfg.runtime.allowed_root
    )


def resolve_admission_limit(cfg: Any) -> int:
    raw = getattr(cfg.runtime, "resolved_admission_limit", None)
    if raw in (None, "", 0):
        raw = getattr(cfg.runtime, "admission_limit", None)
    if raw in (None, "", 0):
        raw = getattr(cfg.runtime, "max_concurrent", 1)
    try:
        return max(1, int(raw if raw is not None else 1))
    except (TypeError, ValueError):
        return 1


def reserve_queue_worker_slot(
    cfg: Any,
    *,
    source: str,
    app_name: str,
    reserve_slot_fn: Callable[..., str | None] = reserve_slot,
) -> str | None:
    return reserve_slot_fn(
        resolve_admission_root(cfg),
        resolve_admission_limit(cfg),
        source=source,
        app_name=app_name,
    )


def dequeue_next_across_roots(
    roots: tuple[Path, ...],
    *,
    list_queue_fn: Callable[[Path], list[T]],
    dequeue_next_fn: Callable[[Path], T | None],
) -> tuple[Path, T] | None:
    if len(roots) == 1:
        entry = dequeue_next_fn(roots[0])
        if entry is None:
            return None
        return roots[0], entry

    selected_root: Path | None = None
    selected_key: tuple[int, str, int, str] | None = None

    for root_index, root in enumerate(roots):
        for entry in list_queue_fn(root):
            status_value = getattr(getattr(entry, "status", None), "value", None)
            status = str(status_value).strip().lower()
            if status != "pending" or getattr(entry, "cancel_requested", False):
                continue
            key = (
                int(getattr(entry, "priority", 10) or 10),
                str(getattr(entry, "enqueued_at", "")),
                root_index,
                str(getattr(entry, "queue_id", "")),
            )
            if selected_key is None or key < selected_key:
                selected_key = key
                selected_root = root

    if selected_root is None:
        return None

    entry = dequeue_next_fn(selected_root)
    if entry is None:
        return None
    return selected_root, entry


def fill_worker_slots(
    *,
    running_count: Callable[[], int],
    max_concurrent: int,
    reserve_next: Callable[[], tuple[str, T | None]],
    start_reserved: Callable[[T], None],
    max_new_jobs: int | None = None,
) -> SlotFillResult:
    started = 0
    while running_count() < max_concurrent:
        if max_new_jobs is not None and started >= max_new_jobs:
            break
        status, reserved = reserve_next()
        if status != "processed" or reserved is None:
            return SlotFillResult(status="processed" if started else status, started=started)
        start_reserved(reserved)
        started += 1
    return SlotFillResult(status="processed" if started else "idle", started=started)


def pop_completed_worker_jobs(
    running: MutableMapping[str, T],
    *,
    poll_job: Callable[[T], int | None],
    finalize_finished: Callable[[str, T, int], None],
) -> int:
    completed: list[tuple[str, T, int]] = []
    for queue_id, job in list(running.items()):
        rc = poll_job(job)
        if rc is None:
            continue
        completed.append((queue_id, job, rc))

    for queue_id, job, rc in completed:
        finalize_finished(queue_id, job, rc)
        running.pop(queue_id, None)
    return len(completed)


def terminate_process_group(
    proc: ManagedProcess,
    *,
    graceful_timeout: float = 10,
    kill_timeout: float = 5,
    killpg_fn: Callable[[int, int], None] | None = None,
    sigterm: int | None = None,
    sigkill: int | None = None,
) -> None:
    if proc.poll() is not None:
        return
    active_killpg = killpg_fn or os.killpg
    active_sigterm = signal.SIGTERM if sigterm is None else sigterm
    active_sigkill = signal.SIGKILL if sigkill is None else sigkill

    try:
        active_killpg(proc.pid, active_sigterm)
    except (ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except Exception:
            pass

    try:
        proc.wait(timeout=graceful_timeout)
    except subprocess.TimeoutExpired:
        try:
            active_killpg(proc.pid, active_sigkill)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except Exception:
                pass
        try:
            proc.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired:
            pass


def install_shutdown_signal_handlers(request_shutdown: Callable[[], None]) -> None:
    def _handle_signal(_signum: int, _frame: object) -> None:
        request_shutdown()

    try:
        signal.signal(signal.SIGTERM, _handle_signal)
        signal.signal(signal.SIGINT, _handle_signal)
    except ValueError:
        pass


def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def read_live_pid_file(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    if pid_is_alive(pid):
        return pid
    try:
        pid_path.unlink()
    except OSError:
        pass
    return None
