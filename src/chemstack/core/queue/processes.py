from __future__ import annotations

import json
import os
import signal
import subprocess
from pathlib import Path
from typing import Callable, Protocol

from chemstack.core.utils import process as process_utils
from chemstack.core.utils.persistence import now_utc_iso


class ManagedProcess(Protocol):
    pid: int

    def kill(self) -> None: ...
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def wait(self, timeout: float | None = None) -> int: ...


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


def _process_start_ticks(pid: int) -> int | None:
    return process_utils.process_start_ticks(pid, proc_root=Path("/proc"))


def current_worker_pid_payload() -> dict[str, int | str]:
    return process_utils.current_pid_payload(
        now_fn=now_utc_iso,
        process_start_ticks_fn=_process_start_ticks,
        pid_fn=os.getpid,
    )


def worker_pid_file_path(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> Path:
    return Path(allowed_root).expanduser().resolve() / file_name


def write_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> None:
    payload = current_worker_pid_payload()
    worker_pid_file_path(allowed_root, file_name).write_text(
        json.dumps(payload, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def remove_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> None:
    _remove_pid_file(worker_pid_file_path(allowed_root, file_name))


def read_worker_pid_file(allowed_root: Path | str, file_name: str = "queue_worker.pid") -> int | None:
    return read_live_pid_file(worker_pid_file_path(allowed_root, file_name))


def _positive_int(value: object) -> int | None:
    return process_utils.positive_int(value)


def _read_pid_payload(pid_path: Path) -> tuple[int | None, int | None]:
    return process_utils.read_pid_payload(pid_path)


def _remove_pid_file(pid_path: Path) -> None:
    process_utils.remove_file_silent(pid_path)


def read_live_pid_file(pid_path: Path) -> int | None:
    return process_utils.read_live_pid_file(
        pid_path,
        is_process_alive_fn=pid_is_alive,
        process_start_ticks_fn=_process_start_ticks,
        remove_file_fn=_remove_pid_file,
    )


__all__ = [
    "ManagedProcess",
    "_positive_int",
    "_read_pid_payload",
    "_remove_pid_file",
    "current_worker_pid_payload",
    "install_shutdown_signal_handlers",
    "pid_is_alive",
    "read_live_pid_file",
    "read_worker_pid_file",
    "remove_worker_pid_file",
    "terminate_process_group",
    "worker_pid_file_path",
    "write_worker_pid_file",
]
