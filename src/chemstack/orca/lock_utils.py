"""Shared lock-file utilities used by run state, queue, and organize index."""

from __future__ import annotations

import logging
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, NoReturn, Optional


def parse_lock_info(lock_path: Path) -> Dict[str, Any]:
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return _empty_lock_info()
    if not raw:
        return _empty_lock_info()

    try:
        parsed = json.loads(raw)
    except Exception:
        return _empty_lock_info()

    if isinstance(parsed, dict):
        return {
            "pid": _positive_int(parsed.get("pid")),
            "started_at": _nonempty_string(parsed.get("started_at")),
            "process_start_ticks": _positive_int(parsed.get("process_start_ticks")),
        }

    return _empty_lock_info()


def _empty_lock_info() -> Dict[str, Any]:
    return {"pid": None, "started_at": None, "process_start_ticks": None}


def _positive_int(value: Any) -> Optional[int]:
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError:
            return None
        return parsed if parsed > 0 else None
    return None


def _nonempty_string(value: Any) -> Optional[str]:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _write_lock_payload(lock_path: Path, lock_payload: str) -> bool:
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(lock_payload + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return True


def _unlink_lock(lock_path: Path) -> bool:
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return True
    return True


def _lock_owner_alive(
    *,
    lock_pid: int,
    lock_start_ticks: Any,
    is_process_alive_fn: Callable[[int], bool],
    process_start_ticks_fn: Callable[[int], Optional[int]],
    logger: logging.Logger,
    stale_pid_reuse_log_template: str,
    lock_path: Path,
) -> bool:
    alive = is_process_alive_fn(lock_pid)
    if not alive or not isinstance(lock_start_ticks, int):
        return alive
    observed_ticks = process_start_ticks_fn(lock_pid)
    if observed_ticks is None or observed_ticks == lock_start_ticks:
        return alive
    logger.info(
        stale_pid_reuse_log_template,
        lock_pid,
        lock_start_ticks,
        observed_ticks,
        lock_path,
    )
    return False


def _raise_lock_timeout(
    *,
    timeout_seconds: Optional[int],
    lock_path: Path,
    timeout_error_builder: Callable[[Path, int], RuntimeError] | None,
) -> NoReturn:
    timeout_value = int(timeout_seconds) if timeout_seconds is not None else 0
    if timeout_error_builder is not None:
        raise timeout_error_builder(lock_path, timeout_value)
    raise RuntimeError(f"Lock acquisition timed out after {timeout_value}s. Lock file: {lock_path}")


def _handle_existing_lock(
    *,
    lock_path: Path,
    lock_info: Dict[str, Any],
    is_process_alive_fn: Callable[[int], bool],
    process_start_ticks_fn: Callable[[int], Optional[int]],
    logger: logging.Logger,
    stale_pid_reuse_log_template: str,
    stale_lock_log_template: str,
    deadline: float | None,
    active_lock_error_builder: Callable[[int, Dict[str, Any], Path], RuntimeError] | None,
    unreadable_lock_error_builder: Callable[[Path], RuntimeError] | None,
    stale_remove_error_builder: Callable[[int, Path, OSError], RuntimeError] | None,
) -> bool:
    lock_pid = lock_info.get("pid")
    if not isinstance(lock_pid, int):
        return _handle_unreadable_lock(
            lock_path=lock_path,
            logger=logger,
            deadline=deadline,
            unreadable_lock_error_builder=unreadable_lock_error_builder,
        )

    alive = _lock_owner_alive(
        lock_pid=lock_pid,
        lock_start_ticks=lock_info.get("process_start_ticks"),
        is_process_alive_fn=is_process_alive_fn,
        process_start_ticks_fn=process_start_ticks_fn,
        logger=logger,
        stale_pid_reuse_log_template=stale_pid_reuse_log_template,
        lock_path=lock_path,
    )
    if not alive:
        logger.info(stale_lock_log_template, lock_pid, lock_path)
        try:
            _unlink_lock(lock_path)
        except OSError as exc:
            if stale_remove_error_builder is not None:
                raise stale_remove_error_builder(lock_pid, lock_path, exc)
            raise
        return True

    if deadline is None:
        if active_lock_error_builder is not None:
            raise active_lock_error_builder(lock_pid, lock_info, lock_path)
        raise RuntimeError(f"Lock is held by active pid={lock_pid}. Lock file: {lock_path}")
    return False


def _handle_unreadable_lock(
    *,
    lock_path: Path,
    logger: logging.Logger,
    deadline: float | None,
    unreadable_lock_error_builder: Callable[[Path], RuntimeError] | None,
) -> bool:
    if deadline is None:
        if unreadable_lock_error_builder is not None:
            raise unreadable_lock_error_builder(lock_path)
        raise RuntimeError(f"Lock file owner is unreadable. Lock file: {lock_path}")
    logger.warning("Lock file has unreadable owner, treating as stale: %s", lock_path)
    try:
        _unlink_lock(lock_path)
    except OSError:
        return False
    return True


def is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def process_start_ticks(pid: int) -> Optional[int]:
    if pid <= 0:
        return None
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        raw = stat_path.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        return None
    if not raw:
        return None

    right_paren = raw.rfind(")")
    if right_paren < 0:
        return None
    fields_after_comm = raw[right_paren + 2 :].split()
    # /proc/<pid>/stat field 22 is starttime. After dropping pid+comm, it is index 19.
    if len(fields_after_comm) <= 19:
        return None
    try:
        value = int(fields_after_comm[19])
    except ValueError:
        return None
    return value if value > 0 else None


def current_process_start_ticks() -> Optional[int]:
    return process_start_ticks(os.getpid())


@contextmanager
def acquire_file_lock(
    *,
    lock_path: Path,
    lock_payload_obj: Dict[str, Any],
    parse_lock_info_fn: Callable[[Path], Dict[str, Any]],
    is_process_alive_fn: Callable[[int], bool],
    process_start_ticks_fn: Callable[[int], Optional[int]],
    logger: logging.Logger,
    acquired_log_template: str,
    released_log_template: str,
    stale_pid_reuse_log_template: str,
    stale_lock_log_template: str,
    timeout_seconds: Optional[int] = None,
    poll_interval_seconds: float = 0.5,
    active_lock_error_builder: Callable[[int, Dict[str, Any], Path], RuntimeError] | None = None,
    unreadable_lock_error_builder: Callable[[Path], RuntimeError] | None = None,
    timeout_error_builder: Callable[[Path, int], RuntimeError] | None = None,
    stale_remove_error_builder: Callable[[int, Path, OSError], RuntimeError] | None = None,
) -> Iterator[None]:
    """Acquire an exclusive lock file with stale-lock recovery.

    If ``timeout_seconds`` is ``None``, active/unreadable lock owners raise
    immediately via the corresponding builders.
    If ``timeout_seconds`` is set, lock acquisition retries until timeout.
    """

    lock_payload = json.dumps(lock_payload_obj, ensure_ascii=True)
    deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None

    while True:
        if _write_lock_payload(lock_path, lock_payload):
            logger.debug(acquired_log_template, lock_path)
            break

        lock_info = parse_lock_info_fn(lock_path)
        if _handle_existing_lock(
            lock_path=lock_path,
            lock_info=lock_info,
            is_process_alive_fn=is_process_alive_fn,
            process_start_ticks_fn=process_start_ticks_fn,
            logger=logger,
            stale_pid_reuse_log_template=stale_pid_reuse_log_template,
            stale_lock_log_template=stale_lock_log_template,
            deadline=deadline,
            active_lock_error_builder=active_lock_error_builder,
            unreadable_lock_error_builder=unreadable_lock_error_builder,
            stale_remove_error_builder=stale_remove_error_builder,
        ):
            continue

        if deadline is None:
            _raise_lock_timeout(
                timeout_seconds=timeout_seconds,
                lock_path=lock_path,
                timeout_error_builder=timeout_error_builder,
            )
        if time.monotonic() >= deadline:
            _raise_lock_timeout(
                timeout_seconds=timeout_seconds,
                lock_path=lock_path,
                timeout_error_builder=timeout_error_builder,
            )
        time.sleep(poll_interval_seconds)

    try:
        yield
    finally:
        try:
            lock_path.unlink()
            logger.debug(released_log_template, lock_path)
        except OSError:
            pass
