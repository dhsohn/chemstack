"""Shared lock-file utilities.

Extracted from state_store and organize_index to eliminate duplication.
Both modules re-import these under their original private names so that
existing test patch targets (e.g. ``core.state_store._is_process_alive``)
continue to work.
"""
from __future__ import annotations

import logging
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Optional


def parse_lock_info(lock_path: Path) -> Dict[str, Any]:
    pid: Optional[int] = None
    started_at: Optional[str] = None
    process_start_ticks: Optional[int] = None
    try:
        raw = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return {"pid": None, "started_at": None, "process_start_ticks": None}
    if not raw:
        return {"pid": None, "started_at": None, "process_start_ticks": None}

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, dict):
        raw_pid = parsed.get("pid")
        if isinstance(raw_pid, int) and raw_pid > 0:
            pid = raw_pid
        elif isinstance(raw_pid, str):
            try:
                parsed_pid = int(raw_pid.strip())
                if parsed_pid > 0:
                    pid = parsed_pid
            except ValueError:
                pid = None
        raw_started_at = parsed.get("started_at")
        if isinstance(raw_started_at, str) and raw_started_at.strip():
            started_at = raw_started_at
        raw_ticks = parsed.get("process_start_ticks")
        if isinstance(raw_ticks, int) and raw_ticks > 0:
            process_start_ticks = raw_ticks
        elif isinstance(raw_ticks, str):
            try:
                parsed_ticks = int(raw_ticks.strip())
                if parsed_ticks > 0:
                    process_start_ticks = parsed_ticks
            except ValueError:
                process_start_ticks = None
        return {"pid": pid, "started_at": started_at, "process_start_ticks": process_start_ticks}

    # Backward-compatible fallback: legacy lock file contained only a pid line.
    first_line = raw.splitlines()[0].strip()
    try:
        parsed_pid = int(first_line)
        if parsed_pid > 0:
            pid = parsed_pid
    except ValueError:
        pid = None
    return {"pid": pid, "started_at": None, "process_start_ticks": None}


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
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(lock_payload + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            logger.debug(acquired_log_template, lock_path)
            break
        except FileExistsError:
            lock_info = parse_lock_info_fn(lock_path)
            lock_pid = lock_info.get("pid")
            lock_start_ticks = lock_info.get("process_start_ticks")

            if isinstance(lock_pid, int):
                alive = is_process_alive_fn(lock_pid)
                if alive and isinstance(lock_start_ticks, int):
                    observed_ticks = process_start_ticks_fn(lock_pid)
                    if observed_ticks is not None and observed_ticks != lock_start_ticks:
                        alive = False
                        logger.info(
                            stale_pid_reuse_log_template,
                            lock_pid,
                            lock_start_ticks,
                            observed_ticks,
                            lock_path,
                        )

                if not alive:
                    logger.info(stale_lock_log_template, lock_pid, lock_path)
                    try:
                        lock_path.unlink()
                    except FileNotFoundError:
                        continue
                    except OSError as exc:
                        if stale_remove_error_builder is not None:
                            raise stale_remove_error_builder(lock_pid, lock_path, exc)
                        raise
                    continue

                if deadline is None:
                    if active_lock_error_builder is not None:
                        raise active_lock_error_builder(lock_pid, lock_info, lock_path)
                    raise RuntimeError(f"Lock is held by active pid={lock_pid}. Lock file: {lock_path}")
            else:
                # Lock owner PID is unreadable
                if deadline is None:
                    if unreadable_lock_error_builder is not None:
                        raise unreadable_lock_error_builder(lock_path)
                    raise RuntimeError(f"Lock file owner is unreadable. Lock file: {lock_path}")
                # With timeout: treat unreadable lock as stale and try to remove it
                logger.warning("Lock file has unreadable owner, treating as stale: %s", lock_path)
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    continue
                except OSError:
                    pass  # Will retry until timeout
                continue

            if time.monotonic() >= deadline:
                timeout_value = int(timeout_seconds) if timeout_seconds is not None else 0
                if timeout_error_builder is not None:
                    raise timeout_error_builder(lock_path, timeout_value)
                raise RuntimeError(
                    f"Lock acquisition timed out after {timeout_value}s. Lock file: {lock_path}"
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
