from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Callable

from .lock_utils import current_process_start_ticks, is_process_alive, parse_lock_info, process_start_ticks
from .persistence_utils import now_utc_iso

RUN_LOCK_FILE_NAME = "run.lock"


def current_process_lock_payload() -> dict[str, int | str]:
    payload: dict[str, int | str] = {
        "pid": os.getpid(),
        "started_at": now_utc_iso(),
    }
    ticks = current_process_start_ticks()
    if ticks is not None:
        payload["process_start_ticks"] = ticks
    return payload


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _read_pid_payload(pid_path: Path) -> tuple[int | None, int | None]:
    try:
        text = pid_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None, None

    pid = _positive_int(text)
    if pid is not None:
        return pid, None

    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(raw, dict):
        return None, None

    return _positive_int(raw.get("pid")), _positive_int(raw.get("process_start_ticks"))


def _remove_pid_file(pid_path: Path) -> None:
    try:
        pid_path.unlink()
    except OSError:
        pass


def active_run_lock_pid(
    reaction_dir: Path,
    *,
    logger: logging.Logger | None = None,
    lock_file_name: str = RUN_LOCK_FILE_NAME,
    on_pid_reuse: Callable[[int, int, int | None], None] | None = None,
) -> int | None:
    lock_info = parse_lock_info(reaction_dir / lock_file_name)
    pid = lock_info.get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return None
    if not is_process_alive(pid):
        return None

    expected_ticks = lock_info.get("process_start_ticks")
    if isinstance(expected_ticks, int) and expected_ticks > 0:
        observed_ticks = process_start_ticks(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            if on_pid_reuse is not None:
                on_pid_reuse(pid, expected_ticks, observed_ticks)
            elif logger is not None:
                logger.info(
                    "Ignoring stale %s due to PID reuse: reaction_dir=%s pid=%d expected=%d observed=%s",
                    lock_file_name,
                    reaction_dir,
                    pid,
                    expected_ticks,
                    observed_ticks,
                )
            return None
    return pid


def read_pid_file(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    pid, expected_ticks = _read_pid_payload(pid_path)
    if pid is None:
        return None
    if not is_process_alive(pid):
        _remove_pid_file(pid_path)
        return None
    if expected_ticks is not None:
        observed_ticks = process_start_ticks(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            _remove_pid_file(pid_path)
            return None
    return pid
