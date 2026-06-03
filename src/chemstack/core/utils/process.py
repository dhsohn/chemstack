from __future__ import annotations

import json
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any


def positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def process_start_ticks(pid: int, *, proc_root: Path = Path("/proc")) -> int | None:
    if pid <= 0:
        return None
    stat_path = proc_root / str(pid) / "stat"
    try:
        text = stat_path.read_text(encoding="utf-8", errors="ignore").strip()
    except OSError:
        return None
    if not text:
        return None
    right_paren = text.rfind(")")
    if right_paren < 0:
        return None
    fields_after_comm = text[right_paren + 2 :].split()
    if len(fields_after_comm) <= 19:
        return None
    try:
        value = int(fields_after_comm[19])
    except ValueError:
        return None
    return value if value > 0 else None


def current_pid_payload(
    *,
    now_fn: Callable[[], str],
    process_start_ticks_fn: Callable[[int], int | None],
    pid_fn: Callable[[], int] = os.getpid,
) -> dict[str, int | str]:
    pid = pid_fn()
    payload: dict[str, int | str] = {
        "pid": pid,
        "started_at": now_fn(),
    }
    ticks = process_start_ticks_fn(pid)
    if ticks is not None:
        payload["process_start_ticks"] = ticks
    return payload


def read_pid_payload(pid_path: Path) -> tuple[int | None, int | None]:
    try:
        text = pid_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None, None

    pid = positive_int(text)
    if pid is not None:
        return pid, None

    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(raw, dict):
        return None, None

    return positive_int(raw.get("pid")), positive_int(raw.get("process_start_ticks"))


def remove_file_silent(path: Path) -> None:
    with suppress(OSError):
        path.unlink()


def read_live_pid_file(
    pid_path: Path,
    *,
    is_process_alive_fn: Callable[[int], bool],
    process_start_ticks_fn: Callable[[int], int | None],
    remove_file_fn: Callable[[Path], None] = remove_file_silent,
) -> int | None:
    if not pid_path.exists():
        return None
    pid, expected_ticks = read_pid_payload(pid_path)
    if pid is None:
        return None
    if not is_process_alive_fn(pid):
        remove_file_fn(pid_path)
        return None
    if expected_ticks is not None:
        observed_ticks = process_start_ticks_fn(pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            remove_file_fn(pid_path)
            return None
    return pid


def memory_limit_preexec(
    max_memory_gb: int,
    *,
    setrlimit_fn: Callable[[int, tuple[int, int]], object],
    limit_resource: int,
) -> Callable[[], None]:
    limit_bytes = max(1, int(max_memory_gb)) * 1024 * 1024 * 1024

    def apply_limit() -> None:
        setrlimit_fn(limit_resource, (limit_bytes, limit_bytes))

    return apply_limit
