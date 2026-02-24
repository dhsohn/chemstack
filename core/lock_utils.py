"""Shared lock-file utilities.

Extracted from state_store and organize_index to eliminate duplication.
Both modules re-import these under their original private names so that
existing test patch targets (e.g. ``core.state_store._is_process_alive``)
continue to work.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


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
