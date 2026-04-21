from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.queue import list_queue

from ..config import load_config


def _display_status(entry: Any) -> str:
    if entry.cancel_requested and entry.status.value == "running":
        return "cancel_requested"
    return entry.status.value


def cmd_list(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    entries = list_queue(cfg.runtime.allowed_root)

    if not entries:
        print("No xTB jobs found.")
        return 0

    print(f"xTB queue: {len(entries)} entries\n")
    print("QUEUE ID                    STATUS            PRI  JOB TYPE         REACTION KEY         DIRECTORY")
    print("---------------------------------------------------------------------------------------------------")
    for entry in entries:
        job_dir = str(entry.metadata.get("job_dir", "")).strip()
        job_name = Path(job_dir).name if job_dir else "-"
        job_type = str(entry.metadata.get("job_type", "")).strip() or "-"
        reaction_key = str(entry.metadata.get("reaction_key", "")).strip() or "-"
        print(
            f"{entry.queue_id:<27} "
            f"{_display_status(entry):<16} "
            f"{entry.priority:<4} "
            f"{job_type:<16} "
            f"{reaction_key:<20} "
            f"{job_name}"
        )
    return 0
