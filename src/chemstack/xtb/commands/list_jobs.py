from __future__ import annotations

from typing import Any

from chemstack.core.commands import queue as _shared_queue
from chemstack.core.queue import list_queue

from ..config import load_config
from ..tracking import runtime_roots_for_cfg


def _display_status(entry: Any) -> str:
    return _shared_queue.display_status(entry)


def cmd_list(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    entries = _shared_queue.sorted_queue_entries(
        cfg,
        runtime_roots_for_cfg_fn=runtime_roots_for_cfg,
        list_queue_fn=list_queue,
    )

    if not entries:
        print("No xTB jobs found.")
        return 0

    print(f"xTB queue: {len(entries)} entries\n")
    print("QUEUE ID                    STATUS            PRI  JOB TYPE         REACTION KEY         DIRECTORY")
    print("---------------------------------------------------------------------------------------------------")
    for entry in entries:
        job_name = _shared_queue.metadata_path_name(entry, "job_dir")
        job_type = _shared_queue.metadata_text(entry, "job_type", default="-")
        reaction_key = _shared_queue.metadata_text(entry, "reaction_key", default="-")
        print(
            f"{entry.queue_id:<27} "
            f"{_display_status(entry):<16} "
            f"{entry.priority:<4} "
            f"{job_type:<16} "
            f"{reaction_key:<20} "
            f"{job_name}"
        )
    return 0
