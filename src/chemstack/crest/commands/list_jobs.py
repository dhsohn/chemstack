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
        print("No CREST jobs found.")
        return 0

    print(f"CREST queue: {len(entries)} entries\n")
    print("QUEUE ID                    STATUS            PRI  JOB ID                      DIRECTORY")
    print("-------------------------------------------------------------------------------------")
    for entry in entries:
        job_name = _shared_queue.metadata_path_name(entry, "job_dir")
        print(
            f"{entry.queue_id:<27} "
            f"{_display_status(entry):<16} "
            f"{entry.priority:<4} "
            f"{entry.task_id:<27} "
            f"{job_name}"
        )
    return 0
