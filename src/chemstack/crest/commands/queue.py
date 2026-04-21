from __future__ import annotations

import time
from typing import Any

from chemstack.core.admission import release_slot, reserve_slot
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    request_cancel,
)

from ..config import load_config
from chemstack.core.utils import now_utc_iso
from ..job_locations import upsert_job_record
from ..notifications import notify_job_finished, notify_job_started
from .organize import organize_job_dir
from ..runner import finalize_crest_job, start_crest_job
from ..worker_execution import (
    WorkerExecutionDependencies,
    _molecule_key,
    _resource_caps,
    _terminate_process,
    _write_execution_artifacts,
    _write_running_state,
    process_dequeued_entry,
)

POLL_INTERVAL_SECONDS = 5


def _display_status(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    normalized = str(status_value).strip() or "unknown"
    if getattr(entry, "cancel_requested", False) and normalized == "running":
        return "cancel_requested"
    return normalized


def _find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    for entry in entries:
        if entry.queue_id == target or entry.task_id == target:
            return entry
    return None


def cmd_queue_cancel(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: queue cancel requires a queue_id or job_id")
        return 1

    entries = list_queue(cfg.runtime.allowed_root)
    entry = _find_entry_by_target(entries, target)
    if entry is None:
        print(f"error: queue target not found: {target}")
        return 1

    updated = request_cancel(cfg.runtime.allowed_root, entry.queue_id)
    if updated is None:
        print(f"error: queue target already terminal: {target}")
        return 1

    print(f"status: {_display_status(updated)}")
    print(f"queue_id: {updated.queue_id}")
    print(f"job_id: {updated.task_id}")
    return 0


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    admission_root = getattr(cfg.runtime, "resolved_admission_root", None) or getattr(cfg.runtime, "admission_root", "") or cfg.runtime.allowed_root
    admission_limit = getattr(cfg.runtime, "resolved_admission_limit", None) or getattr(cfg.runtime, "admission_limit", 0) or cfg.runtime.max_concurrent
    return reserve_slot(
        admission_root,
        admission_limit,
        source="chemstack.crest.queue_worker",
        app_name="crest_auto",
    )


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    slot_token = _try_reserve_admission_slot(cfg)
    if slot_token is None:
        return "blocked"

    try:
        entry = dequeue_next(cfg.runtime.allowed_root)
        if entry is None:
            return "idle"
        outcome = process_dequeued_entry(
            cfg,
            entry,
            auto_organize=auto_organize,
            resource_caps=_resource_caps,
            molecule_key_resolver=_molecule_key,
            dependencies=WorkerExecutionDependencies(
                now_utc_iso=now_utc_iso,
                get_cancel_requested=get_cancel_requested,
                start_crest_job=start_crest_job,
                finalize_crest_job=finalize_crest_job,
                terminate_process=_terminate_process,
                write_running_state=_write_running_state,
                write_execution_artifacts=_write_execution_artifacts,
                mark_completed=mark_completed,
                mark_cancelled=mark_cancelled,
                mark_failed=mark_failed,
                upsert_job_record=upsert_job_record,
                notify_job_started=notify_job_started,
                notify_job_finished=notify_job_finished,
                organize_job_dir=organize_job_dir,
            ),
        )

        print(f"queue_id: {entry.queue_id}")
        print(f"job_id: {entry.task_id}")
        print(f"status: {outcome.result.status}")
        print(f"reason: {outcome.result.reason}")
        return "processed"
    finally:
        admission_root = getattr(cfg.runtime, "resolved_admission_root", None) or getattr(cfg.runtime, "admission_root", "") or cfg.runtime.allowed_root
        release_slot(admission_root, slot_token)


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    run_once = bool(getattr(args, "once", False))
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False

    if run_once:
        outcome = _process_one(cfg, auto_organize=auto_organize)
        if outcome == "idle":
            print("No pending jobs.")
        elif outcome == "blocked":
            print("status: waiting_for_slot")
        return 0

    try:
        while True:
            outcome = _process_one(cfg, auto_organize=auto_organize)
            if outcome != "processed":
                time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        return 0
