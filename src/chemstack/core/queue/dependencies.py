from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QueueTimingDeps:
    POLL_INTERVAL_SECONDS: int
    time: Any
    now_utc_iso: Any


@dataclass(frozen=True)
class QueueStateDeps:
    is_recovery_pending: Any
    load_organized_ref: Any
    load_report_json: Any
    load_state: Any
    mark_recovery_pending: Any
    write_report_json: Any
    write_report_md_lines: Any
    write_state: Any


@dataclass(frozen=True)
class QueueStoreDeps:
    activate_reserved_slot: Any
    get_cancel_requested: Any
    mark_cancelled: Any
    mark_completed: Any
    mark_failed: Any
    reconcile_stale_slots: Any
    release_slot: Any
    requeue_running_entry: Any
    reserve_dequeued_entry: Any


@dataclass(frozen=True)
class QueueJobDeps:
    finalize_job: Any
    notify_job_finished: Any
    notify_job_started: Any
    run_followup_job: Any
    start_job: Any
    upsert_job_record: Any


@dataclass(frozen=True)
class QueueHelperDeps:
    _admission_root: Any
    _build_terminal_result: Any
    _coerce_mapping: Any
    _dequeue_next_entry: Any
    _entry_resource_request: Any
    _ensure_terminal_queue_status: Any
    _execute_queue_entry: Any
    _finalize_execution_result: Any
    _input_summary: Any
    _job_dir: Any
    _job_type: Any
    _load_terminal_summary: Any
    _mark_recovery_pending_state: Any
    _pid_is_alive: Any
    _print_terminal_summary: Any
    _queue_entries_with_roots: Any
    _queue_entry_by_id: Any
    _reaction_key: Any
    _request_job_cancellation: Any
    _selected_xyz: Any
    _start_background_job_process: Any
    _terminate_process: Any
    _try_reserve_admission_slot: Any
    _write_execution_artifacts: Any


@dataclass(frozen=True)
class QueueCommandDeps:
    timing: QueueTimingDeps
    state: QueueStateDeps
    store: QueueStoreDeps
    job: QueueJobDeps
    helpers: QueueHelperDeps

    def __getattr__(self, name: str) -> Any:
        for group in (self.timing, self.state, self.store, self.job, self.helpers):
            if hasattr(group, name):
                return getattr(group, name)
        raise AttributeError(name)


@dataclass(frozen=True)
class QueueWorkerBaseDeps:
    POLL_INTERVAL_SECONDS: int
    time: Any
    _admission_root: Any
    _start_background_job_process: Any


@dataclass(frozen=True)
class ChildQueueWorkerDeps(QueueWorkerBaseDeps):
    release_slot: Any
    reserve_dequeued_entry: Any
    _dequeue_next_entry: Any
    _try_reserve_admission_slot: Any


def build_queue_command_deps(
    *,
    timing: QueueTimingDeps,
    state: QueueStateDeps,
    store: QueueStoreDeps,
    job: QueueJobDeps,
    helpers: QueueHelperDeps,
) -> QueueCommandDeps:
    return QueueCommandDeps(
        timing=timing,
        state=state,
        store=store,
        job=job,
        helpers=helpers,
    )


__all__ = [
    "ChildQueueWorkerDeps",
    "QueueCommandDeps",
    "QueueHelperDeps",
    "QueueJobDeps",
    "QueueStateDeps",
    "QueueStoreDeps",
    "QueueTimingDeps",
    "QueueWorkerBaseDeps",
    "build_queue_command_deps",
]
