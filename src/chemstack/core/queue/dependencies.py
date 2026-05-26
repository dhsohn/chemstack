from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from chemstack.core.facade import resolve_grouped_attr


class SleepTimer(Protocol):
    def sleep(self, seconds: float) -> None: ...


AnyCallable = Callable[..., Any]
MappingLoader = Callable[[Path], dict[str, Any] | None]
PathFromEntry = Callable[[Any], Path]
EntryTextResolver = Callable[..., str]


@dataclass(frozen=True)
class QueueTimingDeps:
    POLL_INTERVAL_SECONDS: int
    time: SleepTimer
    now_utc_iso: Callable[[], str]


@dataclass(frozen=True)
class QueueStateDeps:
    is_recovery_pending: Callable[[dict[str, Any]], bool]
    load_organized_ref: MappingLoader
    load_report_json: MappingLoader
    load_state: MappingLoader
    mark_recovery_pending: AnyCallable
    write_report_json: AnyCallable
    write_report_md_lines: AnyCallable
    write_state: AnyCallable


@dataclass(frozen=True)
class QueueStoreDeps:
    activate_reserved_slot: AnyCallable
    get_cancel_requested: Callable[[str, str], bool]
    mark_cancelled: AnyCallable
    mark_completed: AnyCallable
    mark_failed: AnyCallable
    reconcile_stale_slots: AnyCallable
    release_slot: AnyCallable
    requeue_running_entry: AnyCallable
    reserve_dequeued_entry: AnyCallable


@dataclass(frozen=True)
class QueueJobDeps:
    finalize_job: AnyCallable
    notify_job_finished: AnyCallable
    notify_job_started: AnyCallable
    run_followup_job: AnyCallable
    start_job: AnyCallable
    upsert_job_record: AnyCallable


@dataclass(frozen=True)
class QueueHelperDeps:
    _admission_root: Callable[[Any], str]
    _build_terminal_result: AnyCallable
    _coerce_mapping: Callable[[Any], dict[str, Any]]
    _dequeue_next_entry: AnyCallable
    _entry_resource_request: Callable[[Any, Any], dict[str, int]]
    _ensure_terminal_queue_status: AnyCallable
    _execute_queue_entry: AnyCallable
    _finalize_execution_result: AnyCallable
    _input_summary: Callable[[Any], dict[str, Any]]
    _job_dir: PathFromEntry
    _job_type: Callable[[Any], str]
    _load_terminal_summary: AnyCallable
    _mark_recovery_pending_state: AnyCallable
    _pid_is_alive: Callable[[int], bool]
    _print_terminal_summary: Callable[[Any], None]
    _queue_entries_with_roots: Callable[[Any], list[tuple[Path, Any]]]
    _queue_entry_by_id: Callable[[Path, str], Any | None]
    _reaction_key: EntryTextResolver
    _request_job_cancellation: Callable[[Any], None]
    _selected_xyz: PathFromEntry
    _start_background_job_process: AnyCallable
    _terminate_process: Callable[[Any], Any]
    _try_reserve_admission_slot: Callable[[Any], str | None]
    _write_execution_artifacts: AnyCallable


@dataclass(frozen=True)
class QueueCommandDeps:
    timing: QueueTimingDeps
    state: QueueStateDeps
    store: QueueStoreDeps
    job: QueueJobDeps
    helpers: QueueHelperDeps

    def __getattr__(self, name: str) -> Any:
        return resolve_grouped_attr(
            name,
            (self.timing, self.state, self.store, self.job, self.helpers),
        )


@dataclass(frozen=True)
class QueueWorkerBaseDeps:
    POLL_INTERVAL_SECONDS: int
    time: SleepTimer
    _admission_root: Callable[[Any], str]
    _start_background_job_process: AnyCallable


@dataclass(frozen=True)
class ChildQueueWorkerDeps(QueueWorkerBaseDeps):
    release_slot: AnyCallable
    reserve_dequeued_entry: AnyCallable
    _dequeue_next_entry: AnyCallable
    _try_reserve_admission_slot: Callable[[Any], str | None]


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
    "SleepTimer",
    "build_queue_command_deps",
]
