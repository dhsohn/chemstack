from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.queue.internal_engine import InternalEngineQueueWorkerDeps


@dataclass(frozen=True)
class OrcaQueueWorkerFacadeCallbacks:
    release_slot: Callable[[str | Path, str], Any]
    reserve_slot: Callable[..., str | None]
    start_background_process: Callable[[list[str]], Any]
    build_worker_child_command: Callable[..., list[str]]
    activate_reserved_slot: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    mark_failed: Callable[..., Any]
    handle_worker_start_error: Callable[..., Any]
    finalize_completed_job: Callable[..., Any]
    finalize_child_exit: Callable[..., Any]
    reconcile_worker_state: Callable[[Any], None]
    list_queue: Callable[[Any], list[Any]]
    list_slots: Callable[[Any], list[Any]]
    reconcile_stale_slots: Callable[[Any], Any]
    mark_cancelled: Callable[..., Any]
    requeue_running_entry: Callable[..., Any]
    try_reserve_admission_slot: Callable[[Any], str | None]
    start_background_job_process: Callable[..., Any]
    find_queue_entry: Callable[[Any, str], Any | None]
    load_config: Callable[[Any], Any]
    read_worker_pid: Callable[[Path], int | None]
    worker_class: Callable[..., Any]
    on_worker_process_started: Callable[..., bool]
    shutdown_running_job: Callable[..., Any]
    before_shutdown_all: Callable[..., Any]


def build_orca_runtime_facade_deps(
    callbacks: OrcaQueueWorkerFacadeCallbacks,
    *,
    time_module: Any = time,
) -> InternalEngineQueueWorkerDeps:
    """Build ORCA queue worker dependencies for the shared engine runtime."""

    return InternalEngineQueueWorkerDeps(
        time_module=time_module,
        release_slot=callbacks.release_slot,
        reserve_slot=callbacks.reserve_slot,
        start_background_process=callbacks.start_background_process,
        build_worker_child_command=callbacks.build_worker_child_command,
        config_path_for_worker=lambda args, *, default_config_path_fn: str(
            getattr(args, "config", "") or default_config_path_fn()
        ),
        default_config_path=lambda: "",
        activate_reserved_slot=callbacks.activate_reserved_slot,
        terminate_process=callbacks.terminate_process,
        mark_failed=callbacks.mark_failed,
        handle_worker_start_error=callbacks.handle_worker_start_error,
        finalize_completed_job=callbacks.finalize_completed_job,
        finalize_child_exit=callbacks.finalize_child_exit,
        reconcile_worker_state=callbacks.reconcile_worker_state,
        list_queue=callbacks.list_queue,
        list_slots=callbacks.list_slots,
        reconcile_stale_slots=callbacks.reconcile_stale_slots,
        reconcile_orphaned_child_queue_entries=lambda *_args, **_kwargs: None,
        mark_cancelled=callbacks.mark_cancelled,
        requeue_running_entry=callbacks.requeue_running_entry,
        mark_recovery_pending=lambda *_args, **_kwargs: None,
        try_reserve_admission_slot=callbacks.try_reserve_admission_slot,
        start_background_job_process_fn=callbacks.start_background_job_process,
        find_queue_entry=callbacks.find_queue_entry,
        load_config=callbacks.load_config,
        read_worker_pid=callbacks.read_worker_pid,
        worker_class=callbacks.worker_class,
        on_worker_process_started=callbacks.on_worker_process_started,
        shutdown_running_job=callbacks.shutdown_running_job,
        before_shutdown_all=callbacks.before_shutdown_all,
    )


__all__ = [
    "OrcaQueueWorkerFacadeCallbacks",
    "build_orca_runtime_facade_deps",
]
