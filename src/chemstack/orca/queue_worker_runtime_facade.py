from __future__ import annotations

from pathlib import Path
from typing import Any

from .queue_worker_deps import (
    OrcaQueueWorkerFacadeCallbacks,
    build_orca_runtime_facade_deps,
)


def build_orca_queue_worker_runtime_facade_deps(
    namespace: Any,
    *,
    time_module: Any,
) -> Any:
    return build_orca_runtime_facade_deps(
        OrcaQueueWorkerFacadeCallbacks(
            release_slot=lambda root, token: namespace.release_slot(root, token),
            reserve_slot=lambda *args, **kwargs: namespace._reserve_orca_worker_slot(
                *args,
                **kwargs,
            ),
            start_background_process=lambda command: namespace.start_background_process(command),
            build_worker_child_command=lambda *args, **kwargs: namespace.build_worker_child_command(
                *args,
                **kwargs,
            ),
            activate_reserved_slot=lambda *args, **kwargs: namespace.activate_reserved_slot(
                *args,
                **kwargs,
            ),
            terminate_process=lambda process: namespace._terminate_process(process),
            mark_failed=lambda *args, **kwargs: namespace.mark_failed(*args, **kwargs),
            handle_worker_start_error=lambda *args, **kwargs: namespace._handle_worker_start_error(
                *args,
                **kwargs,
            ),
            finalize_completed_job=lambda *args, **kwargs: namespace._finalize_completed_job(
                *args,
                **kwargs,
            ),
            finalize_child_exit=lambda *args, **kwargs: namespace._finalize_child_exit(
                *args,
                **kwargs,
            ),
            reconcile_worker_state=lambda worker: namespace._reconcile_worker_state(worker),
            list_queue=lambda root: namespace.list_queue(Path(root)),
            list_slots=lambda root: namespace.list_slots(root),
            reconcile_stale_slots=lambda root: namespace.reconcile_stale_slots(root),
            mark_cancelled=lambda *args, **kwargs: namespace.mark_cancelled(*args, **kwargs),
            requeue_running_entry=lambda *args, **kwargs: namespace.requeue_running_entry(
                *args,
                **kwargs,
            ),
            try_reserve_admission_slot=lambda cfg: namespace._try_reserve_admission_slot(cfg),
            start_background_job_process=lambda **kwargs: namespace._start_background_job_process(
                **kwargs,
            ),
            find_queue_entry=lambda root, queue_id: namespace._queue_module.queue_entry_by_id(
                root,
                queue_id,
            ),
            load_config=lambda config_path: namespace.load_config(config_path),
            read_worker_pid=lambda allowed_root: namespace.read_worker_pid(allowed_root),
            worker_class=lambda *args, **kwargs: namespace.QueueWorker(*args, **kwargs),
            on_worker_process_started=lambda *args, **kwargs: namespace._on_worker_process_started(
                *args,
                **kwargs,
            ),
            shutdown_running_job=lambda *args, **kwargs: namespace._shutdown_running_job(
                *args,
                **kwargs,
            ),
            before_shutdown_all=lambda *args, **kwargs: namespace._before_shutdown_all(
                *args,
                **kwargs,
            ),
        ),
        time_module=time_module,
    )


__all__ = ["build_orca_queue_worker_runtime_facade_deps"]
