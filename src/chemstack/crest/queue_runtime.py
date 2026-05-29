from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path
from typing import Any

from chemstack.core.commands import queue as _shared_queue
from chemstack.core.config.engines import (
    default_shared_config_path as default_config_path,
    load_crest_config as load_config,
)
from chemstack.core.notifications.engines import (
    notify_crest_job_finished as notify_job_finished,
    notify_crest_job_started as notify_job_started,
)
from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.queue import (
    dequeue_next,
    execution as _queue_execution,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.worker import (
    BackgroundRunningJob as _RunningJob,
    PidFileChildProcessQueueWorker,
    config_path_for_worker,
    dequeue_next_across_roots,
    make_child_queue_worker_deps,
    read_worker_pid_file,
    reconcile_orphaned_child_queue_entries,
    reserve_dequeued_entry,
    resolve_admission_root,
    shutdown_child_process_with_grace,
    start_background_process,
    queue_entry_by_id as common_queue_entry_by_id,
)
from chemstack.core.utils import now_utc_iso

from . import queue_admission as _queue_admission
from . import queue_lifecycle as _queue_lifecycle
from .job_locations import runtime_roots_for_cfg, upsert_job_record
from .runner import finalize_crest_job, start_crest_job
from .worker_execution import (
    WorkerArtifactDependencies,
    WorkerExecutionDependencies,
    WorkerQueueDependencies,
    WorkerRunnerDependencies,
    WorkerTimingDependencies,
    WorkerTrackingDependencies,
    _mark_recovery_pending_entry,
    _terminate_process,
    _write_execution_artifacts,
    _write_running_state,
    build_worker_execution_dependencies,
    build_worker_child_command,
)

POLL_INTERVAL_SECONDS = 5
WORKER_PID_FILE = "crest_queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0


def _queue_worker_deps() -> Any:
    return make_child_queue_worker_deps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time_module=time,
        release_slot_fn=release_slot,
        reserve_dequeued_entry_fn=reserve_dequeued_entry,
        admission_root_fn=_admission_root_for_cfg,
        dequeue_next_entry_fn=dequeue_next_entry,
        start_background_job_process_fn=_start_background_job_process,
        try_reserve_admission_slot_fn=_try_reserve_admission_slot,
    )


_queue_runtime = _shared_queue.QueueRuntime(
    load_config_fn=load_config,
    runtime_roots_for_cfg_fn=runtime_roots_for_cfg,
    list_queue_fn=lambda root: list_queue(root),
    dequeue_next_fn=lambda root: dequeue_next(root),
    dequeue_next_across_roots_fn=lambda roots, **kwargs: dequeue_next_across_roots(
        roots,
        **kwargs,
    ),
)


def queue_roots(cfg: Any) -> tuple[Path, ...]:
    return _queue_runtime.queue_roots(cfg)


def queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    return _queue_runtime.queue_entries_with_roots(cfg)


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    return common_queue_entry_by_id(queue_root, queue_id, list_queue_fn=list_queue)


def dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return _queue_runtime.dequeue_next_entry(cfg)


def _admission_root_for_cfg(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return _queue_admission.reserve_admission_slot(
        cfg,
        reserve_slot_fn=reserve_slot,
    )


def _worker_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        timing=WorkerTimingDependencies(now_utc_iso=now_utc_iso),
        queue=WorkerQueueDependencies(
            get_cancel_requested=get_cancel_requested,
            mark_completed=mark_completed,
            mark_cancelled=mark_cancelled,
            mark_failed=mark_failed,
        ),
        runner=WorkerRunnerDependencies(
            start_crest_job=start_crest_job,
            finalize_crest_job=finalize_crest_job,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=1,
        ),
        artifacts=WorkerArtifactDependencies(
            write_running_state=_write_running_state,
            write_execution_artifacts=_write_execution_artifacts,
        ),
        tracking=WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
            notify_job_finished=notify_job_finished,
        ),
    )


def read_worker_pid(allowed_root: Path) -> int | None:
    return read_worker_pid_file(allowed_root, WORKER_PID_FILE)


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str | Path,
    admission_token: str,
) -> subprocess.Popen[str]:
    return _queue_admission.start_background_job_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
        start_background_process_fn=start_background_process,
        build_worker_child_command_fn=build_worker_child_command,
    )


class QueueWorker(PidFileChildProcessQueueWorker):
    worker_pid_file_name = WORKER_PID_FILE

    def __init__(
        self,
        cfg: Any,
        config_path: str,
        *,
        max_concurrent: int,
    ) -> None:
        super().__init__(
            cfg,
            config_path=str(config_path).strip() or default_config_path(),
            max_concurrent=max(1, int(max_concurrent)),
            deps=_queue_worker_deps(),
            admission_root=_admission_root_for_cfg(cfg),
        )

    def _reconcile_worker_state(self) -> None:
        self._reconcile_orphaned_running()

    def _reconcile_orphaned_running(self) -> None:
        _queue_lifecycle.reconcile_orphaned_running(
            self.cfg,
            admission_root=self.admission_root,
            queue_roots_fn=queue_roots,
            list_queue_fn=list_queue,
            list_slots_fn=list_slots,
            reconcile_stale_slots_fn=reconcile_stale_slots,
            reconcile_orphaned_child_queue_entries_fn=reconcile_orphaned_child_queue_entries,
            mark_cancelled_fn=mark_cancelled,
            requeue_running_entry_fn=requeue_running_entry,
            mark_recovery_pending_fn=_mark_recovery_pending_entry,
        )

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        _queue_admission.mark_worker_start_error(
            queue_root=queue_root,
            entry=entry,
            admission_token=admission_token,
            exc=exc,
            mark_entry_failed_and_release_fn=self._mark_entry_failed_and_release,
            mark_failed_fn=mark_failed,
        )

    def _on_worker_process_started(
        self,
        queue_root: Path,
        entry: Any,
        *,
        process: subprocess.Popen[str],
        admission_token: str,
    ) -> bool:
        return _queue_admission.attach_started_process(
            admission_root=self.admission_root,
            queue_root=queue_root,
            entry=entry,
            process=process,
            admission_token=admission_token,
            activate_reserved_slot_fn=activate_reserved_slot,
            terminate_process_fn=_terminate_process,
            mark_entry_failed_and_release_fn=self._mark_entry_failed_and_release,
            mark_failed_fn=mark_failed,
        )

    def _finalize_completed_job(self, _queue_id: str, job: Any, rc: int) -> None:
        self._finalize_child_exit(job, rc=rc)

    def _finalize_child_exit(self, job: _RunningJob, *, rc: int) -> None:
        _queue_lifecycle.finalize_child_exit(
            self.cfg,
            job,
            rc=rc,
            shutdown_requested=self._shutdown_requested,
            find_queue_entry_fn=_find_queue_entry,
            mark_cancelled_fn=mark_cancelled,
            requeue_running_entry_fn=requeue_running_entry,
            mark_failed_fn=mark_failed,
            mark_recovery_pending_fn=_mark_recovery_pending_entry,
            release_admission_slot_fn=self._release_admission_slot,
        )

    def _shutdown_running_job(self, _queue_id: str, job: Any) -> None:
        _queue_lifecycle.shutdown_running_job(
            job,
            shutdown_child_process_with_grace_fn=shutdown_child_process_with_grace,
            terminate_process_fn=_terminate_process,
            finalize_child_exit_fn=lambda current_job, rc: self._finalize_child_exit(
                current_job,
                rc=rc,
            ),
            grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
            sleep_fn=time.sleep,
        )


def cmd_queue_worker(args: Any) -> int:
    return _shared_queue.run_queue_worker_command(
        args,
        load_config_fn=load_config,
        config_path_fn=lambda worker_args: config_path_for_worker(
            worker_args,
            default_config_path_fn=default_config_path,
        ),
        existing_pid_fn=lambda cfg: read_worker_pid(
            Path(str(cfg.runtime.allowed_root)).expanduser().resolve(),
        ),
        max_concurrent_fn=lambda cfg: max(1, int(getattr(cfg.runtime, "max_concurrent", 1))),
        worker_factory=lambda cfg, config_path, **kwargs: QueueWorker(
            cfg,
            config_path,
            **kwargs,
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.queue_runtime")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
