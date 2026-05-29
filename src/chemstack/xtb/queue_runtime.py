from __future__ import annotations

import argparse
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

from chemstack.core.commands import queue as _shared_queue
from chemstack.core.config.engines import (
    default_shared_config_path as default_config_path,
    load_xtb_config as load_config,
)
from chemstack.core.notifications.engines import (
    notify_xtb_job_finished as notify_job_finished,
    notify_xtb_job_started as notify_job_started,
)
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.admission import (
    activate_reserved_slot,
    list_slots,
    reconcile_stale_slots,
    release_slot,
    reserve_slot,
)
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.types import QueueStatus
from chemstack.core.queue.worker import (
    BackgroundRunningJob,
    ChildProcessQueueWorker,
    ManagedProcess as _ManagedProcess,
    QueueWorkerPidFileMixin,
    config_path_for_worker,
    dequeue_next_across_roots,
    make_child_queue_worker_deps,
    pid_is_alive as worker_pid_is_alive,
    read_worker_pid_file,
    reconcile_orphaned_child_queue_entries,
    request_job_cancellation,
    reserve_dequeued_entry,
    reserve_engine_queue_worker_slot,
    resolve_admission_root,
    shutdown_child_process_with_grace,
    start_background_process,
    terminate_process_group,
    queue_entry_by_id as common_queue_entry_by_id,
)
from chemstack.core.utils import now_utc_iso

from . import worker_execution as _worker_execution
from .job_locations import (
    runtime_roots_for_cfg,
    upsert_job_record,
)
from .runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from .state import (
    load_organized_ref,
    load_report_json,
    load_state,
    write_report_json,
    write_report_md_lines,
    write_state,
)
from . import queue_artifacts as _queue_artifacts
from . import queue_terminal as _queue_terminal

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_PID_FILE = "queue_worker.pid"
WORKER_SHUTDOWN_GRACE_SECONDS = 10.0
WORKER_CANCEL_SIGNAL = _worker_execution.WORKER_CANCEL_SIGNAL
WORKER_SHUTDOWN_EXIT_CODE = _worker_execution.WORKER_SHUTDOWN_EXIT_CODE
WORKER_JOB_MODULE = _worker_execution.WORKER_JOB_MODULE


def _queue_worker_deps() -> Any:
    return make_child_queue_worker_deps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time_module=time,
        release_slot_fn=release_slot,
        reserve_dequeued_entry_fn=reserve_dequeued_entry,
        admission_root_fn=_admission_root,
        dequeue_next_entry_fn=dequeue_next_entry,
        start_background_job_process_fn=_start_background_job_process,
        try_reserve_admission_slot_fn=_try_reserve_admission_slot,
    )


def _worker_execution_dependencies() -> _worker_execution.WorkerExecutionDependencies:
    deps = _worker_execution.default_worker_execution_dependencies()
    return replace(
        deps,
        config=replace(
            deps.config,
            load_config=load_config,
            queue_entry_by_id=_queue_entry_by_id,
        ),
        admission=replace(
            deps.admission,
            activate_reserved_slot=activate_reserved_slot,
            release_slot=release_slot,
        ),
        artifacts=replace(
            deps.artifacts,
            write_running_state=_write_running_state,
            build_terminal_result=_build_terminal_result,
            finalize_execution_result=_finalize_execution_result,
        ),
        tracking=replace(
            deps.tracking,
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
        ),
        runner=replace(
            deps.runner,
            run_xtb_ranking_job=run_xtb_ranking_job,
            start_xtb_job=start_xtb_job,
            finalize_xtb_job=finalize_xtb_job,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        ),
        execute_queue_entry=_execute_queue_entry,
    )


_RunningJob = BackgroundRunningJob
_TerminalSummary = _queue_terminal.TerminalSummary

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


def dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return _queue_runtime.dequeue_next_entry(cfg)


def _queue_entry_by_id(queue_root: Path | str, queue_id: str) -> Any | None:
    return common_queue_entry_by_id(queue_root, queue_id, list_queue_fn=list_queue)


def _admission_root(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _pid_is_alive(pid: int) -> bool:
    return worker_pid_is_alive(pid)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_execution.coerce_mapping(value)


_worker_execution_hooks = _worker_execution.default_worker_execution_hooks()
_job_dir = _worker_execution_hooks.job_dir
_selected_xyz = _worker_execution_hooks.selected_xyz
_job_type = _worker_execution_hooks.job_type
_reaction_key = _worker_execution_hooks.reaction_key
_input_summary = _worker_execution_hooks.input_summary


def _write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    _queue_artifacts.write_execution_artifacts(
        entry,
        result,
        previous_state=previous_state,
        resumed=resumed,
        coerce_mapping_fn=_coerce_mapping,
        write_state_fn=write_state,
        write_report_json_fn=write_report_json,
        write_report_md_lines_fn=write_report_md_lines,
    )


def _write_running_state(
    cfg: Any,
    entry: Any,
    *,
    worker_job_pid: int | None = None,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    _queue_artifacts.write_running_state(
        cfg,
        entry,
        worker_job_pid=worker_job_pid,
        previous_state=previous_state,
        resumed=resumed,
        input_summary_fn=_input_summary,
        entry_resource_request_fn=_queue_artifacts.entry_resource_request,
        coerce_mapping_fn=_coerce_mapping,
        now_utc_iso_fn=now_utc_iso,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        write_state_fn=write_state,
    )


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    _worker_execution._mark_recovery_pending_entry(cfg, entry, reason=reason)


def _terminate_process(proc: _ManagedProcess) -> None:
    terminate_process_group(proc)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_engine_queue_worker_slot(
        cfg,
        engine="xtb",
        reserve_slot_fn=reserve_slot,
    )


def _build_terminal_result(
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    job_type: str,
    reaction_key: str,
    input_summary: dict[str, Any],
    resource_request: dict[str, int],
    status: str,
    reason: str,
    exit_code: int = 1,
    command: tuple[str, ...] = (),
) -> XtbRunResult:
    return _queue_artifacts.build_terminal_result(
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        resource_request=resource_request,
        status=status,
        reason=reason,
        exit_code=exit_code,
        command=command,
        now_utc_iso_fn=now_utc_iso,
    )


def _print_terminal_summary(summary: _TerminalSummary) -> None:
    _queue_terminal.print_terminal_summary(summary)


def _load_terminal_summary(
    queue_root: Path, entry: Any, *, rc: int | None = None
) -> _TerminalSummary:
    return _queue_terminal.load_terminal_summary(
        queue_root,
        entry,
        rc=rc,
        job_dir_fn=_job_dir,
        load_state_fn=load_state,
        load_report_json_fn=load_report_json,
        load_organized_ref_fn=load_organized_ref,
        queue_entry_by_id_fn=_queue_entry_by_id,
    )


def _ensure_terminal_queue_status(queue_root: Path, entry: Any, summary: _TerminalSummary) -> None:
    _queue_terminal.ensure_terminal_queue_status(
        queue_root,
        entry,
        summary,
        queue_entry_by_id_fn=_queue_entry_by_id,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
    )


def _finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> _worker_execution.WorkerExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
        outcome_cls=_worker_execution.WorkerExecutionOutcome,
        write_execution_artifacts_fn=_write_execution_artifacts,
        selected_xyz_fn=_selected_xyz,
        job_dir_fn=_job_dir,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
        upsert_job_record_fn=upsert_job_record,
        notify_job_finished_fn=notify_job_finished,
    )


def _execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
) -> _worker_execution.WorkerExecutionOutcome:
    return _worker_execution.execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        worker_job_pid=worker_job_pid,
        emit_output=emit_output,
        dependencies=_worker_execution_dependencies(),
    )


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str,
    admission_token: str,
) -> subprocess.Popen[str]:
    return start_background_process(
        _worker_execution.build_worker_child_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_root=admission_root,
            admission_token=admission_token,
        )
    )


def _request_job_cancellation(proc: _ManagedProcess) -> None:
    request_job_cancellation(
        proc,
        cancel_signal=WORKER_CANCEL_SIGNAL,
        terminate_process_fn=_terminate_process,
    )


def _config_path_for_worker(args: Any) -> str:
    return config_path_for_worker(
        args,
        default_config_path_fn=default_config_path,
    )


def read_worker_pid(allowed_root: Path) -> int | None:
    return read_worker_pid_file(allowed_root, WORKER_PID_FILE)


def _entry_status_is_running(entry: Any) -> bool:
    status = getattr(entry, "status", None)
    value = getattr(status, "value", status)
    return str(value).strip().lower() == QueueStatus.RUNNING.value


class QueueWorker(QueueWorkerPidFileMixin, ChildProcessQueueWorker):
    worker_pid_file_name = WORKER_PID_FILE

    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(
            cfg,
            config_path=config_path,
            max_concurrent=max_concurrent,
            deps=_queue_worker_deps(),
        )
        self.allowed_root = Path(str(cfg.runtime.allowed_root)).expanduser().resolve()

    def _before_run(self) -> None:
        self._write_pid_file()
        super()._before_run()

    def _after_run(self) -> None:
        self._remove_pid_file()

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        self._release_admission_slot(admission_token)
        job_dir = _job_dir(entry)
        failure = _build_terminal_result(
            entry,
            job_dir=job_dir,
            selected_xyz=_selected_xyz(entry),
            job_type=_job_type(entry),
            reaction_key=_reaction_key(entry, job_dir),
            input_summary=_input_summary(entry),
            resource_request=_queue_artifacts.entry_resource_request(self.cfg, entry),
            status="failed",
            reason=f"worker_start_error:{exc}",
        )
        _finalize_execution_result(
            self.cfg,
            queue_root=queue_root,
            entry=entry,
            result=failure,
            emit_output=True,
        )

    def _finalize_completed_job(self, _queue_id: str, job: Any, rc: int) -> None:
        summary = _load_terminal_summary(job.queue_root, job.entry, rc=rc)
        _ensure_terminal_queue_status(job.queue_root, job.entry, summary)
        _print_terminal_summary(summary)
        self._release_admission_slot(job.admission_token)

    def _check_cancel_requests(self) -> None:
        for _queue_id, job in self._running_jobs():
            if job.cancel_requested:
                continue
            if get_cancel_requested(str(job.queue_root), job.entry.queue_id):
                _request_job_cancellation(job.process)
                job.cancel_requested = True

    def _finalize_child_exit(self, job: _RunningJob, *, rc: int) -> None:
        del rc
        current = _queue_entry_by_id(job.queue_root, job.entry.queue_id) or job.entry
        if current is not None and _entry_status_is_running(current):
            if getattr(current, "cancel_requested", False):
                mark_cancelled(str(job.queue_root), current.queue_id, error="cancel_requested")
            else:
                requeue_running_entry(str(job.queue_root), current.queue_id)
                _mark_recovery_pending_state(self.cfg, current, reason="worker_shutdown")
        self._release_admission_slot(job.admission_token)

    def _shutdown_running_job(self, queue_id: str, job: Any) -> None:
        del queue_id
        shutdown_child_process_with_grace(
            job,
            terminate_process_fn=_terminate_process,
            finalize_child_exit_fn=lambda current_job, rc: self._finalize_child_exit(
                current_job,
                rc=rc,
            ),
            grace_seconds=WORKER_SHUTDOWN_GRACE_SECONDS,
            sleep_fn=time.sleep,
        )

    def _reconcile_worker_state(self) -> None:
        self._sync_terminal_running_entries()
        self._reconcile_orphaned_running()

    def _sync_terminal_running_entries(self) -> None:
        for queue_root, entry in queue_entries_with_roots(self.cfg):
            if not _entry_status_is_running(entry):
                continue
            summary = _load_terminal_summary(queue_root, entry)
            if summary.status in {"completed", "failed", "cancelled"}:
                _ensure_terminal_queue_status(queue_root, entry, summary)

    def _live_worker_pid_slots(self) -> list[Any]:
        slots: list[Any] = []
        for _queue_root, entry in queue_entries_with_roots(self.cfg):
            if not _entry_status_is_running(entry):
                continue
            state = load_state(_job_dir(entry)) or {}
            try:
                worker_job_pid = int(state.get("worker_job_pid", 0) or 0)
            except (TypeError, ValueError):
                continue
            if worker_job_pid and _pid_is_alive(worker_job_pid):
                slots.append(SimpleNamespace(queue_id=entry.queue_id))
        return slots

    def _list_slots_preserving_live_worker_pids(self, admission_root: str | Path) -> list[Any]:
        return [
            *list_slots(admission_root),
            *self._live_worker_pid_slots(),
        ]

    def _reconcile_orphaned_running(self) -> None:
        reconcile_orphaned_child_queue_entries(
            self.cfg,
            admission_root=self.admission_root,
            queue_roots_fn=queue_roots,
            list_queue_fn=list_queue,
            list_slots_fn=self._list_slots_preserving_live_worker_pids,
            reconcile_stale_slots_fn=reconcile_stale_slots,
            running_status=QueueStatus.RUNNING,
            mark_cancelled_fn=lambda root, queue_id, **kwargs: mark_cancelled(
                str(root),
                queue_id,
                **kwargs,
            ),
            requeue_running_entry_fn=lambda root, queue_id: requeue_running_entry(
                str(root),
                queue_id,
            ),
            mark_recovery_pending_fn=lambda cfg, entry: _mark_recovery_pending_state(
                cfg,
                entry,
                reason="crashed_recovery",
            ),
        )


def cmd_queue_worker(args: Any) -> int:
    return _shared_queue.run_queue_worker_command(
        args,
        load_config_fn=load_config,
        config_path_fn=_config_path_for_worker,
        existing_pid_fn=lambda cfg: read_worker_pid(
            Path(str(cfg.runtime.allowed_root)).expanduser().resolve(),
        ),
        max_concurrent_fn=lambda cfg: max(1, int(getattr(cfg.runtime, "max_concurrent", 1))),
        worker_factory=lambda cfg, config_path, **kwargs: QueueWorker(
            cfg,
            config_path=config_path,
            **kwargs,
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.xtb.queue_runtime")
    parser.add_argument("--config", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    return cmd_queue_worker(build_parser().parse_args(argv))


if __name__ == "__main__":
    raise SystemExit(main())
