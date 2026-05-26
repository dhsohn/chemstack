from __future__ import annotations

import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.commands import queue as _shared_queue
from chemstack.core.queue import child_execution as _child_execution
from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution
from chemstack.core.queue.dependencies import ChildQueueWorkerDeps
from chemstack.core.admission import (
    activate_reserved_slot,
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
    request_cancel,
)
from chemstack.core.queue.worker import (
    BackgroundRunningJob,
    ChildProcessQueueWorker,
    ManagedProcess as _ManagedProcess,
    config_path_for_worker,
    dequeue_next_across_roots,
    pid_is_alive as worker_pid_is_alive,
    request_job_cancellation,
    reserve_dequeued_entry,
    reserve_queue_worker_slot,
    resolve_admission_root,
    start_background_job_process,
    terminate_process_group,
)
from chemstack.core.utils import now_utc_iso

from .. import worker_execution as _worker_execution
from ..config import default_config_path, load_config
from ..job_locations import (
    reaction_key_from_job_dir,
    resource_dict,
    runtime_roots_for_cfg,
    upsert_job_record,
)
from ..notifications import notify_job_finished, notify_job_started
from ..runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from ..state import (
    is_recovery_pending,
    load_organized_ref,
    load_report_json,
    load_state,
    mark_recovery_pending,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)
from .. import queue_artifacts as _queue_artifacts
from .. import queue_terminal as _queue_terminal

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190
WORKER_JOB_MODULE = "chemstack.xtb.worker_job"


def _queue_worker_deps() -> ChildQueueWorkerDeps:
    return ChildQueueWorkerDeps(
        poll_interval_seconds=POLL_INTERVAL_SECONDS,
        time=time,
        release_slot=release_slot,
        reserve_dequeued_entry=reserve_dequeued_entry,
        admission_root=_admission_root,
        dequeue_next_entry=_dequeue_next_entry,
        start_background_job_process=_start_background_job_process,
        try_reserve_admission_slot=_try_reserve_admission_slot,
    )


def _worker_execution_dependencies() -> _worker_execution.WorkerExecutionDependencies:
    return _worker_execution.build_worker_execution_dependencies_from_groups(
        config=_worker_execution.WorkerConfigDependencies(
            load_config=load_config,
            queue_entry_by_id=_queue_entry_by_id,
        ),
        admission=_worker_execution.WorkerAdmissionDependencies(
            activate_reserved_slot=activate_reserved_slot,
            release_slot=release_slot,
        ),
        context=_worker_execution.WorkerContextDependencies(
            job_dir=_job_dir,
            selected_xyz=_selected_xyz,
            job_type=_job_type,
            reaction_key=_reaction_key,
            input_summary=_input_summary,
            entry_resource_request=_entry_resource_request,
            matching_state=_matching_state,
            is_recovery_pending=is_recovery_pending,
        ),
        artifacts=_worker_execution.WorkerArtifactDependencies(
            write_running_state=_write_running_state,
            build_terminal_result=_build_terminal_result,
            finalize_execution_result=_finalize_execution_result,
        ),
        tracking=_worker_execution.WorkerTrackingDependencies(
            upsert_job_record=upsert_job_record,
            notify_job_started=notify_job_started,
        ),
        runner=_worker_execution.WorkerRunnerDependencies(
            run_xtb_ranking_job=run_xtb_ranking_job,
            start_xtb_job=start_xtb_job,
            finalize_xtb_job=finalize_xtb_job,
            terminate_process=_terminate_process,
            wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
            sleep=time.sleep,
            cancel_check_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        ),
        execute_queue_entry_fn=_execute_queue_entry,
    )


@dataclass(frozen=True)
class QueueExecutionOutcome:
    result: XtbRunResult
    organized_output_dir: str = ""


_RunningJob = BackgroundRunningJob
_TerminalSummary = _queue_terminal.TerminalSummary


def _display_status(entry: Any) -> str:
    return _shared_queue.display_status(entry)


def _find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    return _shared_queue.find_entry_by_target(entries, target)


def _queue_roots(cfg: Any) -> tuple[Path, ...]:
    return _shared_queue.queue_roots(
        cfg,
        runtime_roots_for_cfg_fn=runtime_roots_for_cfg,
    )


def _queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    return _shared_queue.queue_entries_with_roots(
        cfg,
        queue_roots_fn=_queue_roots,
        list_queue_fn=list_queue,
    )


def _dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return _shared_queue.dequeue_next_entry(
        cfg,
        queue_roots_fn=_queue_roots,
        list_queue_fn=list_queue,
        dequeue_next_fn=dequeue_next,
        dequeue_next_across_roots_fn=dequeue_next_across_roots,
    )


def _queue_entry_by_id(queue_root: Path | str, queue_id: str) -> Any | None:
    return _child_execution.find_queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=list_queue,
    )


def _job_dir(entry: Any) -> Path:
    return _engine_execution.entry_metadata_resolved_path(entry, "job_dir")


def _selected_xyz(entry: Any) -> Path:
    return _engine_execution.entry_metadata_resolved_path(entry, "selected_input_xyz")


def _admission_root(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _pid_is_alive(pid: int) -> bool:
    return worker_pid_is_alive(pid)


def cmd_queue_cancel(args: Any) -> int:
    return _shared_queue.cmd_queue_cancel(
        args,
        load_config_fn=load_config,
        queue_entries_with_roots_fn=_queue_entries_with_roots,
        request_cancel_fn=request_cancel,
        display_status_fn=_display_status,
    )


def _resource_caps(cfg: Any) -> dict[str, int]:
    return _engine_execution.engine_resource_caps(cfg, resource_dict_fn=resource_dict)


def _coerce_resource_dict(value: Any) -> dict[str, int]:
    return _engine_execution.coerce_resource_request(value)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_execution.coerce_mapping(value)


def _matching_state(
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    job_type: str,
    reaction_key: str,
) -> dict[str, Any]:
    return _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        match_kwargs={
            "selected_input_xyz": str(selected_xyz),
            "job_type": job_type,
            "reaction_key": reaction_key,
        },
    )


def _entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _engine_execution.entry_resource_request(
        cfg,
        entry,
        resource_caps_fn=_resource_caps,
        coerce_resource_request_fn=_coerce_resource_dict,
    )


def _job_type(entry: Any) -> str:
    value = _engine_execution.entry_metadata_text(entry, "job_type").lower()
    return value or "path_search"


def _reaction_key(entry: Any, job_dir: Path) -> str:
    value = _engine_execution.entry_metadata_text(entry, "reaction_key")
    return value or reaction_key_from_job_dir(job_dir)


def _input_summary(entry: Any) -> dict[str, Any]:
    return _engine_execution.entry_metadata_dict(entry, "input_summary")


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
        entry_resource_request_fn=_entry_resource_request,
        coerce_mapping_fn=_coerce_mapping,
        now_utc_iso_fn=now_utc_iso,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        write_state_fn=write_state,
    )


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    _queue_artifacts.mark_recovery_pending_state(
        cfg,
        entry,
        reason=reason,
        job_dir_fn=_job_dir,
        selected_xyz_fn=_selected_xyz,
        job_type_fn=_job_type,
        reaction_key_fn=_reaction_key,
        input_summary_fn=_input_summary,
        entry_resource_request_fn=_entry_resource_request,
        mark_recovery_pending_fn=mark_recovery_pending,
        upsert_job_record_fn=upsert_job_record,
    )


def _terminate_process(proc: _ManagedProcess) -> None:
    terminate_process_group(proc)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_queue_worker_slot(
        cfg,
        source="chemstack.xtb.queue_worker",
        app_name="chemstack_xtb",
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
) -> QueueExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
        outcome_cls=QueueExecutionOutcome,
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
) -> QueueExecutionOutcome:
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
    return start_background_job_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
        worker_job_module=WORKER_JOB_MODULE,
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


class QueueWorker(ChildProcessQueueWorker):
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

    def _handle_worker_start_error(
        self,
        queue_root: Path,
        entry: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        release_slot(self.admission_root, admission_token)
        job_dir = _job_dir(entry)
        failure = _build_terminal_result(
            entry,
            job_dir=job_dir,
            selected_xyz=_selected_xyz(entry),
            job_type=_job_type(entry),
            reaction_key=_reaction_key(entry, job_dir),
            input_summary=_input_summary(entry),
            resource_request=_entry_resource_request(self.cfg, entry),
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
        release_slot(self.admission_root, job.admission_token)

    def _check_cancel_requests(self) -> None:
        for job in self._running.values():
            if job.cancel_requested:
                continue
            if get_cancel_requested(str(job.queue_root), job.entry.queue_id):
                _request_job_cancellation(job.process)
                job.cancel_requested = True

    def _shutdown_running_job(self, queue_id: str, job: Any) -> None:
        _terminate_process(job.process)
        _mark_recovery_pending_state(self.cfg, job.entry, reason="worker_shutdown")
        requeue_running_entry(str(job.queue_root), queue_id)
        release_slot(self.admission_root, job.admission_token)

    def _reconcile_worker_state(self) -> None:
        reconcile_stale_slots(self.admission_root)
        for queue_root, entry in _queue_entries_with_roots(self.cfg):
            status = str(getattr(getattr(entry, "status", None), "value", "")).strip().lower()
            if status != "running":
                continue
            summary = _load_terminal_summary(queue_root, entry)
            if summary.status in {"completed", "failed", "cancelled"}:
                _ensure_terminal_queue_status(queue_root, entry, summary)
                continue

            state = load_state(_job_dir(entry)) or {}
            worker_job_pid = int(state.get("worker_job_pid", 0) or 0)
            if worker_job_pid and _pid_is_alive(worker_job_pid):
                continue
            requeue_running_entry(str(queue_root), entry.queue_id)
            _mark_recovery_pending_state(self.cfg, entry, reason="crashed_recovery")


def _process_one(cfg: Any) -> str:
    def execute(queue_root: Path, entry: Any) -> QueueExecutionOutcome:
        return _execute_queue_entry(
            cfg,
            queue_root=queue_root,
            entry=entry,
            emit_output=True,
        )

    return _shared_queue.process_one_entry(
        cfg,
        reserve_slot_fn=_try_reserve_admission_slot,
        admission_root_fn=_admission_root,
        dequeue_next_entry_fn=_dequeue_next_entry,
        execute_entry_fn=execute,
        release_slot_fn=release_slot,
    )


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
) -> int:
    return _worker_execution.run_worker_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        dependencies=_worker_execution_dependencies(),
    )


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    worker = QueueWorker(
        cfg,
        config_path=_config_path_for_worker(args),
    )
    return worker.run()
