from __future__ import annotations

import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.config import engines as _config_engines
from chemstack.core.commands import queue as _shared_queue
from chemstack.core.queue import execution as _queue_execution
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
    ManagedProcess as _ManagedProcess,
    dequeue_next_across_roots,
    pid_is_alive as worker_pid_is_alive,
    reserve_dequeued_entry,
    reserve_queue_worker_slot,
    resolve_admission_limit,
    resolve_admission_root,
    terminate_process_group,
)
from chemstack.core.utils import coerce_list as _shared_coerce_list, now_utc_iso

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
from .. import queue_worker_loop as _queue_worker_loop

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190
WORKER_JOB_MODULE = "chemstack.xtb.worker_job"


@dataclass(frozen=True)
class _QueueTimingDeps:
    POLL_INTERVAL_SECONDS: int
    time: Any
    now_utc_iso: Any


@dataclass(frozen=True)
class _QueueStateDeps:
    is_recovery_pending: Any
    load_organized_ref: Any
    load_report_json: Any
    load_state: Any
    mark_recovery_pending: Any
    write_report_json: Any
    write_report_md_lines: Any
    write_state: Any


@dataclass(frozen=True)
class _QueueStoreDeps:
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
class _QueueJobDeps:
    finalize_xtb_job: Any
    notify_job_finished: Any
    notify_job_started: Any
    run_xtb_ranking_job: Any
    start_xtb_job: Any
    upsert_job_record: Any


@dataclass(frozen=True)
class _QueueHelperDeps:
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
class _QueueCommandDeps:
    timing: _QueueTimingDeps
    state: _QueueStateDeps
    store: _QueueStoreDeps
    job: _QueueJobDeps
    helpers: _QueueHelperDeps

    def __getattr__(self, name: str) -> Any:
        for group in (self.timing, self.state, self.store, self.job, self.helpers):
            if hasattr(group, name):
                return getattr(group, name)
        raise AttributeError(name)


def _queue_command_deps() -> _QueueCommandDeps:
    return _QueueCommandDeps(
        timing=_QueueTimingDeps(
            POLL_INTERVAL_SECONDS=POLL_INTERVAL_SECONDS,
            time=time,
            now_utc_iso=now_utc_iso,
        ),
        state=_QueueStateDeps(
            is_recovery_pending=is_recovery_pending,
            load_organized_ref=load_organized_ref,
            load_report_json=load_report_json,
            load_state=load_state,
            mark_recovery_pending=mark_recovery_pending,
            write_report_json=write_report_json,
            write_report_md_lines=write_report_md_lines,
            write_state=write_state,
        ),
        store=_QueueStoreDeps(
            activate_reserved_slot=activate_reserved_slot,
            get_cancel_requested=get_cancel_requested,
            mark_cancelled=mark_cancelled,
            mark_completed=mark_completed,
            mark_failed=mark_failed,
            reconcile_stale_slots=reconcile_stale_slots,
            release_slot=release_slot,
            requeue_running_entry=requeue_running_entry,
            reserve_dequeued_entry=reserve_dequeued_entry,
        ),
        job=_QueueJobDeps(
            finalize_xtb_job=finalize_xtb_job,
            notify_job_finished=notify_job_finished,
            notify_job_started=notify_job_started,
            run_xtb_ranking_job=run_xtb_ranking_job,
            start_xtb_job=start_xtb_job,
            upsert_job_record=upsert_job_record,
        ),
        helpers=_QueueHelperDeps(
            _admission_root=_admission_root,
            _build_terminal_result=_build_terminal_result,
            _coerce_mapping=_coerce_mapping,
            _dequeue_next_entry=_dequeue_next_entry,
            _entry_resource_request=_entry_resource_request,
            _ensure_terminal_queue_status=_ensure_terminal_queue_status,
            _execute_queue_entry=_execute_queue_entry,
            _finalize_execution_result=_finalize_execution_result,
            _input_summary=_input_summary,
            _job_dir=_job_dir,
            _job_type=_job_type,
            _load_terminal_summary=_load_terminal_summary,
            _mark_recovery_pending_state=_mark_recovery_pending_state,
            _pid_is_alive=_pid_is_alive,
            _print_terminal_summary=_print_terminal_summary,
            _queue_entries_with_roots=_queue_entries_with_roots,
            _queue_entry_by_id=_queue_entry_by_id,
            _reaction_key=_reaction_key,
            _request_job_cancellation=_request_job_cancellation,
            _selected_xyz=_selected_xyz,
            _start_background_job_process=_start_background_job_process,
            _terminate_process=_terminate_process,
            _try_reserve_admission_slot=_try_reserve_admission_slot,
            _write_execution_artifacts=_write_execution_artifacts,
        ),
    )


def _worker_execution_dependencies() -> _worker_execution.WorkerExecutionDependencies:
    return _worker_execution.WorkerExecutionDependencies(
        load_config=load_config,
        queue_entry_by_id=_queue_entry_by_id,
        activate_reserved_slot=activate_reserved_slot,
        release_slot=release_slot,
        job_dir=_job_dir,
        selected_xyz=_selected_xyz,
        job_type=_job_type,
        reaction_key=_reaction_key,
        input_summary=_input_summary,
        entry_resource_request=_entry_resource_request,
        matching_state=_matching_state,
        is_recovery_pending=is_recovery_pending,
        write_running_state=_write_running_state,
        upsert_job_record=upsert_job_record,
        notify_job_started=notify_job_started,
        build_terminal_result=_build_terminal_result,
        run_xtb_ranking_job=run_xtb_ranking_job,
        start_xtb_job=start_xtb_job,
        finalize_xtb_job=finalize_xtb_job,
        terminate_process=_terminate_process,
        wait_for_cancellable_process=_queue_execution.wait_for_cancellable_process,
        sleep=time.sleep,
        cancel_check_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        finalize_execution_result=_finalize_execution_result,
        execute_queue_entry=_execute_queue_entry,
    )


@dataclass(frozen=True)
class QueueExecutionOutcome:
    result: XtbRunResult
    organized_output_dir: str = ""


_RunningJob = _queue_worker_loop.RunningJob
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
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def _job_dir(entry: Any) -> Path:
    return Path(str(entry.metadata.get("job_dir", ""))).expanduser().resolve()


def _selected_xyz(entry: Any) -> Path:
    return Path(str(entry.metadata.get("selected_input_xyz", ""))).expanduser().resolve()


def _admission_root(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _admission_limit(cfg: Any) -> int:
    return resolve_admission_limit(cfg)


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
    return resource_dict(cfg.resources.max_cores_per_task, cfg.resources.max_memory_gb_per_task)


def _coerce_resource_dict(value: Any) -> dict[str, int]:
    return _config_engines.positive_int_mapping(value)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_execution.coerce_mapping(value)


def _coerce_list(value: Any) -> list[Any]:
    return _shared_coerce_list(value)


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
    return _coerce_resource_dict(entry.metadata.get("resource_request")) or _resource_caps(cfg)


def _job_type(entry: Any) -> str:
    value = str(entry.metadata.get("job_type", "")).strip().lower()
    return value or "path_search"


def _reaction_key(entry: Any, job_dir: Path) -> str:
    value = str(entry.metadata.get("reaction_key", "")).strip()
    return value or reaction_key_from_job_dir(job_dir)


def _input_summary(entry: Any) -> dict[str, Any]:
    payload = entry.metadata.get("input_summary", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _build_state_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> dict[str, Any]:
    return _queue_artifacts.build_state_payload(
        entry,
        result,
        previous_state=previous_state,
        resumed=resumed,
        deps=_queue_command_deps(),
    )


def _build_report_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> dict[str, Any]:
    return _queue_artifacts.build_report_payload(
        entry,
        result,
        previous_state=previous_state,
        resumed=resumed,
        deps=_queue_command_deps(),
    )


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
        deps=_queue_command_deps(),
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
        deps=_queue_command_deps(),
    )


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    _queue_artifacts.mark_recovery_pending_state(
        cfg,
        entry,
        reason=reason,
        deps=_queue_command_deps(),
    )


def _terminate_process(proc: _ManagedProcess) -> None:
    terminate_process_group(proc)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_queue_worker_slot(
        cfg,
        source="chemstack.xtb.queue_worker",
        app_name="xtb_auto",
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
        deps=_queue_command_deps(),
    )


def _print_terminal_summary(summary: _TerminalSummary) -> None:
    _queue_terminal.print_terminal_summary(summary)


def _terminal_status(
    state: dict[str, Any], report: dict[str, Any], refreshed: Any, rc: int | None
) -> str:
    return _queue_terminal.terminal_status(state, report, refreshed, rc)


def _terminal_reason(
    state: dict[str, Any], report: dict[str, Any], refreshed: Any, *, status: str, rc: int | None
) -> str:
    return _queue_terminal.terminal_reason(
        state,
        report,
        refreshed,
        status=status,
        rc=rc,
    )


def _terminal_metadata_update(
    state: dict[str, Any], report: dict[str, Any], entry: Any
) -> dict[str, Any]:
    return _queue_terminal.terminal_metadata_update(state, report, entry)


def _load_terminal_summary(
    queue_root: Path, entry: Any, *, rc: int | None = None
) -> _TerminalSummary:
    return _queue_terminal.load_terminal_summary(
        queue_root,
        entry,
        rc=rc,
        deps=_queue_command_deps(),
    )


def _ensure_terminal_queue_status(queue_root: Path, entry: Any, summary: _TerminalSummary) -> None:
    _queue_terminal.ensure_terminal_queue_status(
        queue_root,
        entry,
        summary,
        deps=_queue_command_deps(),
    )


def _finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    auto_organize: bool,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> QueueExecutionOutcome:
    return _queue_terminal.finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        auto_organize=auto_organize,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
        outcome_cls=QueueExecutionOutcome,
        deps=_queue_command_deps(),
    )


def _execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
) -> QueueExecutionOutcome:
    return _worker_execution.execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=auto_organize,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        worker_job_pid=worker_job_pid,
        emit_output=emit_output,
        dependencies=_worker_execution_dependencies(),
    )


def _build_background_worker_command(
    *,
    config_path: str,
    queue_root: Path,
    queue_id: str,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
) -> list[str]:
    return _queue_worker_loop.build_background_worker_command(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
        auto_organize=auto_organize,
        worker_job_module=WORKER_JOB_MODULE,
    )


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
) -> subprocess.Popen[str]:
    return _queue_worker_loop.start_background_job_process(
        config_path=config_path,
        queue_root=queue_root,
        entry=entry,
        admission_root=admission_root,
        admission_token=admission_token,
        auto_organize=auto_organize,
        worker_job_module=WORKER_JOB_MODULE,
    )


def _request_job_cancellation(proc: _ManagedProcess) -> None:
    _queue_worker_loop.request_job_cancellation(
        proc,
        cancel_signal=WORKER_CANCEL_SIGNAL,
        deps=_queue_command_deps(),
    )


def _config_path_for_worker(args: Any) -> str:
    return _queue_worker_loop.config_path_for_worker(
        args,
        default_config_path_fn=default_config_path,
    )


class QueueWorker(_queue_worker_loop.QueueWorker):
    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        auto_organize: bool,
        max_concurrent: int | None = None,
    ) -> None:
        super().__init__(
            cfg,
            config_path=config_path,
            auto_organize=False,
            max_concurrent=max_concurrent,
            deps=_queue_command_deps(),
        )
        del auto_organize


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    del auto_organize
    return _queue_worker_loop.process_one(
        cfg,
        auto_organize=False,
        deps=_queue_command_deps(),
    )


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
) -> int:
    del auto_organize
    return _worker_execution.run_worker_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
        auto_organize=False,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
        dependencies=_worker_execution_dependencies(),
    )


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    worker = QueueWorker(
        cfg,
        config_path=_config_path_for_worker(args),
        auto_organize=False,
    )
    return worker.run()
