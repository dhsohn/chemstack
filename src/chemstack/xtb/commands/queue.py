from __future__ import annotations

import os
import signal
import subprocess
import sys
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
from .organize import organize_job_dir
from .. import queue_artifacts as _queue_artifacts
from .. import queue_terminal as _queue_terminal
from .. import queue_worker_loop as _queue_worker_loop

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190
WORKER_JOB_MODULE = "chemstack.xtb.worker_job"
_WORKER_EXECUTION_COMPAT = (
    activate_reserved_slot,
    finalize_xtb_job,
    get_cancel_requested,
    is_recovery_pending,
    load_organized_ref,
    load_report_json,
    mark_cancelled,
    mark_completed,
    mark_failed,
    mark_recovery_pending,
    now_utc_iso,
    notify_job_started,
    notify_job_finished,
    os,
    organize_job_dir,
    reconcile_stale_slots,
    release_slot,
    requeue_running_entry,
    reserve_dequeued_entry,
    run_xtb_ranking_job,
    start_xtb_job,
    subprocess,
    time,
    upsert_job_record,
    write_report_json,
    write_report_md_lines,
    write_state,
)


def _this_module() -> Any:
    return sys.modules[__name__]


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
        deps=_this_module(),
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
        deps=_this_module(),
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
        deps=_this_module(),
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
        deps=_this_module(),
    )


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    _queue_artifacts.mark_recovery_pending_state(
        cfg,
        entry,
        reason=reason,
        deps=_this_module(),
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
        deps=_this_module(),
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
        deps=_this_module(),
    )


def _ensure_terminal_queue_status(queue_root: Path, entry: Any, summary: _TerminalSummary) -> None:
    _queue_terminal.ensure_terminal_queue_status(
        queue_root,
        entry,
        summary,
        deps=_this_module(),
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
        deps=_this_module(),
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
        deps=_this_module(),
    )


def _resolve_worker_auto_organize(cfg: Any, args: Any) -> bool:
    return _queue_worker_loop.resolve_worker_auto_organize(cfg, args)


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
            auto_organize=auto_organize,
            max_concurrent=max_concurrent,
            deps=_this_module(),
        )


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    return _queue_worker_loop.process_one(
        cfg,
        auto_organize=auto_organize,
        deps=_this_module(),
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
    return _worker_execution.run_worker_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_root=admission_root,
        admission_token=admission_token,
        auto_organize=auto_organize,
        should_cancel=should_cancel,
        register_running_job=register_running_job,
    )


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    auto_organize = _resolve_worker_auto_organize(cfg, args)
    worker = QueueWorker(
        cfg,
        config_path=_config_path_for_worker(args),
        auto_organize=auto_organize,
    )
    return worker.run()
