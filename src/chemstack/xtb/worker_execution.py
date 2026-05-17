from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue.engine_execution import process_dequeued_engine_entry


def _queue_cmd() -> Any:
    from .commands import queue as queue_cmd

    return queue_cmd


@dataclass(frozen=True)
class _XtbExecutionContext:
    entry: Any
    job_dir: Path
    selected_xyz: Path
    job_type: str
    reaction_key: str
    input_summary: dict[str, Any]
    resource_request: dict[str, int]
    previous_state: dict[str, Any]
    resumed: bool


def _build_execution_context(cfg: Any, entry: Any) -> _XtbExecutionContext:
    q = _queue_cmd()
    job_dir = q._job_dir(entry)
    selected_xyz = q._selected_xyz(entry)
    job_type = q._job_type(entry)
    reaction_key = q._reaction_key(entry, job_dir)
    input_summary = q._input_summary(entry)
    resource_request = q._entry_resource_request(cfg, entry)
    previous_state = q._matching_state(
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
    )
    resumed = q.is_recovery_pending(previous_state) or str(
        previous_state.get("status", "")
    ).strip().lower() == "running"
    return _XtbExecutionContext(
        entry=entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        resource_request=resource_request,
        previous_state=previous_state,
        resumed=resumed,
    )


def _mark_job_running(cfg: Any, context: _XtbExecutionContext, *, worker_job_pid: int | None) -> None:
    q = _queue_cmd()
    q._write_running_state(
        cfg,
        context.entry,
        worker_job_pid=worker_job_pid,
        previous_state=context.previous_state,
        resumed=context.resumed,
    )
    q.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status="running",
        job_dir=context.job_dir,
        job_type=context.job_type,
        selected_input_xyz=str(context.selected_xyz),
        reaction_key=context.reaction_key,
        resource_request=context.resource_request,
        resource_actual=context.resource_request,
    )
    q.notify_job_started(
        cfg,
        job_id=context.entry.task_id,
        queue_id=context.entry.queue_id,
        job_dir=context.job_dir,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        selected_xyz=context.selected_xyz,
    )


def _cancelled_before_start_result(context: _XtbExecutionContext) -> Any:
    q = _queue_cmd()
    return q._build_terminal_result(
        context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        input_summary=context.input_summary,
        resource_request=context.resource_request,
        status="cancelled",
        reason="cancel_requested",
        exit_code=1,
    )


def _failed_result_from_exception(context: _XtbExecutionContext, exc: Exception) -> Any:
    q = _queue_cmd()
    return q._build_terminal_result(
        context.entry,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        job_type=context.job_type,
        reaction_key=context.reaction_key,
        input_summary=context.input_summary,
        resource_request=context.resource_request,
        status="failed",
        reason=f"runner_error:{exc}",
        exit_code=1,
    )


def _run_xtb_job_for_entry(
    cfg: Any,
    context: _XtbExecutionContext,
    _queue_root: Path,
    *,
    should_cancel: Callable[[], bool] | None,
    register_running_job: Callable[[Any | None], None] | None,
) -> Any:
    q = _queue_cmd()
    try:
        if should_cancel is not None and should_cancel():
            return _cancelled_before_start_result(context)
        if context.job_type == "ranking":
            return q.run_xtb_ranking_job(
                cfg,
                job_dir=context.job_dir,
                should_cancel=should_cancel,
                on_running_job=register_running_job,
                terminate_process=q._terminate_process,
            )

        running = q.start_xtb_job(
            cfg,
            job_dir=context.job_dir,
            selected_input_xyz=context.selected_xyz,
        )
        if register_running_job is not None:
            register_running_job(running)
        try:
            return q._queue_execution.wait_for_cancellable_process(
                running,
                finalize_fn=q.finalize_xtb_job,
                terminate_process_fn=q._terminate_process,
                should_cancel=should_cancel,
                sleep_fn=q.time.sleep,
                poll_interval_seconds=q.CANCEL_CHECK_INTERVAL_SECONDS,
                check_cancel_before_poll=True,
            )
        finally:
            if register_running_job is not None:
                register_running_job(None)
    except Exception as exc:
        return _failed_result_from_exception(context, exc)


def _finalize_processed_entry(
    cfg: Any,
    context: _XtbExecutionContext,
    result: Any,
    queue_root: Path,
    *,
    auto_organize: bool,
    emit_output: bool,
) -> Any:
    q = _queue_cmd()
    return q._finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=context.entry,
        result=result,
        auto_organize=auto_organize,
        emit_output=emit_output,
        previous_state=context.previous_state,
        resumed=context.resumed,
    )


def execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
) -> Any:
    del auto_organize
    return process_dequeued_engine_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=False,
        build_context_fn=_build_execution_context,
        check_shutdown_fn=None,
        mark_running_fn=lambda cfg_obj, context: _mark_job_running(
            cfg_obj,
            context,
            worker_job_pid=worker_job_pid,
        ),
        run_job_fn=lambda cfg_obj, context, active_queue_root: _run_xtb_job_for_entry(
            cfg_obj,
            context,
            active_queue_root,
            should_cancel=should_cancel,
            register_running_job=register_running_job,
        ),
        finalize_entry_fn=lambda cfg_obj, context, result, active_queue_root, should_organize: (
            _finalize_processed_entry(
                cfg_obj,
                context,
                result,
                active_queue_root,
                auto_organize=should_organize,
                emit_output=emit_output,
            )
        ),
        build_outcome_fn=lambda _context, _result, outcome: outcome,
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
    q = _queue_cmd()
    cfg = q.load_config(config_path)
    resolved_queue_root = Path(queue_root).expanduser().resolve()
    entry = q._queue_entry_by_id(resolved_queue_root, queue_id)
    if entry is None:
        return 1

    if admission_token:
        activated = q.activate_reserved_slot(
            admission_root,
            admission_token,
            work_dir=q._job_dir(entry),
            queue_id=entry.queue_id,
            source="chemstack.xtb.worker_job",
        )
        if activated is None:
            return 1

    try:
        outcome = q._execute_queue_entry(
            cfg,
            queue_root=resolved_queue_root,
            entry=entry,
            auto_organize=False,
            should_cancel=should_cancel,
            register_running_job=register_running_job,
            emit_output=False,
            worker_job_pid=os.getpid(),
        )
        return 0 if outcome.result.status in {"completed", "cancelled"} else 1
    finally:
        if admission_token:
            q.release_slot(admission_root, admission_token)


__all__ = [
    "execute_queue_entry",
    "run_worker_job",
]
