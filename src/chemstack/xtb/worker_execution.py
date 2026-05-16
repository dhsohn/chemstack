from __future__ import annotations

from pathlib import Path
from typing import Any, Callable


def _queue_cmd() -> Any:
    from .commands import queue as queue_cmd

    return queue_cmd


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

    q._write_running_state(
        cfg,
        entry,
        worker_job_pid=worker_job_pid,
        previous_state=previous_state,
        resumed=resumed,
    )
    q.upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status="running",
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=str(selected_xyz),
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_request,
    )
    q.notify_job_started(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        job_type=job_type,
        reaction_key=reaction_key,
        selected_xyz=selected_xyz,
    )

    try:
        if should_cancel is not None and should_cancel():
            result = q._build_terminal_result(
                entry,
                job_dir=job_dir,
                selected_xyz=selected_xyz,
                job_type=job_type,
                reaction_key=reaction_key,
                input_summary=input_summary,
                resource_request=resource_request,
                status="cancelled",
                reason="cancel_requested",
                exit_code=1,
            )
        elif job_type == "ranking":
            result = q.run_xtb_ranking_job(
                cfg,
                job_dir=job_dir,
                should_cancel=should_cancel,
                on_running_job=register_running_job,
                terminate_process=q._terminate_process,
            )
        else:
            running = q.start_xtb_job(cfg, job_dir=job_dir, selected_input_xyz=selected_xyz)
            if register_running_job is not None:
                register_running_job(running)
            try:
                result = q._queue_execution.wait_for_cancellable_process(
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
        result = q._build_terminal_result(
            entry,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            job_type=job_type,
            reaction_key=reaction_key,
            input_summary=input_summary,
            resource_request=resource_request,
            status="failed",
            reason=f"runner_error:{exc}",
            exit_code=1,
        )

    return q._finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        auto_organize=auto_organize,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
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
            auto_organize=auto_organize,
            should_cancel=should_cancel,
            register_running_job=register_running_job,
            emit_output=False,
            worker_job_pid=q.os.getpid(),
        )
        return 0 if outcome.result.status in {"completed", "cancelled"} else 1
    finally:
        if admission_token:
            q.release_slot(admission_root, admission_token)


__all__ = [
    "execute_queue_entry",
    "run_worker_job",
]
