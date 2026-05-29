from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.queue import execution as _queue_execution


@dataclass(frozen=True)
class EngineWorkerLifecycle:
    build_context: Callable[[Any, Any], Any]
    mark_running: Callable[[Any, Any], None]
    run_job: Callable[[Any, Any, Path], Any]
    finalize_entry: Callable[[Any, Any, Any, Path], Any]
    build_outcome: Callable[[Any, Any, Any], Any]
    check_shutdown: Callable[[Any], None] | None = None


@dataclass(frozen=True)
class CancellableProcessExecution:
    start_job: Callable[[], Any]
    finalize_job: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    build_failure_result: Callable[[Exception], Any]
    wait_for_cancellable_process: Callable[..., Any] = _queue_execution.wait_for_cancellable_process
    should_cancel: Callable[[], bool] | None = None
    shutdown_requested: Callable[[], bool] | None = None
    on_shutdown: Callable[[Any], Any] | None = None
    sleep: Callable[[float], None] | None = None
    poll_interval_seconds: float = 1.0
    check_cancel_before_poll: bool = False
    register_running_job: Callable[[Any | None], None] | None = None
    should_reraise_exception: Callable[[Exception], bool] | None = None


@dataclass(frozen=True)
class TerminalSyncActions:
    write_artifacts: Callable[[], Any]
    mark_queue_terminal: Callable[[], Any]
    sync_job_record: Callable[[], Any]
    notify_finished: Callable[[Any], Any]
    build_outcome: Callable[[Any], Any]
    emit_output: Callable[[Any], Any] | None = None


@dataclass(frozen=True)
class TerminalArtifactPayloads:
    state: dict[str, Any]
    report: dict[str, Any]


@dataclass(frozen=True)
class EngineArtifactFields:
    selected_input_xyz: str
    engine_fields: Mapping[str, Any] | None = None
    detail_fields: Mapping[str, Any] | None = None

    def engine_payload(self) -> dict[str, Any]:
        return dict(self.engine_fields or {})

    def detail_payload(self) -> dict[str, Any]:
        return dict(self.detail_fields or {})


@dataclass(frozen=True)
class TerminalArtifactWriters:
    write_state: Callable[..., Any]
    write_report_json: Callable[..., Any]
    write_report_md_lines: Callable[..., Any]


def sync_terminal_result(
    actions: TerminalSyncActions,
    *,
    emit_output: bool = False,
) -> Any:
    actions.write_artifacts()
    actions.mark_queue_terminal()
    sync_result = actions.sync_job_record()
    actions.notify_finished(sync_result)
    if emit_output and actions.emit_output is not None:
        actions.emit_output(sync_result)
    return actions.build_outcome(sync_result)


def run_cancellable_process_execution(actions: CancellableProcessExecution) -> Any:
    try:
        running = actions.start_job()
        if actions.register_running_job is not None:
            actions.register_running_job(running)
        try:
            wait_kwargs: dict[str, Any] = {
                "finalize_fn": actions.finalize_job,
                "terminate_process_fn": actions.terminate_process,
                "should_cancel": actions.should_cancel,
                "shutdown_requested": actions.shutdown_requested,
                "on_shutdown": actions.on_shutdown,
                "poll_interval_seconds": actions.poll_interval_seconds,
                "check_cancel_before_poll": actions.check_cancel_before_poll,
            }
            if actions.sleep is not None:
                wait_kwargs["sleep_fn"] = actions.sleep
            return actions.wait_for_cancellable_process(running, **wait_kwargs)
        finally:
            if actions.register_running_job is not None:
                actions.register_running_job(None)
    except Exception as exc:
        if actions.should_reraise_exception is not None and actions.should_reraise_exception(exc):
            raise
        return actions.build_failure_result(exc)


def entry_metadata_value(entry: Any, key: str, default: Any = "") -> Any:
    metadata = getattr(entry, "metadata", {})
    getter = getattr(metadata, "get", None)
    if getter is None:
        return default
    return getter(key, default)


def entry_metadata_text(entry: Any, key: str, default: Any = "") -> str:
    return str(entry_metadata_value(entry, key, default)).strip()


def entry_metadata_resolved_path(entry: Any, key: str, default: Any = "") -> Path:
    return Path(str(entry_metadata_value(entry, key, default))).expanduser().resolve()


def entry_metadata_dict(entry: Any, key: str) -> dict[str, Any]:
    payload = entry_metadata_value(entry, key, {})
    return dict(payload) if isinstance(payload, dict) else {}


def engine_resource_caps(
    cfg: Any,
    *,
    resource_dict_fn: Callable[[Any, Any], dict[str, int]],
) -> dict[str, int]:
    return resource_dict_fn(
        cfg.resources.max_cores_per_task,
        cfg.resources.max_memory_gb_per_task,
    )


def coerce_resource_request(value: Any) -> dict[str, int]:
    from chemstack.core.config import engines as _config_engines

    return _config_engines.positive_int_mapping(value)


def entry_resource_request(
    cfg: Any,
    entry: Any,
    *,
    resource_caps_fn: Callable[[Any], dict[str, int]],
    coerce_resource_request_fn: Callable[[Any], dict[str, int]] = coerce_resource_request,
) -> dict[str, int]:
    return coerce_resource_request_fn(
        entry_metadata_value(entry, "resource_request")
    ) or resource_caps_fn(cfg)


def is_resumed_state(
    previous_state: dict[str, Any],
    *,
    is_recovery_pending_fn: Callable[[dict[str, Any]], bool],
) -> bool:
    return (
        is_recovery_pending_fn(previous_state)
        or str(previous_state.get("status", "")).strip().lower() == "running"
    )


def build_running_state_payload(
    entry: Any,
    *,
    job_dir: Path,
    selected_input_xyz: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": selected_input_xyz,
        **dict(engine_fields or {}),
        "status": "running",
        "reason": recovery_reason if resumed else "",
        "started_at": started_at,
        "updated_at": updated_at,
        **dict(detail_fields or {}),
        "resource_request": resource_request,
        "resource_actual": dict(resource_request),
        "created_at": _queue_execution.created_at(previous_state) or started_at,
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def write_running_state_artifact(
    entry: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    write_state_fn: Callable[..., Any],
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
    worker_job_pid: int | None = None,
) -> None:
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    payload = build_running_state_payload(
        entry,
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz,
        started_at=started_at,
        updated_at=updated_at,
        previous_state=previous_state,
        resumed=resumed,
        resource_request=resource_request,
        engine_fields=engine_fields,
        detail_fields=detail_fields,
    )
    if worker_job_pid is not None and worker_job_pid > 0:
        payload["worker_job_pid"] = int(worker_job_pid)
    write_state_fn(job_dir, payload)


def write_running_engine_state_artifact(
    entry: Any,
    *,
    job_dir_text: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    artifact_fields: EngineArtifactFields,
    write_state_fn: Callable[..., Any],
    worker_job_pid: int | None = None,
) -> None:
    write_running_state_artifact(
        entry,
        job_dir_text=job_dir_text,
        selected_input_xyz=artifact_fields.selected_input_xyz,
        started_at=started_at,
        updated_at=updated_at,
        previous_state=previous_state,
        resumed=resumed,
        resource_request=resource_request,
        write_state_fn=write_state_fn,
        engine_fields=artifact_fields.engine_payload(),
        detail_fields=artifact_fields.detail_payload(),
        worker_job_pid=worker_job_pid,
    )


def build_terminal_state_payload(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": job_dir_text,
        "selected_input_xyz": selected_input_xyz,
        **dict(engine_fields or {}),
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        **dict(detail_fields or {}),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(previous_state),
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_terminal_report_payload(
    entry: Any,
    result: Any,
    *,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        **dict(engine_fields or {}),
        "selected_input_xyz": selected_input_xyz,
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        **dict(detail_fields or {}),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(previous_state),
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_terminal_artifact_payloads(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> TerminalArtifactPayloads:
    return TerminalArtifactPayloads(
        state=build_terminal_state_payload(
            entry,
            result,
            job_dir_text=job_dir_text,
            selected_input_xyz=selected_input_xyz,
            previous_state=previous_state,
            resumed=resumed,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
        report=build_terminal_report_payload(
            entry,
            result,
            selected_input_xyz=selected_input_xyz,
            previous_state=previous_state,
            resumed=resumed,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
    )


def write_terminal_execution_artifacts(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None,
    detail_fields: dict[str, Any] | None,
    report_lines: list[str],
    write_state_fn: Callable[..., Any],
    write_report_json_fn: Callable[..., Any],
    write_report_md_lines_fn: Callable[..., Any],
) -> None:
    write_terminal_engine_artifacts(
        entry,
        result,
        job_dir_text=job_dir_text,
        previous_state=previous_state,
        resumed=resumed,
        artifact_fields=EngineArtifactFields(
            selected_input_xyz=selected_input_xyz,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
        report_lines=report_lines,
        writers=TerminalArtifactWriters(
            write_state=write_state_fn,
            write_report_json=write_report_json_fn,
            write_report_md_lines=write_report_md_lines_fn,
        ),
    )


def write_terminal_engine_artifacts(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    artifact_fields: EngineArtifactFields,
    report_lines: list[str],
    writers: TerminalArtifactWriters,
) -> None:
    if not job_dir_text:
        return
    payloads = build_terminal_artifact_payloads(
        entry,
        result,
        job_dir_text=job_dir_text,
        selected_input_xyz=artifact_fields.selected_input_xyz,
        previous_state=previous_state,
        resumed=resumed,
        engine_fields=artifact_fields.engine_payload(),
        detail_fields=artifact_fields.detail_payload(),
    )
    _queue_execution.write_result_artifacts(
        job_dir_text,
        state_payload=payloads.state,
        report_payload=payloads.report,
        report_lines=report_lines,
        write_state_fn=writers.write_state,
        write_report_json_fn=writers.write_report_json,
        write_report_md_lines_fn=writers.write_report_md_lines,
    )


def terminal_report_lines(
    entry: Any,
    result: Any,
    *,
    title: str,
    selected_input_label: str,
    selected_input_xyz: str,
    engine_lines: list[str] | None = None,
    detail_lines: list[str] | None = None,
) -> list[str]:
    return [
        f"# {title}",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        *list(engine_lines or []),
        f"- {selected_input_label}: `{Path(selected_input_xyz).name}`",
        f"- Exit Code: `{result.exit_code}`",
        *list(detail_lines or []),
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]


def mark_engine_job_running(
    cfg: Any,
    *,
    entry: Any,
    job_dir: Path,
    selected_xyz: Path,
    resource_request: dict[str, int],
    write_running_state_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    notify_job_started_fn: Callable[..., Any],
    record_fields: dict[str, Any] | None = None,
    notify_fields: dict[str, Any] | None = None,
    write_running_state_kwargs: dict[str, Any] | None = None,
) -> None:
    write_running_state_fn(cfg, entry, **dict(write_running_state_kwargs or {}))
    upsert_job_record_fn(
        cfg,
        job_id=entry.task_id,
        status="running",
        job_dir=job_dir,
        selected_input_xyz=str(selected_xyz),
        **dict(record_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )
    notify_job_started_fn(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        **dict(notify_fields or {}),
    )


def mark_recovery_pending_and_record(
    cfg: Any,
    *,
    entry: Any,
    job_dir: Path,
    selected_input_xyz: Path | str,
    reason: str,
    resource_request: dict[str, int],
    mark_recovery_pending_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    state_identity_fields: dict[str, Any] | None = None,
    record_identity_fields: dict[str, Any] | None = None,
) -> None:
    selected_input_xyz_text = str(selected_input_xyz)
    mark_recovery_pending_fn(
        job_dir,
        job_id=str(entry.task_id),
        selected_input_xyz=selected_input_xyz_text,
        **dict(state_identity_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
        reason=reason,
    )
    upsert_job_record_fn(
        cfg,
        job_id=entry.task_id,
        status="pending",
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz_text,
        **dict(record_identity_fields or {}),
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )


def build_terminal_result(
    result_cls: type,
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    log_prefix: str,
    manifest_filename: str,
    resource_request: dict[str, int],
    status: str,
    reason: str,
    now_utc_iso_fn: Callable[[], str],
    command: tuple[str, ...] = (),
    exit_code: int = 1,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> Any:
    terminal_time = now_utc_iso_fn()
    manifest_path = (job_dir / manifest_filename).resolve()
    return result_cls(
        status=status,
        reason=reason,
        command=command,
        exit_code=exit_code,
        started_at=entry.started_at or terminal_time,
        finished_at=terminal_time,
        stdout_log=str((job_dir / f"{log_prefix}.stdout.log").resolve()),
        stderr_log=str((job_dir / f"{log_prefix}.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        **dict(engine_fields or {}),
        **dict(detail_fields or {}),
        manifest_path=str(manifest_path) if manifest_path.exists() else "",
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )


def run_engine_worker_lifecycle(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    lifecycle: EngineWorkerLifecycle,
) -> Any:
    active_queue_root = queue_root or Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
    context = lifecycle.build_context(cfg, entry)
    if lifecycle.check_shutdown is not None:
        lifecycle.check_shutdown(context)
    lifecycle.mark_running(cfg, context)
    if lifecycle.check_shutdown is not None:
        lifecycle.check_shutdown(context)

    result = lifecycle.run_job(cfg, context, active_queue_root)
    organized_output_dir = lifecycle.finalize_entry(
        cfg,
        context,
        result,
        active_queue_root,
    )
    return lifecycle.build_outcome(context, result, organized_output_dir)
