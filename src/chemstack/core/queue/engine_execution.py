from __future__ import annotations

from collections.abc import Callable
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
class TerminalSyncActions:
    write_artifacts: Callable[[], Any]
    mark_queue_terminal: Callable[[], Any]
    sync_job_record: Callable[[], Any]
    notify_finished: Callable[[Any], Any]
    build_outcome: Callable[[Any], Any]
    emit_output: Callable[[Any], Any] | None = None


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
