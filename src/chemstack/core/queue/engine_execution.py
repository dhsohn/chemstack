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
    finalize_entry: Callable[[Any, Any, Any, Path, bool], Any]
    build_outcome: Callable[[Any, Any, Any], Any]
    check_shutdown: Callable[[Any], None] | None = None


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


def run_engine_worker_lifecycle(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    auto_organize: bool,
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
        auto_organize,
    )
    return lifecycle.build_outcome(context, result, organized_output_dir)


def process_dequeued_engine_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    auto_organize: bool,
    build_context_fn: Callable[[Any, Any], Any],
    check_shutdown_fn: Callable[[Any], None] | None,
    mark_running_fn: Callable[[Any, Any], None],
    run_job_fn: Callable[[Any, Any, Path], Any],
    finalize_entry_fn: Callable[[Any, Any, Any, Path, bool], Any],
    build_outcome_fn: Callable[[Any, Any, Any], Any],
) -> Any:
    return run_engine_worker_lifecycle(
        cfg,
        entry,
        queue_root=queue_root,
        auto_organize=auto_organize,
        lifecycle=EngineWorkerLifecycle(
            build_context=build_context_fn,
            check_shutdown=check_shutdown_fn,
            mark_running=mark_running_fn,
            run_job=run_job_fn,
            finalize_entry=finalize_entry_fn,
            build_outcome=build_outcome_fn,
        ),
    )
