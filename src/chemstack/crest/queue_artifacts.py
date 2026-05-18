from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.config import engines as _config_engines
from chemstack.core.queue import execution as _queue_execution

from .runner import CrestRunResult
from .state import (
    is_recovery_pending,
    load_state,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)


def coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_execution.coerce_mapping(value)


def matching_result_state(
    entry: Any,
    result: CrestRunResult,
    job_dir: Path,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
) -> dict[str, Any]:
    return _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
        match_kwargs={
            "selected_input_xyz": result.selected_input_xyz,
            "mode": result.mode,
            "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        },
    )


def build_state_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_state = coerce_mapping(previous_state)
    recovery_reason = _queue_execution.recovery_reason(base_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(entry.metadata.get("job_dir", "")).strip(),
        "selected_input_xyz": result.selected_input_xyz,
        "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        "mode": result.mode,
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        "retained_conformer_count": result.retained_conformer_count,
        "retained_conformer_paths": list(result.retained_conformer_paths),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(base_state),
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(base_state),
        "resumed": bool(base_state.get("resumed", False)),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_report_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_state = coerce_mapping(previous_state)
    recovery_reason = _queue_execution.recovery_reason(base_state)
    payload = {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        "mode": result.mode,
        "selected_input_xyz": result.selected_input_xyz,
        "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "retained_conformer_count": result.retained_conformer_count,
        "retained_conformer_paths": list(result.retained_conformer_paths),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(base_state),
        "recovery_count": _queue_execution.recovery_count(base_state),
        "resumed": bool(base_state.get("resumed", False)),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def report_lines(entry: Any, result: CrestRunResult) -> list[str]:
    lines = [
        "# crest_auto Report",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        f"- Mode: `{result.mode}`",
        f"- Selected XYZ: `{Path(result.selected_input_xyz).name}`",
        f"- Molecule Key: `{str(entry.metadata.get('molecule_key', '')).strip() or '-'}`",
        f"- Exit Code: `{result.exit_code}`",
        f"- Retained Conformers: `{result.retained_conformer_count}`",
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]
    if result.retained_conformer_paths:
        lines.append("- Retained Files:")
        for path in result.retained_conformer_paths:
            lines.append(f"  - `{path}`")
    return lines


def write_execution_artifacts(
    entry: Any,
    result: CrestRunResult,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
    write_state_fn: Any = write_state,
    write_report_json_fn: Any = write_report_json,
    write_report_md_lines_fn: Any = write_report_md_lines,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    previous_state = matching_result_state(
        entry,
        result,
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
    )
    _queue_execution.write_result_artifacts(
        job_dir_text,
        state_payload=build_state_payload(entry, result, previous_state=previous_state),
        report_payload=build_report_payload(entry, result, previous_state=previous_state),
        report_lines=report_lines(entry, result),
        write_state_fn=write_state_fn,
        write_report_json_fn=write_report_json_fn,
        write_report_md_lines_fn=write_report_md_lines_fn,
    )


def depsafe_now_utc_iso() -> str:
    from chemstack.core.utils import now_utc_iso as dynamic_now_utc_iso

    return dynamic_now_utc_iso()


def resource_caps(cfg: Any) -> dict[str, int]:
    from .job_locations import resource_dict

    return resource_dict(cfg.resources.max_cores_per_task, cfg.resources.max_memory_gb_per_task)


def coerce_resource_dict(value: Any) -> dict[str, int]:
    return _config_engines.positive_int_mapping(value)


def entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    metadata = getattr(entry, "metadata", {})
    return coerce_resource_dict(metadata.get("resource_request")) or resource_caps(cfg)


def write_running_state(
    cfg: Any,
    entry: Any,
    *,
    load_state_fn: Any = load_state,
    state_matches_job_fn: Any = state_matches_job,
    is_recovery_pending_fn: Any = is_recovery_pending,
    write_state_fn: Any = write_state,
    now_utc_iso_fn: Any = depsafe_now_utc_iso,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    resource_request = entry_resource_request(cfg, entry)
    previous_state = _queue_execution.load_matching_state(
        job_dir,
        load_state_fn=load_state_fn,
        state_matches_job_fn=state_matches_job_fn,
        match_kwargs={
            "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
            "mode": str(entry.metadata.get("mode", "standard")).strip(),
            "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        },
    )
    resumed = False
    recovery_reason = ""
    if previous_state:
        resumed = (
            is_recovery_pending_fn(previous_state)
            or str(previous_state.get("status", "")).strip().lower() == "running"
        )
        recovery_reason = _queue_execution.recovery_reason(previous_state)
    started_at = entry.started_at or now_utc_iso_fn()
    updated_at = now_utc_iso_fn()
    write_state_fn(
        job_dir,
        {
            "job_id": entry.task_id,
            "job_dir": str(job_dir),
            "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
            "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
            "mode": str(entry.metadata.get("mode", "standard")).strip(),
            "status": "running",
            "reason": recovery_reason if resumed else "",
            "started_at": started_at,
            "updated_at": updated_at,
            "resource_request": resource_request,
            "resource_actual": dict(resource_request),
            "created_at": _queue_execution.created_at(previous_state) or started_at,
            "recovery_pending": False,
            "recovery_count": _queue_execution.recovery_count(previous_state),
            "resumed": resumed,
            **({"recovery_reason": recovery_reason} if recovery_reason else {}),
        },
    )


__all__ = [
    "build_report_payload",
    "build_state_payload",
    "entry_resource_request",
    "matching_result_state",
    "resource_caps",
    "write_execution_artifacts",
    "write_running_state",
]
