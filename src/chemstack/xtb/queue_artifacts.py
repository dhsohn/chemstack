from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.queue import execution as _queue_execution

from .runner import XtbRunResult


def build_state_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any,
) -> dict[str, Any]:
    base_state = deps._coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    recovery_reason = _queue_execution.recovery_reason(base_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(entry.metadata.get("job_dir", "")).strip(),
        "selected_input_xyz": result.selected_input_xyz,
        "job_type": result.job_type,
        "reaction_key": result.reaction_key,
        "input_summary": dict(result.input_summary),
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        "candidate_count": result.candidate_count,
        "candidate_paths": candidate_paths,
        "selected_candidate_paths": list(result.selected_candidate_paths),
        "candidate_details": [dict(item) for item in result.candidate_details],
        "analysis_summary": dict(result.analysis_summary),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(base_state),
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(base_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_report_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any,
) -> dict[str, Any]:
    base_state = deps._coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    recovery_reason = _queue_execution.recovery_reason(base_state)
    payload = {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        "job_type": result.job_type,
        "reaction_key": result.reaction_key,
        "selected_input_xyz": result.selected_input_xyz,
        "input_summary": dict(result.input_summary),
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "candidate_count": result.candidate_count,
        "candidate_paths": candidate_paths,
        "selected_candidate_paths": list(result.selected_candidate_paths),
        "candidate_details": [dict(item) for item in result.candidate_details],
        "analysis_summary": dict(result.analysis_summary),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(base_state),
        "recovery_count": _queue_execution.recovery_count(base_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return

    lines = [
        "# xtb_auto Report",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        f"- Job Type: `{result.job_type}`",
        f"- Reaction Key: `{result.reaction_key}`",
        f"- Selected Input XYZ: `{Path(result.selected_input_xyz).name}`",
        f"- Exit Code: `{result.exit_code}`",
        f"- Candidate Count: `{result.candidate_count}`",
        f"- Input Summary: `{result.input_summary}`",
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]
    if result.selected_candidate_paths:
        lines.append("- Selected Candidate Paths:")
        for path in result.selected_candidate_paths:
            lines.append(f"  - `{path}`")
    if result.job_type == "ranking" and result.analysis_summary:
        if result.analysis_summary.get("best_candidate_path"):
            lines.append(
                f"- Best Candidate Path: `{result.analysis_summary.get('best_candidate_path')}`"
            )
        if result.analysis_summary.get("best_total_energy") is not None:
            lines.append(
                f"- Best Total Energy: `{result.analysis_summary.get('best_total_energy')}`"
            )
    if result.analysis_summary:
        lines.append(f"- Analysis Summary: `{result.analysis_summary}`")
    _queue_execution.write_result_artifacts(
        job_dir_text,
        state_payload=build_state_payload(
            entry,
            result,
            previous_state=previous_state,
            resumed=resumed,
            deps=deps,
        ),
        report_payload=build_report_payload(
            entry,
            result,
            previous_state=previous_state,
            resumed=resumed,
            deps=deps,
        ),
        report_lines=lines,
        write_state_fn=deps.write_state,
        write_report_json_fn=deps.write_report_json,
        write_report_md_lines_fn=deps.write_report_md_lines,
    )


def write_running_state(
    cfg: Any,
    entry: Any,
    *,
    worker_job_pid: int | None = None,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    input_summary = deps._input_summary(entry)
    resource_request = deps._entry_resource_request(cfg, entry)
    base_state = deps._coerce_mapping(previous_state)
    recovery_reason = _queue_execution.recovery_reason(base_state)
    started_at = entry.started_at or deps.now_utc_iso()
    updated_at = deps.now_utc_iso()
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
        "job_type": deps._job_type(entry),
        "reaction_key": deps._reaction_key(entry, job_dir),
        "input_summary": input_summary,
        "status": "running",
        "reason": recovery_reason if resumed else "",
        "started_at": started_at,
        "updated_at": updated_at,
        "candidate_count": int(input_summary.get("candidate_count", 0) or 0),
        "candidate_paths": list(input_summary.get("candidate_paths", [])),
        "selected_candidate_paths": [],
        "candidate_details": [],
        "analysis_summary": {},
        "resource_request": resource_request,
        "resource_actual": dict(resource_request),
        "created_at": _queue_execution.created_at(base_state) or started_at,
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(base_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    if worker_job_pid is not None and worker_job_pid > 0:
        payload["worker_job_pid"] = int(worker_job_pid)
    deps.write_state(job_dir, payload)


def mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str, deps: Any) -> None:
    job_dir = deps._job_dir(entry)
    selected_xyz = deps._selected_xyz(entry)
    job_type = deps._job_type(entry)
    reaction_key = deps._reaction_key(entry, job_dir)
    input_summary = deps._input_summary(entry)
    resource_request = deps._entry_resource_request(cfg, entry)
    deps.mark_recovery_pending(
        job_dir,
        job_id=str(entry.task_id),
        selected_input_xyz=str(selected_xyz),
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        resource_request=resource_request,
        resource_actual=resource_request,
        reason=reason,
    )
    deps.upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status="pending",
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=str(selected_xyz),
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_request,
    )


def build_terminal_result(
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
    deps: Any,
) -> XtbRunResult:
    terminal_time = deps.now_utc_iso()
    manifest_path = (job_dir / "xtb_job.yaml").resolve()
    return XtbRunResult(
        status=status,
        reason=reason,
        command=command,
        exit_code=exit_code,
        started_at=entry.started_at or terminal_time,
        finished_at=terminal_time,
        stdout_log=str((job_dir / "xtb.stdout.log").resolve()),
        stderr_log=str((job_dir / "xtb.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        candidate_count=0,
        selected_candidate_paths=(),
        candidate_details=(),
        analysis_summary={},
        manifest_path=str(manifest_path) if manifest_path.exists() else "",
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )
