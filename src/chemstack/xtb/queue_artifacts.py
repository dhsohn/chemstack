from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution

from .runner import XtbRunResult


def _dependency(deps: Any | None, explicit: Any, name: str) -> Any:
    if explicit is not None:
        return explicit
    if deps is not None:
        return getattr(deps, name)
    raise TypeError(f"missing required dependency: {name}")


def build_state_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any | None = None,
    coerce_mapping_fn: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    coerce_mapping = _dependency(deps, coerce_mapping_fn, "_coerce_mapping")
    base_state = coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    return _engine_execution.build_terminal_state_payload(
        entry,
        result,
        job_dir_text=_engine_execution.entry_metadata_text(entry, "job_dir"),
        selected_input_xyz=result.selected_input_xyz,
        previous_state=base_state,
        resumed=resumed,
        engine_fields={
            "job_type": result.job_type,
            "reaction_key": result.reaction_key,
            "input_summary": dict(result.input_summary),
        },
        detail_fields={
            "candidate_count": result.candidate_count,
            "candidate_paths": candidate_paths,
            "selected_candidate_paths": list(result.selected_candidate_paths),
            "candidate_details": [dict(item) for item in result.candidate_details],
            "analysis_summary": dict(result.analysis_summary),
        },
    )


def build_report_payload(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any | None = None,
    coerce_mapping_fn: Callable[[Any], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    coerce_mapping = _dependency(deps, coerce_mapping_fn, "_coerce_mapping")
    base_state = coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    return _engine_execution.build_terminal_report_payload(
        entry,
        result,
        selected_input_xyz=result.selected_input_xyz,
        previous_state=base_state,
        resumed=resumed,
        engine_fields={
            "job_type": result.job_type,
            "reaction_key": result.reaction_key,
            "input_summary": dict(result.input_summary),
        },
        detail_fields={
            "candidate_count": result.candidate_count,
            "candidate_paths": candidate_paths,
            "selected_candidate_paths": list(result.selected_candidate_paths),
            "candidate_details": [dict(item) for item in result.candidate_details],
            "analysis_summary": dict(result.analysis_summary),
        },
    )


def write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any | None = None,
    coerce_mapping_fn: Callable[[Any], dict[str, Any]] | None = None,
    write_state_fn: Callable[..., Any] | None = None,
    write_report_json_fn: Callable[..., Any] | None = None,
    write_report_md_lines_fn: Callable[..., Any] | None = None,
) -> None:
    job_dir_text = _engine_execution.entry_metadata_text(entry, "job_dir")
    if not job_dir_text:
        return
    coerce_mapping = _dependency(deps, coerce_mapping_fn, "_coerce_mapping")
    write_state = _dependency(deps, write_state_fn, "write_state")
    write_report_json = _dependency(deps, write_report_json_fn, "write_report_json")
    write_report_md_lines = _dependency(
        deps,
        write_report_md_lines_fn,
        "write_report_md_lines",
    )

    lines = [
        "# ChemStack xTB Report",
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
            coerce_mapping_fn=coerce_mapping,
        ),
        report_payload=build_report_payload(
            entry,
            result,
            previous_state=previous_state,
            resumed=resumed,
            coerce_mapping_fn=coerce_mapping,
        ),
        report_lines=lines,
        write_state_fn=write_state,
        write_report_json_fn=write_report_json,
        write_report_md_lines_fn=write_report_md_lines,
    )


def write_running_state(
    cfg: Any,
    entry: Any,
    *,
    worker_job_pid: int | None = None,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    deps: Any | None = None,
    input_summary_fn: Callable[[Any], dict[str, Any]] | None = None,
    entry_resource_request_fn: Callable[[Any, Any], dict[str, int]] | None = None,
    coerce_mapping_fn: Callable[[Any], dict[str, Any]] | None = None,
    now_utc_iso_fn: Callable[[], str] | None = None,
    job_type_fn: Callable[[Any], str] | None = None,
    reaction_key_fn: Callable[[Any, Path], str] | None = None,
    write_state_fn: Callable[..., Any] | None = None,
) -> None:
    job_dir_text = _engine_execution.entry_metadata_text(entry, "job_dir")
    if not job_dir_text:
        return
    input_summary = _dependency(deps, input_summary_fn, "_input_summary")
    entry_resource_request = _dependency(
        deps,
        entry_resource_request_fn,
        "_entry_resource_request",
    )
    coerce_mapping = _dependency(deps, coerce_mapping_fn, "_coerce_mapping")
    now_utc_iso = _dependency(deps, now_utc_iso_fn, "now_utc_iso")
    job_type = _dependency(deps, job_type_fn, "_job_type")
    reaction_key = _dependency(deps, reaction_key_fn, "_reaction_key")
    write_state = _dependency(deps, write_state_fn, "write_state")
    job_dir = Path(job_dir_text).expanduser().resolve()
    input_summary_payload = input_summary(entry)
    resource_request = entry_resource_request(cfg, entry)
    base_state = coerce_mapping(previous_state)
    started_at = entry.started_at or now_utc_iso()
    updated_at = now_utc_iso()
    payload = _engine_execution.build_running_state_payload(
        entry,
        job_dir=job_dir,
        selected_input_xyz=_engine_execution.entry_metadata_text(entry, "selected_input_xyz"),
        started_at=started_at,
        updated_at=updated_at,
        previous_state=base_state,
        resumed=resumed,
        resource_request=resource_request,
        engine_fields={
            "job_type": job_type(entry),
            "reaction_key": reaction_key(entry, job_dir),
            "input_summary": input_summary_payload,
        },
        detail_fields={
            "candidate_count": int(input_summary_payload.get("candidate_count", 0) or 0),
            "candidate_paths": list(input_summary_payload.get("candidate_paths", [])),
            "selected_candidate_paths": [],
            "candidate_details": [],
            "analysis_summary": {},
        },
    )
    if worker_job_pid is not None and worker_job_pid > 0:
        payload["worker_job_pid"] = int(worker_job_pid)
    write_state(job_dir, payload)


def mark_recovery_pending_state(
    cfg: Any,
    entry: Any,
    *,
    reason: str,
    deps: Any | None = None,
    job_dir_fn: Callable[[Any], Path] | None = None,
    selected_xyz_fn: Callable[[Any], Path] | None = None,
    job_type_fn: Callable[[Any], str] | None = None,
    reaction_key_fn: Callable[[Any, Path], str] | None = None,
    input_summary_fn: Callable[[Any], dict[str, Any]] | None = None,
    entry_resource_request_fn: Callable[[Any, Any], dict[str, int]] | None = None,
    mark_recovery_pending_fn: Callable[..., Any] | None = None,
    upsert_job_record_fn: Callable[..., Any] | None = None,
) -> None:
    job_dir_resolver = _dependency(deps, job_dir_fn, "_job_dir")
    selected_xyz_resolver = _dependency(deps, selected_xyz_fn, "_selected_xyz")
    job_type_resolver = _dependency(deps, job_type_fn, "_job_type")
    reaction_key_resolver = _dependency(deps, reaction_key_fn, "_reaction_key")
    input_summary_resolver = _dependency(deps, input_summary_fn, "_input_summary")
    entry_resource_request = _dependency(
        deps,
        entry_resource_request_fn,
        "_entry_resource_request",
    )
    mark_recovery_pending = _dependency(
        deps,
        mark_recovery_pending_fn,
        "mark_recovery_pending",
    )
    upsert_job_record = _dependency(deps, upsert_job_record_fn, "upsert_job_record")
    job_dir = job_dir_resolver(entry)
    selected_xyz = selected_xyz_resolver(entry)
    job_type = job_type_resolver(entry)
    reaction_key = reaction_key_resolver(entry, job_dir)
    input_summary = input_summary_resolver(entry)
    resource_request = entry_resource_request(cfg, entry)
    mark_recovery_pending(
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
    upsert_job_record(
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
    deps: Any | None = None,
    now_utc_iso_fn: Callable[[], str] | None = None,
) -> XtbRunResult:
    now_utc_iso = _dependency(deps, now_utc_iso_fn, "now_utc_iso")
    terminal_time = now_utc_iso()
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
