from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from chemstack.core.queue import engine_execution as _engine_execution
from chemstack.core.queue import execution as _queue_execution

from .runner import XtbRunResult


def _required_dependency(explicit: Any, name: str) -> Any:
    if explicit is not None:
        return explicit
    raise TypeError(f"missing required dependency: {name}")


def _candidate_paths(result: XtbRunResult) -> list[Any]:
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    return candidate_paths


def _engine_fields(result: XtbRunResult) -> dict[str, Any]:
    return {
        "job_type": result.job_type,
        "reaction_key": result.reaction_key,
        "input_summary": dict(result.input_summary),
    }


def _detail_fields(result: XtbRunResult) -> dict[str, Any]:
    return {
        "candidate_count": result.candidate_count,
        "candidate_paths": _candidate_paths(result),
        "selected_candidate_paths": list(result.selected_candidate_paths),
        "candidate_details": [dict(item) for item in result.candidate_details],
        "analysis_summary": dict(result.analysis_summary),
    }


def report_lines(entry: Any, result: XtbRunResult) -> list[str]:
    lines = _engine_execution.terminal_report_lines(
        entry,
        result,
        title="ChemStack xTB Report",
        selected_input_label="Selected Input XYZ",
        selected_input_xyz=result.selected_input_xyz,
        engine_lines=[
            f"- Job Type: `{result.job_type}`",
            f"- Reaction Key: `{result.reaction_key}`",
        ],
        detail_lines=[
            f"- Candidate Count: `{result.candidate_count}`",
            f"- Input Summary: `{result.input_summary}`",
        ],
    )
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
    return lines


def write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
    coerce_mapping_fn: Callable[[Any], dict[str, Any]] | None = None,
    write_state_fn: Callable[..., Any] | None = None,
    write_report_json_fn: Callable[..., Any] | None = None,
    write_report_md_lines_fn: Callable[..., Any] | None = None,
) -> None:
    job_dir_text = _engine_execution.entry_metadata_text(entry, "job_dir")
    if not job_dir_text:
        return
    coerce_mapping = coerce_mapping_fn or _queue_execution.coerce_mapping
    write_state = _required_dependency(write_state_fn, "write_state_fn")
    write_report_json = _required_dependency(write_report_json_fn, "write_report_json_fn")
    write_report_md_lines = _required_dependency(
        write_report_md_lines_fn,
        "write_report_md_lines_fn",
    )

    base_state = coerce_mapping(previous_state)
    _engine_execution.write_terminal_execution_artifacts(
        entry,
        result,
        job_dir_text=job_dir_text,
        selected_input_xyz=result.selected_input_xyz,
        previous_state=base_state,
        resumed=resumed,
        engine_fields=_engine_fields(result),
        detail_fields=_detail_fields(result),
        report_lines=report_lines(entry, result),
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
    input_summary = _required_dependency(input_summary_fn, "input_summary_fn")
    entry_resource_request = _required_dependency(
        entry_resource_request_fn,
        "entry_resource_request_fn",
    )
    coerce_mapping = coerce_mapping_fn or _queue_execution.coerce_mapping
    now_utc_iso = _required_dependency(now_utc_iso_fn, "now_utc_iso_fn")
    job_type = _required_dependency(job_type_fn, "job_type_fn")
    reaction_key = _required_dependency(reaction_key_fn, "reaction_key_fn")
    write_state = _required_dependency(write_state_fn, "write_state_fn")
    job_dir = Path(job_dir_text).expanduser().resolve()
    input_summary_payload = input_summary(entry)
    resource_request = entry_resource_request(cfg, entry)
    base_state = coerce_mapping(previous_state)
    started_at = entry.started_at or now_utc_iso()
    updated_at = now_utc_iso()
    _engine_execution.write_running_state_artifact(
        entry,
        job_dir_text=job_dir_text,
        selected_input_xyz=_engine_execution.entry_metadata_text(entry, "selected_input_xyz"),
        started_at=started_at,
        updated_at=updated_at,
        previous_state=base_state,
        resumed=resumed,
        resource_request=resource_request,
        write_state_fn=write_state,
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
        worker_job_pid=worker_job_pid,
    )


def mark_recovery_pending_state(
    cfg: Any,
    entry: Any,
    *,
    reason: str,
    job_dir_fn: Callable[[Any], Path] | None = None,
    selected_xyz_fn: Callable[[Any], Path] | None = None,
    job_type_fn: Callable[[Any], str] | None = None,
    reaction_key_fn: Callable[[Any, Path], str] | None = None,
    input_summary_fn: Callable[[Any], dict[str, Any]] | None = None,
    entry_resource_request_fn: Callable[[Any, Any], dict[str, int]] | None = None,
    mark_recovery_pending_fn: Callable[..., Any] | None = None,
    upsert_job_record_fn: Callable[..., Any] | None = None,
) -> None:
    job_dir_resolver = _required_dependency(job_dir_fn, "job_dir_fn")
    selected_xyz_resolver = _required_dependency(selected_xyz_fn, "selected_xyz_fn")
    job_type_resolver = _required_dependency(job_type_fn, "job_type_fn")
    reaction_key_resolver = _required_dependency(reaction_key_fn, "reaction_key_fn")
    input_summary_resolver = _required_dependency(input_summary_fn, "input_summary_fn")
    entry_resource_request = _required_dependency(
        entry_resource_request_fn,
        "entry_resource_request_fn",
    )
    mark_recovery_pending = _required_dependency(
        mark_recovery_pending_fn,
        "mark_recovery_pending_fn",
    )
    upsert_job_record = _required_dependency(upsert_job_record_fn, "upsert_job_record_fn")
    job_dir = job_dir_resolver(entry)
    selected_xyz = selected_xyz_resolver(entry)
    job_type = job_type_resolver(entry)
    reaction_key = reaction_key_resolver(entry, job_dir)
    input_summary = input_summary_resolver(entry)
    resource_request = entry_resource_request(cfg, entry)
    _engine_execution.mark_recovery_pending_and_record(
        cfg,
        job_dir=job_dir,
        selected_input_xyz=selected_xyz,
        entry=entry,
        reason=reason,
        resource_request=resource_request,
        mark_recovery_pending_fn=mark_recovery_pending,
        upsert_job_record_fn=upsert_job_record,
        state_identity_fields={
            "job_type": job_type,
            "reaction_key": reaction_key,
            "input_summary": input_summary,
        },
        record_identity_fields={
            "job_type": job_type,
            "reaction_key": reaction_key,
        },
    )


def resource_caps(cfg: Any) -> dict[str, int]:
    from chemstack.core.indexing.engines import resource_dict

    return _engine_execution.engine_resource_caps(cfg, resource_dict_fn=resource_dict)


def entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _engine_execution.entry_resource_request(
        cfg,
        entry,
        resource_caps_fn=resource_caps,
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
    now_utc_iso_fn: Callable[[], str] | None = None,
) -> XtbRunResult:
    now_utc_iso = _required_dependency(now_utc_iso_fn, "now_utc_iso_fn")
    return _engine_execution.build_terminal_result(
        XtbRunResult,
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        log_prefix="xtb",
        manifest_filename="xtb_job.yaml",
        resource_request=resource_request,
        status=status,
        reason=reason,
        now_utc_iso_fn=now_utc_iso,
        command=command,
        exit_code=exit_code,
        engine_fields={
            "job_type": job_type,
            "reaction_key": reaction_key,
            "input_summary": input_summary,
        },
        detail_fields={
            "candidate_count": 0,
            "selected_candidate_paths": (),
            "candidate_details": (),
            "analysis_summary": {},
        },
    )
