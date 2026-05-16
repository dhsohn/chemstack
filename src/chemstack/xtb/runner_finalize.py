from __future__ import annotations

from pathlib import Path
from typing import Any


def finalize_xtb_job(
    running: Any,
    *,
    forced_status: str | None = None,
    forced_reason: str | None = None,
    result_cls: Any,
    deps: Any,
) -> Any:
    try:
        running.stdout_handle.flush()
        running.stderr_handle.flush()
    finally:
        running.stdout_handle.close()
        running.stderr_handle.close()

    exit_code = running.process.poll()
    if exit_code is None:
        exit_code = running.process.wait()
    finished_at = deps.now_utc_iso()

    status = forced_status if forced_status is not None else _status_from_exit_code(exit_code)
    reason = forced_reason if forced_reason is not None else _reason_from_exit_code(exit_code)
    candidate_count, candidate_paths, candidate_details, analysis_summary = _collect_candidates(
        running,
        deps=deps,
    )

    return result_cls(
        status=status,
        reason=reason,
        command=running.command,
        exit_code=int(exit_code),
        started_at=running.started_at,
        finished_at=finished_at,
        stdout_log=running.stdout_log,
        stderr_log=running.stderr_log,
        selected_input_xyz=running.selected_input_xyz,
        job_type=running.job_type,
        reaction_key=running.reaction_key,
        input_summary=dict(running.input_summary),
        candidate_count=candidate_count,
        selected_candidate_paths=candidate_paths,
        candidate_details=candidate_details,
        analysis_summary=analysis_summary,
        manifest_path=running.manifest_path,
        resource_request=running.resource_request,
        resource_actual=running.resource_actual,
    )


def _status_from_exit_code(exit_code: int) -> str:
    return "completed" if exit_code == 0 else "failed"


def _reason_from_exit_code(exit_code: int) -> str:
    return "completed" if exit_code == 0 else f"xtb_exit_code_{exit_code}"


def _collect_candidates(running: Any, *, deps: Any) -> tuple[int, tuple[str, ...], tuple[dict[str, Any], ...], dict[str, Any]]:
    if running.job_type == "path_search":
        return deps._collect_path_search_candidates(
            Path(running.job_dir),
            running.stdout_log,
        )
    if running.job_type == "opt":
        return deps._collect_opt_candidates(Path(running.job_dir))
    if running.job_type == "sp":
        return deps._collect_sp_candidates(Path(running.job_dir))
    return 0, (), (), {}
