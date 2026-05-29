from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chemstack.xtb import queue_runtime as queue_cmd


def make_cfg(tmp_path: Path) -> SimpleNamespace:
    allowed_root = tmp_path / "allowed"
    organized_root = tmp_path / "organized"
    admission_root = tmp_path / "admission"
    allowed_root.mkdir()
    organized_root.mkdir()
    admission_root.mkdir()
    return SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(allowed_root),
            organized_root=str(organized_root),
            max_concurrent=2,
            admission_root=str(admission_root),
            admission_limit=2,
        ),
        resources=SimpleNamespace(max_cores_per_task=4, max_memory_gb_per_task=8),
        telegram=SimpleNamespace(bot_token="", chat_id=""),
        paths=SimpleNamespace(xtb_executable=""),
    )


def make_entry(
    job_dir: Path,
    selected_input_xyz: Path,
    *,
    queue_id: str = "queue-1",
    job_id: str = "job-1",
    job_type: str = "path_search",
    reaction_key: str = "reaction-1",
    input_summary: dict[str, object] | None = None,
    status: str = "running",
    cancel_requested: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        queue_id=queue_id,
        task_id=job_id,
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(selected_input_xyz),
            "job_type": job_type,
            "reaction_key": reaction_key,
            "input_summary": dict(input_summary or {}),
        },
        started_at="2026-04-20T00:00:00Z",
        status=SimpleNamespace(value=status),
        cancel_requested=cancel_requested,
        error="",
    )


def make_result(
    selected_input_xyz: Path,
    *,
    status: str,
    reason: str,
    job_type: str = "path_search",
    reaction_key: str = "reaction-1",
    candidate_paths: tuple[str, ...] = (),
) -> queue_cmd.XtbRunResult:
    resource_request = {"max_cores": 4, "max_memory_gb": 8}
    resource_actual = {"assigned_cores": 4, "memory_limit_gb": 8}
    return queue_cmd.XtbRunResult(
        status=status,
        reason=reason,
        command=("xtb", str(selected_input_xyz)),
        exit_code=0 if status == "completed" else 1,
        started_at="2026-04-20T00:00:00Z",
        finished_at="2026-04-20T00:05:00Z",
        stdout_log=str((selected_input_xyz.parent / "xtb.stdout.log").resolve()),
        stderr_log=str((selected_input_xyz.parent / "xtb.stderr.log").resolve()),
        selected_input_xyz=str(selected_input_xyz),
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary={
            "candidate_count": len(candidate_paths),
            "candidate_paths": list(candidate_paths),
        },
        candidate_count=len(candidate_paths),
        selected_candidate_paths=candidate_paths,
        candidate_details=tuple({"path": path} for path in candidate_paths),
        analysis_summary={"candidate_paths": list(candidate_paths)},
        manifest_path="",
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def fake_reserve_slot(
    calls: list[tuple[str, int, str, str]],
    root: str,
    limit: int,
    source: str,
    app_name: str,
) -> str:
    calls.append((root, limit, source, app_name))
    return "slot-1"


def record_finished_call(finished_calls: list[dict[str, object]], kwargs: dict[str, object]) -> bool:
    finished_calls.append(kwargs)
    return True


__all__ = [
    "fake_reserve_slot",
    "make_cfg",
    "make_entry",
    "make_result",
    "record_finished_call",
]
