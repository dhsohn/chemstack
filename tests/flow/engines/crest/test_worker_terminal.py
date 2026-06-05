from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from orca_auto.flow.engines.crest import terminal as worker_terminal
from orca_auto.flow.engines.crest.runner import CrestRunResult
from orca_auto.flow.engines.crest.worker_context import ExecutionContext


def _entry(job_dir: Path, selected_xyz: Path) -> SimpleNamespace:
    return SimpleNamespace(
        task_id="job-001",
        queue_id="queue-001",
        started_at="2026-04-19T00:00:00+00:00",
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(selected_xyz),
            "mode": "standard",
            "molecule_key": "mol-001",
        },
    )


def _context(entry: SimpleNamespace, job_dir: Path, selected_xyz: Path) -> ExecutionContext:
    return ExecutionContext(
        entry=entry,
        job_dir=job_dir.resolve(),
        selected_xyz=selected_xyz.resolve(),
        molecule_key="mol-001",
        mode="standard",
        resource_request={"max_cores": 4, "max_memory_gb": 16},
    )


def _result(job_dir: Path, selected_xyz: Path, *, status: str = "completed") -> CrestRunResult:
    return CrestRunResult(
        status=status,
        reason="completed" if status == "completed" else "runner_error",
        command=("crest", selected_xyz.name),
        exit_code=0 if status == "completed" else 1,
        started_at="2026-04-19T00:00:00+00:00",
        finished_at="2026-04-19T00:05:00+00:00",
        stdout_log=str((job_dir / "crest.stdout.log").resolve()),
        stderr_log=str((job_dir / "crest.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        mode="standard",
        retained_conformer_count=2,
        retained_conformer_paths=("crest_conformers.xyz", "crest_best.xyz"),
        manifest_path=str((job_dir / "crest_job.yaml").resolve()),
        resource_request={"max_cores": 4, "max_memory_gb": 16},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 16},
    )


def test_mark_queue_terminal_sets_status_and_metadata(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "selected_input.xyz"
    entry = _entry(job_dir, selected_xyz)
    context = _context(entry, job_dir, selected_xyz)
    result = _result(job_dir, selected_xyz)
    completed_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    queue_deps = SimpleNamespace(
        mark_completed=lambda *args, **kwargs: completed_calls.append((args, kwargs)),
        mark_cancelled=lambda *args, **kwargs: pytest.fail("unexpected cancelled mark"),
        mark_failed=lambda *args, **kwargs: pytest.fail("unexpected failed mark"),
    )

    worker_terminal.mark_queue_terminal(
        tmp_path,
        context,
        result,
        queue_deps=queue_deps,
    )

    assert completed_calls == [
        (
            (str(tmp_path), "queue-001"),
            {
                "metadata_update": {
                    "retained_conformer_count": 2,
                    "mode": "standard",
                }
            },
        )
    ]


def test_sync_job_tracking_records_without_organized_output(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "selected_input.xyz"
    entry = _entry(job_dir, selected_xyz)
    context = _context(entry, job_dir, selected_xyz)
    result = _result(job_dir, selected_xyz)
    upsert_calls: list[dict[str, Any]] = []
    tracking_deps = SimpleNamespace(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
    )

    organized_output_dir = worker_terminal.sync_job_tracking(
        SimpleNamespace(),
        context,
        result,
        tracking_deps=tracking_deps,
    )

    assert organized_output_dir is None
    assert upsert_calls and upsert_calls[0]["molecule_key"] == "mol-001"
    assert "organized_output_dir" not in upsert_calls[0]


def test_finalize_processed_entry_runs_terminal_side_effects(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    selected_xyz = job_dir / "selected_input.xyz"
    entry = _entry(job_dir, selected_xyz)
    context = _context(entry, job_dir, selected_xyz)
    result = _result(job_dir, selected_xyz)
    artifact_results: list[CrestRunResult] = []
    completed_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    upsert_calls: list[dict[str, Any]] = []
    finished_calls: list[dict[str, Any]] = []

    def fake_notify_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_calls.append(kwargs)
        return True

    deps = SimpleNamespace(
        artifacts=SimpleNamespace(
            write_execution_artifacts=lambda actual_entry, actual_result: artifact_results.append(
                actual_result
            ),
        ),
        queue=SimpleNamespace(
            mark_completed=lambda *args, **kwargs: completed_calls.append((args, kwargs)),
            mark_cancelled=lambda *args, **kwargs: pytest.fail("unexpected cancelled mark"),
            mark_failed=lambda *args, **kwargs: pytest.fail("unexpected failed mark"),
        ),
        tracking=SimpleNamespace(
            upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
            notify_job_finished=fake_notify_finished,
        ),
    )

    organized_output_dir = worker_terminal.finalize_processed_entry(
        SimpleNamespace(),
        context,
        result,
        queue_root=tmp_path,
        dependencies=deps,
    )

    assert organized_output_dir is None
    assert artifact_results == [result]
    assert completed_calls
    assert upsert_calls and upsert_calls[0]["status"] == "completed"
    assert finished_calls and finished_calls[0]["retained_conformer_count"] == 2
