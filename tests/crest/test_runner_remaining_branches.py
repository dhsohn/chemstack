from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from chemstack.core.config.engines import (
    WorkflowEngineAppConfig as AppConfig,
)
from chemstack.core.config.engines import (
    WorkflowEngineBehaviorConfig as BehaviorConfig,
)
from chemstack.core.config.engines import (
    WorkflowEnginePathsConfig as PathsConfig,
)
from chemstack.core.config.schema import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig
from chemstack.core.engines import crest_execution as worker_execution
from chemstack.crest.runner import CrestRunResult, _build_command


def _cfg(tmp_path: Path) -> AppConfig:
    return AppConfig(
        runtime=CommonRuntimeConfig(
            allowed_root=str(tmp_path / "runs"),
            organized_root=str(tmp_path / "organized"),
        ),
        paths=PathsConfig(crest_executable="/opt/crest"),
        behavior=BehaviorConfig(),
        resources=CommonResourceConfig(max_cores_per_task=4, max_memory_gb_per_task=8),
        telegram=TelegramConfig(),
    )


def _write_xyz(path: Path) -> None:
    path.write_text("1\nconf_a\nH 0.0 0.0 0.0\n", encoding="utf-8")


def _result(job_dir: Path, selected_xyz: Path) -> CrestRunResult:
    stdout_log = job_dir / "crest.stdout.log"
    stderr_log = job_dir / "crest.stderr.log"
    stdout_log.write_text("stdout\n", encoding="utf-8")
    stderr_log.write_text("stderr\n", encoding="utf-8")
    return CrestRunResult(
        status="completed",
        reason="completed",
        command=("crest", selected_xyz.name, "--T", "4"),
        exit_code=0,
        started_at="2026-04-19T00:00:00+00:00",
        finished_at="2026-04-19T00:05:00+00:00",
        stdout_log=str(stdout_log.resolve()),
        stderr_log=str(stderr_log.resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        mode="standard",
        retained_conformer_count=0,
        retained_conformer_paths=(),
        manifest_path=str((job_dir / "crest_job.yaml").resolve()),
        resource_request={"max_cores": 4, "max_memory_gb": 8},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 8},
    )


def _dependencies(**overrides: Callable[..., Any]) -> worker_execution.WorkerExecutionDependencies:
    defaults: dict[str, Any] = {
        "now_utc_iso": lambda: "2026-04-19T09:15:00+00:00",
        "get_cancel_requested": lambda *args, **kwargs: False,
        "start_crest_job": lambda *args, **kwargs: None,
        "finalize_crest_job": lambda *args, **kwargs: None,
        "terminate_process": lambda *args, **kwargs: None,
        "wait_for_cancellable_process": worker_execution._queue_execution.wait_for_cancellable_process,
        "sleep": worker_execution.time.sleep,
        "cancel_check_interval_seconds": worker_execution.CANCEL_CHECK_INTERVAL_SECONDS,
        "write_running_state": lambda *args, **kwargs: None,
        "write_execution_artifacts": lambda *args, **kwargs: None,
        "mark_completed": lambda *args, **kwargs: None,
        "mark_cancelled": lambda *args, **kwargs: None,
        "mark_failed": lambda *args, **kwargs: None,
        "upsert_job_record": lambda *args, **kwargs: None,
        "notify_job_started": lambda *args, **kwargs: True,
        "notify_job_finished": lambda *args, **kwargs: True,
    }
    defaults.update(overrides)
    return worker_execution.build_worker_execution_dependencies(
        timing=worker_execution.WorkerTimingDependencies(
            now_utc_iso=defaults["now_utc_iso"],
        ),
        queue=worker_execution.WorkerQueueDependencies(
            get_cancel_requested=defaults["get_cancel_requested"],
            mark_completed=defaults["mark_completed"],
            mark_cancelled=defaults["mark_cancelled"],
            mark_failed=defaults["mark_failed"],
        ),
        runner=worker_execution.WorkerRunnerDependencies(
            start_crest_job=defaults["start_crest_job"],
            finalize_crest_job=defaults["finalize_crest_job"],
            terminate_process=defaults["terminate_process"],
            wait_for_cancellable_process=defaults["wait_for_cancellable_process"],
            sleep=defaults["sleep"],
            cancel_check_interval_seconds=defaults["cancel_check_interval_seconds"],
        ),
        artifacts=worker_execution.WorkerArtifactDependencies(
            write_running_state=defaults["write_running_state"],
            write_execution_artifacts=defaults["write_execution_artifacts"],
        ),
        tracking=worker_execution.WorkerTrackingDependencies(
            upsert_job_record=defaults["upsert_job_record"],
            notify_job_started=defaults["notify_job_started"],
            notify_job_finished=defaults["notify_job_finished"],
        ),
    )


@pytest.mark.parametrize(
    ("gfn_value", "expected_flag"),
    [
        ("1", "--gfn1"),
        ("gfn2", "--gfn2"),
        ("ff", "--gfnff"),
    ],
)
def test_build_command_emits_single_step_gfn_variants(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    gfn_value: str,
    expected_flag: str,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / f"job-{gfn_value}"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    _write_xyz(selected_xyz)
    monkeypatch.setattr("chemstack.crest.runner._resolve_crest_executable", lambda _cfg: "/usr/bin/crest")

    command = _build_command(
        cfg,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        manifest={"gfn": gfn_value},
    )

    assert expected_flag in command


def test_sync_job_tracking_omits_organized_output_for_crest(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    _write_xyz(selected_xyz)
    entry = SimpleNamespace(task_id="job-001", queue_id="queue-001")
    context = worker_execution.ExecutionContext(
        entry=entry,
        job_dir=job_dir.resolve(),
        selected_xyz=selected_xyz.resolve(),
        molecule_key="mol-001",
        mode="standard",
        resource_request={"max_cores": 4, "max_memory_gb": 8},
    )
    result = _result(job_dir, selected_xyz)
    upsert_calls: list[dict[str, Any]] = []

    deps = _dependencies(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
    )

    organized_output_dir = worker_execution._sync_job_tracking(
        SimpleNamespace(),
        context,
        result,
        dependencies=deps,
    )

    assert organized_output_dir is None
    assert len(upsert_calls) == 1
    assert "organized_output_dir" not in upsert_calls[0]
    assert capsys.readouterr().out == ""
