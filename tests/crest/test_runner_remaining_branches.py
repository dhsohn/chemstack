from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable

import pytest

from chemstack.core.config.schema import CommonResourceConfig, CommonRuntimeConfig, TelegramConfig

from chemstack.crest.config import AppConfig, BehaviorConfig, PathsConfig
from chemstack.crest.runner import CrestRunResult, _build_command, _preexec_with_limits
from chemstack.crest import runner as runner_module
from chemstack.crest import worker_execution


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
    defaults: dict[str, Callable[..., Any]] = {
        "now_utc_iso": lambda: "2026-04-19T09:15:00+00:00",
        "get_cancel_requested": lambda *args, **kwargs: False,
        "start_crest_job": lambda *args, **kwargs: None,
        "finalize_crest_job": lambda *args, **kwargs: None,
        "terminate_process": lambda *args, **kwargs: None,
        "write_running_state": lambda *args, **kwargs: None,
        "write_execution_artifacts": lambda *args, **kwargs: None,
        "mark_completed": lambda *args, **kwargs: None,
        "mark_cancelled": lambda *args, **kwargs: None,
        "mark_failed": lambda *args, **kwargs: None,
        "upsert_job_record": lambda *args, **kwargs: None,
        "notify_job_started": lambda *args, **kwargs: True,
        "notify_job_finished": lambda *args, **kwargs: True,
        "organize_job_dir": lambda *args, **kwargs: {"action": "skipped"},
    }
    defaults.update(overrides)
    return worker_execution.WorkerExecutionDependencies(**defaults)


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


def test_preexec_with_limits_applies_address_space_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[int, tuple[int, int]]] = []

    monkeypatch.setattr(
        runner_module.resource,
        "setrlimit",
        lambda limit, bounds: calls.append((limit, bounds)),
    )

    _preexec_with_limits(3)()

    assert calls == [
        (
            runner_module.resource.RLIMIT_AS,
            (3 * 1024 * 1024 * 1024, 3 * 1024 * 1024 * 1024),
        )
    ]


def test_sync_job_tracking_returns_none_for_falsey_organized_target(
    monkeypatch: pytest.MonkeyPatch,
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

    class FalseyPath:
        def __init__(self, value: str) -> None:
            self.value = value

        def __bool__(self) -> bool:
            return False

        def __str__(self) -> str:
            return self.value

    monkeypatch.setattr(worker_execution, "Path", FalseyPath)
    deps = _dependencies(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        organize_job_dir=lambda cfg, actual_job_dir, *, notify_summary: {
            "action": "organized",
            "target_dir": "   ",
        },
    )

    organized_output_dir = worker_execution._sync_job_tracking(
        SimpleNamespace(),
        context,
        result,
        auto_organize=True,
        dependencies=deps,
    )

    assert organized_output_dir is None
    assert len(upsert_calls) == 1
    assert "organized_output_dir" not in upsert_calls[0]
    assert capsys.readouterr().out == ""
