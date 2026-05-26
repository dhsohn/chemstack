from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from chemstack.core.queue import processes as queue_processes_mod
from chemstack.xtb.commands import queue as queue_cmd
from chemstack.xtb import state as state_mod


def _make_cfg(tmp_path: Path) -> SimpleNamespace:
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


def _make_entry(
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


def _make_result(
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


def _fake_reserve_slot(
    calls: list[tuple[str, int, str, str]],
    root: str,
    limit: int,
    source: str,
    app_name: str,
) -> str:
    calls.append((root, limit, source, app_name))
    return "slot-1"


def _record_finished_call(
    finished_calls: list[dict[str, object]], kwargs: dict[str, object]
) -> bool:
    finished_calls.append(kwargs)
    return True


def test_write_execution_artifacts_skips_without_job_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entry = SimpleNamespace(task_id="job-1", queue_id="queue-1", metadata={})
    result = _make_result(tmp_path / "selected.xyz", status="completed", reason="completed")

    monkeypatch.setattr(
        queue_cmd, "write_state", lambda *args, **kwargs: pytest.fail("write_state should not run")
    )
    monkeypatch.setattr(
        queue_cmd,
        "write_report_json",
        lambda *args, **kwargs: pytest.fail("write_report_json should not run"),
    )
    monkeypatch.setattr(
        queue_cmd,
        "write_report_md_lines",
        lambda *args, **kwargs: pytest.fail("write_report_md_lines should not run"),
    )

    queue_cmd._write_execution_artifacts(entry, result)


def test_write_execution_artifacts_includes_ranking_summary_lines(tmp_path: Path) -> None:
    job_dir = tmp_path / "ranking-job"
    job_dir.mkdir()
    selected_xyz = job_dir / "candidate.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = SimpleNamespace(
        task_id="job-1",
        queue_id="queue-1",
        metadata={"job_dir": str(job_dir)},
    )
    result = queue_cmd.XtbRunResult(
        status="completed",
        reason="completed",
        command=("xtb", str(selected_xyz)),
        exit_code=0,
        started_at="2026-04-20T00:00:00Z",
        finished_at="2026-04-20T00:05:00Z",
        stdout_log=str((job_dir / "xtb.stdout.log").resolve()),
        stderr_log=str((job_dir / "xtb.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        job_type="ranking",
        reaction_key="rxn-1",
        input_summary={"candidate_paths": [str(selected_xyz.resolve())]},
        candidate_count=1,
        selected_candidate_paths=(str(selected_xyz.resolve()),),
        candidate_details=({"path": str(selected_xyz.resolve())},),
        analysis_summary={
            "candidate_paths": [str(selected_xyz.resolve())],
            "best_candidate_path": str(selected_xyz.resolve()),
            "best_total_energy": -12.34,
        },
        manifest_path=str((job_dir / "xtb_job.yaml").resolve()),
        resource_request={"max_cores": 4, "max_memory_gb": 8},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 8},
    )

    queue_cmd._write_execution_artifacts(entry, result)

    report_md = (job_dir / state_mod.REPORT_MD_FILE_NAME).read_text(encoding="utf-8")
    assert "Best Candidate Path" in report_md
    assert "Best Total Energy" in report_md


def test_write_running_state_skips_without_job_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_cfg(tmp_path)
    entry = SimpleNamespace(task_id="job-1", metadata={})
    monkeypatch.setattr(
        queue_cmd, "write_state", lambda *args, **kwargs: pytest.fail("write_state should not run")
    )
    queue_cmd._write_running_state(cfg, entry)


def test_write_running_state_records_worker_job_pid(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    job_dir = Path(cfg.runtime.allowed_root) / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)

    queue_cmd._write_running_state(cfg, entry, worker_job_pid=4242)

    state = state_mod.load_state(job_dir)
    assert state is not None
    assert state["worker_job_pid"] == 4242


def test_terminate_process_returns_immediately_for_finished_process() -> None:
    proc = SimpleNamespace(poll=lambda: 0)
    queue_cmd._terminate_process(cast(Any, proc))


def test_terminate_process_uses_fallback_terminate_and_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    killpg_calls: list[tuple[int, int]] = []

    class _Process:
        pid = 4321

        def __init__(self) -> None:
            self.terminate_called = False
            self.kill_called = False
            self.wait_calls = 0

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            self.terminate_called = True

        def kill(self) -> None:
            self.kill_called = True

        def wait(self, timeout: int) -> None:
            self.wait_calls += 1
            raise queue_cmd.subprocess.TimeoutExpired(cmd="xtb", timeout=timeout)

    def fake_killpg(pid: int, sig: int) -> None:
        killpg_calls.append((pid, sig))
        if len(killpg_calls) == 1:
            raise ProcessLookupError("missing")
        raise PermissionError("denied")

    monkeypatch.setattr(queue_processes_mod.os, "killpg", fake_killpg)

    proc = _Process()
    queue_cmd._terminate_process(cast(Any, proc))

    assert killpg_calls == [
        (4321, queue_cmd.signal.SIGTERM),
        (4321, queue_cmd.signal.SIGKILL),
    ]
    assert proc.terminate_called is True
    assert proc.kill_called is True
    assert proc.wait_calls == 2


def test_terminate_process_swallows_terminate_and_kill_exceptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Process:
        pid = 5555

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            raise RuntimeError("terminate failed")

        def kill(self) -> None:
            raise RuntimeError("kill failed")

        def wait(self, timeout: int) -> None:
            raise queue_cmd.subprocess.TimeoutExpired(cmd="xtb", timeout=timeout)

    monkeypatch.setattr(
        queue_processes_mod.os,
        "killpg",
        lambda pid, sig: (_ for _ in ()).throw(ProcessLookupError("missing")),
    )

    queue_cmd._terminate_process(cast(Any, _Process()))


def test_try_reserve_admission_slot_uses_resolved_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    cfg.runtime.resolved_admission_root = str(tmp_path / "resolved-admission")
    cfg.runtime.resolved_admission_limit = 5
    calls: list[tuple[str, int, str, str]] = []

    monkeypatch.setattr(
        queue_cmd,
        "reserve_slot",
        lambda root, limit, *, source, app_name: _fake_reserve_slot(
            calls, root, limit, source, app_name
        ),
    )

    assert queue_cmd._try_reserve_admission_slot(cfg) == "slot-1"
    assert calls == [
        (str(tmp_path / "resolved-admission"), 5, "chemstack.xtb.queue_worker", "chemstack_xtb")
    ]


def test_run_worker_job_activates_reserved_slot_and_releases_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)
    activated: list[tuple[str, str, str, str, str]] = []
    released: list[tuple[str, str]] = []

    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [entry])

    def fake_activate_reserved_slot(
        root: str,
        token: str,
        *,
        work_dir: object,
        queue_id: str,
        source: str,
    ) -> object:
        activated.append((root, token, str(work_dir), queue_id, source))
        return object()

    monkeypatch.setattr(
        queue_cmd,
        "activate_reserved_slot",
        fake_activate_reserved_slot,
    )
    monkeypatch.setattr(
        queue_cmd, "release_slot", lambda root, token: released.append((root, token))
    )
    monkeypatch.setattr(
        queue_cmd,
        "_execute_queue_entry",
        lambda *args, **kwargs: queue_cmd.QueueExecutionOutcome(
            result=_make_result(selected_xyz, status="completed", reason="completed")
        ),
    )

    exit_code = queue_cmd.run_worker_job(
        config_path="/tmp/chemstack.yaml",
        queue_root=queue_root,
        queue_id=entry.queue_id,
        admission_root=cfg.runtime.admission_root,
        admission_token="slot-1",
        should_cancel=lambda: False,
        register_running_job=lambda _value: None,
    )

    assert exit_code == 0
    assert activated == [
        (
            cfg.runtime.admission_root,
            "slot-1",
            str(job_dir.resolve()),
            "queue-1",
            "chemstack.xtb.worker_job",
        )
    ]
    assert released == [(cfg.runtime.admission_root, "slot-1")]
