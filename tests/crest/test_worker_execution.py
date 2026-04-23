from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, cast

import pytest

from chemstack.crest.runner import CrestRunResult
from chemstack.crest.state import REPORT_MD_FILE_NAME, load_report_json, load_state
from chemstack.crest import worker_execution


def _cfg(tmp_path: Path) -> SimpleNamespace:
    allowed_root = tmp_path / "allowed_root"
    allowed_root.mkdir()
    return SimpleNamespace(
        runtime=SimpleNamespace(allowed_root=str(allowed_root)),
        resources=SimpleNamespace(max_cores_per_task=4, max_memory_gb_per_task=16),
    )


def _entry(
    job_dir: Path | str,
    selected_xyz: Path | str,
    *,
    task_id: str = "job-001",
    queue_id: str = "queue-001",
    started_at: str | None = "2026-04-19T00:00:00+00:00",
    mode: str = "standard",
    molecule_key: str = "",
) -> SimpleNamespace:
    return SimpleNamespace(
        task_id=task_id,
        queue_id=queue_id,
        started_at=started_at,
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(selected_xyz),
            "mode": mode,
            "molecule_key": molecule_key,
        },
    )


def _result(
    job_dir: Path,
    selected_xyz: Path,
    *,
    status: str = "completed",
    reason: str = "completed",
    mode: str = "standard",
    exit_code: int = 0,
    retained_names: tuple[str, ...] = (),
) -> CrestRunResult:
    stdout_log = job_dir / "crest.stdout.log"
    stderr_log = job_dir / "crest.stderr.log"
    stdout_log.write_text("stdout\n", encoding="utf-8")
    stderr_log.write_text("stderr\n", encoding="utf-8")

    retained_paths: list[str] = []
    for name in retained_names:
        path = job_dir / name
        path.write_text("1\nretained\nH 0.0 0.0 0.0\n", encoding="utf-8")
        retained_paths.append(str(path.resolve()))

    return CrestRunResult(
        status=status,
        reason=reason,
        command=("crest", selected_xyz.name, "--T", "4"),
        exit_code=exit_code,
        started_at="2026-04-19T00:00:00+00:00",
        finished_at="2026-04-19T00:05:00+00:00",
        stdout_log=str(stdout_log.resolve()),
        stderr_log=str(stderr_log.resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        mode=mode,
        retained_conformer_count=len(retained_paths),
        retained_conformer_paths=tuple(retained_paths),
        manifest_path=str((job_dir / "crest_job.yaml").resolve()),
        resource_request={"max_cores": 4, "max_memory_gb": 16},
        resource_actual={"assigned_cores": 4, "memory_limit_gb": 16},
    )


def _context(
    entry: SimpleNamespace,
    job_dir: Path,
    selected_xyz: Path,
    *,
    molecule_key: str = "mol-001",
    mode: str = "standard",
) -> worker_execution.ExecutionContext:
    return worker_execution.ExecutionContext(
        entry=entry,
        job_dir=job_dir.resolve(),
        selected_xyz=selected_xyz.resolve(),
        molecule_key=molecule_key,
        mode=mode,
        resource_request={"max_cores": 4, "max_memory_gb": 16},
    )


def _noop(*args: Any, **kwargs: Any) -> None:
    return None


def _notify_ok(*args: Any, **kwargs: Any) -> bool:
    return True


def _dependencies(**overrides: Callable[..., Any]) -> worker_execution.WorkerExecutionDependencies:
    defaults: dict[str, Callable[..., Any]] = {
        "now_utc_iso": lambda: "2026-04-19T09:15:00+00:00",
        "get_cancel_requested": lambda *args, **kwargs: False,
        "start_crest_job": _noop,
        "finalize_crest_job": _noop,
        "terminate_process": _noop,
        "write_running_state": _noop,
        "write_execution_artifacts": _noop,
        "mark_completed": _noop,
        "mark_cancelled": _noop,
        "mark_failed": _noop,
        "upsert_job_record": _noop,
        "notify_job_started": _notify_ok,
        "notify_job_finished": _notify_ok,
        "organize_job_dir": lambda *args, **kwargs: {"action": "skipped"},
    }
    defaults.update(overrides)
    return worker_execution.WorkerExecutionDependencies(**defaults)


class FakeProcess:
    def __init__(self, *poll_values: int | None) -> None:
        self._poll_values = list(poll_values) or [None]
        self.pid = 4242

    def poll(self) -> int | None:
        if len(self._poll_values) > 1:
            return self._poll_values.pop(0)
        return self._poll_values[0]


def test_write_execution_artifacts_returns_early_without_job_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    selected_xyz = tmp_path / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry("   ", selected_xyz)
    result = _result(tmp_path, selected_xyz)

    monkeypatch.setattr(worker_execution, "write_state", lambda *args, **kwargs: pytest.fail("unexpected state write"))
    monkeypatch.setattr(
        worker_execution,
        "write_report_json",
        lambda *args, **kwargs: pytest.fail("unexpected report json write"),
    )
    monkeypatch.setattr(
        worker_execution,
        "write_report_md_lines",
        lambda *args, **kwargs: pytest.fail("unexpected report md write"),
    )

    worker_execution._write_execution_artifacts(entry, result)


def test_write_execution_artifacts_writes_retained_paths_to_state_and_report(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz, molecule_key="mol-42")
    result = _result(
        job_dir,
        selected_xyz,
        reason="ok",
        retained_names=("crest_conformers.xyz", "crest_best.xyz"),
    )

    worker_execution._write_execution_artifacts(entry, result)

    state_payload = load_state(job_dir)
    report_payload = load_report_json(job_dir)
    assert state_payload is not None
    assert report_payload is not None
    assert state_payload["status"] == "completed"
    assert state_payload["retained_conformer_count"] == 2
    assert state_payload["retained_conformer_paths"] == list(result.retained_conformer_paths)
    assert report_payload["queue_id"] == entry.queue_id
    assert report_payload["molecule_key"] == "mol-42"
    assert report_payload["retained_conformer_paths"] == list(result.retained_conformer_paths)

    report_md = (job_dir / REPORT_MD_FILE_NAME).read_text(encoding="utf-8")
    assert f"- Selected XYZ: `{selected_xyz.name}`" in report_md
    assert "- Retained Files:" in report_md
    for path in result.retained_conformer_paths:
        assert f"`{path}`" in report_md


def test_write_running_state_returns_early_without_job_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    entry = _entry("   ", "")

    monkeypatch.setattr(worker_execution, "write_state", lambda *args, **kwargs: pytest.fail("unexpected state write"))

    worker_execution._write_running_state(cfg, entry)


def test_write_running_state_writes_running_payload_with_fallback_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz, started_at=None, mode="nci", molecule_key="mol-42")
    captured: dict[str, Any] = {}
    timestamps = iter(
        [
            "2026-04-19T08:00:00+00:00",
            "2026-04-19T08:00:01+00:00",
        ]
    )

    monkeypatch.setattr("chemstack.core.utils.now_utc_iso", lambda: next(timestamps))
    monkeypatch.setattr(
        worker_execution,
        "write_state",
        lambda actual_job_dir, payload: captured.update(job_dir=actual_job_dir, payload=payload),
    )

    worker_execution._write_running_state(cfg, entry)

    assert captured["job_dir"] == job_dir.resolve()
    assert captured["payload"] == {
        "job_id": entry.task_id,
        "job_dir": str(job_dir.resolve()),
        "selected_input_xyz": str(selected_xyz),
        "molecule_key": "mol-42",
        "mode": "nci",
        "status": "running",
        "reason": "",
        "started_at": "2026-04-19T08:00:00+00:00",
        "updated_at": "2026-04-19T08:00:01+00:00",
        "resource_request": {"max_cores": 4, "max_memory_gb": 16},
        "resource_actual": {"max_cores": 4, "max_memory_gb": 16},
    }


def test_molecule_key_prefers_metadata_and_falls_back_to_selected_xyz(tmp_path: Path) -> None:
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "Selected Input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")

    assert (
        worker_execution._molecule_key(
            _entry(job_dir, selected_xyz, molecule_key=" fixed-key "),
            selected_xyz,
            job_dir,
        )
        == "fixed-key"
    )
    assert (
        worker_execution._molecule_key(
            _entry(job_dir, selected_xyz, molecule_key=" "),
            selected_xyz,
            job_dir,
        )
        == "selected_input"
    )


def test_terminate_process_returns_immediately_when_process_has_exited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ExitedProcess:
        pid = 5555

        def __init__(self) -> None:
            self.terminate_calls = 0
            self.kill_calls = 0
            self.wait_calls: list[int] = []

        def poll(self) -> int | None:
            return 0

        def terminate(self) -> None:
            self.terminate_calls += 1

        def kill(self) -> None:
            self.kill_calls += 1

        def wait(self, timeout: int) -> None:
            self.wait_calls.append(timeout)

    proc = ExitedProcess()
    monkeypatch.setattr(
        worker_execution.os,
        "killpg",
        lambda *args, **kwargs: pytest.fail("killpg should not run for an exited process"),
    )

    worker_execution._terminate_process(cast(Any, proc))

    assert proc.terminate_calls == 0
    assert proc.kill_calls == 0
    assert proc.wait_calls == []


def test_terminate_process_falls_back_to_proc_methods_and_escalates_after_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RunningProcess:
        pid = 7777

        def __init__(self) -> None:
            self.terminate_calls = 0
            self.kill_calls = 0
            self.wait_calls: list[int] = []

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            self.terminate_calls += 1

        def kill(self) -> None:
            self.kill_calls += 1

        def wait(self, timeout: int) -> None:
            self.wait_calls.append(timeout)
            raise subprocess.TimeoutExpired(cmd="crest", timeout=timeout)

    proc = RunningProcess()
    killpg_calls: list[tuple[int, signal.Signals]] = []

    def fake_killpg(pid: int, sig: signal.Signals) -> None:
        killpg_calls.append((pid, sig))
        raise PermissionError("denied")

    monkeypatch.setattr(worker_execution.os, "killpg", fake_killpg)

    worker_execution._terminate_process(cast(Any, proc))

    assert killpg_calls == [
        (proc.pid, signal.SIGTERM),
        (proc.pid, signal.SIGKILL),
    ]
    assert proc.terminate_calls == 1
    assert proc.kill_calls == 1
    assert proc.wait_calls == [10, 5]


def test_terminate_process_swallows_proc_method_errors_after_killpg_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FlakyProcess:
        pid = 8888

        def __init__(self) -> None:
            self.terminate_calls = 0
            self.kill_calls = 0
            self.wait_calls: list[int] = []

        def poll(self) -> int | None:
            return None

        def terminate(self) -> None:
            self.terminate_calls += 1
            raise RuntimeError("terminate failed")

        def kill(self) -> None:
            self.kill_calls += 1
            raise RuntimeError("kill failed")

        def wait(self, timeout: int) -> None:
            self.wait_calls.append(timeout)
            if timeout == 10:
                raise subprocess.TimeoutExpired(cmd="crest", timeout=timeout)

    proc = FlakyProcess()
    monkeypatch.setattr(worker_execution.os, "killpg", lambda *args, **kwargs: (_ for _ in ()).throw(ProcessLookupError()))

    worker_execution._terminate_process(cast(Any, proc))

    assert proc.terminate_calls == 1
    assert proc.kill_calls == 1
    assert proc.wait_calls == [10, 5]


def test_sync_job_tracking_skips_organize_when_auto_organize_is_disabled(tmp_path: Path) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz, molecule_key="fixed-key")
    context = _context(entry, job_dir, selected_xyz, molecule_key="fixed-key")
    result = _result(job_dir, selected_xyz, reason="ok")
    upsert_calls: list[dict[str, Any]] = []

    deps = _dependencies(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        organize_job_dir=lambda *args, **kwargs: pytest.fail("organize should not run"),
    )

    organized_output_dir = worker_execution._sync_job_tracking(
        cfg,
        context,
        result,
        auto_organize=False,
        dependencies=deps,
    )

    assert organized_output_dir is None
    assert len(upsert_calls) == 1
    assert upsert_calls[0]["job_id"] == entry.task_id
    assert upsert_calls[0]["job_dir"] == job_dir.resolve()
    assert upsert_calls[0]["molecule_key"] == "fixed-key"


@pytest.mark.parametrize("raises", [False, True], ids=["not-organized", "organize-exception"])
def test_sync_job_tracking_returns_none_when_organize_does_not_finish(
    tmp_path: Path,
    raises: bool,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz)
    context = _context(entry, job_dir, selected_xyz)
    result = _result(job_dir, selected_xyz, reason="ok")
    upsert_calls: list[dict[str, Any]] = []
    organize_calls: list[tuple[Path, bool]] = []

    def fake_organize(cfg: Any, actual_job_dir: Path, *, notify_summary: bool) -> dict[str, str]:
        organize_calls.append((actual_job_dir, notify_summary))
        if raises:
            raise RuntimeError("boom")
        return {"action": "skipped", "target_dir": str(tmp_path / "organized" / "job")}

    deps = _dependencies(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        organize_job_dir=fake_organize,
    )

    organized_output_dir = worker_execution._sync_job_tracking(
        cfg,
        context,
        result,
        auto_organize=True,
        dependencies=deps,
    )

    assert organized_output_dir is None
    assert organize_calls == [(job_dir.resolve(), False)]
    assert len(upsert_calls) == 1
    assert upsert_calls[0]["status"] == "completed"
    assert upsert_calls[0]["selected_input_xyz"] == str(selected_xyz.resolve())


def test_sync_job_tracking_records_organized_output_dir_when_auto_organize_succeeds(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    organized_target = (tmp_path / "organized" / "job").resolve()
    organized_target.mkdir(parents=True)
    entry = _entry(job_dir, selected_xyz, molecule_key="organized-key")
    context = _context(entry, job_dir, selected_xyz, molecule_key="organized-key")
    result = _result(job_dir, selected_xyz, reason="ok")
    upsert_calls: list[dict[str, Any]] = []

    deps = _dependencies(
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        organize_job_dir=lambda cfg, actual_job_dir, *, notify_summary: {
            "action": "organized",
            "target_dir": str(organized_target),
        },
    )

    organized_output_dir = worker_execution._sync_job_tracking(
        cfg,
        context,
        result,
        auto_organize=True,
        dependencies=deps,
    )

    assert organized_output_dir == organized_target
    assert [call["job_dir"] for call in upsert_calls] == [
        job_dir.resolve(),
        job_dir.resolve(),
        organized_target,
    ]
    assert upsert_calls[1]["organized_output_dir"] == organized_target
    assert upsert_calls[2]["organized_output_dir"] == organized_target
    assert capsys.readouterr().out.strip() == f"organized_output_dir: {organized_target}"


def test_process_dequeued_entry_polls_sleeps_and_completes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz)
    proc = FakeProcess(None, 0)
    running = SimpleNamespace(process=proc)
    result = _result(job_dir, selected_xyz, reason="ok")

    sleeps: list[int] = []
    resource_caps_calls: list[Any] = []
    molecule_key_calls: list[tuple[SimpleNamespace, Path, Path]] = []
    cancel_checks: list[tuple[str, str]] = []
    finalize_kwargs: list[dict[str, Any]] = []
    terminate_calls: list[FakeProcess] = []
    running_state_calls: list[str] = []
    artifact_results: list[CrestRunResult] = []
    mark_completed_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    mark_cancelled_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    mark_failed_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    upsert_calls: list[dict[str, Any]] = []
    started_notifications: list[dict[str, Any]] = []
    finished_notifications: list[dict[str, Any]] = []

    monkeypatch.setattr(worker_execution.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_resource_caps(actual_cfg: Any) -> dict[str, int]:
        resource_caps_calls.append(actual_cfg)
        return {"max_cores": 4, "max_memory_gb": 16}

    def fake_molecule_key(actual_entry: Any, actual_selected_xyz: Path, actual_job_dir: Path) -> str:
        molecule_key_calls.append((actual_entry, actual_selected_xyz, actual_job_dir))
        return "derived-key"

    def fake_finalize(running_job: Any, **kwargs: Any) -> CrestRunResult:
        finalize_kwargs.append(kwargs)
        assert running_job is running
        return result

    def fake_get_cancel_requested(root: str, queue_id: str) -> bool:
        cancel_checks.append((root, queue_id))
        return False

    def fake_notify_started(cfg: Any, **kwargs: Any) -> bool:
        started_notifications.append(kwargs)
        return True

    def fake_notify_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_notifications.append(kwargs)
        return True

    deps = _dependencies(
        get_cancel_requested=fake_get_cancel_requested,
        start_crest_job=lambda cfg, *, job_dir, selected_xyz: running,
        finalize_crest_job=fake_finalize,
        terminate_process=lambda proc: terminate_calls.append(proc),
        write_running_state=lambda cfg, actual_entry: running_state_calls.append(actual_entry.task_id),
        write_execution_artifacts=lambda actual_entry, actual_result: artifact_results.append(actual_result),
        mark_completed=lambda *args, **kwargs: mark_completed_calls.append((args, kwargs)),
        mark_cancelled=lambda *args, **kwargs: mark_cancelled_calls.append((args, kwargs)),
        mark_failed=lambda *args, **kwargs: mark_failed_calls.append((args, kwargs)),
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        notify_job_started=fake_notify_started,
        notify_job_finished=fake_notify_finished,
    )

    outcome = worker_execution.process_dequeued_entry(
        cfg,
        entry,
        auto_organize=False,
        resource_caps=fake_resource_caps,
        molecule_key_resolver=fake_molecule_key,
        dependencies=deps,
    )

    assert outcome.result == result
    assert outcome.job_dir == job_dir.resolve()
    assert outcome.selected_xyz == selected_xyz.resolve()
    assert outcome.molecule_key == "derived-key"
    assert outcome.organized_output_dir is None
    assert resource_caps_calls == []
    assert molecule_key_calls == [(entry, selected_xyz.resolve(), job_dir.resolve())]
    assert cancel_checks == [(cfg.runtime.allowed_root, entry.queue_id)]
    assert sleeps == [worker_execution.CANCEL_CHECK_INTERVAL_SECONDS]
    assert finalize_kwargs == [{}]
    assert terminate_calls == []
    assert running_state_calls == [entry.task_id]
    assert artifact_results == [result]
    assert [call["status"] for call in upsert_calls] == ["running", "completed"]
    assert len(mark_completed_calls) == 1
    assert mark_completed_calls[0][0] == (cfg.runtime.allowed_root, entry.queue_id)
    assert mark_completed_calls[0][1]["metadata_update"] == {
        "retained_conformer_count": result.retained_conformer_count,
        "mode": result.mode,
    }
    assert mark_cancelled_calls == []
    assert mark_failed_calls == []
    assert started_notifications[0]["selected_xyz"] == selected_xyz.resolve()
    assert finished_notifications[0]["status"] == "completed"
    assert finished_notifications[0]["resource_request"] == {"max_cores": 4, "max_memory_gb": 16}


def test_process_dequeued_entry_terminates_and_forces_cancelled_result(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz)
    proc = FakeProcess(None)
    running = SimpleNamespace(process=proc)
    result = _result(job_dir, selected_xyz, status="cancelled", reason="cancel_requested", exit_code=-15)

    sleeps: list[int] = []
    finalize_kwargs: list[dict[str, Any]] = []
    terminate_calls: list[FakeProcess] = []
    mark_cancelled_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    finished_notifications: list[dict[str, Any]] = []

    monkeypatch.setattr(worker_execution.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_finalize(running_job: Any, **kwargs: Any) -> CrestRunResult:
        finalize_kwargs.append(kwargs)
        assert running_job is running
        return result

    def fake_notify_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_notifications.append(kwargs)
        return True

    deps = _dependencies(
        get_cancel_requested=lambda *args, **kwargs: True,
        start_crest_job=lambda cfg, *, job_dir, selected_xyz: running,
        finalize_crest_job=fake_finalize,
        terminate_process=lambda actual_proc: terminate_calls.append(actual_proc),
        mark_cancelled=lambda *args, **kwargs: mark_cancelled_calls.append((args, kwargs)),
        notify_job_finished=fake_notify_finished,
    )

    outcome = worker_execution.process_dequeued_entry(
        cfg,
        entry,
        auto_organize=False,
        resource_caps=lambda cfg: {"max_cores": 4, "max_memory_gb": 16},
        molecule_key_resolver=lambda entry, selected_xyz, job_dir: "cancel-key",
        dependencies=deps,
    )

    assert outcome.result == result
    assert sleeps == []
    assert terminate_calls == [proc]
    assert finalize_kwargs == [
        {
            "forced_status": "cancelled",
            "forced_reason": "cancel_requested",
        }
    ]
    assert len(mark_cancelled_calls) == 1
    assert mark_cancelled_calls[0][0] == (cfg.runtime.allowed_root, entry.queue_id)
    assert mark_cancelled_calls[0][1]["error"] == "cancel_requested"
    assert finished_notifications[0]["status"] == "cancelled"
    assert finished_notifications[0]["reason"] == "cancel_requested"


def test_process_dequeued_entry_builds_failed_result_when_runner_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    manifest_path = job_dir / "crest_job.yaml"
    manifest_path.write_text("mode: standard\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz, started_at=None)
    failure_time = "2026-04-19T11:30:00+00:00"

    sleeps: list[int] = []
    resource_caps_calls: list[Any] = []
    artifact_results: list[CrestRunResult] = []
    mark_failed_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    upsert_calls: list[dict[str, Any]] = []
    finished_notifications: list[dict[str, Any]] = []

    monkeypatch.setattr(worker_execution.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_resource_caps(actual_cfg: Any) -> dict[str, int]:
        resource_caps_calls.append(actual_cfg)
        return {"max_cores": 4, "max_memory_gb": 16}

    def fake_notify_finished(cfg: Any, **kwargs: Any) -> bool:
        finished_notifications.append(kwargs)
        return True

    deps = _dependencies(
        now_utc_iso=lambda: failure_time,
        start_crest_job=lambda cfg, *, job_dir, selected_xyz: (_ for _ in ()).throw(RuntimeError("boom")),
        finalize_crest_job=lambda *args, **kwargs: pytest.fail("finalize should not run"),
        get_cancel_requested=lambda *args, **kwargs: pytest.fail("cancel should not be checked"),
        terminate_process=lambda *args, **kwargs: pytest.fail("terminate should not run"),
        write_execution_artifacts=lambda actual_entry, actual_result: artifact_results.append(actual_result),
        mark_failed=lambda *args, **kwargs: mark_failed_calls.append((args, kwargs)),
        upsert_job_record=lambda cfg, **kwargs: upsert_calls.append(kwargs),
        notify_job_finished=fake_notify_finished,
    )

    outcome = worker_execution.process_dequeued_entry(
        cfg,
        entry,
        auto_organize=False,
        resource_caps=fake_resource_caps,
        molecule_key_resolver=lambda entry, selected_xyz, job_dir: "failure-key",
        dependencies=deps,
    )

    result = outcome.result
    assert result.status == "failed"
    assert result.reason == "runner_error:boom"
    assert result.exit_code == 1
    assert result.started_at == failure_time
    assert result.finished_at == failure_time
    assert result.stdout_log == str((job_dir / "crest.stdout.log").resolve())
    assert result.stderr_log == str((job_dir / "crest.stderr.log").resolve())
    assert result.selected_input_xyz == str(selected_xyz.resolve())
    assert result.manifest_path == str(manifest_path.resolve())
    assert result.resource_request == {"max_cores": 4, "max_memory_gb": 16}
    assert result.resource_actual == {"max_cores": 4, "max_memory_gb": 16}
    assert resource_caps_calls == []
    assert sleeps == []
    assert artifact_results == [result]
    assert [call["status"] for call in upsert_calls] == ["running", "failed"]
    assert len(mark_failed_calls) == 1
    assert mark_failed_calls[0][0] == (cfg.runtime.allowed_root, entry.queue_id)
    assert mark_failed_calls[0][1]["error"] == "runner_error:boom"
    assert finished_notifications[0]["status"] == "failed"
    assert finished_notifications[0]["reason"] == "runner_error:boom"


def test_process_dequeued_entry_raises_worker_shutdown_requested_before_start(
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz)

    deps = _dependencies(
        write_running_state=lambda *args, **kwargs: pytest.fail("running state should not be written"),
        upsert_job_record=lambda *args, **kwargs: pytest.fail("job record should not be updated"),
        notify_job_started=lambda *args, **kwargs: pytest.fail("start notification should not run"),
        start_crest_job=lambda *args, **kwargs: pytest.fail("job should not start"),
    )

    with pytest.raises(worker_execution.WorkerShutdownRequested) as exc_info:
        worker_execution.process_dequeued_entry(
            cfg,
            entry,
            auto_organize=False,
            resource_caps=lambda cfg: {"max_cores": 4, "max_memory_gb": 16},
            molecule_key_resolver=lambda entry, selected_xyz, job_dir: "shutdown-key",
            dependencies=deps,
            shutdown_requested=lambda: True,
        )

    assert exc_info.value.context.job_dir == job_dir.resolve()
    assert exc_info.value.context.selected_xyz == selected_xyz.resolve()


def test_process_dequeued_entry_raises_worker_shutdown_requested_after_start(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = _cfg(tmp_path)
    job_dir = tmp_path / "job"
    job_dir.mkdir()
    selected_xyz = job_dir / "selected_input.xyz"
    selected_xyz.write_text("1\nselected\nH 0.0 0.0 0.0\n", encoding="utf-8")
    entry = _entry(job_dir, selected_xyz)
    proc = FakeProcess(None)
    running = SimpleNamespace(process=proc)

    terminate_calls: list[FakeProcess] = []
    finalize_kwargs: list[dict[str, Any]] = []
    sleeps: list[int] = []

    monkeypatch.setattr(worker_execution.time, "sleep", lambda seconds: sleeps.append(seconds))

    def fake_finalize_crest_job(running_job: object, **kwargs: Any) -> CrestRunResult:
        finalize_kwargs.append(kwargs)
        return _result(
            job_dir,
            selected_xyz,
            status="failed",
            reason="worker_shutdown",
            exit_code=143,
        )

    deps = _dependencies(
        get_cancel_requested=lambda *args, **kwargs: False,
        start_crest_job=lambda cfg, *, job_dir, selected_xyz: running,
        terminate_process=lambda actual_proc: terminate_calls.append(actual_proc),
        finalize_crest_job=fake_finalize_crest_job,
        write_execution_artifacts=lambda *args, **kwargs: pytest.fail("artifacts should not be written"),
        mark_completed=lambda *args, **kwargs: pytest.fail("queue should not be marked completed"),
        mark_cancelled=lambda *args, **kwargs: pytest.fail("queue should not be marked cancelled"),
        mark_failed=lambda *args, **kwargs: pytest.fail("queue should not be marked failed"),
        notify_job_finished=lambda *args, **kwargs: pytest.fail("finish notification should not run"),
    )

    shutdown_checks = iter([False, False, True])
    with pytest.raises(worker_execution.WorkerShutdownRequested):
        worker_execution.process_dequeued_entry(
            cfg,
            entry,
            auto_organize=False,
            resource_caps=lambda cfg: {"max_cores": 4, "max_memory_gb": 16},
            molecule_key_resolver=lambda entry, selected_xyz, job_dir: "shutdown-key",
            dependencies=deps,
            shutdown_requested=lambda: next(shutdown_checks),
        )

    assert terminate_calls == [proc]
    assert finalize_kwargs == [{"forced_status": "failed", "forced_reason": "worker_shutdown"}]
    assert sleeps == []
