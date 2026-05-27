from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.xtb import queue_runtime as queue_cmd
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


def test_queue_worker_parser_has_no_organize_flags() -> None:
    args = queue_cmd.build_parser().parse_args(["--config", "/tmp/chemstack.yaml"])

    assert args.config == "/tmp/chemstack.yaml"
    assert not hasattr(args, "auto_organize")
    assert not hasattr(args, "no_auto_organize")

    with pytest.raises(SystemExit):
        queue_cmd.build_parser().parse_args(["--config", "/tmp/chemstack.yaml", "--auto-organize"])


def test_process_one_returns_blocked_when_no_admission_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: None)

    assert queue_cmd._process_one(cfg) == "blocked"


def test_process_one_returns_idle_and_releases_reserved_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    released: list[tuple[object, object]] = []

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: "slot-1")
    monkeypatch.setattr(queue_cmd, "dequeue_next", lambda _root: None)
    monkeypatch.setattr(
        queue_cmd, "release_slot", lambda root, token: released.append((root, token))
    )

    assert queue_cmd._process_one(cfg) == "idle"
    assert released == [(cfg.runtime.admission_root, "slot-1")]


def test_queue_worker_starts_up_to_max_concurrent_children(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    entries = []
    for index in range(2):
        job_dir = queue_root / f"job-{index}"
        job_dir.mkdir()
        selected_xyz = job_dir / f"input-{index}.xyz"
        selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
        entries.append(
            _make_entry(job_dir, selected_xyz, queue_id=f"queue-{index}", job_id=f"job-{index}")
        )

    slots = iter(["slot-1", "slot-2"])
    dequeued = iter(entries)
    started: list[tuple[str, str, str]] = []

    class _Process:
        def __init__(self, pid: int) -> None:
            self.pid = pid

        def poll(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: next(slots))
    monkeypatch.setattr(queue_cmd, "dequeue_next", lambda _root: next(dequeued))

    def fake_start_background_job_process(
        *,
        config_path: str,
        queue_root: Path,
        entry: object,
        admission_root: str,
        admission_token: str,
    ) -> _Process:
        started.append((config_path, str(queue_root), admission_token))
        return _Process(len(started) + 100)

    monkeypatch.setattr(
        queue_cmd,
        "_start_background_job_process",
        fake_start_background_job_process,
    )

    worker = queue_cmd.QueueWorker(
        cfg,
        config_path="/tmp/chemstack.yaml",
        max_concurrent=2,
    )

    assert worker._fill_slots() == "processed"
    assert sorted(worker._running) == ["queue-0", "queue-1"]
    assert started == [
        ("/tmp/chemstack.yaml", str(queue_root), "slot-1"),
        ("/tmp/chemstack.yaml", str(queue_root), "slot-2"),
    ]


def test_queue_worker_check_cancel_requests_signals_each_job_once(
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

    signals: list[int] = []

    class _Process:
        pid = 1234

        def poll(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        def send_signal(self, signum: int) -> None:
            signals.append(signum)

    monkeypatch.setattr(queue_cmd, "get_cancel_requested", lambda _root, _queue_id: True)

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml")
    worker._running[entry.queue_id] = queue_cmd._RunningJob(
        queue_root=queue_root,
        entry=entry,
        process=_Process(),
        admission_token="slot-1",
    )

    worker._check_cancel_requests()
    worker._check_cancel_requests()

    assert signals == [queue_cmd.WORKER_CANCEL_SIGNAL]
    assert worker._running[entry.queue_id].cancel_requested is True


def test_queue_worker_shutdown_requeues_running_entries(
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

    terminated: list[int] = []
    requeued: list[tuple[str, str]] = []
    released: list[tuple[str, str]] = []

    class _Process:
        pid = 9001

        def poll(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

    monkeypatch.setattr(queue_cmd, "_terminate_process", lambda proc: terminated.append(proc.pid))
    monkeypatch.setattr(
        queue_cmd, "requeue_running_entry", lambda root, queue_id: requeued.append((root, queue_id))
    )
    monkeypatch.setattr(
        queue_cmd, "release_slot", lambda root, token: released.append((root, token))
    )

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml")
    worker._running[entry.queue_id] = queue_cmd._RunningJob(
        queue_root=queue_root,
        entry=entry,
        process=_Process(),
        admission_token="slot-1",
    )

    worker._shutdown_all()

    assert terminated == [9001]
    assert requeued == [(str(queue_root), "queue-1")]
    assert released == [(cfg.runtime.admission_root, "slot-1")]
    assert worker._running == {}
    state = state_mod.load_state(job_dir)
    assert state is not None
    assert state["status"] == "queued"
    assert state["reason"] == "worker_shutdown"
    assert state["recovery_pending"] is True


def test_queue_worker_run_once_waits_for_child_completion_and_prints_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)
    sleep_calls: list[float] = []
    released: list[tuple[str, str]] = []

    class _Process:
        def __init__(self) -> None:
            self.pid = 4444
            self._poll_values = iter([None, 0])

        def poll(self) -> int | None:
            return next(self._poll_values)

        def wait(self, timeout: float | None = None) -> int:
            return 0

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

    monkeypatch.setattr(queue_cmd, "reconcile_stale_slots", lambda _root: 0)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [])
    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: "slot-1")
    monkeypatch.setattr(queue_cmd, "dequeue_next", lambda _root: entry)
    monkeypatch.setattr(
        queue_cmd,
        "_start_background_job_process",
        lambda **kwargs: _Process(),
    )
    monkeypatch.setattr(
        queue_cmd,
        "_load_terminal_summary",
        lambda queue_root, entry, rc=None: queue_cmd._TerminalSummary(
            queue_id=entry.queue_id,
            job_id=entry.task_id,
            status="completed",
            reason="xtb_ok",
        ),
    )
    monkeypatch.setattr(
        queue_cmd, "_ensure_terminal_queue_status", lambda queue_root, entry, summary: None
    )
    monkeypatch.setattr(
        queue_cmd, "release_slot", lambda root, token: released.append((root, token))
    )
    monkeypatch.setattr(queue_cmd.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml")
    exit_code = worker.run_once()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "status: completed" in output
    assert "reason: xtb_ok" in output
    assert sleep_calls == [queue_cmd.POLL_INTERVAL_SECONDS]
    assert released == [(cfg.runtime.admission_root, "slot-1")]


def test_queue_worker_reconcile_worker_state_requeues_stale_running_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz, status="running")
    state_mod.write_state(job_dir, {"status": "running", "worker_job_pid": 999_999})
    requeued: list[tuple[str, str]] = []

    monkeypatch.setattr(queue_cmd, "reconcile_stale_slots", lambda _root: 0)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [entry])
    monkeypatch.setattr(queue_cmd, "_pid_is_alive", lambda _pid: False)
    monkeypatch.setattr(
        queue_cmd, "requeue_running_entry", lambda root, queue_id: requeued.append((root, queue_id))
    )

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml")
    worker._reconcile_worker_state()

    assert requeued == [(str(queue_root), "queue-1")]
    state = state_mod.load_state(job_dir)
    assert state is not None
    assert state["status"] == "queued"
    assert state["reason"] == "crashed_recovery"
    assert state["recovery_pending"] is True


def test_cmd_queue_worker_constructs_xtb_worker_without_organize_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    seen: list[tuple[str, str]] = []

    class _FakeWorker:
        def __init__(
            self,
            cfg_obj: object,
            *,
            config_path: str,
            max_concurrent: int | None = None,
        ) -> None:
            seen.append(("init", config_path))

        def run_once(self) -> int:
            seen.append(("run_once", ""))
            return 17

        def run(self) -> int:
            seen.append(("run", ""))
            return 23

    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "QueueWorker", _FakeWorker)
    monkeypatch.setattr(queue_cmd, "default_config_path", lambda: "/tmp/default-chemstack.yaml")

    exit_code = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config=None,
        )
    )

    assert seen[0] == ("init", "/tmp/default-chemstack.yaml")
    assert exit_code == 23
    assert seen[-1] == ("run", "")
