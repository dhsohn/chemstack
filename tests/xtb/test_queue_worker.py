from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from chemstack.xtb.commands import queue as queue_cmd
from chemstack.xtb import state as state_mod


def _make_cfg(tmp_path: Path, *, auto_organize: bool = False) -> SimpleNamespace:
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
        behavior=SimpleNamespace(auto_organize_on_terminal=auto_organize),
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


def _record_finished_call(finished_calls: list[dict[str, object]], kwargs: dict[str, object]) -> bool:
    finished_calls.append(kwargs)
    return True


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        (
            SimpleNamespace(
                cancel_requested=True,
                status=SimpleNamespace(value="running"),
            ),
            "cancel_requested",
        ),
        (
            SimpleNamespace(
                cancel_requested=False,
                status=SimpleNamespace(value=" "),
            ),
            "unknown",
        ),
    ],
)
def test_queue_display_status(entry: object, expected: str) -> None:
    assert queue_cmd._display_status(entry) == expected


def test_find_entry_by_target_matches_queue_id_and_job_id() -> None:
    first = SimpleNamespace(queue_id="q-1", task_id="job-1")
    second = SimpleNamespace(queue_id="q-2", task_id="job-2")

    assert queue_cmd._find_entry_by_target([first, second], "q-2") is second
    assert queue_cmd._find_entry_by_target([first, second], "job-1") is first
    assert queue_cmd._find_entry_by_target([first, second], "missing") is None


def test_execute_queue_entry_processes_completed_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "reactant.xyz"
    selected_xyz.write_text("3\nreactant\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(
        job_dir,
        selected_xyz,
        input_summary={"candidate_count": 1, "candidate_paths": [str(selected_xyz)]},
    )
    result = _make_result(selected_xyz, status="completed", reason="xtb_ok", candidate_paths=(str(selected_xyz),))

    completed_calls: list[tuple[object, object, object | None]] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda _cfg, *, job_dir, selected_input_xyz: SimpleNamespace(process=SimpleNamespace(poll=lambda: 0)),
    )
    monkeypatch.setattr(queue_cmd, "finalize_xtb_job", lambda running, **kwargs: result)
    monkeypatch.setattr(
        queue_cmd,
        "mark_completed",
        lambda root, queue_id, metadata_update=None: completed_calls.append((root, queue_id, metadata_update)),
    )
    monkeypatch.setattr(queue_cmd, "mark_failed", lambda *args, **kwargs: pytest.fail("unexpected failure mark"))
    monkeypatch.setattr(queue_cmd, "mark_cancelled", lambda *args, **kwargs: pytest.fail("unexpected cancel mark"))
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=False,
    )

    assert outcome.result.status == "completed"
    assert completed_calls == [
        (
            cfg.runtime.allowed_root,
            "queue-1",
            {"candidate_count": 1, "job_type": "path_search"},
        )
    ]

    state = state_mod.load_state(job_dir)
    report = state_mod.load_report_json(job_dir)
    assert state is not None
    assert report is not None
    assert state["status"] == "completed"
    assert report["reason"] == "xtb_ok"
    assert report["selected_candidate_paths"] == [str(selected_xyz)]


def test_execute_queue_entry_marks_runner_errors_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "reactant.xyz"
    selected_xyz.write_text("3\nreactant\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)

    failed_calls: list[tuple[object, object, object, object | None]] = []

    monkeypatch.setattr(queue_cmd, "start_xtb_job", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(queue_cmd, "mark_completed", lambda *args, **kwargs: pytest.fail("unexpected completed mark"))
    monkeypatch.setattr(queue_cmd, "mark_cancelled", lambda *args, **kwargs: pytest.fail("unexpected cancel mark"))
    monkeypatch.setattr(
        queue_cmd,
        "mark_failed",
        lambda root, queue_id, error, metadata_update=None: failed_calls.append((root, queue_id, error, metadata_update)),
    )
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=False,
    )

    assert outcome.result.status == "failed"
    assert outcome.result.reason == "runner_error:boom"
    assert failed_calls == [
        (
            cfg.runtime.allowed_root,
            "queue-1",
            "runner_error:boom",
            {"candidate_count": 0, "job_type": "path_search"},
        )
    ]

    state = state_mod.load_state(job_dir)
    report = state_mod.load_report_json(job_dir)
    assert state is not None
    assert report is not None
    assert state["status"] == "failed"
    assert report["reason"] == "runner_error:boom"


def test_execute_queue_entry_cancels_running_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "reactant.xyz"
    selected_xyz.write_text("3\nreactant\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)
    result = _make_result(selected_xyz, status="cancelled", reason="cancel_requested")

    cancelled_calls: list[tuple[object, object, object, object | None]] = []
    finalize_calls: list[tuple[object | None, object | None]] = []

    class _Process:
        pid = 12345

        def poll(self) -> None:
            return None

    terminated: list[_Process] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda _cfg, *, job_dir, selected_input_xyz: SimpleNamespace(process=_Process()),
    )
    monkeypatch.setattr(queue_cmd, "_terminate_process", lambda process: terminated.append(process))

    def fake_finalize_xtb_job(running: object, forced_status: object = None, forced_reason: object = None) -> queue_cmd.XtbRunResult:
        finalize_calls.append((forced_status, forced_reason))
        return result

    monkeypatch.setattr(queue_cmd, "finalize_xtb_job", fake_finalize_xtb_job)
    monkeypatch.setattr(queue_cmd, "mark_completed", lambda *args, **kwargs: pytest.fail("unexpected completed mark"))
    monkeypatch.setattr(queue_cmd, "mark_failed", lambda *args, **kwargs: pytest.fail("unexpected failed mark"))
    monkeypatch.setattr(
        queue_cmd,
        "mark_cancelled",
        lambda root, queue_id, error, metadata_update=None: cancelled_calls.append((root, queue_id, error, metadata_update)),
    )
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    cancel_checks = iter([False, True])

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=False,
        should_cancel=lambda: next(cancel_checks, True),
    )

    assert outcome.result.status == "cancelled"
    assert terminated and terminated[0].pid == 12345
    assert finalize_calls == [("cancelled", "cancel_requested")]
    assert cancelled_calls == [
        (
            cfg.runtime.allowed_root,
            "queue-1",
            "cancel_requested",
            {"candidate_count": 0, "job_type": "path_search"},
        )
    ]

    state = state_mod.load_state(job_dir)
    report = state_mod.load_report_json(job_dir)
    assert state is not None
    assert report is not None
    assert state["status"] == "cancelled"
    assert report["reason"] == "cancel_requested"


def test_execute_queue_entry_auto_organize_failure_still_finishes_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, auto_organize=True)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "job-1"
    job_dir.mkdir()
    selected_xyz = job_dir / "input.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz)
    result = _make_result(selected_xyz, status="completed", reason="completed")
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda _cfg, *, job_dir, selected_input_xyz: SimpleNamespace(process=SimpleNamespace(poll=lambda: 0)),
    )
    monkeypatch.setattr(queue_cmd, "finalize_xtb_job", lambda running, **kwargs: result)
    monkeypatch.setattr(queue_cmd, "mark_completed", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "organize_job_dir", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: _record_finished_call(finished_calls, kwargs))

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=True,
    )

    assert outcome.result.status == "completed"
    assert outcome.organized_output_dir == ""
    assert len(finished_calls) == 1
    assert finished_calls[0]["organized_output_dir"] is None


def test_execute_queue_entry_processes_ranking_job_and_auto_organizes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path, auto_organize=True)
    queue_root = Path(cfg.runtime.allowed_root)
    job_dir = queue_root / "ranking-job"
    job_dir.mkdir()
    selected_xyz = job_dir / "candidate.xyz"
    selected_xyz.write_text("3\ncandidate\nH 0 0 0\n", encoding="utf-8")
    entry = _make_entry(job_dir, selected_xyz, job_type="ranking", reaction_key="")
    result = _make_result(
        selected_xyz,
        status="completed",
        reason="completed",
        job_type="ranking",
        reaction_key="ranking-job",
        candidate_paths=(str(selected_xyz),),
    )
    organized_target = Path(cfg.runtime.organized_root) / "ranking" / "ranking-job" / "job-1"
    finished_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        queue_cmd,
        "run_xtb_ranking_job",
        lambda cfg_obj, **kwargs: result,
    )
    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda *args, **kwargs: pytest.fail("start_xtb_job should not be called for ranking"),
    )
    monkeypatch.setattr(queue_cmd, "mark_completed", lambda *args, **kwargs: None)
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        queue_cmd,
        "organize_job_dir",
        lambda cfg_obj, job_dir, notify_summary=False: {
            "action": "organized",
            "target_dir": str(organized_target),
        },
    )

    def fake_notify_job_finished(cfg_obj: object, **kwargs: object) -> bool:
        finished_calls.append(kwargs)
        return True

    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_job_finished)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        auto_organize=True,
    )

    assert outcome.result.status == "completed"
    assert outcome.organized_output_dir == str(organized_target)
    assert finished_calls and finished_calls[0]["organized_output_dir"] == organized_target


def test_cmd_queue_cancel_accepts_job_id_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    entry = SimpleNamespace(
        queue_id="queue-1",
        task_id="job-1",
        status=SimpleNamespace(value="running"),
        cancel_requested=False,
    )
    updated = SimpleNamespace(
        queue_id="queue-1",
        task_id="job-1",
        status=SimpleNamespace(value="running"),
        cancel_requested=True,
    )

    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [entry])
    monkeypatch.setattr(queue_cmd, "request_cancel", lambda _root, _queue_id: updated)

    exit_code = queue_cmd.cmd_queue_cancel(SimpleNamespace(config=None, target="job-1"))

    captured = capsys.readouterr().out
    assert exit_code == 0
    assert "status: cancel_requested" in captured
    assert "queue_id: queue-1" in captured
    assert "job_id: job-1" in captured


def test_cmd_queue_cancel_requires_non_blank_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)

    exit_code = queue_cmd.cmd_queue_cancel(SimpleNamespace(config=None, target="   "))

    assert exit_code == 1
    assert capsys.readouterr().out == "error: queue cancel requires a queue_id or job_id\n"


def test_cmd_queue_cancel_reports_missing_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [])

    exit_code = queue_cmd.cmd_queue_cancel(SimpleNamespace(config=None, target="job-missing"))

    assert exit_code == 1
    assert capsys.readouterr().out == "error: queue target not found: job-missing\n"


def test_cmd_queue_cancel_rejects_terminal_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = _make_cfg(tmp_path)
    entry = SimpleNamespace(
        queue_id="queue-1",
        task_id="job-1",
        status=SimpleNamespace(value="failed"),
        cancel_requested=False,
    )

    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "list_queue", lambda _root: [entry])
    monkeypatch.setattr(queue_cmd, "request_cancel", lambda _root, _queue_id: None)

    exit_code = queue_cmd.cmd_queue_cancel(SimpleNamespace(config=None, target="queue-1"))

    assert exit_code == 1
    assert capsys.readouterr().out == "error: queue target already terminal: queue-1\n"


def test_process_one_returns_blocked_when_no_admission_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: None)

    assert queue_cmd._process_one(cfg, auto_organize=False) == "blocked"


def test_process_one_returns_idle_and_releases_reserved_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
    released: list[tuple[object, object]] = []

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda _cfg: "slot-1")
    monkeypatch.setattr(queue_cmd, "dequeue_next", lambda _root: None)
    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((root, token)))

    assert queue_cmd._process_one(cfg, auto_organize=False) == "idle"
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
        entries.append(_make_entry(job_dir, selected_xyz, queue_id=f"queue-{index}", job_id=f"job-{index}"))

    slots = iter(["slot-1", "slot-2"])
    dequeued = iter(entries)
    started: list[tuple[str, str, str, bool]] = []

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
    monkeypatch.setattr(
        queue_cmd,
        "_start_background_job_process",
        lambda *, config_path, queue_root, entry, admission_root, admission_token, auto_organize: (
            started.append((config_path, str(queue_root), admission_token, auto_organize)) or _Process(len(started) + 100)
        ),
    )

    worker = queue_cmd.QueueWorker(
        cfg,
        config_path="/tmp/chemstack.yaml",
        auto_organize=True,
        max_concurrent=2,
    )

    assert worker._fill_slots() == "processed"
    assert sorted(worker._running) == ["queue-0", "queue-1"]
    assert started == [
        ("/tmp/chemstack.yaml", str(queue_root), "slot-1", True),
        ("/tmp/chemstack.yaml", str(queue_root), "slot-2", True),
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

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml", auto_organize=False)
    worker._running[entry.queue_id] = queue_cmd._RunningJob(
        queue_root=queue_root,
        entry=entry,
        process=_Process(),
        admission_token="slot-1",
    )
    monkeypatch.setattr(queue_cmd, "get_cancel_requested", lambda _root, _queue_id: True)

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

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml", auto_organize=False)
    worker._running[entry.queue_id] = queue_cmd._RunningJob(
        queue_root=queue_root,
        entry=entry,
        process=_Process(),
        admission_token="slot-1",
    )
    monkeypatch.setattr(queue_cmd, "_terminate_process", lambda proc: terminated.append(proc.pid))
    monkeypatch.setattr(queue_cmd, "requeue_running_entry", lambda root, queue_id: requeued.append((root, queue_id)))
    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((root, token)))

    worker._shutdown_all()

    assert terminated == [9001]
    assert requeued == [(str(queue_root), "queue-1")]
    assert released == [(cfg.runtime.admission_root, "slot-1")]
    assert worker._running == {}


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
    monkeypatch.setattr(queue_cmd, "_ensure_terminal_queue_status", lambda queue_root, entry, summary: None)
    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((root, token)))
    monkeypatch.setattr(queue_cmd.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml", auto_organize=False)
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
    monkeypatch.setattr(queue_cmd, "requeue_running_entry", lambda root, queue_id: requeued.append((root, queue_id)))

    worker = queue_cmd.QueueWorker(cfg, config_path="/tmp/cfg.yaml", auto_organize=False)
    worker._reconcile_worker_state()

    assert requeued == [(str(queue_root), "queue-1")]


@pytest.mark.parametrize(
    ("cfg_auto", "arg_auto", "arg_no_auto", "expected"),
    [
        (False, True, False, True),
        (True, False, True, False),
    ],
)
def test_cmd_queue_worker_respects_auto_organize_flag_overrides(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cfg_auto: bool,
    arg_auto: bool,
    arg_no_auto: bool,
    expected: bool,
) -> None:
    cfg = _make_cfg(tmp_path, auto_organize=cfg_auto)
    seen: list[tuple[str, bool, str]] = []

    class _FakeWorker:
        def __init__(self, cfg_obj: object, *, config_path: str, auto_organize: bool, max_concurrent: int | None = None) -> None:
            seen.append(("init", auto_organize, config_path))

        def run_once(self) -> int:
            seen.append(("run_once", False, ""))
            return 17

        def run(self) -> int:
            seen.append(("run", False, ""))
            return 23

    monkeypatch.setattr(queue_cmd, "load_config", lambda _path=None: cfg)
    monkeypatch.setattr(queue_cmd, "QueueWorker", _FakeWorker)
    monkeypatch.setattr(queue_cmd, "default_config_path", lambda: "/tmp/default-chemstack.yaml")

    exit_code = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config=None,
            auto_organize=arg_auto,
            no_auto_organize=arg_no_auto,
        )
    )

    assert seen[0] == ("init", expected, "/tmp/default-chemstack.yaml")
    assert exit_code == 23
    assert seen[-1] == ("run", False, "")


def test_write_execution_artifacts_skips_without_job_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = SimpleNamespace(task_id="job-1", queue_id="queue-1", metadata={})
    result = _make_result(tmp_path / "selected.xyz", status="completed", reason="completed")

    monkeypatch.setattr(queue_cmd, "write_state", lambda *args, **kwargs: pytest.fail("write_state should not run"))
    monkeypatch.setattr(queue_cmd, "write_report_json", lambda *args, **kwargs: pytest.fail("write_report_json should not run"))
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


def test_write_running_state_skips_without_job_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = _make_cfg(tmp_path)
    entry = SimpleNamespace(task_id="job-1", metadata={})
    monkeypatch.setattr(queue_cmd, "write_state", lambda *args, **kwargs: pytest.fail("write_state should not run"))
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


def test_terminate_process_uses_fallback_terminate_and_kill(monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr(queue_cmd.os, "killpg", fake_killpg)

    proc = _Process()
    queue_cmd._terminate_process(cast(Any, proc))

    assert killpg_calls == [
        (4321, queue_cmd.signal.SIGTERM),
        (4321, queue_cmd.signal.SIGKILL),
    ]
    assert proc.terminate_called is True
    assert proc.kill_called is True
    assert proc.wait_calls == 2


def test_terminate_process_swallows_terminate_and_kill_exceptions(monkeypatch: pytest.MonkeyPatch) -> None:
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

    monkeypatch.setattr(queue_cmd.os, "killpg", lambda pid, sig: (_ for _ in ()).throw(ProcessLookupError("missing")))

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
        lambda root, limit, *, source, app_name: _fake_reserve_slot(calls, root, limit, source, app_name),
    )

    assert queue_cmd._try_reserve_admission_slot(cfg) == "slot-1"
    assert calls == [
        (str(tmp_path / "resolved-admission"), 5, "chemstack.xtb.queue_worker", "xtb_auto")
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
    monkeypatch.setattr(
        queue_cmd,
        "activate_reserved_slot",
        lambda root, token, *, work_dir, queue_id, source: (
            activated.append((root, token, str(work_dir), queue_id, source)) or object()
        ),
    )
    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((root, token)))
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
        auto_organize=False,
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
