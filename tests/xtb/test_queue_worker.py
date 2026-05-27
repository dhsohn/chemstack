from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from chemstack.core.commands import queue as shared_queue_cmd
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
    assert shared_queue_cmd.display_status(entry) == expected


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
    result = _make_result(
        selected_xyz, status="completed", reason="xtb_ok", candidate_paths=(str(selected_xyz),)
    )

    completed_calls: list[tuple[object, object, object | None]] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda _cfg, *, job_dir, selected_input_xyz: SimpleNamespace(
            process=SimpleNamespace(poll=lambda: 0)
        ),
    )
    monkeypatch.setattr(queue_cmd, "finalize_xtb_job", lambda running, **kwargs: result)
    monkeypatch.setattr(
        queue_cmd,
        "mark_completed",
        lambda root, queue_id, metadata_update=None: completed_calls.append(
            (root, queue_id, metadata_update)
        ),
    )
    monkeypatch.setattr(
        queue_cmd, "mark_failed", lambda *args, **kwargs: pytest.fail("unexpected failure mark")
    )
    monkeypatch.setattr(
        queue_cmd, "mark_cancelled", lambda *args, **kwargs: pytest.fail("unexpected cancel mark")
    )
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry
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

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(
        queue_cmd,
        "mark_completed",
        lambda *args, **kwargs: pytest.fail("unexpected completed mark"),
    )
    monkeypatch.setattr(
        queue_cmd, "mark_cancelled", lambda *args, **kwargs: pytest.fail("unexpected cancel mark")
    )
    monkeypatch.setattr(
        queue_cmd,
        "mark_failed",
        lambda root, queue_id, error, metadata_update=None: failed_calls.append(
            (root, queue_id, error, metadata_update)
        ),
    )
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry
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

    def fake_finalize_xtb_job(
        running: object, forced_status: object = None, forced_reason: object = None
    ) -> queue_cmd.XtbRunResult:
        finalize_calls.append((forced_status, forced_reason))
        return result

    monkeypatch.setattr(queue_cmd, "finalize_xtb_job", fake_finalize_xtb_job)
    monkeypatch.setattr(
        queue_cmd,
        "mark_completed",
        lambda *args, **kwargs: pytest.fail("unexpected completed mark"),
    )
    monkeypatch.setattr(
        queue_cmd, "mark_failed", lambda *args, **kwargs: pytest.fail("unexpected failed mark")
    )
    monkeypatch.setattr(
        queue_cmd,
        "mark_cancelled",
        lambda root, queue_id, error, metadata_update=None: cancelled_calls.append(
            (root, queue_id, error, metadata_update)
        ),
    )
    monkeypatch.setattr(queue_cmd, "notify_job_started", lambda *args, **kwargs: True)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", lambda *args, **kwargs: True)

    cancel_checks = iter([False, True])

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
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


def test_execute_queue_entry_cancels_before_start_and_updates_terminal_metadata(
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
        input_summary={"candidate_paths": [str(selected_xyz)]},
    )

    events: list[str] = []
    cancelled_calls: list[tuple[object, object, object, object | None]] = []

    monkeypatch.setattr(
        queue_cmd,
        "start_xtb_job",
        lambda *args, **kwargs: pytest.fail("start_xtb_job should not run"),
    )
    monkeypatch.setattr(
        queue_cmd,
        "finalize_xtb_job",
        lambda *args, **kwargs: pytest.fail("finalize_xtb_job should not run"),
    )
    monkeypatch.setattr(
        queue_cmd,
        "run_xtb_ranking_job",
        lambda *args, **kwargs: pytest.fail("ranking runner should not run"),
    )
    monkeypatch.setattr(
        queue_cmd,
        "mark_completed",
        lambda *args, **kwargs: pytest.fail("unexpected completed mark"),
    )
    monkeypatch.setattr(
        queue_cmd, "mark_failed", lambda *args, **kwargs: pytest.fail("unexpected failed mark")
    )
    monkeypatch.setattr(
        queue_cmd,
        "mark_cancelled",
        lambda root, queue_id, error, metadata_update=None: cancelled_calls.append(
            (root, queue_id, error, metadata_update)
        ),
    )

    def fake_notify_job_started(*args: object, **kwargs: object) -> bool:
        events.append("notify_started")
        return True

    def fake_notify_job_finished(*args: object, **kwargs: object) -> bool:
        events.append(f"notify_finished:{kwargs['status']}")
        return True

    monkeypatch.setattr(queue_cmd, "notify_job_started", fake_notify_job_started)
    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_job_finished)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry,
        should_cancel=lambda: True,
    )

    assert outcome.result.status == "cancelled"
    assert outcome.result.reason == "cancel_requested"
    assert events == ["notify_started", "notify_finished:cancelled"]
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
    assert report["candidate_count"] == 0


def test_execute_queue_entry_processes_ranking_job_without_auto_organizing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _make_cfg(tmp_path)
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

    def fake_notify_job_finished(cfg_obj: object, **kwargs: object) -> bool:
        finished_calls.append(kwargs)
        return True

    monkeypatch.setattr(queue_cmd, "notify_job_finished", fake_notify_job_finished)

    outcome = queue_cmd._execute_queue_entry(
        cfg,
        queue_root=queue_root,
        entry=entry
    )

    assert outcome.result.status == "completed"
    assert outcome.organized_output_dir == ""
    assert finished_calls and finished_calls[0]["organized_output_dir"] is None
