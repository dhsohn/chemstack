from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.queue import dequeue_next, enqueue, list_queue, request_cancel
from chemstack.core.queue.types import QueueStatus

from chemstack.crest import queue_runtime as queue_cmd


@pytest.fixture
def command_env(tmp_path: Path) -> SimpleNamespace:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "01_crest"
    organized_root = allowed_root
    admission_root = tmp_path / "admission_root"
    allowed_root.mkdir(parents=True)
    admission_root.mkdir()

    config_path = tmp_path / "chemstack.yaml"
    config_path.write_text(
        "\n".join(
            [
                "scheduler:",
                "  max_active_simulations: 2",
                f"  admission_root: {admission_root}",
                "workflow:",
                f"  root: {workflow_root}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    return SimpleNamespace(
        allowed_root=allowed_root,
        organized_root=organized_root,
        admission_root=admission_root,
        config_path=config_path,
        tmp_path=tmp_path,
    )


def _enqueue_job(
    env: SimpleNamespace,
    *,
    task_id: str,
    priority: int = 10,
    with_job_dir: bool = True,
) -> Any:
    metadata: dict[str, str] = {}
    if with_job_dir:
        job_dir = env.tmp_path / "jobs" / task_id
        job_dir.mkdir(parents=True)
        metadata["job_dir"] = str(job_dir)
    return enqueue(
        env.allowed_root,
        app_name="chemstack_crest",
        task_id=task_id,
        task_kind="conformer_search",
        engine="crest",
        priority=priority,
        metadata=metadata,
    )


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


def test_cmd_queue_cancel_requires_target(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = queue_cmd.cmd_queue_cancel(
        SimpleNamespace(config=str(command_env.config_path), target="  ")
    )

    assert result == 1
    assert capsys.readouterr().out == "error: queue cancel requires a queue_id or job_id\n"


def test_cmd_queue_cancel_reports_missing_target(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = queue_cmd.cmd_queue_cancel(
        SimpleNamespace(config=str(command_env.config_path), target="job-missing")
    )

    assert result == 1
    assert capsys.readouterr().out == "error: queue target not found: job-missing\n"


def test_cmd_queue_cancel_marks_running_entry_cancel_requested_by_job_id(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entry = _enqueue_job(command_env, task_id="job-running")
    dequeue_next(command_env.allowed_root)

    result = queue_cmd.cmd_queue_cancel(
        SimpleNamespace(config=str(command_env.config_path), target="job-running")
    )

    assert result == 0
    updated = list_queue(command_env.allowed_root)[0]
    assert updated.queue_id == entry.queue_id
    assert updated.status == QueueStatus.RUNNING
    assert updated.cancel_requested is True

    output = capsys.readouterr().out
    assert "status: cancel_requested" in output
    assert f"queue_id: {entry.queue_id}" in output
    assert "job_id: job-running" in output


def test_cmd_queue_cancel_rejects_terminal_entry(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    entry = _enqueue_job(command_env, task_id="job-cancelled")
    request_cancel(command_env.allowed_root, entry.queue_id)

    result = queue_cmd.cmd_queue_cancel(
        SimpleNamespace(config=str(command_env.config_path), target=entry.queue_id)
    )

    assert result == 1
    assert capsys.readouterr().out == f"error: queue target already terminal: {entry.queue_id}\n"


def test_cmd_queue_worker_constructs_crest_worker_without_organize_flags(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            max_concurrent=2,
        ),
    )
    seen: list[tuple[object, str, int]] = []

    monkeypatch.setattr(queue_cmd, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(queue_cmd, "read_worker_pid", lambda allowed_root: None)

    class FakeWorker:
        def __init__(
            self,
            cfg_obj: object,
            config_path: str,
            *,
            max_concurrent: int,
        ) -> None:
            seen.append((cfg_obj, config_path, max_concurrent))

        def run(self) -> int:
            return 0

    monkeypatch.setattr(queue_cmd, "QueueWorker", FakeWorker)

    result = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config="ignored",
        )
    )

    assert result == 0
    assert seen == [(cfg, "ignored", 2)]
    assert capsys.readouterr().out == ""


def test_process_one_returns_blocked_when_no_admission_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root="ignored"))

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda cfg_obj: None)

    assert queue_cmd._process_one(cfg) == "blocked"


def test_process_one_returns_idle_and_releases_reserved_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(tmp_path / "allowed"),
            admission_root="",
            resolved_admission_root=None,
        )
    )
    released: list[tuple[str, str | None]] = []

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda cfg_obj: "slot-1")
    monkeypatch.setattr(queue_cmd, "dequeue_next", lambda root: None)
    monkeypatch.setattr(queue_cmd, "release_slot", lambda root, token: released.append((root, token)))

    assert queue_cmd._process_one(cfg) == "idle"
    assert released == [(cfg.runtime.allowed_root, "slot-1")]


def test_cmd_queue_worker_runs_pool_worker_when_not_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            max_concurrent=3,
        )
    )
    constructed: list[tuple[object, str, int]] = []
    run_calls: list[bool] = []

    monkeypatch.setattr(queue_cmd, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(queue_cmd, "read_worker_pid", lambda allowed_root: None)

    class FakeWorker:
        def __init__(self, cfg_obj: object, config_path: str, *, max_concurrent: int) -> None:
            constructed.append((cfg_obj, config_path, max_concurrent))

        def run(self) -> int:
            run_calls.append(True)
            return 17

    monkeypatch.setattr(queue_cmd, "QueueWorker", FakeWorker)

    result = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config="ignored",
        )
    )

    assert result == 17
    assert constructed == [(cfg, "ignored", 3)]
    assert run_calls == [True]
