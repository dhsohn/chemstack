from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.queue import dequeue_next, enqueue, list_queue, request_cancel
from chemstack.core.queue.types import QueueStatus

from chemstack.crest.commands import list_jobs, queue as queue_cmd


@pytest.fixture
def command_env(tmp_path: Path) -> SimpleNamespace:
    workflow_root = tmp_path / "workflow_root"
    allowed_root = workflow_root / "wf_001" / "internal" / "crest" / "runs"
    organized_root = workflow_root / "wf_001" / "internal" / "crest" / "outputs"
    admission_root = tmp_path / "admission_root"
    allowed_root.mkdir(parents=True)
    organized_root.mkdir(parents=True)
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
                "behavior:",
                "  auto_organize_on_terminal: false",
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
        app_name="crest_auto",
        task_id=task_id,
        task_kind="conformer_search",
        engine="crest",
        priority=priority,
        metadata=metadata,
    )


@pytest.mark.parametrize(
    ("cancel_requested", "status_value", "expected"),
    [
        (True, "running", "cancel_requested"),
        (False, "pending", "pending"),
    ],
)
def test_list_display_status(cancel_requested: bool, status_value: str, expected: str) -> None:
    entry = SimpleNamespace(
        cancel_requested=cancel_requested,
        status=SimpleNamespace(value=status_value),
    )

    assert list_jobs._display_status(entry) == expected


def test_cmd_list_prints_no_jobs_message(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = list_jobs.cmd_list(SimpleNamespace(config=str(command_env.config_path)))

    assert result == 0
    assert capsys.readouterr().out == "No CREST jobs found.\n"


def test_cmd_list_prints_queue_rows_with_status_and_directory_names(
    command_env: SimpleNamespace,
    capsys: pytest.CaptureFixture[str],
) -> None:
    running = _enqueue_job(command_env, task_id="job-running")
    dequeue_next(command_env.allowed_root)
    request_cancel(command_env.allowed_root, running.queue_id)
    pending = _enqueue_job(command_env, task_id="job-pending", with_job_dir=False)

    result = list_jobs.cmd_list(SimpleNamespace(config=str(command_env.config_path)))

    assert result == 0
    output_lines = capsys.readouterr().out.splitlines()
    assert output_lines[0] == "CREST queue: 2 entries"
    assert output_lines[2].startswith("QUEUE ID")

    running_line = next(line for line in output_lines if running.queue_id in line)
    pending_line = next(line for line in output_lines if pending.queue_id in line)
    assert "cancel_requested" in running_line
    assert running_line.rstrip().endswith("job-running")
    assert "pending" in pending_line
    assert pending_line.rstrip().endswith("-")


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


@pytest.mark.parametrize(
    ("config_default", "auto_flag", "no_auto_flag", "expected"),
    [
        (False, False, False, False),
        (False, True, False, True),
        (True, False, True, False),
        (False, True, True, True),
    ],
)
def test_cmd_queue_worker_passes_expected_auto_organize_to_pool_worker(
    config_default: bool,
    auto_flag: bool,
    no_auto_flag: bool,
    expected: bool,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            max_concurrent=2,
        ),
        behavior=SimpleNamespace(auto_organize_on_terminal=config_default),
    )
    seen: list[tuple[object, str, int, bool]] = []

    monkeypatch.setattr(queue_cmd, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(queue_cmd, "read_worker_pid", lambda allowed_root: None)

    class FakeWorker:
        def __init__(
            self,
            cfg_obj: object,
            config_path: str,
            *,
            max_concurrent: int,
            auto_organize: bool,
        ) -> None:
            seen.append((cfg_obj, config_path, max_concurrent, auto_organize))

        def run(self) -> int:
            return 0

    monkeypatch.setattr(queue_cmd, "QueueWorker", FakeWorker)

    result = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config="ignored",
            auto_organize=auto_flag,
            no_auto_organize=no_auto_flag,
        )
    )

    assert result == 0
    assert seen == [(cfg, "ignored", 2, expected)]
    assert capsys.readouterr().out == ""


def test_process_one_returns_blocked_when_no_admission_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root="ignored"))

    monkeypatch.setattr(queue_cmd, "_try_reserve_admission_slot", lambda cfg_obj: None)

    assert queue_cmd._process_one(cfg, auto_organize=False) == "blocked"


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

    assert queue_cmd._process_one(cfg, auto_organize=False) == "idle"
    assert released == [(cfg.runtime.allowed_root, "slot-1")]


def test_cmd_queue_worker_runs_pool_worker_when_not_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            max_concurrent=3,
        ),
        behavior=SimpleNamespace(auto_organize_on_terminal=False),
    )
    constructed: list[tuple[object, str, int, bool]] = []
    run_calls: list[bool] = []

    monkeypatch.setattr(queue_cmd, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(queue_cmd, "read_worker_pid", lambda allowed_root: None)

    class FakeWorker:
        def __init__(self, cfg_obj: object, config_path: str, *, max_concurrent: int, auto_organize: bool) -> None:
            constructed.append((cfg_obj, config_path, max_concurrent, auto_organize))

        def run(self) -> int:
            run_calls.append(True)
            return 17

    monkeypatch.setattr(queue_cmd, "QueueWorker", FakeWorker)

    result = queue_cmd.cmd_queue_worker(
        SimpleNamespace(
            config="ignored",
            auto_organize=False,
            no_auto_organize=False,
        )
    )

    assert result == 17
    assert constructed == [(cfg, "ignored", 3, False)]
    assert run_calls == [True]
