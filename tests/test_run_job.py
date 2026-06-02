from __future__ import annotations

from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from unittest.mock import MagicMock, patch

from chemstack.core.queue.types import QueueEntry, QueueStatus
from chemstack.orca.orca_runner import OrcaRunner
from chemstack.orca.runtime import worker_job
from chemstack.orca.runtime.worker_job import cmd_run_job, execute_run_job


@patch("chemstack.orca.runtime.worker_job._cmd_run_inp_execute", return_value=7)
def test_execute_run_job_forwards_explicit_execution_identity(mock_execute: MagicMock) -> None:
    rc = execute_run_job(
        "/tmp/config.yaml",
        "/tmp/rxn",
        force=True,
        reservation_token="slot_123",
        admission_app_name="chemstack_orca",
        admission_task_id="task_123",
    )

    assert rc == 7
    args = mock_execute.call_args.args[0]
    assert args.config == "/tmp/config.yaml"
    assert args.reaction_dir == "/tmp/rxn"
    assert args.force is True
    assert mock_execute.call_args.kwargs["reservation_token"] == "slot_123"
    assert mock_execute.call_args.kwargs["admission_app_name"] == "chemstack_orca"
    assert mock_execute.call_args.kwargs["admission_task_id"] == "task_123"


@patch("chemstack.orca.runtime.worker_job.execute_run_job", return_value=3)
def test_cmd_run_job_uses_execute_run_job_helper(mock_execute: MagicMock) -> None:
    rc = cmd_run_job(Namespace(config="/tmp/config.yaml", reaction_dir="/tmp/rxn", force=True))

    assert rc == 3
    mock_execute.assert_called_once_with(
        "/tmp/config.yaml",
        "/tmp/rxn",
        force=True,
        runner_cls=OrcaRunner,
    )


@patch("chemstack.orca.runtime.worker_job.subprocess.Popen")
def test_start_background_run_job_uses_internal_runtime_module(mock_popen: MagicMock) -> None:
    proc = MagicMock()
    mock_popen.return_value = proc

    started = worker_job.start_background_run_job(
        config_path="/tmp/config.yaml",
        reaction_dir="/tmp/rxn",
        force=True,
        admission_token="slot_123",
        admission_app_name="chemstack_orca",
        admission_task_id="task_123",
    )

    assert started is proc
    cmd = mock_popen.call_args.args[0]
    assert cmd[:3] == [worker_job.sys.executable, "-m", "chemstack.orca.runtime.worker_job"]
    assert "--config" in cmd
    assert "--reaction-dir" in cmd
    assert "--force" in cmd
    assert "--admission-token" in cmd
    assert "--admission-app-name" in cmd
    assert "--admission-task-id" in cmd
    assert mock_popen.call_args.kwargs["start_new_session"] is True


@patch("chemstack.orca.runtime.worker_job.subprocess.Popen")
def test_start_background_run_job_omits_blank_optional_identity_flags(mock_popen: MagicMock) -> None:
    mock_popen.return_value = MagicMock()

    worker_job.start_background_run_job(
        config_path="/tmp/config.yaml",
        reaction_dir="/tmp/rxn",
        force=False,
        admission_token=None,
        admission_app_name="",
        admission_task_id=None,
    )

    cmd = mock_popen.call_args.args[0]
    assert "--force" not in cmd
    assert "--admission-token" not in cmd
    assert "--admission-app-name" not in cmd
    assert "--admission-task-id" not in cmd


def test_start_background_run_job_rejects_custom_runner_cls() -> None:
    class CustomRunner(OrcaRunner):
        pass

    with pytest.raises(ValueError, match="default OrcaRunner"):
        worker_job.start_background_run_job(
            config_path="/tmp/config.yaml",
            reaction_dir="/tmp/rxn",
            runner_cls=CustomRunner,
        )


def test_build_worker_child_command_uses_queue_identity(tmp_path: Path) -> None:
    command = worker_job.build_worker_child_command(
        config_path="/tmp/config.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-1",
        admission_token="slot-1",
    )

    assert command[:3] == [worker_job.sys.executable, "-m", worker_job.WORKER_JOB_MODULE]
    assert "--queue-root" in command
    assert str(tmp_path / "queue") in command
    assert "--queue-id" in command
    assert "queue-1" in command
    assert "--admission-token" in command
    assert "slot-1" in command
    assert "--admission-root" not in command


def test_run_worker_child_job_loads_queue_entry_and_preserves_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(tmp_path / "queue"),
            admission_root=str(tmp_path / "admission"),
            admission_limit=1,
            max_concurrent=1,
        )
    )
    entry = QueueEntry(
        queue_id="queue-1",
        app_name="chemstack_orca",
        task_id="task-1",
        task_kind="orca_run_inp",
        engine="orca",
        status=QueueStatus.RUNNING,
        metadata={"reaction_dir": str(tmp_path / "rxn"), "force": True},
    )
    calls: dict[str, Any] = {}
    released: list[tuple[str, str]] = []

    monkeypatch.setattr(worker_job, "load_config", lambda _path: cfg)
    monkeypatch.setattr(worker_job, "_queue_entry_by_id", lambda _root, _queue_id: entry)
    monkeypatch.setattr(
        worker_job,
        "release_slot",
        lambda root, token: released.append((str(root), token)),
    )

    def fake_execute_run_job(*args: Any, **kwargs: Any) -> int:
        calls["args"] = args
        calls["kwargs"] = kwargs
        return 5

    monkeypatch.setattr(worker_job, "execute_run_job", fake_execute_run_job)

    rc = worker_job.run_worker_child_job(
        config_path="/tmp/config.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-1",
        admission_token="slot-1",
    )

    assert rc == 5
    assert calls["args"] == ("/tmp/config.yaml", str(tmp_path / "rxn"))
    assert calls["kwargs"] == {
        "force": True,
        "reservation_token": "slot-1",
        "admission_app_name": "chemstack_orca",
        "admission_task_id": "task-1",
    }
    assert released == [(str(tmp_path / "admission"), "slot-1")]


@patch("chemstack.orca.runtime.worker_job.execute_run_job", return_value=0)
def test_worker_job_main_delegates_to_execute_run_job(mock_execute: MagicMock) -> None:
    rc = worker_job.main(
        [
            "--config",
            "/tmp/config.yaml",
            "--reaction-dir",
            "/tmp/rxn",
            "--force",
            "--admission-token",
            "slot_123",
            "--admission-app-name",
            "chemstack_orca",
            "--admission-task-id",
            "task_123",
        ]
    )

    assert rc == 0
    mock_execute.assert_called_once_with(
        "/tmp/config.yaml",
        "/tmp/rxn",
        force=True,
        reservation_token="slot_123",
        admission_app_name="chemstack_orca",
        admission_task_id="task_123",
    )


@patch("chemstack.orca.runtime.worker_job.run_worker_child_job", return_value=6)
def test_worker_job_main_delegates_queue_mode_to_worker_child(mock_run_child: MagicMock) -> None:
    rc = worker_job.main(
        [
            "--config",
            "/tmp/config.yaml",
            "--queue-root",
            "/tmp/queue",
            "--queue-id",
            "queue-1",
            "--admission-token",
            "slot-1",
        ]
    )

    assert rc == 6
    mock_run_child.assert_called_once_with(
        config_path="/tmp/config.yaml",
        queue_root="/tmp/queue",
        queue_id="queue-1",
        admission_token="slot-1",
    )
