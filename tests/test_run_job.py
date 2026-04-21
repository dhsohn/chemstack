from __future__ import annotations

from argparse import Namespace
import pytest
from unittest.mock import MagicMock, patch

from chemstack.orca.commands.run_job import cmd_run_job, execute_run_job
from chemstack.orca.orca_runner import OrcaRunner
from chemstack.orca.runtime import worker_job


@patch("chemstack.orca.commands.run_job._cmd_run_inp_execute", return_value=7)
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


@patch("chemstack.orca.commands.run_job.execute_run_job", return_value=3)
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


@patch("chemstack.orca.commands.run_job.execute_run_job", return_value=0)
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
            "orca_auto",
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
