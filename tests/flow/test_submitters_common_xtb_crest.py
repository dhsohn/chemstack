# ruff: noqa: E402

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from chemstack.flow.submitters import common, crest_auto, xtb_auto


def _completed_process(
    *,
    args: Any,
    returncode: int,
    stdout: str,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=args, returncode=returncode, stdout=stdout, stderr=stderr)


def test_parse_key_value_lines_ignores_invalid_lines_and_keeps_last_value() -> None:
    parsed = common.parse_key_value_lines(
        "\n".join(
            [
                "status: queued",
                "job_id: job-1",
                "no separator here",
                " : ignored",
                "detail: part one: part two",
                "status: updated",
            ]
        )
    )

    assert parsed == {
        "status": "updated",
        "job_id": "job-1",
        "detail": "part one: part two",
    }


def test_queue_submission_status_treats_admission_wait_as_blocked() -> None:
    status, reason = common.queue_submission_status(
        returncode=1,
        parsed_stdout={"status": "waiting_for_slot"},
        stdout="status: waiting_for_slot\n",
        stderr="Admission limit reached",
    )

    assert status == "blocked"
    assert reason == "waiting_for_slot"


def test_sibling_app_command_without_repo_root_uses_module_execution() -> None:
    argv, cwd, env = common.sibling_app_command(
        executable="xtb_auto",
        config_path="/tmp/config.yaml",
        repo_root=None,
        module_name="chemstack.xtb._internal_cli",
        tail_argv=["run-dir", "/tmp/job"],
    )

    assert argv == [
        sys.executable,
        "-m",
        "chemstack.xtb._internal_cli",
        "--config",
        "/tmp/config.yaml",
        "run-dir",
        "/tmp/job",
    ]
    assert cwd is None
    assert env is None


def test_sibling_app_command_with_repo_root_uses_module_execution_and_prepends_pythonpath(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.setenv("PYTHONPATH", "/existing/site-packages")

    argv, cwd, env = common.sibling_app_command(
        executable="ignored",
        config_path="/tmp/config.yaml",
        repo_root=str(repo_root),
        module_name="chemstack.xtb._internal_cli",
        tail_argv=["queue", "cancel", "job-1"],
    )

    assert argv == [
        sys.executable,
        "-m",
        "chemstack.xtb._internal_cli",
        "--config",
        "/tmp/config.yaml",
        "queue",
        "cancel",
        "job-1",
    ]
    assert cwd == str(repo_root.resolve())
    assert env is not None
    assert env["PYTHONPATH"] == f"{repo_root.resolve()}:/existing/site-packages"


def test_run_sibling_app_forwards_command_to_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    expected_result = _completed_process(args=["cmd"], returncode=0, stdout="ok")

    def fake_sibling_app_command(**kwargs: Any) -> tuple[list[str], str | None, dict[str, str] | None]:
        captured["command_kwargs"] = kwargs
        return ["cmd", "--flag"], "/tmp/work", {"PYTHONPATH": "/tmp/work"}

    def fake_run(
        argv: list[str],
        *,
        cwd: str | None,
        env: dict[str, str] | None,
        capture_output: bool,
        text: bool,
        check: bool,
        timeout: float | None,
    ) -> subprocess.CompletedProcess[str]:
        captured["run_kwargs"] = {
            "argv": argv,
            "cwd": cwd,
            "env": env,
            "capture_output": capture_output,
            "text": text,
            "check": check,
            "timeout": timeout,
        }
        return expected_result

    monkeypatch.setattr(common, "sibling_app_command", fake_sibling_app_command)
    monkeypatch.setattr(common.subprocess, "run", fake_run)

    result = common.run_sibling_app(
        executable="xtb_auto",
        config_path="/tmp/config.yaml",
        repo_root="/tmp/repo",
        module_name="chemstack.xtb._internal_cli",
        tail_argv=["run-dir", "/tmp/job"],
        timeout_seconds=7.5,
    )

    assert result is expected_result
    assert captured["command_kwargs"] == {
        "executable": "xtb_auto",
        "config_path": "/tmp/config.yaml",
        "repo_root": "/tmp/repo",
        "module_name": "chemstack.xtb._internal_cli",
        "tail_argv": ["run-dir", "/tmp/job"],
    }
    assert captured["run_kwargs"] == {
        "argv": ["cmd", "--flag"],
        "cwd": "/tmp/work",
        "env": {"PYTHONPATH": "/tmp/work"},
        "capture_output": True,
        "text": True,
        "check": False,
        "timeout": 7.5,
    }


def test_sibling_allowed_root_reads_runtime_allowed_root(tmp_path: Path) -> None:
    allowed_root = tmp_path / "allowed"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        f"runtime:\n  allowed_root: {allowed_root}\n",
        encoding="utf-8",
    )

    assert common.sibling_allowed_root(str(config_path)) == allowed_root.resolve()


def test_sibling_allowed_root_requires_runtime_allowed_root(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("runtime: {}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing runtime.allowed_root"):
        common.sibling_allowed_root(str(config_path))


def test_sibling_runtime_paths_requires_workflow_root_for_xtb(tmp_path: Path) -> None:
    admission_root = tmp_path / "admission"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "scheduler:",
                f"  admission_root: {admission_root}",
                "xtb:",
                "  runtime:",
                "    allowed_root: /tmp/runs",
                "",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"Missing workflow\.root in config"):
        common.sibling_runtime_paths(str(config_path), engine="xtb")


def test_sibling_runtime_paths_requires_runtime_allowed_root(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("runtime:\n  organized_root: /tmp/organized\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Missing runtime.allowed_root"):
        common.sibling_runtime_paths(str(config_path))


def test_sibling_runtime_paths_derives_internal_engine_roots_from_workflow_root(tmp_path: Path) -> None:
    workflow_root = tmp_path / "workflow_root"
    admission_root = tmp_path / "admission"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "scheduler:",
                f"  admission_root: {admission_root}",
                "workflow:",
                f"  root: {workflow_root}",
                "",
            ]
        ),
        encoding="utf-8",
    )

    assert common.sibling_runtime_paths(str(config_path), engine="xtb") == {
        "workflow_root": workflow_root.resolve(),
        "allowed_root": workflow_root.resolve(),
        "organized_root": workflow_root.resolve(),
        "admission_root": admission_root.resolve(),
    }
    assert common.sibling_allowed_root(str(config_path), engine="crest") == workflow_root.resolve()


@pytest.mark.parametrize(
    ("module", "executable", "repo_root", "job_dir", "priority", "stdout", "returncode", "stderr", "expected"),
    [
        (
            xtb_auto,
            "xtb_custom",
            "/repo/xtb",
            "/jobs/xtb-1",
            7,
            "\n".join(
                [
                    "status: queued",
                    "job_id: xtb-job-1",
                    "queue_id: q-xtb-1",
                    "job_dir: /organized/xtb-1",
                    "job_type: path",
                    "reaction_key: rxn-1",
                ]
            ),
            0,
            "",
            {
                "status": "submitted",
                "job_id": "xtb-job-1",
                "queue_id": "q-xtb-1",
                "job_dir": "/organized/xtb-1",
                "job_type": "path",
                "reaction_key": "rxn-1",
                "parsed_status": "queued",
            },
        ),
        (
            crest_auto,
            "crest_custom",
            None,
            "/jobs/crest-1",
            3,
            "status: failed\nqueue_id: q-crest-1",
            1,
            "boom",
            {
                "status": "failed",
                "job_id": "",
                "queue_id": "q-crest-1",
                "job_dir": "/jobs/crest-1",
                "parsed_status": "failed",
            },
        ),
    ],
)
def test_submit_job_dir_maps_success_and_failure(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    executable: str,
    repo_root: str | None,
    job_dir: str,
    priority: int,
    stdout: str,
    returncode: int,
    stderr: str,
    expected: dict[str, str],
) -> None:
    captured: dict[str, Any] = {}
    completed = _completed_process(
        args=[
            "python",
            "-m",
            "chemstack.xtb._internal_cli" if module is xtb_auto else "chemstack.crest._internal_cli",
            "--config",
            "/tmp/config.yaml",
            "run-dir",
            job_dir,
        ],
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )

    def fake_run_sibling_app(**kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["kwargs"] = kwargs
        return completed

    monkeypatch.setattr(module, "run_sibling_app", fake_run_sibling_app)

    result = module.submit_job_dir(
        job_dir=job_dir,
        priority=priority,
        config_path="/tmp/config.yaml",
        executable=executable,
        repo_root=repo_root,
    )

    assert captured["kwargs"] == {
        "executable": executable,
        "config_path": "/tmp/config.yaml",
        "repo_root": repo_root,
        "module_name": "chemstack.xtb._internal_cli" if module is xtb_auto else "chemstack.crest._internal_cli",
        "tail_argv": ["run-dir", job_dir, "--priority", str(priority)],
    }
    assert result["status"] == expected["status"]
    assert result["returncode"] == returncode
    assert result["command_argv"] == completed.args
    assert result["stdout"] == stdout
    assert result["stderr"] == stderr
    assert result["parsed_stdout"]["status"] == expected["parsed_status"]
    assert result["job_id"] == expected["job_id"]
    assert result["queue_id"] == expected["queue_id"]
    assert result["job_dir"] == expected["job_dir"]
    if module is xtb_auto:
        assert result["job_type"] == expected["job_type"]
        assert result["reaction_key"] == expected["reaction_key"]


@pytest.mark.parametrize(
    ("module", "target", "stdout", "returncode", "expected_status", "expected_queue_id", "expected_job_id"),
    [
        (xtb_auto, "xtb-job-1", "status: cancel_requested\nqueue_id: q-1\njob_id: xtb-job-1", 0, "cancel_requested", "q-1", "xtb-job-1"),
        (xtb_auto, "xtb-job-2", "Cancel requested for queue entry q-2", 0, "cancel_requested", "", ""),
        (xtb_auto, "xtb-job-3", "queue_id: q-3", 0, "cancelled", "q-3", ""),
        (xtb_auto, "xtb-job-4", "status: cancelled\njob_id: xtb-job-4", 0, "cancelled", "", "xtb-job-4"),
        (xtb_auto, "xtb-job-5", "status: cancel_requested", 2, "failed", "", ""),
        (crest_auto, "crest-job-1", "status: cancel_requested\nqueue_id: c-1\njob_id: crest-job-1", 0, "cancel_requested", "c-1", "crest-job-1"),
        (crest_auto, "crest-job-2", "Cancel requested for queue entry c-2", 0, "cancel_requested", "", ""),
        (crest_auto, "crest-job-3", "queue_id: c-3", 0, "cancelled", "c-3", ""),
        (crest_auto, "crest-job-4", "status: cancelled\njob_id: crest-job-4", 0, "cancelled", "", "crest-job-4"),
        (crest_auto, "crest-job-5", "status: cancel_requested", 3, "failed", "", ""),
    ],
)
def test_cancel_target_maps_status_from_stdout_and_returncode(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    target: str,
    stdout: str,
    returncode: int,
    expected_status: str,
    expected_queue_id: str,
    expected_job_id: str,
) -> None:
    captured: dict[str, Any] = {}
    completed = _completed_process(
        args=["tool", "--config", "/tmp/config.yaml", "queue", "cancel", target],
        returncode=returncode,
        stdout=stdout,
        stderr="stderr text",
    )

    def fake_run_sibling_app(**kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured["kwargs"] = kwargs
        return completed

    monkeypatch.setattr(module, "run_sibling_app", fake_run_sibling_app)

    result = module.cancel_target(
        target=target,
        config_path="/tmp/config.yaml",
        executable="tool",
        repo_root="/tmp/repo",
    )

    assert captured["kwargs"] == {
        "executable": "tool",
        "config_path": "/tmp/config.yaml",
        "repo_root": "/tmp/repo",
        "module_name": "chemstack.cli" if module is xtb_auto else "chemstack.crest._internal_cli",
        "tail_argv": ["queue", "cancel", target],
        "timeout_seconds": 5.0,
    }
    assert result["status"] == expected_status
    assert result["returncode"] == returncode
    assert result["command_argv"] == completed.args
    assert result["stdout"] == stdout
    assert result["stderr"] == "stderr text"
    assert result["queue_id"] == expected_queue_id
    assert result["job_id"] == expected_job_id


@pytest.mark.parametrize("module", [xtb_auto, crest_auto])
def test_cancel_target_reports_timeout(monkeypatch: pytest.MonkeyPatch, module: Any) -> None:
    def fake_run_sibling_app(**kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=["tool", "queue", "cancel", "job-1"], timeout=5.0, output="slow", stderr="timeout")

    monkeypatch.setattr(module, "run_sibling_app", fake_run_sibling_app)

    result = module.cancel_target(
        target="job-1",
        config_path="/tmp/config.yaml",
        executable="tool",
        repo_root="/tmp/repo",
    )

    assert result["status"] == "failed"
    assert result["reason"] == "cancel_command_timeout"
    assert result["returncode"] == 124
    assert result["stdout"] == "slow"
    assert result["stderr"] == "timeout"
