from __future__ import annotations

import argparse
import json
import signal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack import cli as unified_cli


@pytest.fixture(autouse=True)
def _isolate_shared_config_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    def _explicit_shared_config_path(explicit: str | None) -> str | None:
        if not explicit:
            return None
        return str(Path(explicit).expanduser().resolve())

    monkeypatch.setattr(unified_cli, "_discover_shared_config_path", _explicit_shared_config_path)
    monkeypatch.setattr(unified_cli, "shared_workflow_root_from_config", lambda config_path: None)


class _FakeWorkerProcess:
    def __init__(self, poll_values: list[int | None]) -> None:
        self._poll_values = list(poll_values)
        self._terminal_returncode: int | None = None
        self.terminate_calls = 0
        self.kill_calls = 0

    def poll(self) -> int | None:
        if self._terminal_returncode is not None:
            return self._terminal_returncode
        if self._poll_values:
            value = self._poll_values.pop(0)
            if value is not None:
                self._terminal_returncode = value
            return value
        return None

    def terminate(self) -> None:
        self.terminate_calls += 1
        self._poll_values.clear()
        self._terminal_returncode = -15

    def kill(self) -> None:
        self.kill_calls += 1
        self._poll_values.clear()
        self._terminal_returncode = -9


def test_build_worker_specs_defaults_to_engine_workers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )

    def fake_sibling_app_command(
        *,
        executable: str,
        config_path: str,
        repo_root: str | None,
        module_name: str,
        tail_argv: list[str],
    ) -> tuple[list[str], str | None, dict[str, str] | None]:
        del executable, repo_root
        return (["python", "-m", module_name, "--config", config_path, *tail_argv], None, {})

    monkeypatch.setattr(unified_cli, "sibling_app_command", fake_sibling_app_command)

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=None, workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["orca"]
    assert specs[0].argv[-2:] == ("queue", "worker")
    assert str(specs[0].argv[2]) == "chemstack.orca._internal_cli"
    assert specs[0].env is not None
    assert specs[0].env[unified_cli._DIRECT_ENGINE_WORKER_ENV_VAR] == "1"


def test_build_worker_specs_defaults_to_all_workers_when_workflow_root_is_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )
    monkeypatch.setattr(
        unified_cli, "shared_workflow_root_from_config", lambda config_path: "/tmp/workflows"
    )

    def fake_sibling_app_command(
        *,
        executable: str,
        config_path: str,
        repo_root: str | None,
        module_name: str,
        tail_argv: list[str],
    ) -> tuple[list[str], str | None, dict[str, str] | None]:
        del executable, repo_root
        return (["python", "-m", module_name, "--config", config_path, *tail_argv], None, {})

    monkeypatch.setattr(unified_cli, "sibling_app_command", fake_sibling_app_command)

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=None, workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["orca", "crest", "xtb", "workflow"]
    assert str(specs[1].argv[2]) == "chemstack.crest._internal_cli"
    assert str(specs[2].argv[2]) == "chemstack.xtb._internal_cli"
    assert specs[-1].argv[1:5] == (
        "-m",
        "chemstack.flow.cli",
        "workflow",
        "worker",
    )
    assert "--workflow-root" in specs[-1].argv
    assert "/tmp/workflows" in specs[-1].argv


def test_build_worker_specs_requires_workflow_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )
    with pytest.raises(
        ValueError, match="workflow worker requires workflow.root in chemstack.yaml"
    ):
        unified_cli._build_worker_specs(
            SimpleNamespace(app=["workflow"], workflow_root=None, chemstack_config=None)
        )


def test_build_worker_specs_explicit_workflow_app_uses_configured_workflow_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )
    monkeypatch.setattr(
        unified_cli, "shared_workflow_root_from_config", lambda config_path: "/tmp/workflows"
    )

    specs = unified_cli._build_worker_specs(
        SimpleNamespace(app=["workflow"], workflow_root=None, chemstack_config=None)
    )

    assert [spec.app for spec in specs] == ["crest", "xtb", "workflow"]
    assert str(specs[0].argv[2]) == "chemstack.crest._internal_cli"
    assert str(specs[1].argv[2]) == "chemstack.xtb._internal_cli"
    assert "--workflow-root" in specs[2].argv
    assert "/tmp/workflows" in specs[2].argv


def test_workflow_root_for_args_uses_shared_config(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str | None] = []

    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )

    def _shared_workflow_root(config_path: str | None) -> str:
        seen.append(config_path)
        return "/tmp/from-config-workflows"

    monkeypatch.setattr(
        unified_cli,
        "shared_workflow_root_from_config",
        _shared_workflow_root,
    )

    discovered = unified_cli._workflow_root_for_args(
        SimpleNamespace(
            workflow_root=None,
            chemstack_config=None,
            config=None,
            global_config=None,
        )
    )

    assert discovered == "/tmp/from-config-workflows"
    assert seen == ["/tmp/chemstack.yaml"]


def test_engine_config_for_command_uses_discovered_shared_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )

    discovered = unified_cli._engine_config_for_command(
        argparse.Namespace(
            chemstack_config=None,
            config=None,
            global_config=None,
        )
    )

    assert discovered == str(Path("/tmp/chemstack.yaml").resolve())


def test_cmd_orca_run_dir_uses_discovered_shared_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = tmp_path / "orca_job"
    target.mkdir()
    (target / "job.inp").write_text("! Opt\n", encoding="utf-8")
    captured: list[tuple[str | None, str]] = []

    monkeypatch.setattr(unified_cli, "_configure_orca_logging", lambda args: None)
    monkeypatch.setattr(
        unified_cli, "_discover_shared_config_path", lambda explicit: "/tmp/chemstack.yaml"
    )

    import chemstack.orca.commands.run_inp as run_inp_cmd

    def _fake_cmd_run_inp(args: argparse.Namespace) -> int:
        captured.append((getattr(args, "config", None), getattr(args, "path", "")))
        return 31

    monkeypatch.setattr(
        run_inp_cmd,
        "cmd_run_inp",
        _fake_cmd_run_inp,
    )

    result = unified_cli.cmd_orca_run_dir(
        argparse.Namespace(
            path=str(target),
            chemstack_config=None,
            config=None,
            global_config=None,
            verbose=False,
            log_file=None,
        )
    )

    assert result == 31
    assert captured == [(str(Path("/tmp/chemstack.yaml").resolve()), str(target))]


def test_cmd_queue_worker_delegates_to_supervisor(monkeypatch: pytest.MonkeyPatch) -> None:
    specs = [
        unified_cli.WorkerSpec(
            app="orca", argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker")
        )
    ]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)
    monkeypatch.setattr(
        unified_cli, "_run_worker_supervisor", lambda built_specs: 0 if built_specs == specs else 1
    )

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(app=["orca"], workflow_root=None, chemstack_config=None, json=False)
    )

    assert result == 0


def test_cmd_queue_worker_reports_existing_chemstack_orca_worker_conflict(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    specs = [
        unified_cli.WorkerSpec(
            app="orca", argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker")
        )
    ]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)
    monkeypatch.setattr(
        unified_cli,
        "_detect_existing_orca_worker_conflict",
        lambda built_specs, args: unified_cli._ExistingWorkerConflict(
            app="orca",
            pid=3589996,
            allowed_root="/home/user/orca_runs",
            source="chemstack",
            command="/home/user/chemstack/.venv/bin/python -m chemstack.orca.cli --config /tmp/chemstack.yaml queue worker",
        ),
    )
    monkeypatch.setattr(unified_cli, "_run_worker_supervisor", lambda built_specs: 99)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(
            app=["orca"], workflow_root=None, chemstack_config="/tmp/chemstack.yaml", json=False
        )
    )

    assert result == 1
    out = capsys.readouterr().out
    assert "existing ORCA queue worker detected" in out
    assert "source: chemstack queue worker" in out
    assert "Stop the existing queue-worker service before starting another worker." in out


def test_run_worker_supervisor_keeps_siblings_running_after_clean_exit(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([0]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([None]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            installed_handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 0
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 1
    assert popen_calls == 3
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 0" in out
    assert "restarting worker[workflow]: workflow worker" in out


def test_run_worker_supervisor_restarts_workers_after_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([2]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([None]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            installed_handlers[signal.SIGTERM](signal.SIGTERM, None)

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 0
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 1
    assert popen_calls == 3
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 2" in out
    assert "restarting worker[workflow]: workflow worker" in out


def test_run_worker_supervisor_stops_after_repeated_startup_failures(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    processes = [
        _FakeWorkerProcess([2]),
        _FakeWorkerProcess([None]),
        _FakeWorkerProcess([2]),
    ]
    popen_calls = 0
    installed_handlers: dict[int, Any] = {}
    sleep_calls = 0

    def _fake_popen(*args: Any, **kwargs: Any) -> _FakeWorkerProcess:
        del args, kwargs
        nonlocal popen_calls
        process = processes[popen_calls]
        popen_calls += 1
        return process

    def _fake_signal(sig: int, handler: Any) -> None:
        installed_handlers[sig] = handler

    def _fake_sleep(seconds: float) -> None:
        del seconds
        nonlocal sleep_calls
        sleep_calls += 1

    monkeypatch.setattr(unified_cli.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(unified_cli.signal, "getsignal", lambda sig: None)
    monkeypatch.setattr(unified_cli.signal, "signal", _fake_signal)
    monkeypatch.setattr(unified_cli.time, "sleep", _fake_sleep)

    result = unified_cli._run_worker_supervisor(
        [
            unified_cli.WorkerSpec(app="workflow", argv=("workflow", "worker")),
            unified_cli.WorkerSpec(app="orca", argv=("orca", "worker")),
        ]
    )

    assert result == 2
    assert processes[0].terminate_calls == 0
    assert processes[1].terminate_calls == 1
    assert processes[2].terminate_calls == 0
    assert popen_calls == 3
    assert sleep_calls == 1
    out = capsys.readouterr().out
    assert "worker[workflow] exited with code 2" in out
    assert (
        "worker[workflow] failed repeatedly during startup; stopping supervisor to avoid a restart loop."
        in out
    )
    assert "restarting worker[workflow]: workflow worker" in out


def test_cmd_queue_worker_json_outputs_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    specs = [
        unified_cli.WorkerSpec(
            app="workflow",
            argv=(
                "python",
                "-m",
                "chemstack.flow.cli",
                "workflow",
                "worker",
                "--workflow-root",
                "/tmp/workflows",
            ),
        )
    ]
    monkeypatch.setattr(unified_cli, "_build_worker_specs", lambda args: specs)

    result = unified_cli.cmd_queue_worker(
        SimpleNamespace(
            app=["workflow"], workflow_root="/tmp/workflows", chemstack_config=None, json=True
        )
    )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["workers"][0]["app"] == "workflow"
    assert payload["workers"][0]["argv"][2] == "chemstack.flow.cli"


def test_worker_spec_to_dict_redacts_unrelated_environment_keys() -> None:
    spec = unified_cli.WorkerSpec(
        app="orca",
        argv=("python", "-m", "chemstack.orca._internal_cli", "queue", "worker"),
        cwd="/tmp/chemstack",
        env={
            "PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack",
            "SECRET_TOKEN": "do-not-print",
        },
    )

    payload = spec.to_dict()

    assert payload["env"] == {"PYTHONPATH": "/tmp/chemstack/src:/tmp/chemstack"}
