from __future__ import annotations

import inspect
from pathlib import Path
from typing import Any

from chemstack.core.engines.definitions import (
    EngineDefinition,
    EngineRunnerCallbacks,
)
from chemstack.core.engines.queue_worker import run_engine_queue_worker
from chemstack.core.engines.worker_child import run_engine_worker_child_job


def _definition(**overrides: Any) -> EngineDefinition:
    values: dict[str, Any] = {
        "engine": "demo",
        "load_config": lambda path: path,
        "run_worker_child_job": lambda **_kwargs: 0,
        "queue_worker_module": "chemstack.demo.queue",
        "worker_pid_file_name": "demo_worker.pid",
        "build_worker_child_command": lambda **_kwargs: ["demo-child"],
    }
    values.update(overrides)
    return EngineDefinition(**values)


def test_engine_queue_worker_dispatches_definition_runner(monkeypatch: Any) -> None:
    calls: list[list[str]] = []

    def _runner(argv: list[str]) -> int:
        calls.append(argv)
        return 7

    definition = _definition(queue_worker_runner=_runner)

    from chemstack.core.engines import registry

    monkeypatch.setattr(registry, "get_engine_definition", lambda _engine: definition)

    assert run_engine_queue_worker("demo", ["--config", "/tmp/config.yaml"]) == 7
    assert calls == [["--config", "/tmp/config.yaml"]]


def test_engine_worker_child_dispatches_runner_callbacks(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def _run_child(**kwargs: Any) -> int:
        calls.append(kwargs)
        return 9

    definition = _definition(
        runner_callbacks=EngineRunnerCallbacks(
            run_worker_child_job=_run_child,
            build_worker_child_command=lambda **_kwargs: ["demo-child"],
        )
    )

    from chemstack.core.engines import registry

    monkeypatch.setattr(registry, "get_engine_definition", lambda _engine: definition)

    assert (
        run_engine_worker_child_job(
            engine="demo",
            config_path="/tmp/config.yaml",
            queue_root=Path("/tmp/queue"),
            queue_id="qid",
            admission_token="token",
        )
        == 9
    )
    assert calls == [
        {
            "config_path": "/tmp/config.yaml",
            "queue_root": Path("/tmp/queue"),
            "queue_id": "qid",
            "admission_token": "token",
        }
    ]


def test_real_engine_definitions_expose_runtime_contracts() -> None:
    from chemstack.core.engines import get_engine_definition

    for engine in ("orca", "xtb", "crest"):
        definition = get_engine_definition(engine)
        assert definition.queue_worker_module == "chemstack.core.engines.queue_worker"
        assert definition.queue_functions is not None
        assert definition.runner_callbacks is not None
        assert definition.artifact_adapter is not None
        assert definition.notification_hooks is not None


def test_engine_specific_modules_export_common_queue_worker_factories() -> None:
    from chemstack.crest import queue_runtime as crest_queue_runtime
    from chemstack.orca import queue_worker as orca_queue_worker
    from chemstack.xtb import queue_runtime as xtb_queue_runtime

    for module in (orca_queue_worker, xtb_queue_runtime, crest_queue_runtime):
        assert callable(module.QueueWorker)
        assert not inspect.isclass(module.QueueWorker)
