from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.xtb import worker_child


def test_run_worker_job_activates_admission_delegates_and_releases(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(name="cfg")
    entry = SimpleNamespace(queue_id="queue-1")
    job_dir = tmp_path / "job"
    activated: list[tuple[str, str, str, str, str]] = []
    released: list[tuple[str, str]] = []
    executed: list[dict[str, Any]] = []

    def activate_reserved_slot(root: str, token: str, **kwargs: Any) -> object:
        activated.append(
            (
                str(root),
                token,
                str(kwargs["work_dir"]),
                kwargs["queue_id"],
                kwargs["source"],
            )
        )
        return object()

    deps = SimpleNamespace(
        config=SimpleNamespace(
            load_config=lambda _path: cfg,
            queue_entry_by_id=lambda _root, _queue_id: entry,
        ),
        admission=SimpleNamespace(
            activate_reserved_slot=activate_reserved_slot,
            release_slot=lambda root, token: released.append((str(root), token)),
        ),
        context=SimpleNamespace(job_dir=lambda _entry: job_dir),
        execute_queue_entry=None,
    )

    def execute_queue_entry(*args: Any, **kwargs: Any) -> SimpleNamespace:
        executed.append({"args": args, "kwargs": kwargs})
        return SimpleNamespace(result=SimpleNamespace(status="completed"))

    rc = worker_child.run_worker_job(
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-1",
        admission_root="/tmp/admission",
        admission_token="slot-1",
        dependencies=deps,
        execute_queue_entry_fn=execute_queue_entry,
        should_cancel=lambda: False,
        register_running_job=lambda _value: None,
        getpid_fn=lambda: 12345,
        worker_job_module="chemstack.xtb.worker_execution",
    )

    assert rc == 0
    assert activated == [
        (
            "/tmp/admission",
            "slot-1",
            str(job_dir),
            "queue-1",
            "chemstack.xtb.worker_execution",
        )
    ]
    assert released == [("/tmp/admission", "slot-1")]
    assert executed[0]["args"] == (cfg,)
    assert executed[0]["kwargs"]["queue_root"] == (tmp_path / "queue").resolve()
    assert executed[0]["kwargs"]["entry"] is entry
    assert executed[0]["kwargs"]["worker_job_pid"] == 12345
    assert executed[0]["kwargs"]["dependencies"] is deps


def test_run_worker_job_uses_injected_execute_queue_entry(tmp_path: Path) -> None:
    cfg = SimpleNamespace(name="cfg")
    entry = SimpleNamespace(queue_id="queue-1")
    injected_calls: list[dict[str, Any]] = []

    def injected_execute(*args: Any, **kwargs: Any) -> SimpleNamespace:
        injected_calls.append({"args": args, "kwargs": kwargs})
        return SimpleNamespace(result=SimpleNamespace(status="failed"))

    deps = SimpleNamespace(
        config=SimpleNamespace(
            load_config=lambda _path: cfg,
            queue_entry_by_id=lambda _root, _queue_id: entry,
        ),
        admission=SimpleNamespace(
            activate_reserved_slot=lambda *args, **kwargs: pytest.fail("unexpected activation"),
            release_slot=lambda *args: pytest.fail("unexpected release"),
        ),
        context=SimpleNamespace(job_dir=lambda _entry: tmp_path / "job"),
        execute_queue_entry=injected_execute,
    )

    rc = worker_child.run_worker_job(
        config_path="/tmp/chemstack.yaml",
        queue_root=tmp_path / "queue",
        queue_id="queue-1",
        admission_root="/tmp/admission",
        admission_token=None,
        dependencies=deps,
        execute_queue_entry_fn=lambda *args, **kwargs: pytest.fail("unexpected default execute"),
        getpid_fn=lambda: 777,
    )

    assert rc == 1
    assert injected_calls[0]["args"] == (cfg,)
    assert injected_calls[0]["kwargs"]["worker_job_pid"] == 777


def test_signal_controller_installs_cancel_and_shutdown_handlers() -> None:
    handlers: dict[int, Any] = {}
    terminated: list[object] = []
    process = object()

    signal_module = SimpleNamespace(
        SIGTERM=15,
        SIGINT=2,
        signal=lambda signum, handler: handlers.__setitem__(signum, handler),
    )
    controller = worker_child.SignalController(
        cancel_signal=10,
        shutdown_exit_code=190,
        terminate_process_fn=lambda running: terminated.append(running),
        signal_module=signal_module,
        os_exit_fn=lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )

    controller.install()
    controller.set_running_job(SimpleNamespace(process=process))
    handlers[10](10, None)

    assert controller.should_cancel() is True
    assert terminated == [process]

    with pytest.raises(SystemExit) as exc_info:
        handlers[15](15, None)

    assert exc_info.value.code == 190
    assert terminated == [process, process]
