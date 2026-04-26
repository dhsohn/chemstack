from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.xtb import worker_job


def test_worker_job_main_delegates_to_queue_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    signal_calls: list[tuple[int, Any]] = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr(
        worker_job.signal,
        "signal",
        lambda signum, handler: signal_calls.append((signum, handler)),
    )

    def _fake_run_worker_job(**kwargs: Any) -> int:
        captured.update(kwargs)
        assert kwargs["should_cancel"]() is False
        kwargs["register_running_job"](SimpleNamespace(process=object()))
        kwargs["register_running_job"](None)
        return 37

    monkeypatch.setattr(worker_job.queue_cmd, "run_worker_job", _fake_run_worker_job)

    result = worker_job.main(
        [
            "--config",
            "/tmp/chemstack.yaml",
            "--queue-root",
            "/tmp/queue",
            "--queue-id",
            "q-1",
            "--admission-root",
            "/tmp/admission",
            "--admission-token",
            " slot-1 ",
            "--auto-organize",
        ]
    )

    assert result == 37
    assert captured["config_path"] == "/tmp/chemstack.yaml"
    assert captured["queue_root"] == "/tmp/queue"
    assert captured["queue_id"] == "q-1"
    assert captured["admission_root"] == "/tmp/admission"
    assert captured["admission_token"] == "slot-1"
    assert captured["auto_organize"] is True
    assert [signum for signum, _handler in signal_calls] == [
        worker_job.queue_cmd.WORKER_CANCEL_SIGNAL,
        worker_job.signal.SIGTERM,
        worker_job.signal.SIGINT,
    ]


def test_worker_job_signal_controller_cancel_and_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    terminated: list[object] = []
    process = object()
    controller = worker_job._SignalController()

    monkeypatch.setattr(
        worker_job.queue_cmd,
        "_terminate_process",
        lambda running_process: terminated.append(running_process),
    )
    monkeypatch.setattr(
        worker_job.os,
        "_exit",
        lambda code: (_ for _ in ()).throw(SystemExit(code)),
    )

    controller.set_running_job(SimpleNamespace(process=process))
    controller._handle_cancel(0, None)

    assert controller.should_cancel() is True
    assert terminated == [process]

    with pytest.raises(SystemExit) as exc_info:
        controller._handle_shutdown(0, None)

    assert exc_info.value.code == worker_job.queue_cmd.WORKER_SHUTDOWN_EXIT_CODE
    assert terminated == [process, process]


def test_worker_job_signal_install_ignores_non_main_thread_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_job.signal,
        "signal",
        lambda *args: (_ for _ in ()).throw(ValueError("not in main thread")),
    )

    worker_job._SignalController().install()
