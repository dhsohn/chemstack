from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack.core.queue import engine_execution


def test_run_engine_worker_lifecycle_stops_on_shutdown_before_mark_running(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(tmp_path / "allowed")))
    calls: list[str] = []

    def raise_shutdown(_context: Any) -> None:
        calls.append("check")
        raise RuntimeError("shutdown")

    def build_context(_cfg: Any, _entry: Any) -> SimpleNamespace:
        calls.append("build")
        return SimpleNamespace()

    lifecycle = engine_execution.EngineWorkerLifecycle(
        build_context=build_context,
        check_shutdown=raise_shutdown,
        mark_running=lambda *_args: calls.append("mark"),
        run_job=lambda *_args: calls.append("run"),
        finalize_entry=lambda *_args: calls.append("finalize"),
        build_outcome=lambda *_args: calls.append("outcome"),
    )

    try:
        engine_execution.run_engine_worker_lifecycle(
            cfg,
            SimpleNamespace(queue_id="q-1"),
            queue_root=None,
            lifecycle=lifecycle,
        )
    except RuntimeError as exc:
        assert str(exc) == "shutdown"
    else:
        raise AssertionError("expected shutdown")

    assert calls == ["build", "check"]


def test_run_engine_worker_lifecycle_stops_on_shutdown_after_mark_running(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(tmp_path / "allowed")))
    checks = iter([False, True])
    calls: list[str] = []

    def maybe_raise_shutdown(_context: Any) -> None:
        calls.append("check")
        if next(checks):
            raise RuntimeError("shutdown")

    def build_context(_cfg: Any, _entry: Any) -> SimpleNamespace:
        calls.append("build")
        return SimpleNamespace()

    lifecycle = engine_execution.EngineWorkerLifecycle(
        build_context=build_context,
        check_shutdown=maybe_raise_shutdown,
        mark_running=lambda *_args: calls.append("mark"),
        run_job=lambda *_args: calls.append("run"),
        finalize_entry=lambda *_args: calls.append("finalize"),
        build_outcome=lambda *_args: calls.append("outcome"),
    )

    try:
        engine_execution.run_engine_worker_lifecycle(
            cfg,
            SimpleNamespace(queue_id="q-1"),
            queue_root=None,
            lifecycle=lifecycle,
        )
    except RuntimeError as exc:
        assert str(exc) == "shutdown"
    else:
        raise AssertionError("expected shutdown")

    assert calls == ["build", "check", "mark", "check"]


def test_sync_terminal_result_runs_common_terminal_sequence() -> None:
    calls: list[str] = []

    def sync_job_record() -> str:
        calls.append("sync")
        return "organized"

    outcome = engine_execution.sync_terminal_result(
        engine_execution.TerminalSyncActions(
            write_artifacts=lambda: calls.append("write"),
            mark_queue_terminal=lambda: calls.append("mark"),
            sync_job_record=sync_job_record,
            notify_finished=lambda sync_result: calls.append(f"notify:{sync_result}"),
            emit_output=lambda sync_result: calls.append(f"emit:{sync_result}"),
            build_outcome=lambda sync_result: ("outcome", sync_result),
        ),
        emit_output=True,
    )

    assert calls == ["write", "mark", "sync", "notify:organized", "emit:organized"]
    assert outcome == ("outcome", "organized")


def test_run_cancellable_process_execution_waits_and_clears_running_job() -> None:
    running = SimpleNamespace(process=SimpleNamespace())
    registered: list[Any | None] = []
    wait_calls: list[tuple[Any, dict[str, Any]]] = []

    def wait_for_process(actual_running: Any, **kwargs: Any) -> str:
        wait_calls.append((actual_running, kwargs))
        return "completed"

    outcome = engine_execution.run_cancellable_process_execution(
        engine_execution.CancellableProcessExecution(
            start_job=lambda: running,
            finalize_job=lambda *_args, **_kwargs: "finalized",
            terminate_process=lambda _proc: None,
            build_failure_result=lambda exc: f"failed:{exc}",
            wait_for_cancellable_process=wait_for_process,
            should_cancel=lambda: False,
            sleep=lambda _seconds: None,
            poll_interval_seconds=0.25,
            check_cancel_before_poll=True,
            register_running_job=registered.append,
        )
    )

    assert outcome == "completed"
    assert registered == [running, None]
    assert wait_calls[0][0] is running
    assert wait_calls[0][1]["check_cancel_before_poll"] is True
    assert wait_calls[0][1]["poll_interval_seconds"] == 0.25


def test_run_cancellable_process_execution_builds_failure_result() -> None:
    outcome = engine_execution.run_cancellable_process_execution(
        engine_execution.CancellableProcessExecution(
            start_job=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
            finalize_job=lambda *_args, **_kwargs: "finalized",
            terminate_process=lambda _proc: None,
            build_failure_result=lambda exc: f"failed:{exc}",
        )
    )

    assert outcome == "failed:boom"


def test_run_cancellable_process_execution_can_reraise_policy_exceptions() -> None:
    class ShutdownRequested(RuntimeError):
        pass

    def wait_for_process(_running: Any, **_kwargs: Any) -> str:
        raise ShutdownRequested("shutdown")

    try:
        engine_execution.run_cancellable_process_execution(
            engine_execution.CancellableProcessExecution(
                start_job=lambda: SimpleNamespace(process=SimpleNamespace()),
                finalize_job=lambda *_args, **_kwargs: "finalized",
                terminate_process=lambda _proc: None,
                build_failure_result=lambda exc: f"failed:{exc}",
                wait_for_cancellable_process=wait_for_process,
                should_reraise_exception=lambda exc: isinstance(exc, ShutdownRequested),
            )
        )
    except ShutdownRequested as exc:
        assert str(exc) == "shutdown"
    else:
        raise AssertionError("expected shutdown")
