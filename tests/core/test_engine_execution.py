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
