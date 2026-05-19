from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack.core.queue import engine_execution


def test_process_dequeued_engine_entry_runs_lifecycle_in_order_and_defaults_queue_root(
    tmp_path: Path,
) -> None:
    allowed_root = tmp_path / "allowed"
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(allowed_root)))
    entry = SimpleNamespace(queue_id="q-1")
    calls: list[str] = []
    roots: list[Path] = []

    def build_context(cfg_obj: Any, entry_obj: Any) -> SimpleNamespace:
        assert cfg_obj is cfg
        assert entry_obj is entry
        calls.append("build_context")
        return SimpleNamespace(job_id="job-1")

    def check_shutdown(context: Any) -> None:
        calls.append(f"check:{context.job_id}")

    def mark_running(cfg_obj: Any, context: Any) -> None:
        assert cfg_obj is cfg
        calls.append(f"mark:{context.job_id}")

    def run_job(cfg_obj: Any, context: Any, queue_root: Path) -> SimpleNamespace:
        assert cfg_obj is cfg
        calls.append(f"run:{context.job_id}")
        roots.append(queue_root)
        return SimpleNamespace(status="completed")

    def finalize_entry(
        cfg_obj: Any,
        context: Any,
        result: Any,
        queue_root: Path,
        auto_organize: bool,
    ) -> Path:
        assert cfg_obj is cfg
        assert result.status == "completed"
        assert auto_organize is True
        calls.append(f"finalize:{context.job_id}")
        roots.append(queue_root)
        return tmp_path / "organized"

    def build_outcome(
        context: Any, result: Any, organized_output_dir: Path
    ) -> tuple[str, str, Path]:
        calls.append(f"outcome:{context.job_id}")
        return context.job_id, result.status, organized_output_dir

    outcome = engine_execution.process_dequeued_engine_entry(
        cfg,
        entry,
        queue_root=None,
        auto_organize=True,
        build_context_fn=build_context,
        check_shutdown_fn=check_shutdown,
        mark_running_fn=mark_running,
        run_job_fn=run_job,
        finalize_entry_fn=finalize_entry,
        build_outcome_fn=build_outcome,
    )

    assert calls == [
        "build_context",
        "check:job-1",
        "mark:job-1",
        "check:job-1",
        "run:job-1",
        "finalize:job-1",
        "outcome:job-1",
    ]
    assert roots == [allowed_root.resolve(), allowed_root.resolve()]
    assert outcome == ("job-1", "completed", tmp_path / "organized")


def test_process_dequeued_engine_entry_uses_explicit_queue_root_without_shutdown_check(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(tmp_path / "allowed")))
    explicit_queue_root = tmp_path / "queue"
    calls: list[str] = []

    def run_job(_cfg: Any, _context: Any, queue_root: Path) -> str:
        calls.append(f"run:{queue_root.name}")
        return "result"

    def finalize_entry(
        _cfg: Any,
        _context: Any,
        _result: Any,
        queue_root: Path,
        auto_organize: bool,
    ) -> Path:
        calls.append(f"finalize:{queue_root.name}:{auto_organize}")
        return tmp_path / "out"

    outcome = engine_execution.process_dequeued_engine_entry(
        cfg,
        SimpleNamespace(queue_id="q-1"),
        queue_root=explicit_queue_root,
        auto_organize=False,
        build_context_fn=lambda _cfg, _entry: SimpleNamespace(job_id="job-1"),
        check_shutdown_fn=None,
        mark_running_fn=lambda _cfg, _context: calls.append("mark"),
        run_job_fn=run_job,
        finalize_entry_fn=finalize_entry,
        build_outcome_fn=lambda _context, result, organized: (result, organized),
    )

    assert calls == ["mark", "run:queue", "finalize:queue:False"]
    assert outcome == ("result", tmp_path / "out")


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
            auto_organize=False,
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
            auto_organize=False,
            lifecycle=lifecycle,
        )
    except RuntimeError as exc:
        assert str(exc) == "shutdown"
    else:
        raise AssertionError("expected shutdown")

    assert calls == ["build", "check", "mark", "check"]
