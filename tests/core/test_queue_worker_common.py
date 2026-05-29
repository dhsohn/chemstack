from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chemstack.core.queue import child_process as child_process_helpers
from chemstack.core.queue import processes as process_helpers
from chemstack.core.queue import worker as worker_common


def _cfg(**runtime_overrides: object) -> SimpleNamespace:
    runtime: dict[str, object] = {
        "allowed_root": "/allowed",
        "admission_root": "",
        "admission_limit": None,
        "max_concurrent": 3,
    }
    runtime.update(runtime_overrides)
    return SimpleNamespace(runtime=SimpleNamespace(**runtime))


def _entry(
    queue_id: str,
    *,
    status: str = "pending",
    priority: int = 10,
    enqueued_at: str = "2026-01-01T00:00:00Z",
    cancel_requested: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        status=SimpleNamespace(value=status),
        priority=priority,
        enqueued_at=enqueued_at,
        queue_id=queue_id,
        cancel_requested=cancel_requested,
    )


def test_resolve_admission_root_and_limit_prefer_resolved_values() -> None:
    cfg = _cfg(
        resolved_admission_root="/resolved",
        resolved_admission_limit="5",
        admission_root="/configured",
        admission_limit=2,
    )

    assert worker_common.resolve_admission_root(cfg) == "/resolved"
    assert worker_common.resolve_admission_limit(cfg) == 5


def test_resolve_admission_limit_falls_back_and_handles_invalid_values() -> None:
    assert (
        worker_common.resolve_admission_limit(_cfg(resolved_admission_limit=0, max_concurrent=7))
        == 7
    )
    assert worker_common.resolve_admission_limit(_cfg(resolved_admission_limit="bad")) == 1


def test_reserve_queue_worker_slot_uses_common_resolved_values() -> None:
    calls: list[tuple[str, int, str, str]] = []

    def reserve_slot(root: str, limit: int, *, source: str, app_name: str) -> str:
        calls.append((root, limit, source, app_name))
        return "slot-1"

    result = worker_common.reserve_queue_worker_slot(
        _cfg(admission_root="/admission", admission_limit=4),
        source="source-name",
        app_name="app-name",
        reserve_slot_fn=reserve_slot,
    )

    assert result == "slot-1"
    assert calls == [("/admission", 4, "source-name", "app-name")]


def test_dequeue_next_across_roots_handles_single_root_idle_and_selected_entry(
    tmp_path: Path,
) -> None:
    root = tmp_path / "queue"
    entry = _entry("q-1")

    assert (
        worker_common.dequeue_next_across_roots(
            (root,),
            list_queue_fn=lambda _root: [],
            dequeue_next_fn=lambda _root: None,
        )
        is None
    )
    assert worker_common.dequeue_next_across_roots(
        (root,),
        list_queue_fn=lambda _root: [],
        dequeue_next_fn=lambda _root: entry,
    ) == (root, entry)


def test_dequeue_next_across_roots_selects_best_pending_entry(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    queues = {
        first: [
            _entry("running", status="running", priority=1),
            _entry("cancelled", priority=1, cancel_requested=True),
            _entry("later", priority=5, enqueued_at="2026-01-02T00:00:00Z"),
        ],
        second: [_entry("winner", priority=1, enqueued_at="2026-01-01T00:00:00Z")],
    }

    result = worker_common.dequeue_next_across_roots(
        (first, second),
        list_queue_fn=lambda root: queues[root],
        dequeue_next_fn=lambda root: queues[root][0],
    )

    assert result == (second, queues[second][0])


def test_dequeue_next_across_roots_returns_none_when_selected_root_dequeues_empty(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"

    assert (
        worker_common.dequeue_next_across_roots(
            (root, tmp_path / "other"),
            list_queue_fn=lambda _root: [_entry("pending")],
            dequeue_next_fn=lambda _root: None,
        )
        is None
    )


def test_queue_entry_by_id_scans_queue_with_injected_lister(tmp_path: Path) -> None:
    entries = [_entry("q-1"), _entry("q-2")]

    assert (
        worker_common.queue_entry_by_id(
            tmp_path,
            "q-2",
            list_queue_fn=lambda root: entries if root == tmp_path else [],
        )
        is entries[1]
    )
    assert (
        worker_common.queue_entry_by_id(
            tmp_path,
            "missing",
            list_queue_fn=lambda _root: entries,
        )
        is None
    )


def test_start_background_process_uses_detached_devnull_popen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    expected = object()

    def fake_popen(command: list[str], **kwargs: object) -> object:
        calls.append({"command": command, **kwargs})
        return expected

    monkeypatch.setattr(child_process_helpers.subprocess, "Popen", fake_popen)

    assert worker_common.start_background_process(("python", "-m", "worker")) is expected
    assert calls == [
        {
            "command": ["python", "-m", "worker"],
            "stdout": child_process_helpers.subprocess.DEVNULL,
            "stderr": child_process_helpers.subprocess.DEVNULL,
            "stdin": child_process_helpers.subprocess.DEVNULL,
            "start_new_session": True,
            "text": True,
        }
    ]


def test_child_worker_command_requires_admission_root_when_included() -> None:
    with pytest.raises(ValueError, match="admission_root is required"):
        child_process_helpers.build_background_worker_command(
            config_path="/tmp/chemstack.yaml",
            queue_root="/tmp/queue",
            queue_id="queue-1",
            worker_job_module="chemstack.worker",
        )

    assert child_process_helpers.build_background_worker_command(
        config_path="/tmp/chemstack.yaml",
        queue_root="/tmp/queue",
        queue_id="queue-1",
        worker_job_module="chemstack.worker",
        include_admission_root=False,
    ) == [
        child_process_helpers.sys.executable,
        "-m",
        "chemstack.worker",
        "--config",
        "/tmp/chemstack.yaml",
        "--queue-root",
        "/tmp/queue",
        "--queue-id",
        "queue-1",
    ]


def test_start_background_job_process_builds_child_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands: list[list[str]] = []
    expected = object()

    monkeypatch.setattr(
        child_process_helpers,
        "start_background_process",
        lambda command: commands.append(list(command)) or expected,
    )

    result = child_process_helpers.start_background_job_process(
        config_path="/tmp/chemstack.yaml",
        queue_root="/tmp/queue",
        entry=SimpleNamespace(queue_id="queue-1"),
        worker_job_module="chemstack.worker",
        admission_root="/tmp/admission",
        admission_token="slot-1",
    )

    assert result is expected
    assert commands == [
        [
            child_process_helpers.sys.executable,
            "-m",
            "chemstack.worker",
            "--config",
            "/tmp/chemstack.yaml",
            "--queue-root",
            "/tmp/queue",
            "--queue-id",
            "queue-1",
            "--admission-root",
            "/tmp/admission",
            "--admission-token",
            "slot-1",
        ]
    ]


def test_fill_worker_slots_starts_until_capacity_and_reports_processed() -> None:
    running: list[str] = []
    reservations = iter(
        [
            ("processed", "slot-1"),
            ("processed", "slot-2"),
            ("idle", None),
        ]
    )

    result = worker_common.fill_worker_slots(
        running_count=lambda: len(running),
        max_concurrent=2,
        reserve_next=lambda: next(reservations),
        start_reserved=lambda reserved: running.append(reserved),
    )

    assert result.status == "processed"
    assert result.started == 2
    assert running == ["slot-1", "slot-2"]


def test_fill_worker_slots_preserves_blocked_status_before_starting() -> None:
    result = worker_common.fill_worker_slots(
        running_count=lambda: 0,
        max_concurrent=2,
        reserve_next=lambda: ("blocked", None),
        start_reserved=lambda _reserved: pytest.fail("start should not run"),
    )

    assert result.status == "blocked"
    assert result.started == 0


def test_fill_worker_slots_respects_max_new_jobs() -> None:
    running: list[str] = []

    result = worker_common.fill_worker_slots(
        running_count=lambda: len(running),
        max_concurrent=5,
        reserve_next=lambda: ("processed", "slot"),
        start_reserved=lambda reserved: running.append(reserved),
        max_new_jobs=1,
    )

    assert result.status == "processed"
    assert result.started == 1
    assert running == ["slot"]


def test_pop_completed_worker_jobs_finalizes_and_removes_finished_jobs() -> None:
    running = {
        "q-running": SimpleNamespace(rc=None),
        "q-done": SimpleNamespace(rc=0),
        "q-failed": SimpleNamespace(rc=2),
    }
    finalized: list[tuple[str, int]] = []

    count = worker_common.pop_completed_worker_jobs(
        running,
        poll_job=lambda job: job.rc,
        finalize_finished=lambda queue_id, _job, rc: finalized.append((queue_id, rc)),
    )

    assert count == 2
    assert finalized == [("q-done", 0), ("q-failed", 2)]
    assert list(running) == ["q-running"]


def test_terminate_process_group_handles_finished_process() -> None:
    worker_common.terminate_process_group(SimpleNamespace(poll=lambda: 0))


def test_terminate_process_group_falls_back_to_proc_methods() -> None:
    calls: list[str] = []

    class Process:
        pid = 123

        def poll(self) -> None:
            return None

        def terminate(self) -> None:
            calls.append("terminate")

        def kill(self) -> None:
            calls.append("kill")

        def wait(self, timeout: float | None = None) -> int:
            calls.append(f"wait:{timeout}")
            raise subprocess.TimeoutExpired(
                cmd="worker",
                timeout=float(timeout if timeout is not None else 0),
            )

    def killpg(_pid: int, _signal: int) -> None:
        calls.append("killpg")
        raise ProcessLookupError("missing")

    worker_common.terminate_process_group(
        Process(),
        graceful_timeout=1,
        kill_timeout=2,
        killpg_fn=killpg,
        sigterm=15,
        sigkill=9,
    )

    assert calls == ["killpg", "terminate", "wait:1", "killpg", "kill", "wait:2"]


def test_install_shutdown_signal_handlers_invokes_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers: list[Any] = []
    requested: list[bool] = []

    monkeypatch.setattr(
        worker_common.signal,
        "signal",
        lambda _signum, handler: handlers.append(handler),
    )

    worker_common.install_shutdown_signal_handlers(lambda: requested.append(True))
    handlers[0](0, None)

    assert requested == [True]
    assert len(handlers) == 2


def test_install_shutdown_signal_handlers_ignores_non_main_thread_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker_common.signal,
        "signal",
        lambda *_args: (_ for _ in ()).throw(ValueError("not main thread")),
    )

    worker_common.install_shutdown_signal_handlers(lambda: pytest.fail("should not be called"))


def test_pid_helpers_handle_alive_missing_and_stale_pids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert worker_common.pid_is_alive(0) is False
    monkeypatch.setattr(process_helpers.os, "kill", lambda _pid, _signal: None)
    assert worker_common.pid_is_alive(123) is True

    pid_path = tmp_path / "worker.pid"
    pid_path.write_text("123\n", encoding="utf-8")
    assert process_helpers.read_live_pid_file(pid_path) == 123

    json_pid_path = tmp_path / "json-worker.pid"
    json_pid_path.write_text(json.dumps({"pid": 123, "process_start_ticks": 111}), encoding="utf-8")
    monkeypatch.setattr(process_helpers, "_process_start_ticks", lambda _pid: 111)
    assert process_helpers.read_live_pid_file(json_pid_path) == 123

    reused_pid_path = tmp_path / "reused-worker.pid"
    reused_pid_path.write_text(
        json.dumps({"pid": 123, "process_start_ticks": 111}), encoding="utf-8"
    )
    monkeypatch.setattr(process_helpers, "_process_start_ticks", lambda _pid: 222)
    assert process_helpers.read_live_pid_file(reused_pid_path) is None
    assert not reused_pid_path.exists()

    monkeypatch.setattr(
        process_helpers.os,
        "kill",
        lambda _pid, _signal: (_ for _ in ()).throw(OSError()),
    )
    assert worker_common.pid_is_alive(123) is False
    assert process_helpers.read_live_pid_file(pid_path) is None
    assert not pid_path.exists()

    missing = tmp_path / "missing.pid"
    assert process_helpers.read_live_pid_file(missing) is None

    invalid = tmp_path / "invalid.pid"
    invalid.write_text("not-a-pid\n", encoding="utf-8")
    assert process_helpers.read_live_pid_file(invalid) is None


def test_reconcile_orphaned_child_queue_entries_cancels_or_requeues_only_orphans(
    tmp_path: Path,
) -> None:
    queue_root = tmp_path / "queue"
    entries = [
        _entry("live", status="running"),
        _entry("cancelled", status="running", cancel_requested=True),
        _entry("orphaned", status="running"),
        _entry("pending", status="pending"),
    ]
    stale_reconciled: list[str] = []
    cancelled: list[tuple[str, str, str]] = []
    requeued: list[tuple[str, str]] = []
    recovery_pending: list[str] = []

    child_process_helpers.reconcile_orphaned_child_queue_entries(
        _cfg(),
        admission_root="/tmp/admission",
        queue_roots_fn=lambda _cfg: (queue_root,),
        list_queue_fn=lambda _queue_root: entries,
        list_slots_fn=lambda _admission_root: [SimpleNamespace(queue_id="live")],
        reconcile_stale_slots_fn=lambda admission_root: stale_reconciled.append(
            str(admission_root)
        ),
        running_status=SimpleNamespace(value="running"),
        mark_cancelled_fn=lambda root, queue_id, *, error: cancelled.append(
            (str(root), queue_id, error)
        ),
        requeue_running_entry_fn=lambda root, queue_id: requeued.append((str(root), queue_id)),
        mark_recovery_pending_fn=lambda _cfg, entry: recovery_pending.append(entry.queue_id),
    )

    assert stale_reconciled == ["/tmp/admission"]
    assert cancelled == [(str(queue_root), "cancelled", "cancel_requested")]
    assert requeued == [(str(queue_root), "orphaned")]
    assert recovery_pending == ["orphaned"]


def test_shutdown_child_process_with_grace_forces_after_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monotonic_values = iter([0.0, 0.05, 0.2])

    class Process:
        rc: int | None = None

        def poll(self) -> int | None:
            return self.rc

        def terminate(self) -> None:
            calls.append("terminate")

    process = Process()
    job = SimpleNamespace(process=process)
    finalized: list[tuple[object, int]] = []

    monkeypatch.setattr(child_process_helpers.time, "monotonic", lambda: next(monotonic_values))

    child_process_helpers.shutdown_child_process_with_grace(
        job,
        terminate_process_fn=lambda proc: calls.append("force") or setattr(proc, "rc", 9),
        finalize_child_exit_fn=lambda job_arg, rc: finalized.append((job_arg, rc)),
        grace_seconds=0.1,
        sleep_fn=lambda seconds: calls.append(f"sleep:{seconds}"),
    )

    assert calls == ["terminate", "sleep:0.1", "force"]
    assert finalized == [(job, 9)]


def test_shutdown_child_process_with_grace_continues_when_terminate_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Process:
        rc: int | None = None

        def poll(self) -> int | None:
            return self.rc

        def terminate(self) -> None:
            raise RuntimeError("cannot terminate")

    process = Process()
    job = SimpleNamespace(process=process)
    finalized: list[int] = []

    monkeypatch.setattr(child_process_helpers.time, "monotonic", lambda: 0.0)

    child_process_helpers.shutdown_child_process_with_grace(
        job,
        terminate_process_fn=lambda proc: setattr(proc, "rc", 9),
        finalize_child_exit_fn=lambda _job, rc: finalized.append(rc),
        grace_seconds=0.0,
        sleep_fn=lambda _seconds: pytest.fail("sleep should not run after deadline"),
    )

    assert finalized == [9]


def test_request_job_cancellation_uses_signal_then_kill_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sent_signals: list[int] = []
    child_process_helpers.request_job_cancellation(
        SimpleNamespace(send_signal=lambda signal_value: sent_signals.append(signal_value)),
        cancel_signal=15,
        terminate_process_fn=lambda _proc: pytest.fail("send_signal should be enough"),
    )
    assert sent_signals == [15]

    killed: list[tuple[int, int]] = []
    terminated: list[int] = []

    def fake_kill(pid: int, signal_value: int) -> None:
        killed.append((pid, signal_value))
        raise ProcessLookupError("missing process")

    monkeypatch.setattr(child_process_helpers.os, "kill", fake_kill)

    child_process_helpers.request_job_cancellation(
        SimpleNamespace(pid=4242),
        cancel_signal=2,
        terminate_process_fn=lambda proc: terminated.append(proc.pid),
    )

    assert killed == [(4242, 2)]
    assert terminated == [4242]
