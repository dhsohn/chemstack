from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from chemstack.core.queue.engine_runtime import EngineQueueRuntime


def _runtime(
    tmp_path: Path,
    *,
    entries: dict[Path, list[Any]] | None = None,
    dequeued: dict[Path, Any | None] | None = None,
) -> EngineQueueRuntime:
    return EngineQueueRuntime(
        load_config=lambda value: value,
        runtime_roots_for_cfg=lambda _cfg: (tmp_path / "a", tmp_path / "b"),
        list_queue=lambda root: dict(entries or {}).get(Path(root), []),
        dequeue_next=lambda root: dict(dequeued or {}).get(root),
        worker_pid_file_name="engine_worker.pid",
    )


def test_engine_queue_runtime_delegates_queue_roots_entries_and_dequeue(
    tmp_path: Path,
) -> None:
    root_a = tmp_path / "a"
    root_b = tmp_path / "b"
    entry_a = SimpleNamespace(queue_id="a", status=SimpleNamespace(value="pending"))
    entry_b = SimpleNamespace(
        queue_id="b",
        status=SimpleNamespace(value="pending"),
        priority=1,
        enqueued_at="2026-01-01T00:00:00Z",
        cancel_requested=False,
    )
    runtime = _runtime(
        tmp_path,
        entries={root_a: [entry_a], root_b: [entry_b]},
        dequeued={root_b: entry_b},
    )

    assert runtime.queue_roots(SimpleNamespace()) == (root_a, root_b)
    assert runtime.queue_entries_with_roots(SimpleNamespace()) == [
        (root_a, entry_a),
        (root_b, entry_b),
    ]
    assert runtime.dequeue_next_entry(SimpleNamespace()) == (root_b, entry_b)


def test_engine_queue_runtime_common_accessors(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1")
    runtime = _runtime(tmp_path, entries={tmp_path / "a": [entry]})
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            admission_root="/tmp/admission",
            admission_limit=1,
            max_concurrent=1,
        )
    )

    assert runtime.queue_entry_by_id(tmp_path / "a", "queue-1") is entry
    assert runtime.admission_root(cfg) == "/tmp/admission"

    (tmp_path / "engine_worker.pid").write_text("123\n", encoding="utf-8")
    assert runtime.read_worker_pid(tmp_path) is None


def test_engine_queue_runtime_builds_child_worker_deps(tmp_path: Path) -> None:
    entry = SimpleNamespace(queue_id="queue-1", status=SimpleNamespace(value="pending"))
    runtime = _runtime(
        tmp_path,
        entries={tmp_path / "a": [entry]},
        dequeued={tmp_path / "a": entry},
    )
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            admission_root="/tmp/admission",
            admission_limit=1,
            max_concurrent=1,
        )
    )
    released: list[tuple[str, str]] = []
    started: list[dict[str, Any]] = []

    deps = runtime.child_worker_deps(
        poll_interval_seconds=5,
        time_module=SimpleNamespace(sleep=lambda _seconds: None),
        release_slot_fn=lambda root, token: released.append((str(root), token)),
        start_background_job_process_fn=lambda **kwargs: started.append(kwargs),
        try_reserve_admission_slot_fn=lambda _cfg: "slot-1",
    )

    assert deps.admission_root(cfg) == "/tmp/admission"
    assert deps.dequeue_next_entry(cfg) == (tmp_path / "a", entry)
    status, reserved = deps.reserve_dequeued_entry(
        cfg,
        admission_root=deps.admission_root(cfg),
        reserve_slot_fn=deps.try_reserve_admission_slot,
        dequeue_next_fn=deps.dequeue_next_entry,
        release_slot_fn=deps.release_slot,
    )

    assert status == "processed"
    assert reserved is not None
    assert reserved.queue_root == tmp_path / "a"
    assert reserved.entry is entry
    assert reserved.admission_token == "slot-1"
    assert released == []

    deps.start_background_job_process(
        config_path="/tmp/config.yaml",
        queue_root=reserved.queue_root,
        entry=reserved.entry,
        admission_root=deps.admission_root(cfg),
        admission_token=reserved.admission_token,
    )

    assert started == [
        {
            "config_path": "/tmp/config.yaml",
            "queue_root": tmp_path / "a",
            "entry": entry,
            "admission_root": "/tmp/admission",
            "admission_token": "slot-1",
        }
    ]


def test_engine_queue_runtime_reserves_admission_slot(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root="/tmp/allowed",
            admission_root="/tmp/admission",
            admission_limit=2,
            max_concurrent=4,
        )
    )
    calls: list[dict[str, Any]] = []

    def reserve_slot(root: str, limit: int, **kwargs: Any) -> str:
        calls.append({"root": root, "limit": limit, "kwargs": kwargs})
        return "slot-1"

    assert (
        runtime.reserve_admission_slot(
            cfg,
            engine="xtb",
            reserve_slot_fn=reserve_slot,
        )
        == "slot-1"
    )
    assert calls == [
        {
            "root": "/tmp/admission",
            "limit": 2,
            "kwargs": {
                "source": "chemstack.xtb.queue_worker",
                "app_name": "chemstack_xtb",
            },
        }
    ]


def test_engine_queue_runtime_starts_child_process_with_optional_admission_root(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    entry = SimpleNamespace(queue_id="queue-1")
    commands: list[list[str]] = []

    def build_child_command(**kwargs: Any) -> list[str]:
        return [f"{key}={value}" for key, value in sorted(kwargs.items())]

    def start_background_process(command: list[str]) -> str:
        commands.append(command)
        return "proc"

    result = runtime.start_child_process(
        config_path="/tmp/config.yaml",
        queue_root=tmp_path / "queue",
        entry=entry,
        admission_root="/tmp/admission",
        admission_token="slot-1",
        start_background_process_fn=start_background_process,
        build_worker_child_command_fn=build_child_command,
        include_admission_root=False,
    )

    assert result == "proc"
    assert commands == [
        [
            "admission_token=slot-1",
            "config_path=/tmp/config.yaml",
            "queue_id=queue-1",
            f"queue_root={tmp_path / 'queue'}",
        ]
    ]


def test_engine_queue_runtime_builds_common_child_worker_hooks(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    entry = SimpleNamespace(
        queue_id="queue-1",
        metadata={"job_dir": str(tmp_path / "job-1")},
    )
    events: list[tuple[str, Any]] = []

    class Worker:
        admission_root = "/tmp/admission"

        def _mark_entry_failed_and_release(
            self,
            queue_root: Path,
            entry_arg: Any,
            admission_token: str,
            **kwargs: Any,
        ) -> None:
            events.append(
                (
                    "failed_release",
                    {
                        "queue_root": queue_root,
                        "queue_id": entry_arg.queue_id,
                        "admission_token": admission_token,
                        "kwargs": kwargs,
                    },
                )
            )

    class Process:
        pid = 2468

        def __init__(self) -> None:
            self.terminated = False

        def poll(self) -> int | None:
            return 0 if self.terminated else None

        def terminate(self) -> None:
            self.terminated = True

    def handle_worker_start_error(
        worker: Any,
        queue_root: Path,
        entry_arg: Any,
        admission_token: str,
        exc: OSError,
    ) -> None:
        events.append(("start_error", (queue_root, entry_arg.queue_id, admission_token, str(exc))))

    def finalize_completed_job(worker: Any, queue_id: str, job: Any, rc: int) -> None:
        events.append(("completed", (queue_id, job.entry.queue_id, rc)))

    def finalize_child_exit(worker: Any, job: Any, *, rc: int) -> None:
        events.append(("child_exit", (job.entry.queue_id, rc)))

    def reconcile_worker_state(worker: Any) -> None:
        events.append(("reconcile", worker.admission_root))

    def activate_reserved_slot(root: str, token: str, **kwargs: Any) -> object:
        events.append(("activated", {"root": root, "token": token, "kwargs": kwargs}))
        return object()

    hooks = runtime.child_worker_hooks(
        engine="xtb",
        handle_worker_start_error_fn=handle_worker_start_error,
        finalize_completed_job_fn=finalize_completed_job,
        finalize_child_exit_fn=finalize_child_exit,
        reconcile_worker_state_fn=reconcile_worker_state,
        activate_reserved_slot_fn=activate_reserved_slot,
        terminate_process_fn=lambda process: events.append(("terminate", process.pid)),
        mark_failed_fn=lambda *args, **kwargs: events.append(("mark_failed", (args, kwargs))),
        shutdown_grace_seconds=0,
        sleep_fn=lambda seconds: events.append(("sleep", seconds)),
    )

    worker = Worker()
    process = Process()
    assert hooks.on_worker_process_started(
        worker,
        tmp_path,
        entry,
        process,
        "slot-1",
    )
    hooks.finalize_completed_job(worker, "queue-1", SimpleNamespace(entry=entry), 0)
    hooks.reconcile_worker_state(worker)
    hooks.handle_worker_start_error(worker, tmp_path, entry, "slot-2", OSError("boom"))
    hooks.shutdown_running_job(
        worker,
        "queue-1",
        SimpleNamespace(queue_root=tmp_path, entry=entry, process=Process()),
    )

    assert events == [
        (
            "activated",
            {
                "root": "/tmp/admission",
                "token": "slot-1",
                "kwargs": {
                    "owner_pid": 2468,
                    "source": "chemstack.xtb.queue_worker.child",
                    "queue_id": "queue-1",
                    "work_dir": str(tmp_path / "job-1"),
                },
            },
        ),
        ("completed", ("queue-1", "queue-1", 0)),
        ("reconcile", "/tmp/admission"),
        ("start_error", (tmp_path, "queue-1", "slot-2", "boom")),
        ("child_exit", ("queue-1", 0)),
    ]


def test_engine_queue_runtime_child_worker_hooks_accept_engine_overrides(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    events: list[tuple[str, Any]] = []
    worker = SimpleNamespace(name="worker")
    entry = SimpleNamespace(queue_id="queue-override")
    process = SimpleNamespace(pid=8642)
    job = SimpleNamespace(name="job")

    def record_started(
        worker_arg: Any,
        queue_root: Path,
        entry_arg: Any,
        process_arg: Any,
        token: str,
    ) -> bool:
        events.append(
            (
                "started",
                (worker_arg.name, queue_root, entry_arg.queue_id, process_arg.pid, token),
            )
        )
        return True

    hooks = runtime.child_worker_hooks(
        engine="orca",
        handle_worker_start_error_fn=lambda *args: events.append(("start_error", args)),
        finalize_completed_job_fn=lambda *args: events.append(("completed", args)),
        finalize_child_exit_fn=lambda *args, **kwargs: events.append(
            ("child_exit", (args, kwargs))
        ),
        reconcile_worker_state_fn=lambda worker_arg: events.append(("reconcile", worker_arg.name)),
        activate_reserved_slot_fn=lambda *args, **kwargs: events.append(
            ("activate", (args, kwargs))
        ),
        terminate_process_fn=lambda process_arg: events.append(("terminate", process_arg.pid)),
        mark_failed_fn=lambda *args, **kwargs: events.append(("failed", (args, kwargs))),
        shutdown_grace_seconds=10,
        sleep_fn=lambda seconds: events.append(("sleep", seconds)),
        on_worker_process_started_fn=record_started,
        shutdown_running_job_fn=lambda worker_arg, queue_id, job_arg: events.append(
            ("shutdown", (worker_arg.name, queue_id, job_arg.name))
        ),
        before_shutdown_all_fn=lambda worker_arg, count: events.append(
            ("before_shutdown", (worker_arg.name, count))
        ),
    )

    assert hooks.on_worker_process_started(worker, tmp_path, entry, process, "slot-1")
    hooks.shutdown_running_job(worker, "queue-override", job)
    before_shutdown_all = hooks.before_shutdown_all
    assert before_shutdown_all is not None
    before_shutdown_all(worker, 3)
    hooks.reconcile_worker_state(worker)

    assert events == [
        ("started", ("worker", tmp_path, "queue-override", 8642, "slot-1")),
        ("shutdown", ("worker", "queue-override", "job")),
        ("before_shutdown", ("worker", 3)),
        ("reconcile", "worker"),
    ]


def test_engine_queue_runtime_runs_pidfile_worker_command(tmp_path: Path) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(tmp_path),
            admission_root="",
            admission_limit=None,
            max_concurrent=4,
        )
    )
    runtime = EngineQueueRuntime(
        load_config=lambda config: cfg if config == "/tmp/config.yaml" else None,
        runtime_roots_for_cfg=lambda _cfg: (),
        list_queue=lambda _root: [],
        dequeue_next=lambda _root: None,
        worker_pid_file_name="engine_worker.pid",
    )
    calls: list[dict[str, Any]] = []

    class Worker:
        def __init__(self, cfg_arg: Any, config_path: str, **kwargs: Any) -> None:
            calls.append(
                {
                    "cfg": cfg_arg,
                    "config_path": config_path,
                    "kwargs": kwargs,
                }
            )

        def run(self) -> int:
            return 7

    result = runtime.run_pidfile_worker_command(
        SimpleNamespace(config="/tmp/config.yaml"),
        config_path_fn=lambda args: str(args.config),
        worker_factory=Worker,
    )

    assert result == 7
    assert calls == [
        {
            "cfg": cfg,
            "config_path": "/tmp/config.yaml",
            "kwargs": {"max_concurrent": 4},
        }
    ]


def test_engine_queue_runtime_pidfile_command_reports_existing_worker(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(runtime=SimpleNamespace(allowed_root=str(tmp_path)))
    runtime = EngineQueueRuntime(
        load_config=lambda _config: cfg,
        runtime_roots_for_cfg=lambda _cfg: (),
        list_queue=lambda _root: [],
        dequeue_next=lambda _root: None,
        worker_pid_file_name="engine_worker.pid",
    )
    reports: list[int] = []

    result = runtime.run_pidfile_worker_command(
        SimpleNamespace(config="/tmp/config.yaml"),
        config_path_fn=lambda args: str(args.config),
        read_worker_pid_fn=lambda root: 12345 if root == tmp_path else None,
        existing_pid_report_fn=lambda pid: reports.append(pid),
        worker_factory=lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("worker should not start")
        ),
    )

    assert result == 1
    assert reports == [12345]
