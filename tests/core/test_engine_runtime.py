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
