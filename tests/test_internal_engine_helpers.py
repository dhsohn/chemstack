from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chemstack.core.queue.internal_engine import InternalEngineQueueRuntime, InternalEngineSpec
from chemstack.core.queue.types import QueueStatus


def test_internal_engine_worker_child_preserves_legacy_admission_parser_arg() -> None:
    child = InternalEngineSpec(
        engine="demo",
        worker_job_module="chemstack.demo.worker_execution",
        include_legacy_admission_root_arg=True,
    ).worker_child(RuntimeError)

    args = child.build_parser().parse_args(
        [
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
    )

    assert args.admission_root == "/tmp/admission"
    assert args.admission_token == "slot-1"


def test_internal_engine_lifecycle_policy_can_coerce_roots_and_recover_job_entry(
    tmp_path: Path,
) -> None:
    cfg = object()
    current_entry = SimpleNamespace(queue_id="queue-1", status=QueueStatus.RUNNING)
    job_entry = SimpleNamespace(queue_id="original", status=QueueStatus.RUNNING)
    job = SimpleNamespace(
        queue_root=tmp_path / "queue",
        entry=job_entry,
        admission_token="slot-1",
    )
    requeued: list[tuple[str, str]] = []
    recovery: list[tuple[object, object, str]] = []
    released: list[str] = []

    InternalEngineSpec(engine="xtb", coerce_queue_root_to_str=True).lifecycle().finalize_child_exit(
        cfg,
        job,
        rc=0,
        shutdown_requested=True,
        find_queue_entry_fn=lambda _root, _queue_id: current_entry,
        mark_cancelled_fn=lambda *args, **kwargs: None,
        requeue_running_entry_fn=lambda root, queue_id: requeued.append((root, queue_id)),
        mark_failed_fn=lambda *args, **kwargs: None,
        mark_recovery_pending_fn=lambda cfg_obj, entry_obj, *, reason: recovery.append(
            (cfg_obj, entry_obj, reason)
        ),
        release_admission_slot_fn=lambda token: released.append(token),
    )

    assert requeued == [(str(tmp_path / "queue"), "queue-1")]
    assert recovery == [(cfg, job_entry, "worker_shutdown")]
    assert released == ["slot-1"]


def test_internal_engine_admission_uses_engine_identity() -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            admission_root="/tmp/admission",
            admission_limit=2,
        )
    )
    calls: list[tuple[str, int, str, str]] = []

    def reserve_slot(root: str, limit: int, *, source: str, app_name: str) -> str:
        calls.append((root, limit, source, app_name))
        return "slot-1"

    token = InternalEngineSpec(engine="demo-engine").admission().reserve_admission_slot(
        cfg,
        reserve_slot_fn=reserve_slot,
    )

    assert token == "slot-1"
    assert calls == [
        ("/tmp/admission", 2, "chemstack.demo_engine.queue_worker", "chemstack_demo_engine")
    ]


def test_internal_engine_queue_runtime_worker_command_uses_late_bound_namespace(
    tmp_path: Path,
) -> None:
    cfg = SimpleNamespace(
        runtime=SimpleNamespace(
            allowed_root=str(tmp_path),
            max_concurrent=3,
        )
    )
    runtime = InternalEngineQueueRuntime.create(
        spec=InternalEngineSpec(engine="demo", worker_pid_file_name="demo_worker.pid"),
        load_config=lambda _path: cfg,
        runtime_roots_for_cfg=lambda _cfg: (),
        list_queue=lambda _root: [],
        dequeue_next=lambda _root: None,
    )
    seen: list[tuple[object, str, int]] = []

    class Worker:
        def __init__(
            self,
            cfg_arg: object,
            *,
            config_path: str,
            max_concurrent: int,
        ) -> None:
            seen.append((cfg_arg, config_path, max_concurrent))

        def run(self) -> int:
            return 19

    namespace = {
        "load_config": lambda _path: cfg,
        "read_worker_pid": lambda _root: None,
        "QueueWorker": object,
    }
    namespace["QueueWorker"] = Worker

    result = runtime.run_pidfile_worker_command_from_namespace(
        SimpleNamespace(config="/tmp/demo.yaml"),
        namespace=namespace,
        config_path_fn=lambda args: args.config,
    )

    assert result == 19
    assert seen == [(cfg, "/tmp/demo.yaml", 3)]
