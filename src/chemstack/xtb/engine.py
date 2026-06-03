from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.config.engines import load_xtb_config
from chemstack.core.engines import (
    EngineArtifactAdapter,
    EngineDefinition,
    EngineNotificationHooks,
    EngineQueueFunctions,
    EngineRunnerCallbacks,
)
from chemstack.core.engines.artifacts import (
    build_engine_artifact_payload,
    build_engine_report_markdown,
    load_engine_artifact_payload,
)
from chemstack.core.engines.worker_child import build_worker_child_command as _build_child
from chemstack.core.indexing.roots import runtime_roots_for_cfg
from chemstack.core.notifications.engines import (
    notify_xtb_job_finished,
    notify_xtb_job_started,
)
from chemstack.core.queue import dequeue_next, list_queue
from chemstack.core.queue.child_execution import find_queue_entry_by_id


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
    admission_root: str | Path | None = None,
) -> list[str]:
    return _build_child(
        engine="xtb",
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
        admission_root=admission_root,
    )


def _run_worker_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_token: str | None = None,
    **_unused: Any,
) -> int:
    from chemstack.core.engines.xtb_execution import run_worker_job

    return run_worker_job(
        config_path=config_path,
        queue_root=queue_root,
        queue_id=queue_id,
        admission_token=admission_token,
    )


def _queue_worker_main(argv: list[str]) -> int:
    from .queue_runtime import main

    return main(argv)


def _queue_entry_by_id(queue_root: str | Path, queue_id: str) -> Any | None:
    return find_queue_entry_by_id(
        queue_root,
        queue_id,
        list_queue_fn=lambda root: list_queue(Path(root)),
    )


ENGINE_DEFINITION = EngineDefinition(
    engine="xtb",
    load_config=load_xtb_config,
    run_worker_child_job=_run_worker_child_job,
    queue_worker_module="chemstack.core.engines.queue_worker",
    worker_pid_file_name="xtb_queue_worker.pid",
    build_worker_child_command=build_worker_child_command,
    runtime_roots_for_cfg=lambda cfg: runtime_roots_for_cfg(cfg, engine="xtb"),
    queue_functions=EngineQueueFunctions(
        runtime_roots_for_cfg=lambda cfg: runtime_roots_for_cfg(cfg, engine="xtb"),
        list_queue=lambda root: list_queue(root),
        dequeue_next=lambda root: dequeue_next(root),
        queue_entry_by_id=_queue_entry_by_id,
        worker_pid_file_name="xtb_queue_worker.pid",
    ),
    runner_callbacks=EngineRunnerCallbacks(
        run_worker_child_job=_run_worker_child_job,
        build_worker_child_command=build_worker_child_command,
    ),
    artifact_adapter=EngineArtifactAdapter(
        build_payload=build_engine_artifact_payload,
        load_payload=load_engine_artifact_payload,
        build_report_markdown=build_engine_report_markdown,
    ),
    notification_hooks=EngineNotificationHooks(
        job_started=notify_xtb_job_started,
        job_finished=notify_xtb_job_finished,
    ),
    queue_worker_runner=_queue_worker_main,
)


__all__ = ["ENGINE_DEFINITION", "build_worker_child_command"]
