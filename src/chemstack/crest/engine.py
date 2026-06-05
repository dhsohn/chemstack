from __future__ import annotations

from chemstack.core.config.engines import load_crest_config
from chemstack.core.engines import (
    build_lazy_queue_worker_runner,
    build_lazy_worker_child_runner,
    build_queue_engine_definition,
)
from chemstack.core.notifications.engines import (
    notify_crest_job_finished,
    notify_crest_job_started,
)

ENGINE_DEFINITION = build_queue_engine_definition(
    engine="crest",
    load_config=load_crest_config,
    run_worker_child_job=build_lazy_worker_child_runner(
        "chemstack.core.engines.crest_execution",
        "run_worker_child_job",
    ),
    queue_worker_runner=build_lazy_queue_worker_runner("chemstack.crest.queue_runtime"),
    worker_pid_file_name="crest_queue_worker.pid",
    job_started=notify_crest_job_started,
    job_finished=notify_crest_job_finished,
)
build_worker_child_command = ENGINE_DEFINITION.build_worker_child_command


__all__ = ["ENGINE_DEFINITION", "build_worker_child_command"]
