from __future__ import annotations

from chemstack.core.config.engines import load_xtb_config
from chemstack.core.engines import (
    build_lazy_queue_worker_runner,
    build_lazy_worker_child_runner,
    build_queue_engine_definition,
)
from chemstack.core.notifications.engines import (
    notify_xtb_job_finished,
    notify_xtb_job_started,
)

ENGINE_DEFINITION = build_queue_engine_definition(
    engine="xtb",
    load_config=load_xtb_config,
    run_worker_child_job=build_lazy_worker_child_runner(
        "chemstack.core.engines.xtb_execution",
        "run_worker_job",
    ),
    queue_worker_runner=build_lazy_queue_worker_runner("chemstack.xtb.queue_runtime"),
    worker_pid_file_name="xtb_queue_worker.pid",
    job_started=notify_xtb_job_started,
    job_finished=notify_xtb_job_finished,
)
build_worker_child_command = ENGINE_DEFINITION.build_worker_child_command


__all__ = ["ENGINE_DEFINITION", "build_worker_child_command"]
