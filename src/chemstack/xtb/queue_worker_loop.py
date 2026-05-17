from __future__ import annotations

from typing import Any

from chemstack.core.queue.worker import (
    BackgroundRunningJob as RunningJob,
    ChildProcessQueueWorker as QueueWorker,
    build_background_worker_command,
    config_path_for_worker,
    process_one_child_queue as process_one,
    request_job_cancellation as _request_job_cancellation,
    start_background_job_process,
)

__all__ = [
    "QueueWorker",
    "RunningJob",
    "build_background_worker_command",
    "config_path_for_worker",
    "process_one",
    "request_job_cancellation",
    "start_background_job_process",
]


def request_job_cancellation(proc: Any, *, cancel_signal: int, deps: Any) -> None:
    _request_job_cancellation(
        proc,
        cancel_signal=cancel_signal,
        terminate_process_fn=deps._terminate_process,
    )
