from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from chemstack.core.queue import execution as _queue_execution


@dataclass(frozen=True)
class CancellableProcessExecution:
    start_job: Callable[[], Any]
    finalize_job: Callable[..., Any]
    terminate_process: Callable[[Any], Any]
    build_failure_result: Callable[[Exception], Any]
    wait_for_cancellable_process: Callable[..., Any] = _queue_execution.wait_for_cancellable_process
    should_cancel: Callable[[], bool] | None = None
    shutdown_requested: Callable[[], bool] | None = None
    on_shutdown: Callable[[Any], Any] | None = None
    sleep: Callable[[float], None] | None = None
    poll_interval_seconds: float = 1.0
    check_cancel_before_poll: bool = False
    register_running_job: Callable[[Any | None], None] | None = None
    should_reraise_exception: Callable[[Exception], bool] | None = None


def run_cancellable_process_execution(actions: CancellableProcessExecution) -> Any:
    try:
        running = actions.start_job()
        if actions.register_running_job is not None:
            actions.register_running_job(running)
        try:
            wait_kwargs: dict[str, Any] = {
                "finalize_fn": actions.finalize_job,
                "terminate_process_fn": actions.terminate_process,
                "should_cancel": actions.should_cancel,
                "shutdown_requested": actions.shutdown_requested,
                "on_shutdown": actions.on_shutdown,
                "poll_interval_seconds": actions.poll_interval_seconds,
                "check_cancel_before_poll": actions.check_cancel_before_poll,
            }
            if actions.sleep is not None:
                wait_kwargs["sleep_fn"] = actions.sleep
            return actions.wait_for_cancellable_process(running, **wait_kwargs)
        finally:
            if actions.register_running_job is not None:
                actions.register_running_job(None)
    except Exception as exc:
        if actions.should_reraise_exception is not None and actions.should_reraise_exception(exc):
            raise
        return actions.build_failure_result(exc)


__all__ = ["CancellableProcessExecution", "run_cancellable_process_execution"]
