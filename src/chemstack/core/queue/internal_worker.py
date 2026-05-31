from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .cancellable import run_cancellable_engine_process
from .engine_lifecycle import EngineWorkerLifecycle, run_engine_worker_lifecycle


@dataclass(frozen=True)
class InternalWorkerOptions:
    should_cancel: Callable[[], bool] | None = None
    shutdown_requested: Callable[[], bool] | None = None
    register_running_job: Callable[[Any | None], None] | None = None
    worker_job_pid: int | None = None
    emit_output: bool = False


@dataclass(frozen=True)
class InternalEngineWorkerAdapter:
    build_context: Callable[[Any, Any], Any]
    mark_running: Callable[[Any, Any, InternalWorkerOptions], None]
    run_job: Callable[[Any, Any, Path, InternalWorkerOptions], Any]
    finalize_entry: Callable[[Any, Any, Any, Path, InternalWorkerOptions], Any]
    build_outcome: Callable[[Any, Any, Any], Any] = lambda _context, _result, finalized: finalized
    check_shutdown: Callable[[Any, InternalWorkerOptions], None] | None = None


def raise_if_shutdown_requested(
    context: Any,
    options: InternalWorkerOptions,
    *,
    shutdown_exception_type: type[BaseException],
) -> None:
    if options.shutdown_requested is not None and options.shutdown_requested():
        raise shutdown_exception_type(context)


def run_internal_engine_worker_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    adapter: InternalEngineWorkerAdapter,
    options: InternalWorkerOptions | None = None,
) -> Any:
    active_options = options or InternalWorkerOptions()
    check_shutdown = adapter.check_shutdown
    return run_engine_worker_lifecycle(
        cfg,
        entry,
        queue_root=queue_root,
        lifecycle=EngineWorkerLifecycle(
            build_context=adapter.build_context,
            check_shutdown=(
                None
                if check_shutdown is None
                else lambda context: check_shutdown(context, active_options)
            ),
            mark_running=lambda cfg_obj, context: adapter.mark_running(
                cfg_obj,
                context,
                active_options,
            ),
            run_job=lambda cfg_obj, context, active_queue_root: adapter.run_job(
                cfg_obj,
                context,
                active_queue_root,
                active_options,
            ),
            finalize_entry=lambda cfg_obj, context, result, active_queue_root: (
                adapter.finalize_entry(
                    cfg_obj,
                    context,
                    result,
                    active_queue_root,
                    active_options,
                )
            ),
            build_outcome=adapter.build_outcome,
        ),
    )


def run_internal_cancellable_engine_process(
    context: Any,
    *,
    options: InternalWorkerOptions,
    shutdown_exception_type: type[BaseException],
    start_job: Callable[[], Any],
    finalize_job: Callable[..., Any],
    terminate_process: Callable[[Any], Any],
    build_failure_result: Callable[[Exception], Any],
    wait_for_cancellable_process: Callable[..., Any] | None = None,
    sleep: Callable[[float], None] | None = None,
    poll_interval_seconds: float = 1.0,
    check_cancel_before_poll: bool = False,
    should_reraise_exception: Callable[[Exception], bool] | None = None,
) -> Any:
    def raise_shutdown(_running: Any) -> None:
        raise shutdown_exception_type(context)

    return run_cancellable_engine_process(
        start_job=start_job,
        finalize_job=finalize_job,
        terminate_process=terminate_process,
        build_failure_result=build_failure_result,
        wait_for_cancellable_process=wait_for_cancellable_process,
        should_cancel=options.should_cancel,
        shutdown_requested=options.shutdown_requested,
        on_shutdown=raise_shutdown,
        sleep=sleep,
        poll_interval_seconds=poll_interval_seconds,
        check_cancel_before_poll=check_cancel_before_poll,
        register_running_job=options.register_running_job,
        should_reraise_exception=should_reraise_exception
        or (lambda exc: isinstance(exc, shutdown_exception_type)),
    )


__all__ = [
    "InternalEngineWorkerAdapter",
    "InternalWorkerOptions",
    "raise_if_shutdown_requested",
    "run_internal_cancellable_engine_process",
    "run_internal_engine_worker_entry",
]
