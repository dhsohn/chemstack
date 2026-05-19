from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class EngineWorkerLifecycle:
    build_context: Callable[[Any, Any], Any]
    mark_running: Callable[[Any, Any], None]
    run_job: Callable[[Any, Any, Path], Any]
    finalize_entry: Callable[[Any, Any, Any, Path, bool], Any]
    build_outcome: Callable[[Any, Any, Any], Any]
    check_shutdown: Callable[[Any], None] | None = None


def run_engine_worker_lifecycle(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    auto_organize: bool,
    lifecycle: EngineWorkerLifecycle,
) -> Any:
    active_queue_root = queue_root or Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
    context = lifecycle.build_context(cfg, entry)
    if lifecycle.check_shutdown is not None:
        lifecycle.check_shutdown(context)
    lifecycle.mark_running(cfg, context)
    if lifecycle.check_shutdown is not None:
        lifecycle.check_shutdown(context)

    result = lifecycle.run_job(cfg, context, active_queue_root)
    organized_output_dir = lifecycle.finalize_entry(
        cfg,
        context,
        result,
        active_queue_root,
        auto_organize,
    )
    return lifecycle.build_outcome(context, result, organized_output_dir)


def process_dequeued_engine_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None,
    auto_organize: bool,
    build_context_fn: Callable[[Any, Any], Any],
    check_shutdown_fn: Callable[[Any], None] | None,
    mark_running_fn: Callable[[Any, Any], None],
    run_job_fn: Callable[[Any, Any, Path], Any],
    finalize_entry_fn: Callable[[Any, Any, Any, Path, bool], Any],
    build_outcome_fn: Callable[[Any, Any, Any], Any],
) -> Any:
    return run_engine_worker_lifecycle(
        cfg,
        entry,
        queue_root=queue_root,
        auto_organize=auto_organize,
        lifecycle=EngineWorkerLifecycle(
            build_context=build_context_fn,
            check_shutdown=check_shutdown_fn,
            mark_running=mark_running_fn,
            run_job=run_job_fn,
            finalize_entry=finalize_entry_fn,
            build_outcome=build_outcome_fn,
        ),
    )
