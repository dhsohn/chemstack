from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any


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
    active_queue_root = queue_root or Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
    context = build_context_fn(cfg, entry)
    if check_shutdown_fn is not None:
        check_shutdown_fn(context)
    mark_running_fn(cfg, context)
    if check_shutdown_fn is not None:
        check_shutdown_fn(context)

    result = run_job_fn(cfg, context, active_queue_root)
    organized_output_dir = finalize_entry_fn(
        cfg,
        context,
        result,
        active_queue_root,
        auto_organize,
    )
    return build_outcome_fn(context, result, organized_output_dir)
