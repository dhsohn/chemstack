from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LoadRequest:
    target: str
    queue_id: str
    run_id: str
    reaction_dir: str


@dataclass(frozen=True)
class LoadRoots:
    allowed: Path | None
    organized: Path | None


@dataclass
class LoaderContext:
    tracked_artifact_dir: Path | None
    tracked_dir: Path | None
    tracked_record: Any
    state: dict[str, Any]
    report: dict[str, Any]
    organized_ref: dict[str, Any]
    queue_entry: dict[str, Any] | None
    precomputed_organized_dir: Path | None = None
    current_dir: Path | None = None
    organized_dir: Path | None = None
    resolved_run_id: str = ""


def resolve_roots(
    orca_allowed_root: str | Path | None,
    orca_organized_root: str | Path | None,
    deps: Any,
) -> LoadRoots:
    allowed = (
        deps.path_type(orca_allowed_root).expanduser().resolve() if orca_allowed_root else None
    )
    organized = (
        deps.path_type(orca_organized_root).expanduser().resolve()
        if orca_organized_root
        else None
    )
    return LoadRoots(allowed=allowed, organized=organized)


def load_context(
    request: LoadRequest,
    roots: LoadRoots,
    deps: Any,
) -> LoaderContext:
    runtime_context = deps.tracked_runtime_context_fn(
        index_root=roots.allowed,
        organized_root=roots.organized,
        target=request.target,
        queue_id=request.queue_id,
        run_id=request.run_id,
        reaction_dir=request.reaction_dir,
    )
    if runtime_context is not None:
        context = context_from_runtime(runtime_context)
    else:
        tracked_dir, record, state, report, organized_ref = deps.tracked_artifact_context_fn(
            index_root=roots.allowed,
            targets=(request.target, request.run_id, request.reaction_dir),
        )
        context = LoaderContext(
            tracked_dir, tracked_dir, record, dict(state), dict(report), dict(organized_ref), None
        )
    apply_context_fallbacks(context, request, roots, deps)
    return context


def context_from_runtime(runtime_context: tuple[Any, ...]) -> LoaderContext:
    tracked_dir, record, state, report, organized_ref, queue_entry, organized_dir = runtime_context
    return LoaderContext(
        tracked_artifact_dir=tracked_dir,
        tracked_dir=tracked_dir,
        tracked_record=record,
        state=dict(state),
        report=dict(report),
        organized_ref=dict(organized_ref),
        queue_entry=queue_entry,
        precomputed_organized_dir=organized_dir,
    )


def apply_context_fallbacks(
    context: LoaderContext,
    request: LoadRequest,
    roots: LoadRoots,
    deps: Any,
) -> None:
    if context.tracked_dir is None or context.tracked_record is None:
        fallback_dir, fallback_record = deps.resolve_job_dir_fn(roots.allowed, request.target)
        context.tracked_dir = context.tracked_dir or fallback_dir
        context.tracked_record = context.tracked_record or fallback_record
    if context.queue_entry is None:
        context.queue_entry = deps.find_queue_entry_fn(
            allowed_root=roots.allowed,
            target=request.target,
            queue_id=request.queue_id,
            run_id=request.run_id,
            reaction_dir=request.reaction_dir,
        )


def refresh_context_from_queue_reaction_dir(
    context: LoaderContext,
    roots: LoadRoots,
    deps: Any,
) -> Path | None:
    queue_reaction_dir = deps.resolve_candidate_path_fn(
        (context.queue_entry or {}).get("reaction_dir")
    )
    if context.tracked_artifact_dir is None and queue_reaction_dir is not None:
        refreshed = deps.tracked_artifact_context_fn(
            index_root=roots.allowed, targets=(str(queue_reaction_dir),)
        )
        merge_refreshed_context(context, refreshed)
    return queue_reaction_dir


def merge_refreshed_context(context: LoaderContext, refreshed: tuple[Any, ...]) -> None:
    refreshed_dir, refreshed_record, refreshed_state, refreshed_report, refreshed_organized_ref = (
        refreshed
    )
    context.tracked_artifact_dir = context.tracked_artifact_dir or refreshed_dir
    context.tracked_record = context.tracked_record or refreshed_record
    if not context.state:
        context.state = dict(refreshed_state)
    if not context.report:
        context.report = dict(refreshed_report)
    if not context.organized_ref:
        context.organized_ref = dict(refreshed_organized_ref)


def set_current_dir(
    request: LoadRequest,
    context: LoaderContext,
    queue_reaction_dir: Path | None,
    deps: Any,
) -> None:
    context.current_dir = (
        context.tracked_artifact_dir
        or context.tracked_dir
        or deps.direct_dir_target_fn(request.target)
        or deps.resolve_candidate_path_fn(request.reaction_dir)
        or queue_reaction_dir
    )


def load_context_payloads(context: LoaderContext, deps: Any) -> None:
    if not context.state and context.current_dir is not None:
        context.state = deps.load_json_dict_fn(context.current_dir / "run_state.json")
    if not context.report and context.current_dir is not None:
        context.report = deps.load_json_dict_fn(context.current_dir / "run_report.json")
    if not context.organized_ref and context.current_dir is not None:
        context.organized_ref = deps.load_json_dict_fn(context.current_dir / "organized_ref.json")
    if not context.organized_ref:
        context.organized_ref = deps.load_tracked_organized_ref_fn(
            context.tracked_record, context.current_dir
        )


def resolve_run_id(request: LoadRequest, context: LoaderContext, deps: Any) -> str:
    queue = context.queue_entry or {}
    return (
        deps.normalize_text_fn(request.run_id)
        or deps.normalize_text_fn(context.state.get("run_id"))
        or deps.normalize_text_fn(context.report.get("run_id"))
        or deps.normalize_text_fn(context.organized_ref.get("run_id"))
        or deps.normalize_text_fn(queue.get("run_id"))
    )


def resolve_organized_context(
    request: LoadRequest,
    roots: LoadRoots,
    context: LoaderContext,
    deps: Any,
) -> None:
    if context.precomputed_organized_dir is not None:
        context.organized_dir = context.precomputed_organized_dir
        return
    context.organized_dir = find_organized_dir(request, roots, context, deps)
    if should_refresh_from_organized_dir(context):
        refresh_from_organized_dir(request, roots, context, deps)


def find_organized_dir(
    request: LoadRequest,
    roots: LoadRoots,
    context: LoaderContext,
    deps: Any,
) -> Path | None:
    organized_record = None
    tracked_organized_dir = deps.record_organized_dir_fn(context.tracked_record)
    if tracked_organized_dir is None:
        organized_record = deps.find_organized_record_fn(
            organized_root=roots.organized,
            target=request.target,
            run_id=context.resolved_run_id,
            reaction_dir=str(context.current_dir)
            if context.current_dir is not None
            else request.reaction_dir,
        )
    return tracked_organized_dir or deps.organized_dir_from_record_fn(
        roots.organized, organized_record
    )


def should_refresh_from_organized_dir(context: LoaderContext) -> bool:
    if context.organized_dir is None:
        return False
    return (
        context.current_dir is None
        or not context.current_dir.exists()
        or (not context.state and not context.report)
    )


def refresh_from_organized_dir(
    request: LoadRequest,
    roots: LoadRoots,
    context: LoaderContext,
    deps: Any,
) -> None:
    current_dir = context.organized_dir
    if current_dir is None:
        return
    context.current_dir = current_dir
    refreshed = deps.tracked_artifact_context_fn(
        index_root=roots.allowed,
        targets=(
            str(current_dir),
            request.target,
            context.resolved_run_id,
            request.reaction_dir,
        ),
    )
    refreshed_dir, refreshed_record, refreshed_state, refreshed_report, refreshed_organized_ref = (
        refreshed
    )
    current_dir = refreshed_dir or current_dir
    context.current_dir = current_dir
    context.tracked_record = context.tracked_record or refreshed_record
    context.state = dict(refreshed_state) or deps.load_json_dict_fn(current_dir / "run_state.json")
    context.report = dict(refreshed_report) or deps.load_json_dict_fn(current_dir / "run_report.json")
    context.organized_ref = dict(refreshed_organized_ref) or deps.load_json_dict_fn(
        current_dir / "organized_ref.json"
    )
    if not context.organized_ref:
        context.organized_ref = deps.load_tracked_organized_ref_fn(
            context.tracked_record, current_dir
        )
    context.resolved_run_id = context.resolved_run_id or resolve_run_id(request, context, deps)


__all__ = [
    "LoadRequest",
    "LoadRoots",
    "LoaderContext",
    "load_context",
    "load_context_payloads",
    "refresh_context_from_queue_reaction_dir",
    "resolve_organized_context",
    "resolve_roots",
    "resolve_run_id",
    "set_current_dir",
]
