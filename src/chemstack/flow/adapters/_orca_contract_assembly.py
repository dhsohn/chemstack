from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME
from chemstack.orca import _job_location_contract_payload as _canonical_payload

from . import _orca_contract_status as _contract_status_helpers


def attempt_count_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    safe_int_fn: Callable[[Any, int], int],
) -> int:
    return _contract_status_helpers.attempt_count_impl(
        state,
        report,
        safe_int_fn=safe_int_fn,
    )


def max_retries_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    safe_int_fn: Callable[[Any, int], int],
) -> int:
    return _contract_status_helpers.max_retries_impl(
        state,
        report,
        safe_int_fn=safe_int_fn,
    )


def coerce_attempts_impl(
    state: dict[str, Any],
    report: dict[str, Any],
    *,
    normalize_text_fn: Callable[[Any], str],
    safe_int_fn: Callable[[Any, int], int],
) -> tuple[dict[str, Any], ...]:
    return _contract_status_helpers.coerce_attempts_impl(
        state,
        report,
        normalize_text_fn=normalize_text_fn,
        safe_int_fn=safe_int_fn,
    )


def final_result_payload_impl(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    return _contract_status_helpers.final_result_payload_impl(state, report)


@dataclass(frozen=True)
class OrcaContractLoaderDeps:
    path_type: type[Path]
    normalize_text_fn: Callable[[Any], str]
    normalize_bool_fn: Callable[[Any], bool]
    safe_int_fn: Callable[[Any, int], int]
    tracked_contract_payload_fn: Callable[..., dict[str, Any] | None]
    tracked_runtime_context_fn: Callable[
        ...,
        tuple[
            Path | None,
            Any,
            dict[str, Any],
            dict[str, Any],
            dict[str, Any],
            dict[str, Any] | None,
            Path | None,
        ]
        | None,
    ]
    tracked_artifact_context_fn: Callable[
        ..., tuple[Path | None, Any, dict[str, Any], dict[str, Any], dict[str, Any]]
    ]
    resolve_job_dir_fn: Callable[[Path | None, str], tuple[Path | None, Any]]
    find_queue_entry_fn: Callable[..., dict[str, Any] | None]
    resolve_candidate_path_fn: Callable[[Any], Path | None]
    direct_dir_target_fn: Callable[[str], Path | None]
    record_organized_dir_fn: Callable[[Any], Path | None]
    find_organized_record_fn: Callable[..., dict[str, Any] | None]
    organized_dir_from_record_fn: Callable[[Path | None, dict[str, Any] | None], Path | None]
    load_json_dict_fn: Callable[[Path], dict[str, Any]]
    load_tracked_organized_ref_fn: Callable[[Any, Path | None], dict[str, Any]]
    status_from_payloads_fn: Callable[..., tuple[str, str, str, str]]
    resolve_artifact_path_fn: Callable[[Any, Path | None], str]
    derive_selected_input_xyz_fn: Callable[[str], str]
    prefer_orca_optimized_xyz_fn: Callable[..., str]
    is_subpath_fn: Callable[[Path, Path | None], bool]
    coerce_resource_dict_fn: Callable[[Any], dict[str, int]]
    attempt_count_fn: Callable[[dict[str, Any], dict[str, Any]], int]
    max_retries_fn: Callable[[dict[str, Any], dict[str, Any]], int]
    coerce_attempts_fn: Callable[[dict[str, Any], dict[str, Any]], tuple[dict[str, Any], ...]]
    final_result_payload_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    contract_cls: type


@dataclass(frozen=True)
class _LoadRequest:
    target: str
    queue_id: str
    run_id: str
    reaction_dir: str


@dataclass(frozen=True)
class _LoadRoots:
    allowed: Path | None
    organized: Path | None


@dataclass
class _LoaderContext:
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


@dataclass(frozen=True)
class _ArtifactPaths:
    selected_inp: str
    selected_input_xyz: str
    optimized_xyz_path: str
    last_out_path: str


@dataclass(frozen=True)
class _ContractPayloadContext:
    resolved_run_id: str
    status: str
    reason: str
    state_status: str
    reaction_dir: str
    current_dir: Path | None
    latest_known_path: str
    organized_output_dir: str
    optimized_xyz_path: str
    queue_entry: dict[str, Any]
    selected_inp: str
    selected_input_xyz: str
    analyzer_status: str
    completed_at: str
    last_out_path: str
    state: dict[str, Any]
    report: dict[str, Any]
    resource_request: dict[str, int]
    resource_actual: dict[str, int]


class _ContractPayloadDeps:
    def __init__(self, deps: OrcaContractLoaderDeps) -> None:
        self._deps = deps

    def normalize_text(self, value: Any) -> str:
        return self._deps.normalize_text_fn(value)

    def normalize_bool(self, value: Any) -> bool:
        return self._deps.normalize_bool_fn(value)

    def _runtime_paths(self, current_dir: Path | None) -> dict[str, str]:
        return _metadata_paths(current_dir)

    def attempt_count(self, state: dict[str, Any], report: dict[str, Any]) -> int:
        return self._deps.attempt_count_fn(state, report)

    def max_retries(self, state: dict[str, Any], report: dict[str, Any]) -> int:
        return self._deps.max_retries_fn(state, report)

    def coerce_attempts(
        self, state: dict[str, Any], report: dict[str, Any]
    ) -> tuple[dict[str, Any], ...]:
        return self._deps.coerce_attempts_fn(state, report)

    def final_result_payload(self, state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
        return self._deps.final_result_payload_fn(state, report)


_StatusPayload = _contract_status_helpers.StatusPayload


def status_from_payloads_impl(
    *,
    queue_entry: dict[str, Any] | None,
    state: dict[str, Any],
    report: dict[str, Any],
    normalize_text_fn: Callable[[Any], str],
    normalize_bool_fn: Callable[[Any], bool],
) -> tuple[str, str, str, str]:
    return _contract_status_helpers.status_from_payloads_impl(
        queue_entry=queue_entry,
        state=state,
        report=report,
        normalize_text_fn=normalize_text_fn,
        normalize_bool_fn=normalize_bool_fn,
    )


def _status_payload(
    *,
    queue_entry: dict[str, Any] | None,
    state: dict[str, Any],
    report: dict[str, Any],
    normalize_text_fn: Callable[[Any], str],
    normalize_bool_fn: Callable[[Any], bool],
) -> _StatusPayload:
    return _contract_status_helpers.status_payload(
        queue_entry=queue_entry,
        state=state,
        report=report,
        normalize_text_fn=normalize_text_fn,
        normalize_bool_fn=normalize_bool_fn,
    )


def _final_status_source(state: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    return _contract_status_helpers.final_status_source(state, report)


def _resolve_status(
    final_status: str,
    queue_status: str,
    cancel_requested: bool,
    state_status: str,
    report_status: str,
) -> str:
    return _contract_status_helpers.resolve_status(
        final_status,
        queue_status,
        cancel_requested,
        state_status,
        report_status,
    )


def load_orca_artifact_contract_impl(
    *,
    target: str,
    orca_allowed_root: str | Path | None,
    orca_organized_root: str | Path | None,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
    deps: OrcaContractLoaderDeps,
) -> Any:
    request = _LoadRequest(
        target=target, queue_id=queue_id, run_id=run_id, reaction_dir=reaction_dir
    )
    roots = _resolve_roots(orca_allowed_root, orca_organized_root, deps)
    tracked_payload = _tracked_payload(request, roots, deps)
    if tracked_payload is not None:
        return _contract_from_payload(tracked_payload, request, deps)

    context = _load_context(request, roots, deps)
    queue_reaction_dir = _refresh_context_from_queue_reaction_dir(context, roots, deps)
    _set_current_dir(request, context, queue_reaction_dir, deps)
    _load_context_payloads(context, deps)
    context.resolved_run_id = _resolve_run_id(request, context, deps)
    _resolve_organized_context(request, roots, context, deps)
    return _contract_from_context(request, roots, context, queue_reaction_dir, deps)


def _resolve_roots(
    orca_allowed_root: str | Path | None,
    orca_organized_root: str | Path | None,
    deps: OrcaContractLoaderDeps,
) -> _LoadRoots:
    allowed = (
        deps.path_type(orca_allowed_root).expanduser().resolve() if orca_allowed_root else None
    )
    organized = (
        deps.path_type(orca_organized_root).expanduser().resolve() if orca_organized_root else None
    )
    return _LoadRoots(allowed=allowed, organized=organized)


def _tracked_payload(
    request: _LoadRequest,
    roots: _LoadRoots,
    deps: OrcaContractLoaderDeps,
) -> dict[str, Any] | None:
    return deps.tracked_contract_payload_fn(
        index_root=roots.allowed,
        organized_root=roots.organized,
        target=request.target,
        queue_id=request.queue_id,
        run_id=request.run_id,
        reaction_dir=request.reaction_dir,
    )


def _contract_from_tracked_payload(
    payload: dict[str, Any],
    request: _LoadRequest,
    deps: OrcaContractLoaderDeps,
) -> Any:
    return _contract_from_payload(payload, request, deps)


def _contract_from_payload(
    payload: dict[str, Any],
    request: _LoadRequest,
    deps: OrcaContractLoaderDeps,
) -> Any:
    final_result = payload.get("final_result")
    return deps.contract_cls(
        run_id=deps.normalize_text_fn(payload.get("run_id")),
        status=deps.normalize_text_fn(payload.get("status")) or "unknown",
        reason=deps.normalize_text_fn(payload.get("reason")),
        state_status=deps.normalize_text_fn(payload.get("state_status")),
        reaction_dir=deps.normalize_text_fn(payload.get("reaction_dir") or request.reaction_dir),
        latest_known_path=deps.normalize_text_fn(
            payload.get("latest_known_path") or request.target
        ),
        organized_output_dir=deps.normalize_text_fn(payload.get("organized_output_dir")),
        optimized_xyz_path=deps.normalize_text_fn(payload.get("optimized_xyz_path")),
        queue_id=deps.normalize_text_fn(payload.get("queue_id") or request.queue_id),
        queue_status=deps.normalize_text_fn(payload.get("queue_status")).lower(),
        cancel_requested=deps.normalize_bool_fn(payload.get("cancel_requested")),
        selected_inp=deps.normalize_text_fn(payload.get("selected_inp")),
        selected_input_xyz=deps.normalize_text_fn(payload.get("selected_input_xyz")),
        analyzer_status=deps.normalize_text_fn(payload.get("analyzer_status")),
        completed_at=deps.normalize_text_fn(payload.get("completed_at")),
        last_out_path=deps.normalize_text_fn(payload.get("last_out_path")),
        run_state_path=deps.normalize_text_fn(payload.get("run_state_path")),
        report_json_path=deps.normalize_text_fn(payload.get("report_json_path")),
        report_md_path=deps.normalize_text_fn(payload.get("report_md_path")),
        attempt_count=deps.safe_int_fn(payload.get("attempt_count"), 0),
        max_retries=deps.safe_int_fn(payload.get("max_retries"), 0),
        attempts=_payload_attempts(payload),
        final_result=dict(final_result) if isinstance(final_result, dict) else {},
        resource_request=deps.coerce_resource_dict_fn(payload.get("resource_request")),
        resource_actual=deps.coerce_resource_dict_fn(payload.get("resource_actual")),
    )


def _payload_attempts(payload: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    attempts_payload = payload.get("attempts")
    if not isinstance(attempts_payload, (list, tuple)):
        return ()
    return tuple(dict(item) for item in attempts_payload if isinstance(item, dict))


def _load_context(
    request: _LoadRequest,
    roots: _LoadRoots,
    deps: OrcaContractLoaderDeps,
) -> _LoaderContext:
    runtime_context = deps.tracked_runtime_context_fn(
        index_root=roots.allowed,
        organized_root=roots.organized,
        target=request.target,
        queue_id=request.queue_id,
        run_id=request.run_id,
        reaction_dir=request.reaction_dir,
    )
    if runtime_context is not None:
        context = _context_from_runtime(runtime_context)
    else:
        tracked_dir, record, state, report, organized_ref = deps.tracked_artifact_context_fn(
            index_root=roots.allowed,
            targets=(request.target, request.run_id, request.reaction_dir),
        )
        context = _LoaderContext(
            tracked_dir, tracked_dir, record, dict(state), dict(report), dict(organized_ref), None
        )
    _apply_context_fallbacks(context, request, roots, deps)
    return context


def _context_from_runtime(runtime_context: tuple[Any, ...]) -> _LoaderContext:
    tracked_dir, record, state, report, organized_ref, queue_entry, organized_dir = runtime_context
    return _LoaderContext(
        tracked_artifact_dir=tracked_dir,
        tracked_dir=tracked_dir,
        tracked_record=record,
        state=dict(state),
        report=dict(report),
        organized_ref=dict(organized_ref),
        queue_entry=queue_entry,
        precomputed_organized_dir=organized_dir,
    )


def _apply_context_fallbacks(
    context: _LoaderContext,
    request: _LoadRequest,
    roots: _LoadRoots,
    deps: OrcaContractLoaderDeps,
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


def _refresh_context_from_queue_reaction_dir(
    context: _LoaderContext,
    roots: _LoadRoots,
    deps: OrcaContractLoaderDeps,
) -> Path | None:
    queue_reaction_dir = deps.resolve_candidate_path_fn(
        (context.queue_entry or {}).get("reaction_dir")
    )
    if context.tracked_artifact_dir is None and queue_reaction_dir is not None:
        refreshed = deps.tracked_artifact_context_fn(
            index_root=roots.allowed, targets=(str(queue_reaction_dir),)
        )
        _merge_refreshed_context(context, refreshed)
    return queue_reaction_dir


def _merge_refreshed_context(context: _LoaderContext, refreshed: tuple[Any, ...]) -> None:
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


def _set_current_dir(
    request: _LoadRequest,
    context: _LoaderContext,
    queue_reaction_dir: Path | None,
    deps: OrcaContractLoaderDeps,
) -> None:
    context.current_dir = (
        context.tracked_artifact_dir
        or context.tracked_dir
        or deps.direct_dir_target_fn(request.target)
        or deps.resolve_candidate_path_fn(request.reaction_dir)
        or queue_reaction_dir
    )


def _load_context_payloads(context: _LoaderContext, deps: OrcaContractLoaderDeps) -> None:
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


def _resolve_run_id(
    request: _LoadRequest, context: _LoaderContext, deps: OrcaContractLoaderDeps
) -> str:
    queue = context.queue_entry or {}
    return (
        deps.normalize_text_fn(request.run_id)
        or deps.normalize_text_fn(context.state.get("run_id"))
        or deps.normalize_text_fn(context.report.get("run_id"))
        or deps.normalize_text_fn(context.organized_ref.get("run_id"))
        or deps.normalize_text_fn(queue.get("run_id"))
    )


def _resolve_organized_context(
    request: _LoadRequest,
    roots: _LoadRoots,
    context: _LoaderContext,
    deps: OrcaContractLoaderDeps,
) -> None:
    if context.precomputed_organized_dir is not None:
        context.organized_dir = context.precomputed_organized_dir
        return
    context.organized_dir = _find_organized_dir(request, roots, context, deps)
    if _should_refresh_from_organized_dir(context):
        _refresh_from_organized_dir(request, roots, context, deps)


def _find_organized_dir(
    request: _LoadRequest,
    roots: _LoadRoots,
    context: _LoaderContext,
    deps: OrcaContractLoaderDeps,
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


def _should_refresh_from_organized_dir(context: _LoaderContext) -> bool:
    if context.organized_dir is None:
        return False
    return (
        context.current_dir is None
        or not context.current_dir.exists()
        or (not context.state and not context.report)
    )


def _refresh_from_organized_dir(
    request: _LoadRequest,
    roots: _LoadRoots,
    context: _LoaderContext,
    deps: OrcaContractLoaderDeps,
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
    context.report = dict(refreshed_report) or deps.load_json_dict_fn(
        current_dir / "run_report.json"
    )
    context.organized_ref = dict(refreshed_organized_ref) or deps.load_json_dict_fn(
        current_dir / "organized_ref.json"
    )
    if not context.organized_ref:
        context.organized_ref = deps.load_tracked_organized_ref_fn(
            context.tracked_record, current_dir
        )
    context.resolved_run_id = context.resolved_run_id or _resolve_run_id(request, context, deps)


def _contract_from_context(
    request: _LoadRequest,
    roots: _LoadRoots,
    context: _LoaderContext,
    queue_reaction_dir: Path | None,
    deps: OrcaContractLoaderDeps,
) -> Any:
    return _contract_from_payload(
        _payload_from_context(request, roots, context, queue_reaction_dir, deps),
        request,
        deps,
    )


def _payload_from_context(
    request: _LoadRequest,
    roots: _LoadRoots,
    context: _LoaderContext,
    queue_reaction_dir: Path | None,
    deps: OrcaContractLoaderDeps,
) -> dict[str, Any]:
    latest_known_path = _latest_known_path(request, context, queue_reaction_dir, deps)
    status = _contract_status(context, deps)
    paths = _artifact_paths(context, latest_known_path, deps)
    resource_request, resource_actual = _resource_payloads(context, deps)
    _ensure_orca_record(context.tracked_record)
    queue = dict(context.queue_entry or {})
    if not deps.normalize_text_fn(queue.get("queue_id")) and request.queue_id:
        queue["queue_id"] = request.queue_id
    payload_context = _ContractPayloadContext(
        resolved_run_id=context.resolved_run_id,
        status=status.status,
        reason=status.reason,
        state_status=deps.normalize_text_fn(context.state.get("status")).lower(),
        reaction_dir=request.reaction_dir,
        current_dir=context.current_dir,
        latest_known_path=latest_known_path,
        organized_output_dir=_organized_output_dir(context, roots, deps),
        optimized_xyz_path=paths.optimized_xyz_path,
        queue_entry=queue,
        selected_inp=paths.selected_inp,
        selected_input_xyz=paths.selected_input_xyz,
        analyzer_status=status.analyzer_status,
        completed_at=status.completed_at,
        last_out_path=paths.last_out_path,
        state=context.state,
        report=context.report,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    return _canonical_payload.orca_contract_payload(
        payload_context,
        deps=_ContractPayloadDeps(deps),
    )


def _latest_known_path(
    request: _LoadRequest,
    context: _LoaderContext,
    queue_reaction_dir: Path | None,
    deps: OrcaContractLoaderDeps,
) -> str:
    record_path = (
        deps.normalize_text_fn(context.tracked_record.latest_known_path)
        if context.tracked_record is not None
        else ""
    )
    if record_path:
        return record_path
    if context.organized_dir is not None:
        return str(context.organized_dir)
    if context.current_dir is not None or queue_reaction_dir is not None:
        return str(context.current_dir or queue_reaction_dir)
    return deps.normalize_text_fn(request.target)


def _contract_status(context: _LoaderContext, deps: OrcaContractLoaderDeps) -> _StatusPayload:
    status, analyzer_status, reason, completed_at = deps.status_from_payloads_fn(
        queue_entry=context.queue_entry,
        state=context.state,
        report=context.report,
    )
    tracked_status = deps.normalize_text_fn(
        context.tracked_record.status if context.tracked_record is not None else ""
    ).lower()
    if status == "unknown" and tracked_status:
        status = tracked_status
    return _StatusPayload(status, analyzer_status, reason, completed_at)


def _artifact_paths(
    context: _LoaderContext, latest_known_path: str, deps: OrcaContractLoaderDeps
) -> _ArtifactPaths:
    selected_inp = deps.resolve_artifact_path_fn(
        _selected_input_source(context), context.current_dir
    )
    last_out_path = deps.resolve_artifact_path_fn(_last_out_source(context), context.current_dir)
    selected_input_xyz = deps.resolve_artifact_path_fn(
        _selected_xyz_source(context), context.current_dir
    )
    if not selected_input_xyz.lower().endswith(".xyz"):
        selected_input_xyz = ""
    selected_input_xyz = selected_input_xyz or deps.derive_selected_input_xyz_fn(selected_inp)
    optimized_xyz_path = deps.prefer_orca_optimized_xyz_fn(
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        current_dir=context.current_dir,
        organized_dir=context.organized_dir,
        latest_known_path=latest_known_path,
        last_out_path=last_out_path,
    )
    return _ArtifactPaths(selected_inp, selected_input_xyz, optimized_xyz_path, last_out_path)


def _selected_input_source(context: _LoaderContext) -> Any:
    return (
        context.state.get("selected_inp")
        or context.report.get("selected_inp")
        or context.organized_ref.get("selected_inp")
        or context.organized_ref.get("selected_input_xyz")
        or (context.tracked_record.selected_input_xyz if context.tracked_record is not None else "")
    )


def _selected_xyz_source(context: _LoaderContext) -> Any:
    return context.organized_ref.get("selected_input_xyz") or (
        context.tracked_record.selected_input_xyz if context.tracked_record is not None else ""
    )


def _last_out_source(context: _LoaderContext) -> Any:
    state_final = context.state.get("final_result")
    report_final = context.report.get("final_result")
    state_final_payload = state_final if isinstance(state_final, dict) else {}
    report_final_payload = report_final if isinstance(report_final, dict) else {}
    return state_final_payload.get("last_out_path") or report_final_payload.get("last_out_path")


def _resource_payloads(
    context: _LoaderContext,
    deps: OrcaContractLoaderDeps,
) -> tuple[dict[str, int], dict[str, int]]:
    queue = context.queue_entry or {}
    queue_request = queue.get("resource_request")
    queue_actual = queue.get("resource_actual")
    resource_request = deps.coerce_resource_dict_fn(
        queue_request if isinstance(queue_request, dict) else {}
    ) or deps.coerce_resource_dict_fn(
        context.tracked_record.resource_request if context.tracked_record is not None else {}
    )
    resource_actual = deps.coerce_resource_dict_fn(
        queue_actual if isinstance(queue_actual, dict) else {}
    ) or deps.coerce_resource_dict_fn(
        context.tracked_record.resource_actual if context.tracked_record is not None else {}
    )
    return resource_request, resource_actual or dict(resource_request)


def _ensure_orca_record(tracked_record: Any) -> None:
    if (
        tracked_record is not None
        and tracked_record.app_name
        and tracked_record.app_name != CHEMSTACK_ORCA_APP_NAME
    ):
        raise ValueError(f"Expected chemstack_orca index record, got: {tracked_record.app_name}")


def _organized_output_dir(
    context: _LoaderContext, roots: _LoadRoots, deps: OrcaContractLoaderDeps
) -> str:
    record_output = (
        context.tracked_record.organized_output_dir if context.tracked_record is not None else ""
    )
    current_output = ""
    if context.current_dir is not None and deps.is_subpath_fn(context.current_dir, roots.organized):
        current_output = str(context.current_dir)
    return deps.normalize_text_fn(
        record_output
        or context.organized_ref.get("organized_output_dir")
        or (str(context.organized_dir) if context.organized_dir is not None else "")
        or current_output
    )


def _metadata_paths(current_dir: Path | None) -> dict[str, str]:
    return {
        "run_state_path": _existing_child_path(current_dir, "run_state.json"),
        "report_json_path": _existing_child_path(current_dir, "run_report.json"),
        "report_md_path": _existing_child_path(current_dir, "run_report.md"),
    }


def _existing_child_path(current_dir: Path | None, filename: str) -> str:
    path = current_dir / filename if current_dir is not None else None
    return str(path.resolve()) if path is not None and path.exists() else ""


__all__ = [
    "OrcaContractLoaderDeps",
    "attempt_count_impl",
    "coerce_attempts_impl",
    "final_result_payload_impl",
    "load_orca_artifact_contract_impl",
    "max_retries_impl",
    "status_from_payloads_impl",
]
