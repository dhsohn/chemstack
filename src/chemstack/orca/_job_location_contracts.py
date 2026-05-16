from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import sys
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location

from . import _job_location_contract_payload as _contract_payload
from . import _job_location_runtime_context as _runtime_context
from ._job_location_records import list_job_location_records, resolve_record_job_dir
from ._job_location_utils import (
    QUEUE_FILE_NAME,
    attempt_count,
    coerce_attempts,
    derive_selected_input_xyz,
    final_result_payload,
    is_subpath,
    load_json_list,
    max_retries,
    normalize_bool,
    normalize_text,
    prefer_orca_optimized_xyz,
    resolve_artifact_path,
    resolve_existing_job_dir,
    resource_dict_from_any,
    status_from_payloads,
)
from .state import (
    REPORT_JSON_NAME,
    REPORT_MD_NAME,
    STATE_FILE_NAME,
    load_organized_ref,
    load_report_json,
    load_state,
)


def _this_module() -> Any:
    return sys.modules[__name__]


_CONTRACT_PAYLOAD_COMPAT = (
    attempt_count,
    coerce_attempts,
    derive_selected_input_xyz,
    final_result_payload,
    is_subpath,
    max_retries,
    normalize_bool,
    prefer_orca_optimized_xyz,
    resolve_artifact_path,
    resource_dict_from_any,
    status_from_payloads,
)

_RUNTIME_CONTEXT_COMPAT = (resolve_record_job_dir,)


@dataclass(frozen=True)
class JobArtifactContext:
    record: JobLocationRecord | None = None
    job_dir: Path | None = None
    state: dict[str, Any] | None = None
    report: dict[str, Any] | None = None
    organized_ref: dict[str, Any] | None = None


@dataclass(frozen=True)
class JobRuntimeContext:
    artifact: JobArtifactContext = field(default_factory=JobArtifactContext)
    queue_entry: dict[str, Any] | None = None
    organized_dir: Path | None = None


@dataclass(frozen=True)
class OrcaContractPayloadContext:
    runtime: JobRuntimeContext
    target: str
    reaction_dir: str
    record: JobLocationRecord | None
    queue_entry: dict[str, Any]
    state: dict[str, Any]
    report: dict[str, Any]
    organized_ref: dict[str, Any]
    current_dir: Path | None
    resolved_run_id: str
    latest_known_path: str
    state_status: str
    status: str
    analyzer_status: str
    reason: str
    completed_at: str
    selected_inp: str
    selected_input_xyz: str
    last_out_path: str
    optimized_xyz_path: str
    organized_output_dir: str
    resource_request: dict[str, int]
    resource_actual: dict[str, int]

    @property
    def missing(self) -> bool:
        return self.record is None and self.current_dir is None and not self.queue_entry


def _record_matches_job_dir(record: JobLocationRecord, job_dir: Path) -> bool:
    resolved_job_dir = job_dir.expanduser().resolve()
    for value in (record.latest_known_path, record.organized_output_dir, record.original_run_dir):
        raw = normalize_text(value)
        if not raw:
            continue
        try:
            resolved = Path(raw).expanduser().resolve()
        except OSError:
            continue
        if resolved == resolved_job_dir:
            return True
    return False


def _first_artifact_context(index_root: str | Path, targets: tuple[str, ...]) -> JobArtifactContext:
    for raw_target in targets:
        target = normalize_text(raw_target)
        if not target:
            continue
        context = load_job_artifact_context(index_root, target)
        if context.job_dir is not None:
            return context
    return JobArtifactContext()


def _hydrated_organized_ref(context: JobArtifactContext) -> dict[str, Any] | None:
    payload = dict(context.organized_ref) if isinstance(context.organized_ref, dict) else None
    if payload:
        return payload
    if context.record is None:
        return payload
    original_dir = resolve_existing_job_dir(context.record.original_run_dir)
    if original_dir is None or original_dir == context.job_dir:
        return payload
    return load_organized_ref(original_dir)


def _job_artifact_context(
    *,
    record: JobLocationRecord | None,
    job_dir: Path | None,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> JobArtifactContext:
    return JobArtifactContext(
        record=record,
        job_dir=job_dir,
        state=dict(state) if isinstance(state, dict) else None,
        report=dict(report) if isinstance(report, dict) else None,
        organized_ref=dict(organized_ref) if isinstance(organized_ref, dict) else None,
    )


def _queue_entry_matches(
    entry: dict[str, Any],
    *,
    target: str,
    queue_id: str,
    run_id: str,
    direct_target: Path | None,
    resolved_reaction_dir: Path | None,
) -> bool:
    entry_queue_id = normalize_text(entry.get("queue_id"))
    entry_task_id = normalize_text(entry.get("task_id"))
    entry_run_id = normalize_text(entry.get("run_id"))
    entry_reaction_dir = resolve_existing_job_dir(entry.get("reaction_dir"))

    return (
        (bool(queue_id) and entry_queue_id == queue_id)
        or (bool(target) and entry_queue_id == target)
        or (bool(target) and entry_task_id == target)
        or (bool(run_id) and entry_run_id == run_id)
        or (bool(target) and entry_run_id == target)
        or (resolved_reaction_dir is not None and entry_reaction_dir == resolved_reaction_dir)
        or (direct_target is not None and entry_reaction_dir == direct_target)
    )


def _find_queue_entry(
    *,
    index_root: Path,
    target: str,
    queue_id: str,
    run_id: str,
    reaction_dir: str,
) -> dict[str, Any] | None:
    entries = load_json_list(index_root / QUEUE_FILE_NAME)
    if not entries:
        return None

    direct_target = resolve_existing_job_dir(target)
    resolved_reaction_dir = resolve_existing_job_dir(reaction_dir)

    for entry in reversed(entries):
        if _queue_entry_matches(
            entry,
            target=target,
            queue_id=queue_id,
            run_id=run_id,
            direct_target=direct_target,
            resolved_reaction_dir=resolved_reaction_dir,
        ):
            return dict(entry)
    return None


def _record_organized_dir(record: JobLocationRecord | None) -> Path | None:
    if record is None:
        return None
    organized_candidate = resolve_existing_job_dir(record.organized_output_dir)
    if organized_candidate is not None and organized_candidate.is_dir():
        return organized_candidate
    latest_known_candidate = resolve_existing_job_dir(record.latest_known_path)
    original_candidate = resolve_existing_job_dir(record.original_run_dir)
    if (
        latest_known_candidate is not None
        and latest_known_candidate.is_dir()
        and latest_known_candidate != original_candidate
    ):
        return latest_known_candidate
    return None


def _organized_job_dir(job_dir: Path) -> Path | None:
    organized_ref = load_organized_ref(job_dir)
    if not organized_ref:
        return None
    organized_dir = resolve_existing_job_dir(organized_ref.get("organized_output_dir"))
    if organized_dir is None or not organized_dir.is_dir():
        return None
    return organized_dir


def _matching_tracked_job_dirs(index_root: str | Path, target: str) -> list[Path]:
    return _runtime_context.matching_tracked_job_dirs(
        index_root,
        target,
        deps=_this_module(),
    )


def _job_dir_candidates(index_root: str | Path, target: str) -> list[Path]:
    record = resolve_job_location(index_root, target)
    raw_candidates: list[Any] = []
    if record is not None:
        raw_candidates.extend(
            [record.latest_known_path, record.organized_output_dir, record.original_run_dir]
        )
    raw_candidates.append(target)

    candidates: list[Path] = []
    seen: set[Path] = set()
    for value in raw_candidates:
        candidate = resolve_existing_job_dir(value)
        if candidate is None or not candidate.is_dir():
            continue
        for resolved in (_organized_job_dir(candidate), candidate):
            if resolved is None or resolved in seen:
                continue
            seen.add(resolved)
            candidates.append(resolved)

    for candidate in _matching_tracked_job_dirs(index_root, target):
        if candidate in seen:
            continue
        seen.add(candidate)
        candidates.append(candidate)
    return candidates


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    candidates = _job_dir_candidates(index_root, target)
    return candidates[0] if candidates else None


def _record_for_job_dir(
    index_root: str | Path, target: str, job_dir: Path
) -> JobLocationRecord | None:
    record = resolve_job_location(index_root, target)
    if record is not None:
        return record
    for candidate_record in list_job_location_records(index_root):
        if _record_matches_job_dir(candidate_record, job_dir):
            return candidate_record
    return None


def _first_state_report(
    candidates: list[Path],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    state_payload: dict[str, Any] | None = None
    report_payload: dict[str, Any] | None = None
    for job_dir in candidates:
        if state_payload is None:
            state = load_state(job_dir)
            state_payload = dict(state) if state is not None else None
        if report_payload is None:
            report_payload = load_report_json(job_dir)
        if state_payload is not None and report_payload is not None:
            break
    return state_payload, report_payload


def _organized_ref_for_primary_dir(
    record: JobLocationRecord | None, primary_dir: Path
) -> dict[str, Any] | None:
    organized_ref_payload = load_organized_ref(primary_dir)
    if organized_ref_payload or record is None:
        return organized_ref_payload
    original_dir = resolve_existing_job_dir(record.original_run_dir)
    if original_dir is None or original_dir == primary_dir:
        return organized_ref_payload
    return load_organized_ref(original_dir)


def load_job_artifact_context(
    index_root: str | Path,
    target: str,
) -> JobArtifactContext:
    candidates = _job_dir_candidates(index_root, target)
    if not candidates:
        return JobArtifactContext()

    primary_dir = candidates[0]
    record = _record_for_job_dir(index_root, target, primary_dir)
    state_payload, report_payload = _first_state_report(candidates)
    organized_ref_payload = _organized_ref_for_primary_dir(record, primary_dir)

    return JobArtifactContext(
        record=record,
        job_dir=primary_dir,
        state=state_payload,
        report=report_payload,
        organized_ref=organized_ref_payload,
    )


def load_job_runtime_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> JobRuntimeContext:
    return _runtime_context.load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=_this_module(),
    )


def _runtime_paths(current_dir: Path | None) -> dict[str, str]:
    return _contract_payload.runtime_paths(
        current_dir,
        state_file_name=STATE_FILE_NAME,
        report_json_name=REPORT_JSON_NAME,
        report_md_name=REPORT_MD_NAME,
    )


def _runtime_payloads(
    runtime: JobRuntimeContext,
) -> tuple[
    JobLocationRecord | None,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    return _contract_payload.runtime_payloads(runtime)


def _runtime_current_dir(
    runtime: JobRuntimeContext,
    *,
    queue_entry: dict[str, Any],
    reaction_dir: str,
) -> Path | None:
    return _contract_payload.runtime_current_dir(
        runtime,
        queue_entry=queue_entry,
        reaction_dir=reaction_dir,
        deps=_this_module(),
    )


def _resolved_run_id(
    *,
    run_id: str,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    queue_entry: dict[str, Any],
) -> str:
    return _contract_payload.resolved_run_id(
        run_id=run_id,
        state=state,
        report=report,
        organized_ref=organized_ref,
        queue_entry=queue_entry,
        deps=_this_module(),
    )


def _latest_known_path(
    *,
    record: JobLocationRecord | None,
    runtime: JobRuntimeContext,
    current_dir: Path | None,
    target: str,
) -> str:
    return _contract_payload.latest_known_path(
        record=record,
        runtime=runtime,
        current_dir=current_dir,
        target=target,
        deps=_this_module(),
    )


def _selected_artifact_paths(
    *,
    record: JobLocationRecord | None,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    current_dir: Path | None,
    organized_dir: Path | None,
    latest_known_path: str,
) -> tuple[str, str, str, str]:
    return _contract_payload.selected_artifact_paths(
        record=record,
        state=state,
        report=report,
        organized_ref=organized_ref,
        current_dir=current_dir,
        organized_dir=organized_dir,
        latest_known_path=latest_known_path,
        deps=_this_module(),
    )


def _runtime_resources(
    *,
    record: JobLocationRecord | None,
    queue_entry: dict[str, Any],
) -> tuple[dict[str, int], dict[str, int]]:
    return _contract_payload.runtime_resources(
        record=record,
        queue_entry=queue_entry,
        deps=_this_module(),
    )


def _organized_output_dir(
    *,
    record: JobLocationRecord | None,
    organized_ref: dict[str, Any],
    organized_dir: Path | None,
    current_dir: Path | None,
    organized_root: str | Path | None,
) -> str:
    return _contract_payload.organized_output_dir(
        record=record,
        organized_ref=organized_ref,
        organized_dir=organized_dir,
        current_dir=current_dir,
        organized_root=organized_root,
        deps=_this_module(),
    )


def _tracked_status(record: JobLocationRecord | None) -> str:
    return normalize_text(record.status if record is not None else "").lower()


def _resolved_status(
    *,
    record: JobLocationRecord | None,
    queue_entry: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any],
) -> tuple[str, str, str, str]:
    return _contract_payload.resolved_status(
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
        deps=_this_module(),
    )


def _orca_contract_payload_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> OrcaContractPayloadContext:
    runtime = load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    record, queue_entry, state, report, organized_ref = _runtime_payloads(runtime)
    current_dir = _runtime_current_dir(
        runtime,
        queue_entry=queue_entry,
        reaction_dir=reaction_dir,
    )

    resolved_run_id = _resolved_run_id(
        run_id=run_id,
        state=state,
        report=report,
        organized_ref=organized_ref,
        queue_entry=queue_entry,
    )
    latest_known_path = _latest_known_path(
        record=record,
        runtime=runtime,
        current_dir=current_dir,
        target=target,
    )
    state_status = normalize_text(state.get("status")).lower()
    status, analyzer_status, reason, completed_at = _resolved_status(
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
    )

    selected_inp, selected_input_xyz, last_out_path, optimized_xyz_path = _selected_artifact_paths(
        record=record,
        state=state,
        report=report,
        organized_ref=organized_ref,
        current_dir=current_dir,
        organized_dir=runtime.organized_dir,
        latest_known_path=latest_known_path,
    )
    resource_request, resource_actual = _runtime_resources(
        record=record,
        queue_entry=queue_entry,
    )
    organized_output_dir = _organized_output_dir(
        record=record,
        organized_ref=organized_ref,
        organized_dir=runtime.organized_dir,
        current_dir=current_dir,
        organized_root=organized_root,
    )

    return OrcaContractPayloadContext(
        runtime=runtime,
        target=target,
        reaction_dir=reaction_dir,
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
        organized_ref=organized_ref,
        current_dir=current_dir,
        resolved_run_id=resolved_run_id,
        latest_known_path=latest_known_path,
        state_status=state_status,
        status=status,
        analyzer_status=analyzer_status,
        reason=reason,
        completed_at=completed_at,
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        last_out_path=last_out_path,
        optimized_xyz_path=optimized_xyz_path,
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def _orca_contract_payload(ctx: OrcaContractPayloadContext) -> dict[str, Any]:
    return _contract_payload.orca_contract_payload(ctx, deps=_this_module())


def load_orca_contract_payload(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> dict[str, Any]:
    ctx = _orca_contract_payload_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )
    if ctx.missing:
        return {}
    payload = _orca_contract_payload(ctx)
    if not payload["queue_id"]:
        payload["queue_id"] = normalize_text(queue_id)
    return payload


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    context = load_job_artifact_context(index_root, target)
    return context.job_dir, context.state, context.report
