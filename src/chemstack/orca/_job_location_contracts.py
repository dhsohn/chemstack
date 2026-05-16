from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord, resolve_job_location

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
    target_text = normalize_text(target)
    if not target_text:
        return []

    candidates: list[Path] = []
    seen: set[Path] = set()
    for record in list_job_location_records(index_root):
        job_dir = resolve_record_job_dir(record)
        if job_dir is None or job_dir in seen:
            continue

        state = load_state(job_dir)
        report = load_report_json(job_dir) or {}
        organized_ref = load_organized_ref(job_dir) or {}

        if not organized_ref:
            original_dir = resolve_existing_job_dir(record.original_run_dir)
            if original_dir is not None and original_dir != job_dir:
                organized_ref = load_organized_ref(original_dir) or {}

        lookup_values = (
            record.job_id,
            report.get("job_id"),
            (state or {}).get("job_id"),
            organized_ref.get("job_id"),
            report.get("run_id"),
            (state or {}).get("run_id"),
            organized_ref.get("run_id"),
        )
        if any(normalize_text(value) == target_text for value in lookup_values):
            seen.add(job_dir)
            candidates.append(job_dir)

    return candidates


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
    resolved_index_root = Path(index_root).expanduser().resolve()

    artifact = _first_artifact_context(resolved_index_root, (target, run_id, reaction_dir))
    queue_entry = _find_queue_entry(
        index_root=resolved_index_root,
        target=target,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
    )

    queue_reaction_dir = resolve_existing_job_dir((queue_entry or {}).get("reaction_dir"))
    if artifact.job_dir is None and queue_reaction_dir is not None:
        artifact = _first_artifact_context(
            resolved_index_root,
            (str(queue_reaction_dir), target, run_id, reaction_dir),
        )

    artifact = _job_artifact_context(
        record=artifact.record,
        job_dir=artifact.job_dir,
        state=artifact.state,
        report=artifact.report,
        organized_ref=_hydrated_organized_ref(artifact),
    )

    state_payload = dict(artifact.state) if isinstance(artifact.state, dict) else {}
    report_payload = dict(artifact.report) if isinstance(artifact.report, dict) else {}
    organized_ref_payload = (
        dict(artifact.organized_ref) if isinstance(artifact.organized_ref, dict) else {}
    )
    current_dir = artifact.job_dir or resolve_existing_job_dir(reaction_dir) or queue_reaction_dir

    resolved_run_id = (
        normalize_text(run_id)
        or normalize_text(state_payload.get("run_id"))
        or normalize_text(report_payload.get("run_id"))
        or normalize_text(organized_ref_payload.get("run_id"))
        or normalize_text((queue_entry or {}).get("run_id"))
    )
    organized_dir = _record_organized_dir(artifact.record)

    if organized_dir is not None and (
        current_dir is None
        or not current_dir.exists()
        or (not state_payload and not report_payload)
    ):
        refreshed = _first_artifact_context(
            resolved_index_root,
            (str(organized_dir), target, resolved_run_id, reaction_dir),
        )
        refreshed_dir = refreshed.job_dir or organized_dir
        artifact = _job_artifact_context(
            record=refreshed.record or artifact.record,
            job_dir=refreshed_dir,
            state=refreshed.state or dict(load_state(refreshed_dir) or {}),
            report=refreshed.report or load_report_json(refreshed_dir),
            organized_ref=_hydrated_organized_ref(refreshed) or load_organized_ref(refreshed_dir),
        )

    return JobRuntimeContext(
        artifact=artifact,
        queue_entry=queue_entry,
        organized_dir=organized_dir,
    )


def _runtime_paths(current_dir: Path | None) -> dict[str, str]:
    return {
        "run_state_path": str((current_dir / STATE_FILE_NAME).resolve())
        if current_dir is not None and (current_dir / STATE_FILE_NAME).exists()
        else "",
        "report_json_path": str((current_dir / REPORT_JSON_NAME).resolve())
        if current_dir is not None and (current_dir / REPORT_JSON_NAME).exists()
        else "",
        "report_md_path": str((current_dir / REPORT_MD_NAME).resolve())
        if current_dir is not None and (current_dir / REPORT_MD_NAME).exists()
        else "",
    }


def _runtime_payloads(
    runtime: JobRuntimeContext,
) -> tuple[
    JobLocationRecord | None,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    artifact = runtime.artifact
    return (
        artifact.record,
        dict(runtime.queue_entry) if isinstance(runtime.queue_entry, dict) else {},
        dict(artifact.state) if isinstance(artifact.state, dict) else {},
        dict(artifact.report) if isinstance(artifact.report, dict) else {},
        dict(artifact.organized_ref) if isinstance(artifact.organized_ref, dict) else {},
    )


def _runtime_current_dir(
    runtime: JobRuntimeContext,
    *,
    queue_entry: dict[str, Any],
    reaction_dir: str,
) -> Path | None:
    return (
        runtime.artifact.job_dir
        or resolve_existing_job_dir(reaction_dir)
        or resolve_existing_job_dir(queue_entry.get("reaction_dir"))
    )


def _resolved_run_id(
    *,
    run_id: str,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    queue_entry: dict[str, Any],
) -> str:
    return (
        normalize_text(run_id)
        or normalize_text(state.get("run_id"))
        or normalize_text(report.get("run_id"))
        or normalize_text(organized_ref.get("run_id"))
        or normalize_text(queue_entry.get("run_id"))
    )


def _latest_known_path(
    *,
    record: JobLocationRecord | None,
    runtime: JobRuntimeContext,
    current_dir: Path | None,
    target: str,
) -> str:
    if record is not None and normalize_text(record.latest_known_path):
        return normalize_text(record.latest_known_path)
    if runtime.organized_dir is not None:
        return str(runtime.organized_dir)
    if current_dir is not None:
        return str(current_dir)
    return normalize_text(target)


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
    selected_inp = resolve_artifact_path(
        state.get("selected_inp")
        or report.get("selected_inp")
        or organized_ref.get("selected_inp")
        or organized_ref.get("selected_input_xyz")
        or (record.selected_input_xyz if record is not None else ""),
        current_dir,
    )
    state_final_result = state.get("final_result")
    state_final = state_final_result if isinstance(state_final_result, dict) else {}
    report_final_result = report.get("final_result")
    report_final = report_final_result if isinstance(report_final_result, dict) else {}
    last_out_path = resolve_artifact_path(
        state_final.get("last_out_path") or report_final.get("last_out_path"),
        current_dir,
    )
    selected_input_xyz = resolve_artifact_path(
        organized_ref.get("selected_input_xyz")
        or (record.selected_input_xyz if record is not None else ""),
        current_dir,
    )
    if not selected_input_xyz.lower().endswith(".xyz"):
        selected_input_xyz = ""
    selected_input_xyz = selected_input_xyz or derive_selected_input_xyz(selected_inp)
    optimized_xyz_path = prefer_orca_optimized_xyz(
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        current_dir=current_dir,
        organized_dir=organized_dir,
        latest_known_path=latest_known_path,
        last_out_path=last_out_path,
    )
    return selected_inp, selected_input_xyz, last_out_path, optimized_xyz_path


def _runtime_resources(
    *,
    record: JobLocationRecord | None,
    queue_entry: dict[str, Any],
) -> tuple[dict[str, int], dict[str, int]]:
    resource_request = resource_dict_from_any(
        queue_entry.get("resource_request")
    ) or resource_dict_from_any(record.resource_request if record is not None else {})
    resource_actual = (
        resource_dict_from_any(queue_entry.get("resource_actual"))
        or resource_dict_from_any(record.resource_actual if record is not None else {})
        or dict(resource_request)
    )
    return resource_request, resource_actual


def _organized_output_dir(
    *,
    record: JobLocationRecord | None,
    organized_ref: dict[str, Any],
    organized_dir: Path | None,
    current_dir: Path | None,
    organized_root: str | Path | None,
) -> str:
    resolved_organized_root = (
        Path(organized_root).expanduser().resolve() if organized_root else None
    )
    return normalize_text(
        (record.organized_output_dir if record is not None else "")
        or organized_ref.get("organized_output_dir")
        or (str(organized_dir) if organized_dir is not None else "")
        or (
            str(current_dir)
            if current_dir is not None and is_subpath(current_dir, resolved_organized_root)
            else ""
        )
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
    status, analyzer_status, reason, completed_at = status_from_payloads(
        queue_entry=queue_entry,
        state=state,
        report=report,
    )
    tracked_status = _tracked_status(record)
    if status == "unknown" and tracked_status:
        status = tracked_status
    return status, analyzer_status, reason, completed_at


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
    return {
        "run_id": ctx.resolved_run_id,
        "status": ctx.status,
        "reason": ctx.reason,
        "state_status": ctx.state_status,
        "reaction_dir": str(current_dir)
        if (current_dir := ctx.current_dir) is not None
        else normalize_text(ctx.reaction_dir),
        "latest_known_path": ctx.latest_known_path,
        "organized_output_dir": ctx.organized_output_dir,
        "optimized_xyz_path": ctx.optimized_xyz_path,
        "queue_id": normalize_text(ctx.queue_entry.get("queue_id") or ""),
        "queue_status": normalize_text(ctx.queue_entry.get("status")).lower(),
        "cancel_requested": normalize_bool(ctx.queue_entry.get("cancel_requested")),
        "selected_inp": ctx.selected_inp,
        "selected_input_xyz": ctx.selected_input_xyz,
        "analyzer_status": ctx.analyzer_status,
        "completed_at": ctx.completed_at,
        "last_out_path": ctx.last_out_path,
        **_runtime_paths(ctx.current_dir),
        "attempt_count": attempt_count(ctx.state, ctx.report),
        "max_retries": max_retries(ctx.state, ctx.report),
        "attempts": coerce_attempts(ctx.state, ctx.report),
        "final_result": final_result_payload(ctx.state, ctx.report),
        "resource_request": ctx.resource_request,
        "resource_actual": ctx.resource_actual,
    }


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
