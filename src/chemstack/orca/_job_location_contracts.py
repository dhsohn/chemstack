from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from chemstack.core.indexing import JobLocationRecord

from . import _job_location_artifacts as _artifacts
from . import _job_location_contract_payload as _contract_payload
from . import _job_location_runtime_context as _runtime_context
from ._job_location_records import list_job_location_records, resolve_record_job_dir
from ._job_location_models import (
    JobArtifactContext,
    JobRuntimeContext,
    OrcaContractPayloadContext,
    OrcaContractResolvedFields as _OrcaContractResolvedFields,
)
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
    queue_entry_metadata_value,
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
class _JobLocationDeps:
    JobRuntimeContext: Any
    _runtime_paths: Any
    list_job_location_records: Any
    load_organized_ref: Any
    load_report_json: Any
    load_state: Any
    resolve_record_job_dir: Any
    _first_artifact_context: Any
    _hydrated_organized_ref: Any
    _job_artifact_context: Any
    _record_organized_dir: Any
    final_result_payload: Any
    queue_entry_metadata_value: Any
    status_from_payloads: Any
    _find_queue_entry: Any
    attempt_count: Any
    coerce_attempts: Any
    derive_selected_input_xyz: Any
    is_subpath: Any
    max_retries: Any
    normalize_bool: Any
    normalize_text: Any
    prefer_orca_optimized_xyz: Any
    resolve_artifact_path: Any
    resolve_existing_job_dir: Any
    resource_dict_from_any: Any


def _job_location_deps() -> _JobLocationDeps:
    return _JobLocationDeps(
        JobRuntimeContext=JobRuntimeContext,
        _runtime_paths=_runtime_paths,
        list_job_location_records=list_job_location_records,
        load_organized_ref=load_organized_ref,
        load_report_json=load_report_json,
        load_state=load_state,
        resolve_record_job_dir=resolve_record_job_dir,
        _first_artifact_context=_first_artifact_context,
        _hydrated_organized_ref=_hydrated_organized_ref,
        _job_artifact_context=_job_artifact_context,
        _record_organized_dir=_record_organized_dir,
        final_result_payload=final_result_payload,
        queue_entry_metadata_value=queue_entry_metadata_value,
        status_from_payloads=status_from_payloads,
        _find_queue_entry=_find_queue_entry,
        attempt_count=attempt_count,
        coerce_attempts=coerce_attempts,
        derive_selected_input_xyz=derive_selected_input_xyz,
        is_subpath=is_subpath,
        max_retries=max_retries,
        normalize_bool=normalize_bool,
        normalize_text=normalize_text,
        prefer_orca_optimized_xyz=prefer_orca_optimized_xyz,
        resolve_artifact_path=resolve_artifact_path,
        resolve_existing_job_dir=resolve_existing_job_dir,
        resource_dict_from_any=resource_dict_from_any,
    )


def _first_artifact_context(index_root: str | Path, targets: tuple[str, ...]) -> JobArtifactContext:
    return _artifacts.first_artifact_context(index_root, targets)


def _hydrated_organized_ref(context: JobArtifactContext) -> dict[str, Any] | None:
    return _artifacts.hydrated_organized_ref(context)


def _job_artifact_context(
    *,
    record: JobLocationRecord | None,
    job_dir: Path | None,
    state: dict[str, Any] | None,
    report: dict[str, Any] | None,
    organized_ref: dict[str, Any] | None,
) -> JobArtifactContext:
    return _artifacts.job_artifact_context(
        record=record,
        job_dir=job_dir,
        state=state,
        report=report,
        organized_ref=organized_ref,
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
    entry_run_id = normalize_text(queue_entry_metadata_value(entry, "run_id"))
    entry_reaction_dir = resolve_existing_job_dir(queue_entry_metadata_value(entry, "reaction_dir"))

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
    return _artifacts.record_organized_dir(record)


def resolve_latest_job_dir(index_root: str | Path, target: str) -> Path | None:
    return _artifacts.resolve_latest_job_dir(index_root, target)


def load_job_artifact_context(
    index_root: str | Path,
    target: str,
) -> JobArtifactContext:
    return _artifacts.load_job_artifact_context(index_root, target)


def load_job_runtime_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> JobRuntimeContext:
    return _load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=_job_location_deps(),
    )


def _load_job_runtime_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
    deps: _JobLocationDeps,
) -> JobRuntimeContext:
    return _runtime_context.load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=deps,
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
) -> _contract_payload.RuntimePayloads:
    return _contract_payload.runtime_payloads(runtime)


def _runtime_current_dir(
    runtime: JobRuntimeContext,
    *,
    queue_entry: dict[str, Any],
    reaction_dir: str,
    deps: _JobLocationDeps,
) -> Path | None:
    return _contract_payload.runtime_current_dir(
        runtime,
        queue_entry=queue_entry,
        reaction_dir=reaction_dir,
        deps=deps,
    )


def _resolved_run_id(
    *,
    run_id: str,
    state: dict[str, Any],
    report: dict[str, Any],
    organized_ref: dict[str, Any],
    queue_entry: dict[str, Any],
    deps: _JobLocationDeps,
) -> str:
    return _contract_payload.resolved_run_id(
        run_id=run_id,
        state=state,
        report=report,
        organized_ref=organized_ref,
        queue_entry=queue_entry,
        deps=deps,
    )


def _latest_known_path(
    *,
    record: JobLocationRecord | None,
    runtime: JobRuntimeContext,
    current_dir: Path | None,
    target: str,
    deps: _JobLocationDeps,
) -> str:
    return _contract_payload.latest_known_path(
        record=record,
        runtime=runtime,
        current_dir=current_dir,
        target=target,
        deps=deps,
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
    deps: _JobLocationDeps,
) -> tuple[str, str, str, str]:
    return _contract_payload.selected_artifact_paths(
        record=record,
        state=state,
        report=report,
        organized_ref=organized_ref,
        current_dir=current_dir,
        organized_dir=organized_dir,
        latest_known_path=latest_known_path,
        deps=deps,
    )


def _runtime_resources(
    *,
    record: JobLocationRecord | None,
    queue_entry: dict[str, Any],
    deps: _JobLocationDeps,
) -> tuple[dict[str, int], dict[str, int]]:
    return _contract_payload.runtime_resources(
        record=record,
        queue_entry=queue_entry,
        deps=deps,
    )


def _organized_output_dir(
    *,
    record: JobLocationRecord | None,
    organized_ref: dict[str, Any],
    organized_dir: Path | None,
    current_dir: Path | None,
    organized_root: str | Path | None,
    deps: _JobLocationDeps,
) -> str:
    return _contract_payload.organized_output_dir(
        record=record,
        organized_ref=organized_ref,
        organized_dir=organized_dir,
        current_dir=current_dir,
        organized_root=organized_root,
        deps=deps,
    )


def _resolved_status(
    *,
    record: JobLocationRecord | None,
    queue_entry: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any],
    deps: _JobLocationDeps,
) -> tuple[str, str, str, str]:
    return _contract_payload.resolved_status(
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
        deps=deps,
    )


def _orca_contract_resolved_fields(
    *,
    runtime: JobRuntimeContext,
    payloads: _contract_payload.RuntimePayloads,
    current_dir: Path | None,
    target: str,
    run_id: str,
    organized_root: str | Path | None,
    deps: _JobLocationDeps,
) -> _OrcaContractResolvedFields:
    record = payloads.record
    queue_entry = payloads.queue_entry
    state = payloads.state
    report = payloads.report
    organized_ref = payloads.organized_ref
    latest_known_path = _latest_known_path(
        record=record,
        runtime=runtime,
        current_dir=current_dir,
        target=target,
        deps=deps,
    )
    selected_inp, selected_input_xyz, last_out_path, optimized_xyz_path = _selected_artifact_paths(
        record=record,
        state=state,
        report=report,
        organized_ref=organized_ref,
        current_dir=current_dir,
        organized_dir=runtime.organized_dir,
        latest_known_path=latest_known_path,
        deps=deps,
    )
    status, analyzer_status, reason, completed_at = _resolved_status(
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
        deps=deps,
    )
    resource_request, resource_actual = _runtime_resources(
        record=record,
        queue_entry=queue_entry,
        deps=deps,
    )
    return _OrcaContractResolvedFields(
        resolved_run_id=_resolved_run_id(
            run_id=run_id,
            state=state,
            report=report,
            organized_ref=organized_ref,
            queue_entry=queue_entry,
            deps=deps,
        ),
        latest_known_path=latest_known_path,
        state_status=deps.normalize_text(state.get("status")).lower(),
        status=status,
        analyzer_status=analyzer_status,
        reason=reason,
        completed_at=completed_at,
        selected_inp=selected_inp,
        selected_input_xyz=selected_input_xyz,
        last_out_path=last_out_path,
        optimized_xyz_path=optimized_xyz_path,
        organized_output_dir=_organized_output_dir(
            record=record,
            organized_ref=organized_ref,
            organized_dir=runtime.organized_dir,
            current_dir=current_dir,
            organized_root=organized_root,
            deps=deps,
        ),
        resource_request=resource_request,
        resource_actual=resource_actual,
    )


def _orca_contract_payload_context(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
    deps: _JobLocationDeps,
) -> OrcaContractPayloadContext:
    runtime = _load_job_runtime_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=deps,
    )
    payloads = _runtime_payloads(runtime)
    record = payloads.record
    queue_entry = payloads.queue_entry
    state = payloads.state
    report = payloads.report
    organized_ref = payloads.organized_ref
    current_dir = _runtime_current_dir(
        runtime,
        queue_entry=queue_entry,
        reaction_dir=reaction_dir,
        deps=deps,
    )
    resolved = _orca_contract_resolved_fields(
        runtime=runtime,
        payloads=payloads,
        current_dir=current_dir,
        target=target,
        run_id=run_id,
        organized_root=organized_root,
        deps=deps,
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
        **asdict(resolved),
    )


def _orca_contract_payload(
    ctx: OrcaContractPayloadContext,
    *,
    deps: _JobLocationDeps,
) -> dict[str, Any]:
    return _contract_payload.orca_contract_payload(ctx, deps=deps)


def load_orca_contract_payload(
    index_root: str | Path,
    target: str,
    *,
    organized_root: str | Path | None = None,
    queue_id: str = "",
    run_id: str = "",
    reaction_dir: str = "",
) -> dict[str, Any]:
    deps = _job_location_deps()
    ctx = _orca_contract_payload_context(
        index_root,
        target,
        organized_root=organized_root,
        queue_id=queue_id,
        run_id=run_id,
        reaction_dir=reaction_dir,
        deps=deps,
    )
    if ctx.missing:
        return {}
    payload = _orca_contract_payload(ctx, deps=deps)
    if not payload["queue_id"]:
        payload["queue_id"] = deps.normalize_text(queue_id)
    return payload


def load_job_artifacts(
    index_root: str | Path,
    target: str,
) -> tuple[Path | None, dict[str, Any] | None, dict[str, Any] | None]:
    return _artifacts.load_job_artifacts(index_root, target)
