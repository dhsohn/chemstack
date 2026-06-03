from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from . import _job_location_contract_payload as _contract_payload
from ._job_location_models import (
    JobRuntimeContext,
    OrcaContractPayloadContext,
    OrcaContractResolvedFields,
)


def resolved_contract_fields(
    *,
    runtime: JobRuntimeContext,
    payloads: _contract_payload.RuntimePayloads,
    current_dir: Path | None,
    target: str,
    run_id: str,
    organized_root: str | Path | None,
    deps: Any,
) -> OrcaContractResolvedFields:
    record = payloads.record
    queue_entry = payloads.queue_entry
    state = payloads.state
    report = payloads.report
    organized_ref = payloads.organized_ref
    latest_known_path = _contract_payload.latest_known_path(
        record=record,
        runtime=runtime,
        current_dir=current_dir,
        target=target,
        deps=deps,
    )
    selected_inp, selected_input_xyz, last_out_path, optimized_xyz_path = (
        _contract_payload.selected_artifact_paths(
            record=record,
            state=state,
            report=report,
            organized_ref=organized_ref,
            current_dir=current_dir,
            organized_dir=runtime.organized_dir,
            latest_known_path=latest_known_path,
            deps=deps,
        )
    )
    status, analyzer_status, reason, completed_at = _contract_payload.resolved_status(
        record=record,
        queue_entry=queue_entry,
        state=state,
        report=report,
        deps=deps,
    )
    resource_request, resource_actual = _contract_payload.runtime_resources(
        record=record,
        queue_entry=queue_entry,
        deps=deps,
    )
    return OrcaContractResolvedFields(
        resolved_run_id=_contract_payload.resolved_run_id(
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
        organized_output_dir=_contract_payload.organized_output_dir(
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


def payload_context_from_runtime(
    *,
    runtime: JobRuntimeContext,
    target: str,
    run_id: str,
    reaction_dir: str,
    organized_root: str | Path | None,
    deps: Any,
    resolved_fields_fn: Any | None = None,
) -> OrcaContractPayloadContext:
    payloads = _contract_payload.runtime_payloads(runtime)
    current_dir = _contract_payload.runtime_current_dir(
        runtime,
        queue_entry=payloads.queue_entry,
        reaction_dir=reaction_dir,
        deps=deps,
    )
    field_resolver = resolved_fields_fn or resolved_contract_fields
    resolved = field_resolver(
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
        record=payloads.record,
        queue_entry=payloads.queue_entry,
        state=payloads.state,
        report=payloads.report,
        organized_ref=payloads.organized_ref,
        current_dir=current_dir,
        **asdict(resolved),
    )


def payload_from_context(
    ctx: OrcaContractPayloadContext,
    *,
    queue_id: str,
    deps: Any,
) -> dict[str, Any]:
    if ctx.missing:
        return {}
    payload = _contract_payload.orca_contract_payload(ctx, deps=deps)
    if not payload["queue_id"]:
        payload["queue_id"] = deps.normalize_text(queue_id)
    return payload
