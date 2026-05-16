from __future__ import annotations

from pathlib import Path
from typing import Any

from ._orchestration_deps import OrchestrationDeps, orchestration_deps
from . import _orchestration_stage_builders as _stage_builders
from .state import workflow_workspace_internal_engine_paths


def _orchestration_context() -> OrchestrationDeps:
    return orchestration_deps()


def _call_engine_aware(func: Any, config_path: str | None, *, engine: str) -> Any:
    try:
        return func(config_path, engine=engine)
    except TypeError as exc:
        if "engine" not in str(exc):
            raise
        return func(config_path)


def _engine_stages(o: Any, payload: dict[str, Any], engine: str) -> list[dict[str, Any]]:
    return [
        stage
        for stage in payload.get("stages", [])
        if isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == engine
    ]


def _clear_workflow_error_scope(o: Any, payload_metadata: dict[str, Any], scopes: set[str]) -> None:
    workflow_error = payload_metadata.get("workflow_error")
    if (
        isinstance(workflow_error, dict)
        and o._normalize_text(workflow_error.get("scope")) in scopes
    ):
        payload_metadata.pop("workflow_error", None)


def _record_endpoint_pairing_summary(
    o: Any,
    payload: dict[str, Any],
    pairing_policy: Any,
    *,
    candidate_pair_count: int,
    selected_pair_count: int,
) -> None:
    if not pairing_policy.enabled:
        return
    payload_metadata = payload.setdefault("metadata", {})
    if not isinstance(payload_metadata, dict):
        return
    payload_metadata["endpoint_pairing"] = {
        **pairing_policy.to_summary(),
        "candidate_pair_count": candidate_pair_count,
        "selected_pair_count": selected_pair_count,
    }
    if selected_pair_count:
        _clear_workflow_error_scope(o, payload_metadata, {"reaction_ts_search_endpoint_pairing"})


def _record_endpoint_pairing_failure(payload: dict[str, Any]) -> None:
    payload_metadata = payload.setdefault("metadata", {})
    if isinstance(payload_metadata, dict):
        payload_metadata["workflow_error"] = {
            "status": "failed",
            "scope": "reaction_ts_search_endpoint_pairing",
            "reason": "no_endpoint_pairs",
            "message": "No CREST reactant/product conformer pair passed endpoint pairing filters.",
        }


def _completed_or_recoverable_xtb_stages(o: Any, payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        stage
        for stage in payload.get("stages", [])
        if isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == "xtb"
        and (
            o._normalize_text(stage.get("status")) == "completed"
            or o._stage_failure_is_recoverable(stage)
        )
    ]


def _load_xtb_contract_for_stage(
    o: Any, xtb_stage: dict[str, Any], *, xtb_allowed_root: Path
) -> Any | None:
    task = xtb_stage.get("task")
    if not isinstance(task, dict):
        return None
    payload_dict = o._task_payload_dict(task)
    target = o._normalize_text(payload_dict.get("job_dir")) or o._submission_target(xtb_stage)
    if not target:
        return None
    try:
        return o.load_xtb_artifact_contract(xtb_index_root=xtb_allowed_root, target=target)
    except Exception:
        return None


def _xtb_ts_guess_inputs(o: Any, contract: Any) -> list[Any]:
    max_candidate_count = max(
        1,
        len(getattr(contract, "candidate_details", ()) or ()),
        len(getattr(contract, "selected_candidate_paths", ()) or ()),
    )
    return o.select_xtb_downstream_inputs(
        contract,
        policy=o.XtbDownstreamPolicy.build(
            preferred_kinds=("ts_guess",),
            allowed_kinds=("ts_guess",),
            max_candidates=max_candidate_count,
            selected_only=False,
            fallback_to_selected_paths=False,
        ),
        require_geometry=True,
    )


def _record_xtb_handoff_error(o: Any, xtb_stage: dict[str, Any], contract: Any) -> dict[str, str]:
    stage_metadata = o._stage_metadata(xtb_stage)
    error = o._reaction_ts_guess_error(contract)
    stage_metadata["reaction_handoff_status"] = "failed"
    stage_metadata["reaction_handoff_reason"] = error["reason"]
    stage_metadata["reaction_handoff_message"] = error["message"]
    return {
        "stage_id": o._normalize_text(xtb_stage.get("stage_id")),
        "job_id": o._normalize_text(getattr(contract, "job_id", "")),
        "reason": error["reason"],
        "message": error["message"],
    }


def _mark_xtb_handoff_ready(o: Any, xtb_stage: dict[str, Any]) -> None:
    stage_metadata = o._stage_metadata(xtb_stage)
    stage_metadata.pop("reaction_handoff_reason", None)
    stage_metadata.pop("reaction_handoff_message", None)
    stage_metadata["reaction_handoff_status"] = "ready"


def _reaction_orca_candidate_pool_rows(
    o: Any, xtb_stage: dict[str, Any], contract: Any, inputs: list[Any], stage_order: int
) -> list[tuple[int, int, str, Any]]:
    rows: list[tuple[int, int, str, Any]] = []
    for candidate in inputs:
        candidate_path = o._normalize_text(candidate.artifact_path)
        if not candidate_path:
            continue
        candidate_metadata = {
            **dict(candidate.metadata),
            "xtb_stage_id": o._normalize_text(xtb_stage.get("stage_id")),
            "xtb_stage_order": int(stage_order),
            "xtb_source_job_id": o._normalize_text(getattr(contract, "job_id", "")),
            "xtb_source_job_type": o._normalize_text(getattr(contract, "job_type", "")),
        }
        rows.append(
            (
                int(stage_order),
                candidate.rank if candidate.rank > 0 else 10_000,
                candidate_path,
                o.WorkflowStageInput(
                    source_job_id=candidate.source_job_id,
                    source_job_type=candidate.source_job_type,
                    reaction_key=candidate.reaction_key,
                    selected_input_xyz=candidate.selected_input_xyz,
                    rank=candidate.rank,
                    kind=candidate.kind,
                    artifact_path=candidate.artifact_path,
                    selected=candidate.selected,
                    score=candidate.score,
                    metadata=candidate_metadata,
                ),
            )
        )
    return rows


def _collect_reaction_orca_candidates(
    o: Any,
    xtb_stages: list[dict[str, Any]],
    *,
    xtb_allowed_root: Path,
) -> tuple[list[tuple[int, int, str, Any]], list[dict[str, str]]]:
    candidate_pool: list[tuple[int, int, str, Any]] = []
    handoff_errors: list[dict[str, str]] = []
    for xtb_stage_index, xtb_stage in enumerate(xtb_stages, start=1):
        contract = _load_xtb_contract_for_stage(o, xtb_stage, xtb_allowed_root=xtb_allowed_root)
        if contract is None:
            continue
        inputs = _xtb_ts_guess_inputs(o, contract)
        if not inputs:
            handoff_errors.append(_record_xtb_handoff_error(o, xtb_stage, contract))
            continue
        _mark_xtb_handoff_ready(o, xtb_stage)
        candidate_pool.extend(
            _reaction_orca_candidate_pool_rows(o, xtb_stage, contract, inputs, xtb_stage_index)
        )
    return candidate_pool, handoff_errors


def _unique_ordered_candidates(candidate_pool: list[tuple[int, int, str, Any]]) -> list[Any]:
    candidate_pool.sort(key=lambda item: (item[0], item[1], item[2]))
    ordered_candidates: list[Any] = []
    seen_candidate_paths: set[str] = set()
    for _, _, candidate_path, candidate in candidate_pool:
        if candidate_path in seen_candidate_paths:
            continue
        seen_candidate_paths.add(candidate_path)
        ordered_candidates.append(candidate)
    return ordered_candidates


def _has_pending_xtb_stage(o: Any, payload: dict[str, Any]) -> bool:
    active_xtb_statuses = {"planned", "queued", "running", "submitted", "cancel_requested"}
    return any(
        isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == "xtb"
        and (
            o._normalize_text(stage.get("status")).lower() in active_xtb_statuses
            or o._normalize_text(((stage.get("task") or {}).get("status"))).lower()
            in active_xtb_statuses
        )
        for stage in payload.get("stages", [])
    )


def _remaining_orca_candidates(
    o: Any, existing: list[dict[str, Any]], ordered_candidates: list[Any]
) -> list[Any]:
    attempted_paths = {
        o._reaction_orca_source_candidate_path(stage)
        for stage in existing
        if o._reaction_orca_source_candidate_path(stage)
    }
    return [
        candidate
        for candidate in ordered_candidates
        if o._normalize_text(candidate.artifact_path) not in attempted_paths
    ]


def _record_reaction_handoff_failure(
    payload_metadata: dict[str, Any],
    *,
    existing: list[dict[str, Any]],
    has_pending_xtb: bool,
    handoff_errors: list[dict[str, str]],
) -> None:
    if (
        not existing
        and not has_pending_xtb
        and handoff_errors
        and isinstance(payload_metadata, dict)
    ):
        payload_metadata["workflow_error"] = {
            "status": "failed",
            "scope": "reaction_ts_search_xtb_handoff",
            **handoff_errors[0],
        }


def append_reaction_xtb_stages_impl(
    payload: dict[str, Any], *, workspace_dir: Path, crest_auto_config: str | None
) -> bool:
    o = _orchestration_context()
    if _engine_stages(o, payload, "xtb"):
        return False
    roles = o._completed_crest_roles(payload)
    if set(roles.keys()) != {"reactant", "product"}:
        return False
    reactant_contract = o._completed_crest_stage(
        roles["reactant"], crest_auto_config=crest_auto_config
    )
    product_contract = o._completed_crest_stage(
        roles["product"], crest_auto_config=crest_auto_config
    )
    if reactant_contract is None or product_contract is None:
        return False
    request = (payload.get("metadata") or {}).get("request") or {}
    params = request.get("parameters") or {}
    reactant_inputs = o.select_crest_downstream_inputs(
        reactant_contract,
        policy=o.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_crest_candidates", 3) or 3)
        ),
    )
    product_inputs = o.select_crest_downstream_inputs(
        product_contract,
        policy=o.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_crest_candidates", 3) or 3)
        ),
    )
    pairing_policy = o.EndpointPairingPolicy.from_raw(
        params.get("endpoint_pairing"),
        default_max_pairs=int(params.get("max_xtb_stages", 0) or 0),
    )
    endpoint_pairs = o.select_endpoint_pairs(
        reactant_inputs,
        product_inputs,
        policy=pairing_policy,
    )
    candidate_pair_count = len(reactant_inputs) * len(product_inputs)
    _record_endpoint_pairing_summary(
        o,
        payload,
        pairing_policy,
        candidate_pair_count=candidate_pair_count,
        selected_pair_count=len(endpoint_pairs),
    )
    if pairing_policy.enabled and not endpoint_pairs:
        _record_endpoint_pairing_failure(payload)
        return False

    created = _stage_builders.append_reaction_xtb_pair_stages(
        o,
        payload,
        params,
        endpoint_pairs=endpoint_pairs,
        pairing_enabled=pairing_policy.enabled,
    )
    return created > 0


def append_reaction_orca_stages_impl(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_auto_config: str | None,
    orca_auto_config: str | None,
) -> bool:
    o = _orchestration_context()
    xtb_stages = _completed_or_recoverable_xtb_stages(o, payload)
    if not xtb_stages:
        return False
    xtb_allowed_root = _call_engine_aware(o._load_config_root, xtb_auto_config, engine="xtb")
    if xtb_allowed_root is None:
        return False
    if _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca") is None:
        return False
    orca_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="orca")
    payload_metadata = o._coerce_mapping(payload.get("metadata"))
    request = o._coerce_mapping(payload_metadata.get("request"))
    params = o._coerce_mapping(request.get("parameters"))
    payload_metadata = payload.setdefault("metadata", {})
    candidate_pool, handoff_errors = _collect_reaction_orca_candidates(
        o,
        xtb_stages,
        xtb_allowed_root=xtb_allowed_root,
    )
    ordered_candidates = _unique_ordered_candidates(candidate_pool)
    existing = _engine_stages(o, payload, "orca")
    remaining_candidates = _remaining_orca_candidates(o, existing, ordered_candidates)
    has_pending_xtb = _has_pending_xtb_stage(o, payload)

    if not remaining_candidates:
        _record_reaction_handoff_failure(
            payload_metadata,
            existing=existing,
            has_pending_xtb=has_pending_xtb,
            handoff_errors=handoff_errors,
        )
        return False

    if isinstance(payload_metadata, dict) and isinstance(
        payload_metadata.get("workflow_error"), dict
    ):
        _clear_workflow_error_scope(
            o,
            payload_metadata,
            {"reaction_ts_search_xtb_handoff", "reaction_ts_search_orca_candidate_exhausted"},
        )

    created = _stage_builders.append_reaction_orca_candidate_stages(
        o,
        payload,
        params,
        orca_allowed_root=orca_runtime_paths["allowed_root"],
        ordered_candidates=ordered_candidates,
        remaining_candidates=remaining_candidates,
        existing=existing,
    )
    return created > 0


def append_crest_orca_stages_impl(
    payload: dict[str, Any],
    *,
    template_name: str,
    crest_auto_config: str | None,
    orca_auto_config: str | None,
    stage_id_prefix: str,
    xyz_filename: str,
    inp_filename: str,
) -> bool:
    o = _orchestration_context()
    existing = [
        stage
        for stage in payload.get("stages", [])
        if isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == "orca"
    ]
    if existing:
        return False
    crest_stage = next(
        (
            stage
            for stage in payload.get("stages", [])
            if isinstance(stage, dict)
            and o._normalize_text((stage.get("task") or {}).get("engine")) == "crest"
            and o._normalize_text(stage.get("status")) == "completed"
        ),
        None,
    )
    if crest_stage is None:
        return False
    crest_contract = o._completed_crest_stage(crest_stage, crest_auto_config=crest_auto_config)
    if (
        crest_contract is None
        or _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca") is None
    ):
        return False
    payload_metadata = o._coerce_mapping(payload.get("metadata"))
    workspace_dir_text = o._normalize_text(payload_metadata.get("workspace_dir"))
    workspace_dir = (
        Path(workspace_dir_text).expanduser().resolve()
        if workspace_dir_text
        else Path(".").resolve()
    )
    orca_stage_dirname = "02_orca" if template_name == "conformer_screening" else None
    orca_runtime_paths = workflow_workspace_internal_engine_paths(
        workspace_dir,
        engine="orca",
        stage_dirname=orca_stage_dirname,
    )
    request = (payload.get("metadata") or {}).get("request") or {}
    params = request.get("parameters") or {}
    candidates = o.select_crest_downstream_inputs(
        crest_contract,
        policy=o.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_orca_stages", 3) or 3)
        ),
    )
    created = _stage_builders.append_crest_orca_candidate_stages(
        o,
        payload,
        params,
        candidates=candidates,
        template_name=template_name,
        stage_id_prefix=stage_id_prefix,
        orca_allowed_root=orca_runtime_paths["allowed_root"],
        xyz_filename=xyz_filename,
        inp_filename=inp_filename,
    )
    return created > 0


__all__ = [
    "append_crest_orca_stages_impl",
    "append_reaction_orca_stages_impl",
    "append_reaction_xtb_stages_impl",
]
