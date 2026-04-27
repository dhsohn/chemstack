from __future__ import annotations

from pathlib import Path
from typing import Any

from .state import workflow_workspace_internal_engine_paths


def _orchestration_module():
    from . import orchestration as o

    return o


def _call_engine_aware(func: Any, config_path: str | None, *, engine: str) -> Any:
    try:
        return func(config_path, engine=engine)
    except TypeError as exc:
        if "engine" not in str(exc):
            raise
        return func(config_path)


def append_reaction_xtb_stages_impl(payload: dict[str, Any], *, workspace_dir: Path, crest_auto_config: str | None) -> bool:
    o = _orchestration_module()
    existing = [stage for stage in payload.get("stages", []) if isinstance(stage, dict) and o._normalize_text((stage.get("task") or {}).get("engine")) == "xtb"]
    if existing:
        return False
    roles = o._completed_crest_roles(payload)
    if set(roles.keys()) != {"reactant", "product"}:
        return False
    reactant_contract = o._completed_crest_stage(roles["reactant"], crest_auto_config=crest_auto_config)
    product_contract = o._completed_crest_stage(roles["product"], crest_auto_config=crest_auto_config)
    if reactant_contract is None or product_contract is None:
        return False
    request = ((payload.get("metadata") or {}).get("request") or {})
    params = request.get("parameters") or {}
    reactant_inputs = o.select_crest_downstream_inputs(reactant_contract, policy=o.CrestDownstreamPolicy.build(max_candidates=int(params.get("max_crest_candidates", 3) or 3)))
    product_inputs = o.select_crest_downstream_inputs(product_contract, policy=o.CrestDownstreamPolicy.build(max_candidates=int(params.get("max_crest_candidates", 3) or 3)))
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
    if pairing_policy.enabled:
        payload_metadata = payload.setdefault("metadata", {})
        if isinstance(payload_metadata, dict):
            payload_metadata["endpoint_pairing"] = {
                **pairing_policy.to_summary(),
                "candidate_pair_count": candidate_pair_count,
                "selected_pair_count": len(endpoint_pairs),
            }
            workflow_error = payload_metadata.get("workflow_error")
            if (
                isinstance(workflow_error, dict)
                and o._normalize_text(workflow_error.get("scope"))
                == "reaction_ts_search_endpoint_pairing"
                and endpoint_pairs
            ):
                payload_metadata.pop("workflow_error", None)
    if pairing_policy.enabled and not endpoint_pairs:
        payload_metadata = payload.setdefault("metadata", {})
        if isinstance(payload_metadata, dict):
            payload_metadata["workflow_error"] = {
                "status": "failed",
                "scope": "reaction_ts_search_endpoint_pairing",
                "reason": "no_endpoint_pairs",
                "message": "No CREST reactant/product conformer pair passed endpoint pairing filters.",
            }
        return False

    created = 0
    for endpoint_pair in endpoint_pairs:
        created += 1
        stage = o._new_xtb_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            stage_id=f"xtb_path_search_{created:02d}",
            reaction_key=f"{payload.get('reaction_key', 'reaction')}_{created:02d}",
            reactant_input=endpoint_pair.reactant.to_dict(),
            product_input=endpoint_pair.product.to_dict(),
            priority=int(params.get("priority", 10) or 10),
            max_cores=int(params.get("max_cores", 8) or 8),
            max_memory_gb=int(params.get("max_memory_gb", 32) or 32),
            max_handoff_retries=int(params.get("max_xtb_handoff_retries", 2) or 2),
            manifest_overrides=o._coerce_mapping(params.get("xtb_job_manifest")),
        )
        if pairing_policy.enabled:
            stage_metadata = o._stage_metadata(stage)
            stage_metadata["endpoint_pairing"] = dict(endpoint_pair.metadata)
            task = stage.get("task")
            if isinstance(task, dict):
                task_metadata = task.setdefault("metadata", {})
                if isinstance(task_metadata, dict):
                    task_metadata["endpoint_pairing"] = dict(endpoint_pair.metadata)
        payload.setdefault("stages", []).append(stage)
    return created > 0


def append_reaction_orca_stages_impl(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_auto_config: str | None,
    orca_auto_config: str | None,
) -> bool:
    o = _orchestration_module()
    xtb_stages = [
        stage for stage in payload.get("stages", [])
        if isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == "xtb"
        and (
            o._normalize_text(stage.get("status")) == "completed"
            or o._stage_failure_is_recoverable(stage)
        )
    ]
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
    candidate_pool: list[tuple[int, int, str, Any]] = []
    handoff_errors: list[dict[str, str]] = []
    payload_metadata = payload.setdefault("metadata", {})
    for xtb_stage_index, xtb_stage in enumerate(xtb_stages, start=1):
        task = xtb_stage.get("task")
        if not isinstance(task, dict):
            continue
        payload_dict = o._task_payload_dict(task)
        target = o._normalize_text(payload_dict.get("job_dir")) or o._submission_target(xtb_stage)
        if not target:
            continue
        try:
            contract = o.load_xtb_artifact_contract(xtb_index_root=xtb_allowed_root, target=target)
        except Exception:
            continue
        max_candidate_count = max(
            1,
            len(getattr(contract, "candidate_details", ()) or ()),
            len(getattr(contract, "selected_candidate_paths", ()) or ()),
        )
        inputs = o.select_xtb_downstream_inputs(
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
        stage_metadata = o._stage_metadata(xtb_stage)
        if not inputs:
            error = o._reaction_ts_guess_error(contract)
            stage_metadata["reaction_handoff_status"] = "failed"
            stage_metadata["reaction_handoff_reason"] = error["reason"]
            stage_metadata["reaction_handoff_message"] = error["message"]
            handoff_errors.append(
                {
                    "stage_id": o._normalize_text(xtb_stage.get("stage_id")),
                    "job_id": o._normalize_text(getattr(contract, "job_id", "")),
                    "reason": error["reason"],
                    "message": error["message"],
                }
            )
            continue
        stage_metadata.pop("reaction_handoff_reason", None)
        stage_metadata.pop("reaction_handoff_message", None)
        stage_metadata["reaction_handoff_status"] = "ready"
        for candidate in inputs:
            candidate_path = o._normalize_text(candidate.artifact_path)
            if not candidate_path:
                continue
            candidate_metadata = {
                **dict(candidate.metadata),
                "xtb_stage_id": o._normalize_text(xtb_stage.get("stage_id")),
                "xtb_stage_order": int(xtb_stage_index),
                "xtb_source_job_id": o._normalize_text(getattr(contract, "job_id", "")),
                "xtb_source_job_type": o._normalize_text(getattr(contract, "job_type", "")),
            }
            candidate_pool.append(
                (
                    int(xtb_stage_index),
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

    candidate_pool.sort(key=lambda item: (item[0], item[1], item[2]))
    ordered_candidates: list[Any] = []
    seen_candidate_paths: set[str] = set()
    for _, _, candidate_path, candidate in candidate_pool:
        if candidate_path in seen_candidate_paths:
            continue
        seen_candidate_paths.add(candidate_path)
        ordered_candidates.append(candidate)

    existing = [
        stage for stage in payload.get("stages", [])
        if isinstance(stage, dict) and o._normalize_text((stage.get("task") or {}).get("engine")) == "orca"
    ]
    attempted_paths = {
        o._reaction_orca_source_candidate_path(stage)
        for stage in existing
        if o._reaction_orca_source_candidate_path(stage)
    }
    remaining_candidates = [
        candidate for candidate in ordered_candidates
        if o._normalize_text(candidate.artifact_path) not in attempted_paths
    ]
    active_xtb_statuses = {"planned", "queued", "running", "submitted", "cancel_requested"}
    has_pending_xtb = any(
        isinstance(stage, dict)
        and o._normalize_text((stage.get("task") or {}).get("engine")) == "xtb"
        and (
            o._normalize_text(stage.get("status")).lower() in active_xtb_statuses
            or o._normalize_text(((stage.get("task") or {}).get("status"))).lower() in active_xtb_statuses
        )
        for stage in payload.get("stages", [])
    )

    if not remaining_candidates:
        if not existing and not has_pending_xtb and handoff_errors and isinstance(payload_metadata, dict):
            payload_metadata["workflow_error"] = {
                "status": "failed",
                "scope": "reaction_ts_search_xtb_handoff",
                **handoff_errors[0],
            }
        return False

    if isinstance(payload_metadata, dict) and isinstance(payload_metadata.get("workflow_error"), dict):
        scope = o._normalize_text(payload_metadata["workflow_error"].get("scope"))
        if scope in {"reaction_ts_search_xtb_handoff", "reaction_ts_search_orca_candidate_exhausted"}:
            payload_metadata.pop("workflow_error", None)

    created = 0
    starting_index = len(existing)
    for offset, candidate in enumerate(remaining_candidates, start=1):
        next_index = starting_index + offset
        stage = o.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name="reaction_ts_search",
            stage_id=f"orca_optts_freq_{next_index:02d}",
            stage_key=f"{next_index:02d}_{o.safe_name(candidate.kind, fallback='candidate')}",
            stage_root_name="stage_03_orca",
            workspace_dir=orca_runtime_paths["allowed_root"],
            input_artifact_kind="xtb_candidate",
            candidate=candidate,
            task_kind="optts_freq",
            route_line=str(params.get("orca_route_line", "! r2scan-3c OptTS Freq TightSCF")),
            charge=int(params.get("charge", 0) or 0),
            multiplicity=int(params.get("multiplicity", 1) or 1),
            max_cores=int(params.get("max_cores", 8) or 8),
            max_memory_gb=int(params.get("max_memory_gb", 32) or 32),
            priority=int(params.get("priority", 10) or 10),
            xyz_filename="ts_guess.xyz",
            inp_filename="ts_guess.inp",
        ).to_dict()
        stage_metadata = o._stage_metadata(stage)
        stage_metadata["reaction_candidate_attempt_index"] = next_index
        stage_metadata["reaction_candidate_pool_size"] = len(ordered_candidates)
        stage_metadata["reaction_remaining_candidates_after_this"] = max(0, len(remaining_candidates) - offset)
        payload.setdefault("stages", []).append(stage)
        created += 1
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
    o = _orchestration_module()
    existing = [stage for stage in payload.get("stages", []) if isinstance(stage, dict) and o._normalize_text((stage.get("task") or {}).get("engine")) == "orca"]
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
    if crest_contract is None or _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca") is None:
        return False
    payload_metadata = o._coerce_mapping(payload.get("metadata"))
    workspace_dir_text = o._normalize_text(payload_metadata.get("workspace_dir"))
    workspace_dir = Path(workspace_dir_text).expanduser().resolve() if workspace_dir_text else Path(".").resolve()
    orca_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="orca")
    request = ((payload.get("metadata") or {}).get("request") or {})
    params = request.get("parameters") or {}
    candidates = o.select_crest_downstream_inputs(crest_contract, policy=o.CrestDownstreamPolicy.build(max_candidates=int(params.get("max_orca_stages", 3) or 3)))
    created = 0
    for candidate in candidates:
        created += 1
        stage = o.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name=template_name,
            stage_id=f"{stage_id_prefix}_{created:02d}",
            stage_key=f"{created:02d}_{o.safe_name(candidate.kind, fallback='conformer')}",
            stage_root_name="stage_02_orca",
            workspace_dir=orca_runtime_paths["allowed_root"],
            input_artifact_kind="crest_conformer",
            candidate=candidate,
            task_kind="opt",
            route_line=str(params.get("orca_route_line", "! r2scan-3c Opt TightSCF")),
            charge=int(params.get("charge", 0) or 0),
            multiplicity=int(params.get("multiplicity", 1) or 1),
            max_cores=int(params.get("max_cores", 8) or 8),
            max_memory_gb=int(params.get("max_memory_gb", 32) or 32),
            priority=int(params.get("priority", 10) or 10),
            xyz_filename=xyz_filename,
            inp_filename=inp_filename,
        ).to_dict()
        payload.setdefault("stages", []).append(stage)
    return created > 0

__all__ = [
    "append_crest_orca_stages_impl",
    "append_reaction_orca_stages_impl",
    "append_reaction_xtb_stages_impl",
]
