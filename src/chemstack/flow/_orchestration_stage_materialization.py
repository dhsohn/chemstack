from __future__ import annotations

from pathlib import Path
from typing import Any


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
    limit = int(params.get("max_xtb_stages", 3) or 3)
    created = 0
    for reactant in reactant_inputs:
        for product in product_inputs:
            created += 1
            if created > limit:
                return created > 1
            stage = o._new_xtb_stage(
                workflow_id=str(payload.get("workflow_id", "")),
                stage_id=f"xtb_path_search_{created:02d}",
                reaction_key=f"{payload.get('reaction_key', 'reaction')}_{created:02d}",
                reactant_input=reactant.to_dict(),
                product_input=product.to_dict(),
                priority=int(params.get("priority", 10) or 10),
                max_cores=int(params.get("max_cores", 8) or 8),
                max_memory_gb=int(params.get("max_memory_gb", 32) or 32),
                max_handoff_retries=int(params.get("max_xtb_handoff_retries", 2) or 2),
                manifest_overrides=o._coerce_mapping(params.get("xtb_job_manifest")),
            )
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
    orca_allowed_root = _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca")
    if orca_allowed_root is None:
        return False
    payload_metadata = o._coerce_mapping(payload.get("metadata"))
    request = o._coerce_mapping(payload_metadata.get("request"))
    params = o._coerce_mapping(request.get("parameters"))
    max_orca_stages = int(params.get("max_orca_stages", 3) or 3)
    candidate_pool: list[tuple[int, int, str, Any]] = []
    handoff_errors: list[dict[str, str]] = []
    payload_metadata = payload.setdefault("metadata", {})
    if isinstance(payload_metadata, dict) and isinstance(payload_metadata.get("workflow_error"), dict):
        if o._normalize_text(payload_metadata["workflow_error"].get("scope")) == "reaction_ts_search_xtb_handoff":
            payload_metadata.pop("workflow_error", None)
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
        inputs = o.select_xtb_downstream_inputs(
            contract,
            policy=o.XtbDownstreamPolicy.build(
                preferred_kinds=("ts_guess",),
                allowed_kinds=("ts_guess",),
                max_candidates=max_orca_stages,
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
        if len(ordered_candidates) >= max_orca_stages:
            break

    existing = [
        stage for stage in payload.get("stages", [])
        if isinstance(stage, dict) and o._normalize_text((stage.get("task") or {}).get("engine")) == "orca"
    ]
    active_statuses = {"planned", "queued", "running", "submitted", "cancel_requested"}
    if any(o._normalize_text(stage.get("status")).lower() in active_statuses for stage in existing):
        return False
    if any(o._normalize_text(stage.get("status")).lower() == "completed" for stage in existing):
        return False

    attempted_paths = {
        o._reaction_orca_source_candidate_path(stage)
        for stage in existing
        if o._reaction_orca_source_candidate_path(stage)
    }
    remaining_candidates = [
        candidate for candidate in ordered_candidates
        if o._normalize_text(candidate.artifact_path) not in attempted_paths
    ]

    if not existing:
        if not remaining_candidates:
            if handoff_errors and isinstance(payload_metadata, dict):
                payload_metadata["workflow_error"] = {
                    "status": "failed",
                    "scope": "reaction_ts_search_xtb_handoff",
                    **handoff_errors[0],
                }
            return False
        next_candidate = remaining_candidates[0]
    else:
        latest_stage = existing[-1]
        if not o._reaction_orca_allows_next_candidate(latest_stage):
            return False
        latest_metadata = o._stage_metadata(latest_stage)
        if not remaining_candidates:
            latest_metadata["reaction_candidate_status"] = "exhausted"
            latest_metadata["reaction_candidate_exhausted_at"] = o.now_utc_iso()
            if isinstance(payload_metadata, dict):
                payload_metadata["workflow_error"] = {
                    "status": "failed",
                    "scope": "reaction_ts_search_orca_candidate_exhausted",
                    "stage_id": o._normalize_text(latest_stage.get("stage_id")),
                    "reason": "orca_ts_guess_exhausted",
                    "message": "ORCA exhausted all ranked xTB TS guesses without finding a valid transition state.",
                }
            return False
        next_candidate = remaining_candidates[0]
        latest_metadata["reaction_candidate_status"] = "superseded"
        latest_metadata["reaction_candidate_superseded_at"] = o.now_utc_iso()
        latest_metadata["reaction_next_candidate_path"] = o._normalize_text(next_candidate.artifact_path)
        latest_metadata["reaction_next_candidate_rank"] = o._safe_int(next_candidate.rank, default=0)
        if isinstance(payload_metadata, dict) and isinstance(payload_metadata.get("workflow_error"), dict):
            if o._normalize_text(payload_metadata["workflow_error"].get("scope")) == "reaction_ts_search_orca_candidate_exhausted":
                payload_metadata.pop("workflow_error", None)

    next_index = len(existing) + 1
    stage = o.build_materialized_orca_stage(
        workflow_id=str(payload.get("workflow_id", "")),
        template_name="reaction_ts_search",
        stage_id=f"orca_optts_freq_{next_index:02d}",
        stage_key=f"{next_index:02d}_{o.safe_name(next_candidate.kind, fallback='candidate')}",
        stage_root_name=f"workflow_jobs/{payload.get('workflow_id', '')}/stage_03_orca",
        workspace_dir=orca_allowed_root,
        input_artifact_kind="xtb_candidate",
        candidate=next_candidate,
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
    stage_metadata["reaction_remaining_candidates_after_this"] = max(0, len(remaining_candidates) - 1)
    payload.setdefault("stages", []).append(stage)
    return True


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
    orca_allowed_root = _call_engine_aware(o._load_config_root, orca_auto_config, engine="orca")
    if crest_contract is None or orca_allowed_root is None:
        return False
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
            stage_root_name=f"workflow_jobs/{payload.get('workflow_id', '')}/stage_02_orca",
            workspace_dir=orca_allowed_root,
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
