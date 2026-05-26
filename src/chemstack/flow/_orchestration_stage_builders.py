from __future__ import annotations

from typing import Any


def attach_endpoint_pairing_metadata(o: Any, stage: dict[str, Any], endpoint_pair: Any) -> None:
    stage_metadata = o.stages._stage_metadata(stage)
    stage_metadata["endpoint_pairing"] = dict(endpoint_pair.metadata)
    task = stage.get("task")
    if not isinstance(task, dict):
        return
    task_metadata = task.setdefault("metadata", {})
    if isinstance(task_metadata, dict):
        task_metadata["endpoint_pairing"] = dict(endpoint_pair.metadata)


def reaction_xtb_stage_kwargs(
    o: Any,
    payload: dict[str, Any],
    params: dict[str, Any],
    endpoint_pair: Any,
    index: int,
) -> dict[str, Any]:
    return {
        "workflow_id": str(payload.get("workflow_id", "")),
        "stage_id": f"xtb_path_search_{index:02d}",
        "reaction_key": f"{payload.get('reaction_key', 'reaction')}_{index:02d}",
        "reactant_input": endpoint_pair.reactant.to_dict(),
        "product_input": endpoint_pair.product.to_dict(),
        "priority": int(params.get("priority", 10) or 10),
        "max_cores": int(params.get("max_cores", 8) or 8),
        "max_memory_gb": int(params.get("max_memory_gb", 32) or 32),
        "max_handoff_retries": int(params.get("max_xtb_handoff_retries", 2) or 2),
        "manifest_overrides": o.stages._coerce_mapping(params.get("xtb_job_manifest")),
    }


def append_reaction_xtb_pair_stages(
    o: Any,
    payload: dict[str, Any],
    params: dict[str, Any],
    *,
    endpoint_pairs: tuple[Any, ...],
    pairing_enabled: bool,
) -> int:
    created = 0
    for endpoint_pair in endpoint_pairs:
        created += 1
        stage = o.stages._new_xtb_stage(
            **reaction_xtb_stage_kwargs(o, payload, params, endpoint_pair, created)
        )
        if pairing_enabled:
            attach_endpoint_pairing_metadata(o, stage, endpoint_pair)
        payload.setdefault("stages", []).append(stage)
    return created


def append_reaction_orca_candidate_stages(
    o: Any,
    payload: dict[str, Any],
    params: dict[str, Any],
    *,
    orca_allowed_root: Any,
    ordered_candidates: list[Any],
    remaining_candidates: list[Any],
    existing: list[dict[str, Any]],
) -> int:
    created = 0
    starting_index = len(existing)
    for offset, candidate in enumerate(remaining_candidates, start=1):
        next_index = starting_index + offset
        stage = o.engines.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name="reaction_ts_search",
            stage_id=f"orca_optts_freq_{next_index:02d}",
            stage_key=f"{next_index:02d}_{o.engines.safe_name(candidate.kind, fallback='candidate')}",
            stage_root_name="",
            workspace_dir=orca_allowed_root,
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
        stage_metadata = o.stages._stage_metadata(stage)
        stage_metadata["reaction_candidate_attempt_index"] = next_index
        stage_metadata["reaction_candidate_pool_size"] = len(ordered_candidates)
        stage_metadata["reaction_remaining_candidates_after_this"] = max(
            0, len(remaining_candidates) - offset
        )
        payload.setdefault("stages", []).append(stage)
        created += 1
    return created


def append_crest_orca_candidate_stages(
    o: Any,
    payload: dict[str, Any],
    params: dict[str, Any],
    *,
    candidates: tuple[Any, ...],
    template_name: str,
    stage_id_prefix: str,
    orca_allowed_root: Any,
    xyz_filename: str,
    inp_filename: str,
) -> int:
    created = 0
    for candidate in candidates:
        created += 1
        stage = o.engines.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name=template_name,
            stage_id=f"{stage_id_prefix}_{created:02d}",
            stage_key=f"{created:02d}_{o.engines.safe_name(candidate.kind, fallback='conformer')}",
            stage_root_name="",
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
    return created
