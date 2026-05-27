from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._orchestration_deps import (
    OrchestrationDeps,
    call_engine_aware as _call_engine_aware,
    orchestration_deps,
)
from .state import workflow_workspace_internal_engine_paths


def _orchestration_context(deps: OrchestrationDeps | None = None) -> OrchestrationDeps:
    return deps or orchestration_deps()


@dataclass(frozen=True)
class WorkflowTaskView:
    raw: dict[str, Any]

    def engine(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("engine")).lower()

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()


@dataclass(frozen=True)
class WorkflowStageView:
    raw: dict[str, Any]

    @classmethod
    def from_raw(cls, value: Any) -> WorkflowStageView | None:
        return cls(value) if isinstance(value, dict) else None

    @property
    def task(self) -> WorkflowTaskView:
        task = self.raw.get("task")
        return WorkflowTaskView(task if isinstance(task, dict) else {})

    def stage_id(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("stage_id"))

    def status(self, o: Any) -> str:
        return o.stages._normalize_text(self.raw.get("status")).lower()

    def task_engine(self, o: Any) -> str:
        return self.task.engine(o)

    def task_status(self, o: Any) -> str:
        return self.task.status(o)


def _stage_views(payload: dict[str, Any]) -> list[WorkflowStageView]:
    return [
        view
        for raw_stage in payload.get("stages", [])
        if (view := WorkflowStageView.from_raw(raw_stage)) is not None
    ]


def _engine_stages(o: Any, payload: dict[str, Any], engine: str) -> list[dict[str, Any]]:
    return [view.raw for view in _stage_views(payload) if view.task_engine(o) == engine]


def _engine_stage_views(
    o: Any,
    payload: dict[str, Any],
    engine: str,
) -> list[WorkflowStageView]:
    return [view for view in _stage_views(payload) if view.task_engine(o) == engine]


@dataclass(frozen=True)
class _ReactionXtbStagePlan:
    params: dict[str, Any]
    endpoint_pairs: tuple[Any, ...]
    pairing_enabled: bool


@dataclass(frozen=True)
class _ReactionOrcaStagePlan:
    payload_metadata: dict[str, Any]
    params: dict[str, Any]
    orca_allowed_root: Path
    ordered_candidates: list[Any]
    remaining_candidates: list[Any]
    existing_stages: list[dict[str, Any]]
    has_pending_xtb: bool
    handoff_errors: list[dict[str, str]]


@dataclass(frozen=True)
class _CrestOrcaStagePlan:
    params: dict[str, Any]
    candidates: tuple[Any, ...]
    orca_allowed_root: Path


def _request_params(o: Any, payload: dict[str, Any]) -> dict[str, Any]:
    metadata = o.stages._coerce_mapping(payload.get("metadata"))
    request = o.stages._coerce_mapping(metadata.get("request"))
    return o.stages._coerce_mapping(request.get("parameters"))


def _clear_workflow_error_scope(o: Any, payload_metadata: dict[str, Any], scopes: set[str]) -> None:
    workflow_error = payload_metadata.get("workflow_error")
    if (
        isinstance(workflow_error, dict)
        and o.stages._normalize_text(workflow_error.get("scope")) in scopes
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
        view.raw
        for view in _engine_stage_views(o, payload, "xtb")
        if view.status(o) == "completed" or o.stages._stage_failure_is_recoverable(view.raw)
    ]


def _load_xtb_contract_for_stage(
    o: Any, xtb_stage: dict[str, Any], *, xtb_allowed_root: Path
) -> Any | None:
    view = WorkflowStageView.from_raw(xtb_stage)
    if view is None or not view.task.raw:
        return None
    payload_dict = o.stages._task_payload_dict(view.task.raw)
    target = o.stages._normalize_text(payload_dict.get("job_dir")) or o.stages._submission_target(xtb_stage)
    if not target:
        return None
    try:
        return o.engines.load_xtb_artifact_contract(xtb_index_root=xtb_allowed_root, target=target)
    except Exception:
        return None


def _xtb_ts_guess_inputs(o: Any, contract: Any) -> list[Any]:
    max_candidate_count = max(
        1,
        len(getattr(contract, "candidate_details", ()) or ()),
        len(getattr(contract, "selected_candidate_paths", ()) or ()),
    )
    return o.engines.select_xtb_downstream_inputs(
        contract,
        policy=o.contracts.XtbDownstreamPolicy.build(
            preferred_kinds=("ts_guess",),
            allowed_kinds=("ts_guess",),
            max_candidates=max_candidate_count,
            selected_only=False,
            fallback_to_selected_paths=False,
        ),
        require_geometry=True,
    )


def _record_xtb_handoff_error(o: Any, xtb_stage: dict[str, Any], contract: Any) -> dict[str, str]:
    stage_metadata = o.stages._stage_metadata(xtb_stage)
    error = o.stages._reaction_ts_guess_error(contract)
    stage_metadata["reaction_handoff_status"] = "failed"
    stage_metadata["reaction_handoff_reason"] = error["reason"]
    stage_metadata["reaction_handoff_message"] = error["message"]
    return {
        "stage_id": WorkflowStageView(xtb_stage).stage_id(o),
        "job_id": o.stages._normalize_text(getattr(contract, "job_id", "")),
        "reason": error["reason"],
        "message": error["message"],
    }


def _mark_xtb_handoff_ready(o: Any, xtb_stage: dict[str, Any]) -> None:
    stage_metadata = o.stages._stage_metadata(xtb_stage)
    stage_metadata.pop("reaction_handoff_reason", None)
    stage_metadata.pop("reaction_handoff_message", None)
    stage_metadata["reaction_handoff_status"] = "ready"


def _reaction_orca_candidate_pool_rows(
    o: Any, xtb_stage: dict[str, Any], contract: Any, inputs: list[Any], stage_order: int
) -> list[tuple[int, int, str, Any]]:
    rows: list[tuple[int, int, str, Any]] = []
    for candidate in inputs:
        candidate_path = o.stages._normalize_text(candidate.artifact_path)
        if not candidate_path:
            continue
        candidate_metadata = {
            **dict(candidate.metadata),
            "xtb_stage_id": WorkflowStageView(xtb_stage).stage_id(o),
            "xtb_stage_order": int(stage_order),
            "xtb_source_job_id": o.stages._normalize_text(getattr(contract, "job_id", "")),
            "xtb_source_job_type": o.stages._normalize_text(getattr(contract, "job_type", "")),
        }
        rows.append(
            (
                int(stage_order),
                candidate.rank if candidate.rank > 0 else 10_000,
                candidate_path,
                o.contracts.WorkflowStageInput(
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
        view.task_engine(o) == "xtb"
        and (view.status(o) or view.task_status(o)) in active_xtb_statuses
        for view in _stage_views(payload)
    )


def _remaining_orca_candidates(
    o: Any, existing: list[dict[str, Any]], ordered_candidates: list[Any]
) -> list[Any]:
    attempted_paths = {
        o.stages._reaction_orca_source_candidate_path(stage)
        for stage in existing
        if o.stages._reaction_orca_source_candidate_path(stage)
    }
    return [
        candidate
        for candidate in ordered_candidates
        if o.stages._normalize_text(candidate.artifact_path) not in attempted_paths
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


def _completed_reaction_crest_contracts(
    o: Any,
    payload: dict[str, Any],
    *,
    crest_config: str | None,
) -> tuple[Any, Any] | None:
    roles = o.stages._completed_crest_roles(payload)
    if set(roles.keys()) != {"reactant", "product"}:
        return None
    reactant_contract = o.stages._completed_crest_stage(
        roles["reactant"], crest_config=crest_config
    )
    product_contract = o.stages._completed_crest_stage(
        roles["product"], crest_config=crest_config
    )
    if reactant_contract is None or product_contract is None:
        return None
    return reactant_contract, product_contract


def _reaction_xtb_stage_plan(
    o: Any,
    payload: dict[str, Any],
    *,
    crest_config: str | None,
) -> _ReactionXtbStagePlan | None:
    contracts = _completed_reaction_crest_contracts(
        o,
        payload,
        crest_config=crest_config,
    )
    if contracts is None:
        return None
    reactant_contract, product_contract = contracts
    params = _request_params(o, payload)
    reactant_inputs = o.engines.select_crest_downstream_inputs(
        reactant_contract,
        policy=o.contracts.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_crest_candidates", 3) or 3)
        ),
    )
    product_inputs = o.engines.select_crest_downstream_inputs(
        product_contract,
        policy=o.contracts.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_crest_candidates", 3) or 3)
        ),
    )
    pairing_policy = o.contracts.EndpointPairingPolicy.from_raw(
        params.get("endpoint_pairing"),
        default_max_pairs=int(params.get("max_xtb_stages", 0) or 0),
    )
    endpoint_pairs = o.engines.select_endpoint_pairs(
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
        return None
    return _ReactionXtbStagePlan(
        params=params,
        endpoint_pairs=endpoint_pairs,
        pairing_enabled=pairing_policy.enabled,
    )


def append_reaction_xtb_stages_impl(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    crest_config: str | None,
    deps: OrchestrationDeps | None = None,
) -> bool:
    o = _orchestration_context(deps)
    if _engine_stages(o, payload, "xtb"):
        return False
    del workspace_dir
    plan = _reaction_xtb_stage_plan(o, payload, crest_config=crest_config)
    if plan is None:
        return False

    created = 0
    for endpoint_pair in plan.endpoint_pairs:
        created += 1
        stage = o.stages._new_xtb_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            stage_id=f"xtb_path_search_{created:02d}",
            reaction_key=f"{payload.get('reaction_key', 'reaction')}_{created:02d}",
            reactant_input=endpoint_pair.reactant.to_dict(),
            product_input=endpoint_pair.product.to_dict(),
            priority=int(plan.params.get("priority", 10) or 10),
            max_cores=int(plan.params.get("max_cores", 8) or 8),
            max_memory_gb=int(plan.params.get("max_memory_gb", 32) or 32),
            max_handoff_retries=int(plan.params.get("max_xtb_handoff_retries", 2) or 2),
            manifest_overrides=o.stages._coerce_mapping(plan.params.get("xtb_job_manifest")),
        )
        if plan.pairing_enabled:
            pairing_metadata = dict(endpoint_pair.metadata)
            stage_metadata = o.stages._stage_metadata(stage)
            stage_metadata["endpoint_pairing"] = pairing_metadata
            task = stage.get("task")
            if isinstance(task, dict):
                task_metadata = task.setdefault("metadata", {})
                if isinstance(task_metadata, dict):
                    task_metadata["endpoint_pairing"] = pairing_metadata
        payload.setdefault("stages", []).append(stage)
    return created > 0


def _reaction_orca_stage_plan(
    o: Any,
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_config: str | None,
    orca_config: str | None,
) -> _ReactionOrcaStagePlan | None:
    xtb_stages = _completed_or_recoverable_xtb_stages(o, payload)
    if not xtb_stages:
        return None
    xtb_allowed_root = _call_engine_aware(o.stages._load_config_root, xtb_config, engine="xtb")
    if xtb_allowed_root is None:
        return None
    if _call_engine_aware(o.stages._load_config_root, orca_config, engine="orca") is None:
        return None
    orca_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="orca")
    params = _request_params(o, payload)
    payload_metadata_raw = payload.setdefault("metadata", {})
    payload_metadata = payload_metadata_raw if isinstance(payload_metadata_raw, dict) else {}
    candidate_pool, handoff_errors = _collect_reaction_orca_candidates(
        o,
        xtb_stages,
        xtb_allowed_root=xtb_allowed_root,
    )
    ordered_candidates = _unique_ordered_candidates(candidate_pool)
    existing = _engine_stages(o, payload, "orca")
    remaining_candidates = _remaining_orca_candidates(o, existing, ordered_candidates)
    return _ReactionOrcaStagePlan(
        payload_metadata=payload_metadata,
        params=params,
        orca_allowed_root=orca_runtime_paths["allowed_root"],
        ordered_candidates=ordered_candidates,
        remaining_candidates=remaining_candidates,
        existing_stages=existing,
        has_pending_xtb=_has_pending_xtb_stage(o, payload),
        handoff_errors=handoff_errors,
    )


def append_reaction_orca_stages_impl(
    payload: dict[str, Any],
    *,
    workspace_dir: Path,
    xtb_config: str | None,
    orca_config: str | None,
    deps: OrchestrationDeps | None = None,
) -> bool:
    o = _orchestration_context(deps)
    plan = _reaction_orca_stage_plan(
        o,
        payload,
        workspace_dir=workspace_dir,
        xtb_config=xtb_config,
        orca_config=orca_config,
    )
    if plan is None:
        return False

    if not plan.remaining_candidates:
        _record_reaction_handoff_failure(
            plan.payload_metadata,
            existing=plan.existing_stages,
            has_pending_xtb=plan.has_pending_xtb,
            handoff_errors=plan.handoff_errors,
        )
        return False

    if isinstance(plan.payload_metadata, dict) and isinstance(
        plan.payload_metadata.get("workflow_error"), dict
    ):
        _clear_workflow_error_scope(
            o,
            plan.payload_metadata,
            {"reaction_ts_search_xtb_handoff", "reaction_ts_search_orca_candidate_exhausted"},
        )

    created = 0
    starting_index = len(plan.existing_stages)
    for offset, candidate in enumerate(plan.remaining_candidates, start=1):
        next_index = starting_index + offset
        stage = o.engines.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name="reaction_ts_search",
            stage_id=f"orca_optts_freq_{next_index:02d}",
            stage_key=f"{next_index:02d}_{o.engines.safe_name(candidate.kind, fallback='candidate')}",
            stage_root_name="",
            workspace_dir=plan.orca_allowed_root,
            input_artifact_kind="xtb_candidate",
            candidate=candidate,
            task_kind="optts_freq",
            route_line=str(
                plan.params.get("orca_route_line", "! r2scan-3c OptTS Freq TightSCF")
            ),
            charge=int(plan.params.get("charge", 0) or 0),
            multiplicity=int(plan.params.get("multiplicity", 1) or 1),
            max_cores=int(plan.params.get("max_cores", 8) or 8),
            max_memory_gb=int(plan.params.get("max_memory_gb", 32) or 32),
            priority=int(plan.params.get("priority", 10) or 10),
            xyz_filename="ts_guess.xyz",
            inp_filename="ts_guess.inp",
        ).to_dict()
        stage_metadata = o.stages._stage_metadata(stage)
        stage_metadata["reaction_candidate_attempt_index"] = next_index
        stage_metadata["reaction_candidate_pool_size"] = len(plan.ordered_candidates)
        stage_metadata["reaction_remaining_candidates_after_this"] = max(
            0, len(plan.remaining_candidates) - offset
        )
        payload.setdefault("stages", []).append(stage)
        created += 1
    return created > 0


def _completed_crest_stage_for_orca(
    o: Any,
    payload: dict[str, Any],
    *,
    crest_config: str | None,
) -> Any | None:
    crest_stage = next(
        (
            view.raw
            for view in _engine_stage_views(o, payload, "crest")
            if view.status(o) == "completed"
        ),
        None,
    )
    if crest_stage is None:
        return None
    return o.stages._completed_crest_stage(crest_stage, crest_config=crest_config)


def _crest_orca_stage_plan(
    o: Any,
    payload: dict[str, Any],
    *,
    template_name: str,
    crest_config: str | None,
    orca_config: str | None,
) -> _CrestOrcaStagePlan | None:
    if _engine_stages(o, payload, "orca"):
        return None
    crest_contract = _completed_crest_stage_for_orca(
        o,
        payload,
        crest_config=crest_config,
    )
    if (
        crest_contract is None
        or _call_engine_aware(o.stages._load_config_root, orca_config, engine="orca") is None
    ):
        return None
    payload_metadata = o.stages._coerce_mapping(payload.get("metadata"))
    workspace_dir_text = o.stages._normalize_text(payload_metadata.get("workspace_dir"))
    workspace_dir = (
        Path(workspace_dir_text).expanduser().resolve()
        if workspace_dir_text
        else Path(".").resolve()
    )
    orca_runtime_paths = workflow_workspace_internal_engine_paths(
        workspace_dir,
        engine="orca",
    )
    params = _request_params(o, payload)
    candidates = o.engines.select_crest_downstream_inputs(
        crest_contract,
        policy=o.contracts.CrestDownstreamPolicy.build(
            max_candidates=int(params.get("max_orca_stages", 3) or 3)
        ),
    )
    return _CrestOrcaStagePlan(
        params=params,
        candidates=candidates,
        orca_allowed_root=orca_runtime_paths["allowed_root"],
    )


def append_crest_orca_stages_impl(
    payload: dict[str, Any],
    *,
    template_name: str,
    crest_config: str | None,
    orca_config: str | None,
    stage_id_prefix: str,
    xyz_filename: str,
    inp_filename: str,
    deps: OrchestrationDeps | None = None,
) -> bool:
    o = _orchestration_context(deps)
    plan = _crest_orca_stage_plan(
        o,
        payload,
        template_name=template_name,
        crest_config=crest_config,
        orca_config=orca_config,
    )
    if plan is None:
        return False
    created = 0
    for candidate in plan.candidates:
        created += 1
        stage = o.engines.build_materialized_orca_stage(
            workflow_id=str(payload.get("workflow_id", "")),
            template_name=template_name,
            stage_id=f"{stage_id_prefix}_{created:02d}",
            stage_key=f"{created:02d}_{o.engines.safe_name(candidate.kind, fallback='conformer')}",
            stage_root_name="",
            workspace_dir=plan.orca_allowed_root,
            input_artifact_kind="crest_conformer",
            candidate=candidate,
            task_kind="opt",
            route_line=str(plan.params.get("orca_route_line", "! r2scan-3c Opt TightSCF")),
            charge=int(plan.params.get("charge", 0) or 0),
            multiplicity=int(plan.params.get("multiplicity", 1) or 1),
            max_cores=int(plan.params.get("max_cores", 8) or 8),
            max_memory_gb=int(plan.params.get("max_memory_gb", 32) or 32),
            priority=int(plan.params.get("priority", 10) or 10),
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
