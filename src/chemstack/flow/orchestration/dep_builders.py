from __future__ import annotations

from collections.abc import Callable, Mapping
from functools import partial, wraps
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from chemstack.flow.orchestration.deps import (
        OrchestrationAdvanceDeps,
        OrchestrationContractDeps,
        OrchestrationDeps,
        OrchestrationEngineDeps,
        OrchestrationPersistenceDeps,
        OrchestrationStageDeps,
    )

AnyCallable = Callable[..., Any]


class _LazyOrchestrationDeps:
    def __init__(self, overrides: Mapping[str, Any] | None) -> None:
        self._overrides = overrides
        self._deps: OrchestrationDeps | None = None

    def resolve_to(self, deps: OrchestrationDeps) -> None:
        self._deps = deps

    def get(self) -> OrchestrationDeps:
        if self._deps is None:
            from chemstack.flow.orchestration.deps import orchestration_deps

            self._deps = orchestration_deps(overrides=self._overrides)
        return self._deps


def _override(overrides: Mapping[str, Any] | None, name: str, fallback: Any) -> Any:
    if overrides is not None and name in overrides:
        return overrides[name]
    return fallback


def _apply_overrides(
    overrides: Mapping[str, Any] | None,
    items: dict[str, Any],
) -> dict[str, Any]:
    return {name: _override(overrides, name, fallback) for name, fallback in items.items()}


def _build_dep_dataclass(
    deps_type: type[Any],
    overrides: Mapping[str, Any] | None,
    fallbacks: dict[str, Any],
) -> Any:
    return deps_type(**_apply_overrides(overrides, fallbacks))


def _build_dep_group(
    deps_type: type[Any],
    dep_names: tuple[str, ...],
    resolved: Mapping[str, Any],
) -> Any:
    return deps_type(**{name: resolved[name] for name in dep_names})


def _deps_provider(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps | None,
) -> _LazyOrchestrationDeps:
    return deps_provider or _LazyOrchestrationDeps(overrides)


def _bind_with_deps(deps_provider: _LazyOrchestrationDeps, func: AnyCallable) -> AnyCallable:
    @wraps(func)
    def call(*args: Any, **kwargs: Any) -> Any:
        if "deps" not in kwargs:
            kwargs["deps"] = deps_provider.get()
        return func(*args, **kwargs)

    return call


def _bind_many_with_deps(
    deps_provider: _LazyOrchestrationDeps,
    items: Mapping[str, AnyCallable],
) -> dict[str, AnyCallable]:
    return {name: _bind_with_deps(deps_provider, fallback) for name, fallback in items.items()}


def _coerce_mapping_fallback(value: Any) -> dict[str, Any]:
    from chemstack.core.utils import mapping_or_empty

    return mapping_or_empty(value)


def _normalize_text_fallback(value: Any) -> str:
    from chemstack.core.utils import normalize_text

    return normalize_text(value)


def _safe_int_fallback(value: Any, *, default: int = 0) -> int:
    from chemstack.core.utils import safe_int

    return safe_int(value, default=default)


def _normalize_text_override(overrides: Mapping[str, Any] | None = None) -> Any:
    return _override(overrides, "_normalize_text", _normalize_text_fallback)


def _stage_metadata_override(overrides: Mapping[str, Any] | None = None) -> Any:
    from chemstack.flow.orchestration.support import stage_metadata_impl

    return _override(overrides, "_stage_metadata", stage_metadata_impl)


def _stage_failure_is_recoverable_override(
    overrides: Mapping[str, Any] | None = None,
) -> Any:
    override = _override(overrides, "_stage_failure_is_recoverable", None)
    if override is not None:
        return override

    def stage_failure_is_recoverable(stage: dict[str, Any]) -> bool:
        return _stage_failure_is_recoverable_fallback(stage, overrides=overrides)

    return stage_failure_is_recoverable


def _workflow_sync_only_fallback(
    payload: dict[str, Any],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> bool:
    from chemstack.flow.orchestration.lifecycle import workflow_sync_only_impl

    return workflow_sync_only_impl(
        payload,
        normalize_text_fn=_normalize_text_override(overrides),
    )


def _workflow_has_active_children_fallback(
    payload: dict[str, Any],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> bool:
    from chemstack.flow.orchestration.lifecycle import workflow_has_active_children_impl
    from chemstack.flow.state import workflow_has_active_downstream

    return workflow_has_active_children_impl(
        payload,
        normalize_text_fn=_normalize_text_override(overrides),
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
    )


def _stage_failure_is_recoverable_fallback(
    stage: dict[str, Any],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> bool:
    from chemstack.flow.orchestration.lifecycle import stage_failure_is_recoverable_impl

    return stage_failure_is_recoverable_impl(
        stage,
        normalize_text_fn=_normalize_text_override(overrides),
        stage_metadata_fn=_stage_metadata_override(overrides),
    )


def _recompute_workflow_status_fallback(
    payload: dict[str, Any],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> str:
    from chemstack.flow.orchestration.lifecycle import (
        effective_stage_status_impl,
        recompute_workflow_status_impl,
    )

    def effective_stage_status(stage: dict[str, Any]) -> str:
        return effective_stage_status_impl(
            stage,
            normalize_text_fn=_normalize_text_override(overrides),
            stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(overrides),
        )

    return recompute_workflow_status_impl(
        payload,
        normalize_text_fn=_normalize_text_override(overrides),
        effective_stage_status_fn=effective_stage_status,
    )


def _persist_workflow_progress_fallback(
    workflow_root: Path,
    workspace_dir: Path,
    payload: dict[str, Any],
    *,
    sync_only: bool,
    overrides: Mapping[str, Any] | None = None,
) -> None:
    from chemstack.flow.registry import sync_workflow_registry
    from chemstack.flow.state import write_workflow_payload

    normalize = _normalize_text_override(overrides)
    if not sync_only:
        status = normalize(payload.get("status")).lower()
        if status not in {
            "completed",
            "failed",
            "cancel_requested",
            "cancelled",
            "cancel_failed",
        }:
            payload["status"] = "running"
    _override(overrides, "write_workflow_payload", write_workflow_payload)(workspace_dir, payload)
    _override(overrides, "sync_workflow_registry", sync_workflow_registry)(
        workflow_root,
        workspace_dir,
        payload,
    )


def _maybe_notify_workflow_phase_summary_fallback(
    payload: dict[str, Any],
    *,
    config_path: str | None,
    phase_engine: str,
    extra_lines: list[str] | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> bool:
    from chemstack.flow.workflow_notifications import maybe_notify_workflow_phase_summary

    return maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=config_path,
        phase_engine=phase_engine,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(overrides),
        extra_lines=extra_lines,
    )


def _build_contract_deps(overrides: Mapping[str, Any] | None) -> OrchestrationContractDeps:
    from chemstack.flow.orchestration.deps import OrchestrationContractDeps
    from chemstack.flow.contracts import CrestDownstreamPolicy, WorkflowStageInput, XtbDownstreamPolicy
    from chemstack.flow.endpoint_pairing import EndpointPairingPolicy

    return _build_dep_dataclass(
        OrchestrationContractDeps,
        overrides,
        {
            "CrestDownstreamPolicy": CrestDownstreamPolicy,
            "EndpointPairingPolicy": EndpointPairingPolicy,
            "WorkflowStageInput": WorkflowStageInput,
            "XtbDownstreamPolicy": XtbDownstreamPolicy,
        },
    )


def _build_persistence_deps(
    overrides: Mapping[str, Any] | None,
) -> OrchestrationPersistenceDeps:
    from chemstack.core.utils import now_utc_iso

    from chemstack.flow.orchestration.deps import OrchestrationPersistenceDeps
    from chemstack.flow.registry import sync_workflow_registry
    from chemstack.flow.state import acquire_workflow_lock, load_workflow_payload
    from chemstack.flow.state import resolve_workflow_workspace, write_workflow_payload

    return _build_dep_dataclass(
        OrchestrationPersistenceDeps,
        overrides,
        {
            "acquire_workflow_lock": acquire_workflow_lock,
            "load_workflow_payload": load_workflow_payload,
            "now_utc_iso": now_utc_iso,
            "resolve_workflow_workspace": resolve_workflow_workspace,
            "sync_workflow_registry": sync_workflow_registry,
            "write_workflow_payload": write_workflow_payload,
        },
    )


def _build_engine_deps(overrides: Mapping[str, Any] | None) -> OrchestrationEngineDeps:
    from chemstack.flow._orca_stage_materialization import build_materialized_orca_stage, safe_name
    from chemstack.flow.orchestration.deps import OrchestrationEngineDeps
    from chemstack.flow.adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
    from chemstack.flow.adapters.orca import load_orca_artifact_contract
    from chemstack.flow.adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
    from chemstack.flow.endpoint_pairing import select_endpoint_pairs
    from chemstack.flow.engine_runtime import engine_runtime_paths
    from chemstack.flow.submitters.crest import (
        cancel_target as crest_cancel_target,
        submit_job_dir as submit_crest_job_dir,
    )
    from chemstack.flow.submitters.orca import cancel_target as orca_cancel_target
    from chemstack.flow.submitters.orca import submit_reaction_dir
    from chemstack.flow.submitters.xtb import (
        cancel_target as xtb_cancel_target,
        submit_job_dir as submit_xtb_job_dir,
    )
    from chemstack.flow.xyz_utils import choose_orca_geometry_frame

    return _build_dep_dataclass(
        OrchestrationEngineDeps,
        overrides,
        {
            "build_materialized_orca_stage": build_materialized_orca_stage,
            "choose_orca_geometry_frame": choose_orca_geometry_frame,
            "crest_cancel_target": crest_cancel_target,
            "load_crest_artifact_contract": load_crest_artifact_contract,
            "load_orca_artifact_contract": load_orca_artifact_contract,
            "load_xtb_artifact_contract": load_xtb_artifact_contract,
            "orca_cancel_target": orca_cancel_target,
            "safe_name": safe_name,
            "select_crest_downstream_inputs": select_crest_downstream_inputs,
            "select_endpoint_pairs": select_endpoint_pairs,
            "select_xtb_downstream_inputs": select_xtb_downstream_inputs,
            "engine_runtime_paths": engine_runtime_paths,
            "submit_crest_job_dir": submit_crest_job_dir,
            "submit_reaction_dir": submit_reaction_dir,
            "submit_xtb_job_dir": submit_xtb_job_dir,
            "xtb_cancel_target": xtb_cancel_target,
        },
    )


def _stage_builder_fallbacks() -> dict[str, Any]:
    from chemstack.flow.orchestration.stage_builders import new_xtb_stage_impl

    return {
        "_new_xtb_stage": new_xtb_stage_impl,
    }


def _stage_materialization_fallbacks(
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    from chemstack.flow.orchestration.materialization import (
        append_crest_orca_stages_impl,
        append_reaction_orca_stages_impl,
        append_reaction_xtb_stages_impl,
    )

    return _bind_many_with_deps(
        deps_provider,
        {
            "_append_crest_orca_stages": append_crest_orca_stages_impl,
            "_append_reaction_orca_stages": append_reaction_orca_stages_impl,
            "_append_reaction_xtb_stages": append_reaction_xtb_stages_impl,
        },
    )


def _stage_runtime_fallbacks(deps_provider: _LazyOrchestrationDeps) -> dict[str, Any]:
    from chemstack.flow.orchestration.stage_runtime.crest import (
        completed_crest_roles_impl,
        completed_crest_stage_impl,
        ensure_crest_job_dir_impl,
        sync_crest_stage_impl,
    )
    from chemstack.flow.orchestration.stage_runtime.orca import sync_orca_stage_impl
    from chemstack.flow.orchestration.stage_runtime.shared import append_unique_artifact_impl
    from chemstack.flow.orchestration.stage_runtime.xtb_handoff import xtb_handoff_status_impl
    from chemstack.flow.orchestration.stage_runtime.xtb_path_jobs import (
        ensure_xtb_job_dir_impl,
        write_xtb_path_job_impl,
    )
    from chemstack.flow.orchestration.stage_runtime.xtb_retry import (
        xtb_attempt_record_impl,
        xtb_attempt_rows_impl,
        xtb_current_attempt_number_impl,
        xtb_path_retry_limit_impl,
        xtb_retry_recipe_impl,
    )
    from chemstack.flow.orchestration.stage_runtime.xtb_sync import sync_xtb_stage_impl

    return {
        **_bind_many_with_deps(
            deps_provider,
            {
                "_append_unique_artifact": append_unique_artifact_impl,
                "_completed_crest_roles": completed_crest_roles_impl,
                "_completed_crest_stage": completed_crest_stage_impl,
                "_ensure_crest_job_dir": ensure_crest_job_dir_impl,
                "_ensure_xtb_job_dir": ensure_xtb_job_dir_impl,
                "_sync_crest_stage": sync_crest_stage_impl,
                "_sync_orca_stage": sync_orca_stage_impl,
                "_sync_xtb_stage": sync_xtb_stage_impl,
                "_write_xtb_path_job": write_xtb_path_job_impl,
                "_xtb_attempt_record": xtb_attempt_record_impl,
                "_xtb_attempt_rows": xtb_attempt_rows_impl,
                "_xtb_current_attempt_number": xtb_current_attempt_number_impl,
                "_xtb_handoff_status": xtb_handoff_status_impl,
                "_xtb_path_retry_limit": xtb_path_retry_limit_impl,
            },
        ),
        "_xtb_retry_recipe": xtb_retry_recipe_impl,
    }


def _stage_support_fallbacks(deps_provider: _LazyOrchestrationDeps) -> dict[str, Any]:
    from chemstack.flow.orchestration.support import (
        clear_reaction_xtb_handoff_error_if_recovering_impl,
        load_config_organized_root_impl,
        load_config_root_impl,
        reaction_orca_source_candidate_path_impl,
        reaction_ts_guess_error_impl,
        stage_metadata_impl,
        submission_target_impl,
        task_payload_dict_impl,
    )

    return {
        **_bind_many_with_deps(
            deps_provider,
            {
                "_clear_reaction_xtb_handoff_error_if_recovering": (
                    clear_reaction_xtb_handoff_error_if_recovering_impl
                ),
                "_load_config_organized_root": load_config_organized_root_impl,
                "_load_config_root": load_config_root_impl,
                "_reaction_orca_source_candidate_path": reaction_orca_source_candidate_path_impl,
                "_reaction_ts_guess_error": reaction_ts_guess_error_impl,
                "_submission_target": submission_target_impl,
            },
        ),
        "_coerce_mapping": _coerce_mapping_fallback,
        "_normalize_text": _normalize_text_fallback,
        "_safe_int": _safe_int_fallback,
        "_stage_metadata": stage_metadata_impl,
        "_task_payload_dict": task_payload_dict_impl,
    }


def _stage_workflow_fallbacks(
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "_maybe_notify_workflow_phase_summary": partial(
            _maybe_notify_workflow_phase_summary_fallback,
            overrides=overrides,
        ),
        "_persist_workflow_progress": partial(
            _persist_workflow_progress_fallback,
            overrides=overrides,
        ),
        "_recompute_workflow_status": partial(
            _recompute_workflow_status_fallback,
            overrides=overrides,
        ),
        "_stage_failure_is_recoverable": _stage_failure_is_recoverable_override(overrides),
        "_workflow_has_active_children": partial(
            _workflow_has_active_children_fallback,
            overrides=overrides,
        ),
        "_workflow_sync_only": partial(_workflow_sync_only_fallback, overrides=overrides),
    }


def _stage_dep_fallbacks(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    fallbacks: dict[str, Any] = {}
    for group in (
        _stage_builder_fallbacks(),
        _stage_materialization_fallbacks(deps_provider),
        _stage_runtime_fallbacks(deps_provider),
        _stage_support_fallbacks(deps_provider),
        _stage_workflow_fallbacks(overrides),
    ):
        fallbacks.update(group)
    return fallbacks


def _build_stage_deps(
    overrides: Mapping[str, Any] | None,
    *,
    deps_provider: _LazyOrchestrationDeps | None = None,
) -> OrchestrationStageDeps:
    from chemstack.flow.orchestration.deps import (
        _ORCHESTRATION_STAGE_DEP_REGISTRY,
        OrchestrationStageDeps,
    )

    provider = _deps_provider(overrides, deps_provider)
    resolved = _apply_overrides(overrides, _stage_dep_fallbacks(overrides, provider))

    return OrchestrationStageDeps(
        **{
            group.name: _build_dep_group(group.deps_type, group.dep_names, resolved)
            for group in _ORCHESTRATION_STAGE_DEP_REGISTRY
        },
    )


def _build_advance_deps(
    overrides: Mapping[str, Any] | None,
    *,
    deps_provider: _LazyOrchestrationDeps | None = None,
) -> OrchestrationAdvanceDeps:
    from chemstack.flow.orchestration import advance
    from chemstack.flow.orchestration.deps import OrchestrationAdvanceDeps

    provider = _deps_provider(overrides, deps_provider)
    return _build_dep_dataclass(
        OrchestrationAdvanceDeps,
        overrides,
        {
            "_cancel_active_workflow_stages": _bind_with_deps(
                provider,
                advance._cancel_active_workflow_stages,
            ),
            "_cancel_stage_activity": _bind_with_deps(
                provider,
                advance._cancel_stage_activity,
            ),
        },
    )
