from __future__ import annotations

from collections.abc import Callable, Mapping
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._orchestration_deps import (
        OrchestrationAdvanceDeps,
        OrchestrationContractDeps,
        OrchestrationEngineDeps,
        OrchestrationPersistenceDeps,
        OrchestrationStageDeps,
    )

AnyCallable = Callable[..., Any]


def _override(overrides: Mapping[str, Any] | None, name: str, fallback: Any) -> Any:
    if overrides is not None and name in overrides:
        return overrides[name]
    return fallback


def _apply_overrides(
    overrides: Mapping[str, Any] | None,
    items: dict[str, Any],
) -> dict[str, Any]:
    return {name: _override(overrides, name, fallback) for name, fallback in items.items()}


def _bind_with_deps(overrides: Mapping[str, Any] | None, func: AnyCallable) -> AnyCallable:
    def call(*args: Any, **kwargs: Any) -> Any:
        from ._orchestration_deps import orchestration_deps

        kwargs.setdefault("deps", orchestration_deps(overrides=overrides))
        return func(*args, **kwargs)

    return call


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
    from ._orchestration_support import stage_metadata_impl

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
    from ._orchestration_lifecycle import workflow_sync_only_impl

    return workflow_sync_only_impl(
        payload,
        normalize_text_fn=_normalize_text_override(overrides),
    )


def _workflow_has_active_children_fallback(
    payload: dict[str, Any],
    *,
    overrides: Mapping[str, Any] | None = None,
) -> bool:
    from ._orchestration_lifecycle import workflow_has_active_children_impl
    from .state import workflow_has_active_downstream

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
    from ._orchestration_lifecycle import stage_failure_is_recoverable_impl

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
    from ._orchestration_lifecycle import (
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
    from .registry import sync_workflow_registry
    from .state import write_workflow_payload

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
    from .workflow_notifications import maybe_notify_workflow_phase_summary

    return maybe_notify_workflow_phase_summary(
        payload=payload,
        config_path=config_path,
        phase_engine=phase_engine,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable_override(overrides),
        extra_lines=extra_lines,
    )


def _build_contract_deps(overrides: Mapping[str, Any] | None) -> OrchestrationContractDeps:
    from ._orchestration_deps import OrchestrationContractDeps
    from .contracts import CrestDownstreamPolicy, WorkflowStageInput, XtbDownstreamPolicy
    from .endpoint_pairing import EndpointPairingPolicy

    return OrchestrationContractDeps(
        **_apply_overrides(
            overrides,
            {
                "CrestDownstreamPolicy": CrestDownstreamPolicy,
                "EndpointPairingPolicy": EndpointPairingPolicy,
                "WorkflowStageInput": WorkflowStageInput,
                "XtbDownstreamPolicy": XtbDownstreamPolicy,
            },
        )
    )


def _build_persistence_deps(
    overrides: Mapping[str, Any] | None,
) -> OrchestrationPersistenceDeps:
    from chemstack.core.utils import now_utc_iso

    from ._orchestration_deps import OrchestrationPersistenceDeps
    from .registry import sync_workflow_registry
    from .state import acquire_workflow_lock, load_workflow_payload
    from .state import resolve_workflow_workspace, write_workflow_payload

    return OrchestrationPersistenceDeps(
        **_apply_overrides(
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
    )


def _build_engine_deps(overrides: Mapping[str, Any] | None) -> OrchestrationEngineDeps:
    from ._orca_stage_materialization import build_materialized_orca_stage, safe_name
    from ._orchestration_deps import OrchestrationEngineDeps
    from .adapters.crest import load_crest_artifact_contract, select_crest_downstream_inputs
    from .adapters.orca import load_orca_artifact_contract
    from .adapters.xtb import load_xtb_artifact_contract, select_xtb_downstream_inputs
    from .endpoint_pairing import select_endpoint_pairs
    from .engine_runtime import engine_runtime_paths
    from .submitters.crest import (
        cancel_target as crest_cancel_target,
        submit_job_dir as submit_crest_job_dir,
    )
    from .submitters.orca import cancel_target as orca_cancel_target
    from .submitters.orca import submit_reaction_dir
    from .submitters.xtb import (
        cancel_target as xtb_cancel_target,
        submit_job_dir as submit_xtb_job_dir,
    )
    from .xyz_utils import choose_orca_geometry_frame

    return OrchestrationEngineDeps(
        **_apply_overrides(
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
    )


def _stage_builder_fallbacks() -> dict[str, Any]:
    from ._orchestration_builders import new_xtb_stage_impl

    return {
        "_new_xtb_stage": new_xtb_stage_impl,
    }


def _stage_materialization_fallbacks(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    from ._orchestration_stage_materialization import (
        append_crest_orca_stages_impl,
        append_reaction_orca_stages_impl,
        append_reaction_xtb_stages_impl,
    )

    return {
        "_append_crest_orca_stages": _bind_with_deps(overrides, append_crest_orca_stages_impl),
        "_append_reaction_orca_stages": _bind_with_deps(
            overrides,
            append_reaction_orca_stages_impl,
        ),
        "_append_reaction_xtb_stages": _bind_with_deps(overrides, append_reaction_xtb_stages_impl),
    }


def _stage_runtime_fallbacks(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    from ._orchestration_stage_runtime_crest import (
        completed_crest_roles_impl,
        completed_crest_stage_impl,
        ensure_crest_job_dir_impl,
        sync_crest_stage_impl,
    )
    from ._orchestration_stage_runtime_orca import sync_orca_stage_impl
    from ._orchestration_stage_runtime_shared import append_unique_artifact_impl
    from ._orchestration_stage_runtime_xtb_handoff import xtb_handoff_status_impl
    from ._orchestration_stage_runtime_xtb_path_jobs import (
        ensure_xtb_job_dir_impl,
        write_xtb_path_job_impl,
    )
    from ._orchestration_stage_runtime_xtb_retry import (
        xtb_attempt_record_impl,
        xtb_attempt_rows_impl,
        xtb_current_attempt_number_impl,
        xtb_path_retry_limit_impl,
        xtb_retry_recipe_impl,
    )
    from ._orchestration_stage_runtime_xtb_sync import sync_xtb_stage_impl

    return {
        "_append_unique_artifact": _bind_with_deps(overrides, append_unique_artifact_impl),
        "_completed_crest_roles": _bind_with_deps(overrides, completed_crest_roles_impl),
        "_completed_crest_stage": _bind_with_deps(overrides, completed_crest_stage_impl),
        "_ensure_crest_job_dir": _bind_with_deps(overrides, ensure_crest_job_dir_impl),
        "_ensure_xtb_job_dir": _bind_with_deps(overrides, ensure_xtb_job_dir_impl),
        "_sync_crest_stage": _bind_with_deps(overrides, sync_crest_stage_impl),
        "_sync_orca_stage": _bind_with_deps(overrides, sync_orca_stage_impl),
        "_sync_xtb_stage": _bind_with_deps(overrides, sync_xtb_stage_impl),
        "_write_xtb_path_job": _bind_with_deps(overrides, write_xtb_path_job_impl),
        "_xtb_attempt_record": _bind_with_deps(overrides, xtb_attempt_record_impl),
        "_xtb_attempt_rows": _bind_with_deps(overrides, xtb_attempt_rows_impl),
        "_xtb_current_attempt_number": _bind_with_deps(
            overrides,
            xtb_current_attempt_number_impl,
        ),
        "_xtb_handoff_status": _bind_with_deps(overrides, xtb_handoff_status_impl),
        "_xtb_path_retry_limit": _bind_with_deps(overrides, xtb_path_retry_limit_impl),
        "_xtb_retry_recipe": xtb_retry_recipe_impl,
    }


def _stage_support_fallbacks(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    from ._orchestration_support import (
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
        "_clear_reaction_xtb_handoff_error_if_recovering": (
            _bind_with_deps(overrides, clear_reaction_xtb_handoff_error_if_recovering_impl)
        ),
        "_load_config_organized_root": _bind_with_deps(
            overrides,
            load_config_organized_root_impl,
        ),
        "_load_config_root": _bind_with_deps(overrides, load_config_root_impl),
        "_reaction_orca_source_candidate_path": _bind_with_deps(
            overrides,
            reaction_orca_source_candidate_path_impl,
        ),
        "_reaction_ts_guess_error": _bind_with_deps(overrides, reaction_ts_guess_error_impl),
        "_stage_metadata": stage_metadata_impl,
        "_submission_target": _bind_with_deps(overrides, submission_target_impl),
        "_task_payload_dict": task_payload_dict_impl,
    }


def _stage_workflow_fallbacks(
    overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "_coerce_mapping": _coerce_mapping_fallback,
        "_maybe_notify_workflow_phase_summary": partial(
            _maybe_notify_workflow_phase_summary_fallback,
            overrides=overrides,
        ),
        "_normalize_text": _normalize_text_fallback,
        "_persist_workflow_progress": partial(
            _persist_workflow_progress_fallback,
            overrides=overrides,
        ),
        "_recompute_workflow_status": partial(
            _recompute_workflow_status_fallback,
            overrides=overrides,
        ),
        "_safe_int": _safe_int_fallback,
        "_workflow_has_active_children": partial(
            _workflow_has_active_children_fallback,
            overrides=overrides,
        ),
        "_workflow_sync_only": partial(_workflow_sync_only_fallback, overrides=overrides),
    }


def _stage_dep_fallbacks(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    fallbacks: dict[str, Any] = {}
    for group in (
        _stage_builder_fallbacks(),
        _stage_materialization_fallbacks(overrides),
        _stage_runtime_fallbacks(overrides),
        _stage_support_fallbacks(overrides),
        _stage_workflow_fallbacks(overrides),
    ):
        fallbacks.update(group)
    return fallbacks


def _build_stage_deps(overrides: Mapping[str, Any] | None) -> OrchestrationStageDeps:
    from ._orchestration_deps import (
        OrchestrationStageBuilderDeps,
        OrchestrationStageDeps,
        OrchestrationStageMaterializationDeps,
        OrchestrationStageRuntimeDeps,
        OrchestrationStageSupportDeps,
        OrchestrationStageWorkflowDeps,
    )

    resolved = _apply_overrides(overrides, _stage_dep_fallbacks(overrides))
    resolved["_stage_failure_is_recoverable"] = _stage_failure_is_recoverable_override(overrides)

    def pick(names: tuple[str, ...]) -> dict[str, Any]:
        return {name: resolved[name] for name in names}

    return OrchestrationStageDeps(
        builders=OrchestrationStageBuilderDeps(
            **pick(
                (
                    "_new_xtb_stage",
                )
            )
        ),
        materialization=OrchestrationStageMaterializationDeps(
            **pick(
                (
                    "_append_crest_orca_stages",
                    "_append_reaction_orca_stages",
                    "_append_reaction_xtb_stages",
                )
            )
        ),
        runtime=OrchestrationStageRuntimeDeps(
            **pick(
                (
                    "_append_unique_artifact",
                    "_completed_crest_roles",
                    "_completed_crest_stage",
                    "_ensure_crest_job_dir",
                    "_ensure_xtb_job_dir",
                    "_sync_crest_stage",
                    "_sync_orca_stage",
                    "_sync_xtb_stage",
                    "_write_xtb_path_job",
                    "_xtb_attempt_record",
                    "_xtb_attempt_rows",
                    "_xtb_current_attempt_number",
                    "_xtb_handoff_status",
                    "_xtb_path_retry_limit",
                    "_xtb_retry_recipe",
                )
            )
        ),
        support=OrchestrationStageSupportDeps(
            **pick(
                (
                    "_clear_reaction_xtb_handoff_error_if_recovering",
                    "_coerce_mapping",
                    "_load_config_organized_root",
                    "_load_config_root",
                    "_normalize_text",
                    "_reaction_orca_source_candidate_path",
                    "_reaction_ts_guess_error",
                    "_safe_int",
                    "_stage_metadata",
                    "_submission_target",
                    "_task_payload_dict",
                )
            )
        ),
        workflow=OrchestrationStageWorkflowDeps(
            **pick(
                (
                    "_maybe_notify_workflow_phase_summary",
                    "_persist_workflow_progress",
                    "_recompute_workflow_status",
                    "_stage_failure_is_recoverable",
                    "_workflow_has_active_children",
                    "_workflow_sync_only",
                )
            )
        ),
    )


def _build_advance_deps(overrides: Mapping[str, Any] | None) -> OrchestrationAdvanceDeps:
    from . import _orchestration_advance
    from ._orchestration_deps import OrchestrationAdvanceDeps

    return OrchestrationAdvanceDeps(
        _cancel_active_workflow_stages=_override(
            overrides,
            "_cancel_active_workflow_stages",
            _bind_with_deps(overrides, _orchestration_advance._cancel_active_workflow_stages),
        ),
        _cancel_stage_activity=_override(
            overrides,
            "_cancel_stage_activity",
            _bind_with_deps(overrides, _orchestration_advance._cancel_stage_activity),
        ),
    )
