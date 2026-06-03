from __future__ import annotations

from collections.abc import Mapping
from functools import partial
from typing import Any

from .dep_builder_core import _LazyOrchestrationDeps, _bind_many_with_deps
from .dep_builder_fallbacks import (
    _coerce_mapping_fallback,
    _maybe_notify_workflow_phase_summary_fallback,
    _normalize_text_fallback,
    _persist_workflow_progress_fallback,
    _recompute_workflow_status_fallback,
    _safe_int_fallback,
    _stage_failure_is_recoverable_override,
    _workflow_has_active_children_fallback,
    _workflow_sync_only_fallback,
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


def _stage_builder_fallbacks_for_context(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    del overrides, deps_provider
    return _stage_builder_fallbacks()


def _stage_materialization_fallbacks_for_context(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    del overrides
    return _stage_materialization_fallbacks(deps_provider)


def _stage_runtime_fallbacks_for_context(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    del overrides
    return _stage_runtime_fallbacks(deps_provider)


def _stage_support_fallbacks_for_context(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    del overrides
    return _stage_support_fallbacks(deps_provider)


def _stage_workflow_fallbacks_for_context(
    overrides: Mapping[str, Any] | None,
    deps_provider: _LazyOrchestrationDeps,
) -> dict[str, Any]:
    del deps_provider
    return _stage_workflow_fallbacks(overrides)


__all__ = [
    "_stage_builder_fallbacks",
    "_stage_builder_fallbacks_for_context",
    "_stage_materialization_fallbacks",
    "_stage_materialization_fallbacks_for_context",
    "_stage_runtime_fallbacks",
    "_stage_runtime_fallbacks_for_context",
    "_stage_support_fallbacks",
    "_stage_support_fallbacks_for_context",
    "_stage_workflow_fallbacks",
    "_stage_workflow_fallbacks_for_context",
]
