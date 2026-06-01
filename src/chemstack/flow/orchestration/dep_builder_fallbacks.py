from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .dep_builder_core import _override


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


__all__ = [
    "_coerce_mapping_fallback",
    "_maybe_notify_workflow_phase_summary_fallback",
    "_normalize_text_fallback",
    "_normalize_text_override",
    "_persist_workflow_progress_fallback",
    "_recompute_workflow_status_fallback",
    "_safe_int_fallback",
    "_stage_failure_is_recoverable_fallback",
    "_stage_failure_is_recoverable_override",
    "_stage_metadata_override",
    "_workflow_has_active_children_fallback",
    "_workflow_sync_only_fallback",
]
