from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._orchestration_deps import OrchestrationDeps, orchestration_deps
from ._workflow_phases import phase_finished
from .engine_options import WorkflowEngineOptions


@dataclass(frozen=True)
class _AdvanceContext:
    deps: OrchestrationDeps
    workflow_root_path: Path
    workspace_dir: Path
    workflow_id: str
    template_name: str
    sync_only: bool
    submit_ready: bool


def _workflow_stage_dicts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [stage for stage in payload.get("stages", []) if isinstance(stage, dict)]


def _checkpoint_advance_phase(
    payload: dict[str, Any],
    previous_payload: dict[str, Any],
    context: _AdvanceContext,
) -> None:
    if payload == previous_payload:
        return
    context.deps.stages._persist_workflow_progress(
        context.workflow_root_path,
        context.workspace_dir,
        payload,
        sync_only=context.sync_only,
    )


def _run_advance_phase(
    payload: dict[str, Any],
    context: _AdvanceContext,
    phase: Any,
) -> None:
    from copy import deepcopy

    before_phase = deepcopy(payload)
    phase(payload, context)
    _checkpoint_advance_phase(payload, before_phase, context)


def _sync_crest_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    o = context.deps
    for stage in _workflow_stage_dicts(payload):
        o.stages._sync_crest_stage(
            stage,
            crest_config=config.crest_config,
            submit_ready=context.submit_ready,
            workflow_id=context.workflow_id,
            workspace_dir=context.workspace_dir,
        )


def _append_reaction_xtb_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    if context.sync_only or context.template_name != "reaction_ts_search":
        return
    context.deps.stages._append_reaction_xtb_stages(
        payload,
        workspace_dir=context.workspace_dir,
        crest_config=config.crest_config,
    )


def _notify_crest_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    if context.sync_only:
        return
    context.deps.stages._maybe_notify_workflow_phase_summary(
        payload,
        config_path=config.crest_config,
        phase_engine="crest",
    )


def _sync_xtb_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    o = context.deps
    for stage in _workflow_stage_dicts(payload):
        o.stages._sync_xtb_stage(
            stage,
            xtb_config=config.xtb_config,
            submit_ready=context.submit_ready,
            workflow_id=context.workflow_id,
            workspace_dir=context.workspace_dir,
        )


def _clear_xtb_handoff_phase(
    payload: dict[str, Any], context: _AdvanceContext, _config: WorkflowEngineOptions
) -> None:
    context.deps.stages._clear_reaction_xtb_handoff_error_if_recovering(payload)


def _reaction_orca_ready(payload: dict[str, Any], context: _AdvanceContext) -> bool:
    return (
        not context.sync_only
        and context.template_name == "reaction_ts_search"
        and phase_finished(payload.get("stages", []), engine="xtb")
    )


def _append_reaction_orca_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    if not _reaction_orca_ready(payload, context):
        return
    context.deps.stages._append_reaction_orca_stages(
        payload,
        workspace_dir=context.workspace_dir,
        xtb_config=config.xtb_config,
        orca_config=config.orca_config,
    )


def _orca_stage_count(
    payload: dict[str, Any], *, deps: OrchestrationDeps | None = None
) -> int:
    o = deps or orchestration_deps()
    return sum(
        1
        for stage in _workflow_stage_dicts(payload)
        if o.stages._normalize_text((stage.get("task") or {}).get("engine")).lower() == "orca"
    )


def _notify_xtb_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    if not _reaction_orca_ready(payload, context):
        return
    context.deps.stages._maybe_notify_workflow_phase_summary(
        payload,
        config_path=config.xtb_config,
        phase_engine="xtb",
        extra_lines=[f"planned_orca_stages: {_orca_stage_count(payload, deps=context.deps)}"],
    )


def _append_conformer_orca_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    if context.sync_only or context.template_name != "conformer_screening":
        return
    context.deps.stages._append_crest_orca_stages(
        payload,
        template_name="conformer_screening",
        crest_config=config.crest_config,
        orca_config=config.orca_config,
        stage_id_prefix="orca_conformer",
        xyz_filename="conformer_guess.xyz",
        inp_filename="conformer_opt.inp",
    )


def _sync_orca_phase(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    o = context.deps
    for stage in _workflow_stage_dicts(payload):
        o.stages._sync_orca_stage(
            stage,
            orca_config=config.orca_config,
            orca_repo_root=config.orca_repo_root,
            submit_ready=context.submit_ready,
        )


def _advance_phases(config: WorkflowEngineOptions) -> tuple[Any, ...]:
    def bind(phase: Any) -> Any:
        return lambda payload, context: phase(payload, context, config)

    return (
        bind(_sync_crest_phase),
        bind(_append_reaction_xtb_phase),
        bind(_notify_crest_phase),
        bind(_sync_xtb_phase),
        bind(_clear_xtb_handoff_phase),
        bind(_append_reaction_orca_phase),
        bind(_notify_xtb_phase),
        bind(_append_conformer_orca_phase),
        bind(_sync_orca_phase),
    )


def _finalize_advanced_workflow(
    payload: dict[str, Any], context: _AdvanceContext, config: WorkflowEngineOptions
) -> None:
    o = context.deps
    payload["status"] = o.stages._recompute_workflow_status(payload)
    if o.stages._normalize_text(payload.get("status")).lower() == "failed":
        o.advance._cancel_active_workflow_stages(payload, config=config)
        payload["status"] = o.stages._recompute_workflow_status(payload)

    metadata = payload.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        return
    metadata["last_advanced_at"] = o.persistence.now_utc_iso()
    metadata["sync_only"] = bool(context.sync_only)
    final_child_sync_pending = o.stages._normalize_text(payload.get("status")).lower() in {
        "completed",
        "failed",
        "cancel_requested",
        "cancelled",
        "cancel_failed",
    } and o.stages._workflow_has_active_children(payload)
    metadata["final_child_sync_pending"] = final_child_sync_pending
    if final_child_sync_pending:
        metadata["final_child_sync_completed_at"] = ""
    else:
        metadata["final_child_sync_completed_at"] = o.persistence.now_utc_iso()


def _cancel_stage_activity(
    stage: dict[str, Any],
    *,
    config: WorkflowEngineOptions,
    deps: OrchestrationDeps | None = None,
) -> dict[str, Any]:
    o = deps or orchestration_deps()
    task = stage.get("task")
    if not isinstance(task, dict):
        return {"status": "skipped", "reason": "missing_task"}

    terminal_statuses = {
        "completed",
        "failed",
        "cancelled",
        "cancel_failed",
        "submission_failed",
    }
    cancellable_statuses = {
        "planned",
        "queued",
        "running",
        "submitted",
    }

    stage_status = o.stages._normalize_text(stage.get("status")).lower()
    task_status = o.stages._normalize_text(task.get("status")).lower()
    if stage_status == "cancel_requested" or task_status == "cancel_requested":
        return {"status": "skipped", "reason": "cancel_requested"}
    if stage_status in terminal_statuses or task_status in terminal_statuses:
        return {"status": "skipped", "reason": "terminal"}
    if stage_status not in cancellable_statuses and task_status not in cancellable_statuses:
        return {"status": "skipped", "reason": "not_cancellable"}

    engine = o.stages._normalize_text(task.get("engine"))
    cancel_target = o.stages._submission_target(stage)
    if not cancel_target:
        task["status"] = "cancelled"
        stage["status"] = "cancelled"
        return {"status": "cancelled", "mode": "local"}

    if (
        engine == "crest"
        and o.stages._normalize_text(config.crest.config)
    ):
        result = o.engines.crest_cancel_target(
            target=cancel_target,
            config_path=str(config.crest.config),
        )
    elif (
        engine == "xtb"
        and o.stages._normalize_text(config.xtb.config)
    ):
        result = o.engines.xtb_cancel_target(
            target=cancel_target,
            config_path=str(config.xtb.config),
        )
    elif (
        engine == "orca"
        and o.stages._normalize_text(config.orca.config)
    ):
        result = o.engines.orca_cancel_target(
            target=cancel_target,
            config_path=str(config.orca.config),
            repo_root=config.orca.repo_root,
        )
    else:
        result = {"status": "failed", "reason": "missing_engine_config"}

    task["cancel_result"] = result
    if result.get("status") in {"cancelled", "cancel_requested"}:
        task["status"] = result["status"]
        stage["status"] = result["status"]
        return {"status": result["status"]}
    return {
        "status": "failed",
        "reason": o.stages._normalize_text(result.get("reason")) or "cancel_failed",
    }


def _cancel_active_workflow_stages(
    payload: dict[str, Any],
    *,
    config: WorkflowEngineOptions,
    deps: OrchestrationDeps | None = None,
) -> dict[str, list[dict[str, Any]]]:
    o = deps or orchestration_deps()
    cancelled: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        outcome = o.advance._cancel_stage_activity(stage, config=config)
        if outcome.get("status") in {"cancelled", "cancel_requested"}:
            if outcome.get("mode"):
                cancelled.append(
                    {
                        "stage_id": stage.get("stage_id", ""),
                        "mode": outcome["mode"],
                    }
                )
            else:
                cancelled.append(
                    {
                        "stage_id": stage.get("stage_id", ""),
                        "status": outcome.get("status", ""),
                    }
                )
            continue
        if outcome.get("status") == "failed":
            failed.append(
                {
                    "stage_id": stage.get("stage_id", ""),
                    "reason": outcome.get("reason", "cancel_failed"),
                }
            )

    return {
        "cancelled": cancelled,
        "failed": failed,
    }


def advance_workflow(
    *,
    target: str,
    workflow_root: str | Path,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    engine_options: WorkflowEngineOptions | None = None,
    submit_ready: bool = True,
    deps: OrchestrationDeps | None = None,
) -> dict[str, Any]:
    o = deps or orchestration_deps()
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = o.persistence.resolve_workflow_workspace(target=target, workflow_root=workflow_root_path)
    with o.persistence.acquire_workflow_lock(workspace_dir):
        payload = o.persistence.load_workflow_payload(workspace_dir)
        sync_only = o.stages._workflow_sync_only(payload)
        config = engine_options or WorkflowEngineOptions.from_values(
            crest_config=crest_config,
            xtb_config=xtb_config,
            orca_config=orca_config,
            orca_repo_root=orca_repo_root,
        )
        context = _AdvanceContext(
            deps=o,
            workflow_root_path=workflow_root_path,
            workspace_dir=workspace_dir,
            workflow_id=o.stages._normalize_text(payload.get("workflow_id")),
            template_name=o.stages._normalize_text(payload.get("template_name")),
            sync_only=sync_only,
            submit_ready=bool(submit_ready) and not sync_only,
        )
        for phase in _advance_phases(config):
            _run_advance_phase(payload, context, phase)

        _finalize_advanced_workflow(payload, context, config)
        o.persistence.write_workflow_payload(workspace_dir, payload)
        o.persistence.sync_workflow_registry(workflow_root_path, workspace_dir, payload)
        return payload


def cancel_materialized_workflow(
    *,
    target: str,
    workflow_root: str | Path,
    crest_config: str | None = None,
    xtb_config: str | None = None,
    orca_config: str | None = None,
    orca_repo_root: str | None = None,
    engine_options: WorkflowEngineOptions | None = None,
    deps: OrchestrationDeps | None = None,
) -> dict[str, Any]:
    o = deps or orchestration_deps()
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = o.persistence.resolve_workflow_workspace(target=target, workflow_root=workflow_root_path)
    try:
        lock_context = o.persistence.acquire_workflow_lock(workspace_dir, timeout_seconds=5.0)
        with lock_context:
            payload = o.persistence.load_workflow_payload(workspace_dir)
            config = engine_options or WorkflowEngineOptions.from_values(
                crest_config=crest_config,
                xtb_config=xtb_config,
                orca_config=orca_config,
                orca_repo_root=orca_repo_root,
            )
            cancellation = o.advance._cancel_active_workflow_stages(payload, config=config)
            cancelled = cancellation["cancelled"]
            failed = cancellation["failed"]

            payload["status"] = (
                "cancel_requested"
                if any(item.get("status") == "cancel_requested" for item in cancelled)
                else "cancel_failed"
                if failed
                else "cancelled"
            )
            o.persistence.write_workflow_payload(workspace_dir, payload)
            o.persistence.sync_workflow_registry(workflow_root_path, workspace_dir, payload)
            return {
                "workflow_id": payload.get("workflow_id", ""),
                "workspace_dir": str(workspace_dir),
                "status": payload.get("status", ""),
                "cancelled": cancelled,
                "failed": failed,
            }
    except TimeoutError as exc:
        raise ValueError(
            f"Workflow is busy and could not be locked for cancellation within 5s: {workspace_dir}"
        ) from exc


__all__ = [
    "_AdvanceContext",
    "_advance_phases",
    "_cancel_active_workflow_stages",
    "_cancel_stage_activity",
    "advance_workflow",
    "cancel_materialized_workflow",
]
