from __future__ import annotations

from pathlib import Path
from typing import Any

from orca_auto.flow.engine_options import WorkflowEngineOptions
from orca_auto.flow.orchestration.advance_phases import (
    AdvanceContext as _AdvanceContext,
)
from orca_auto.flow.orchestration.advance_phases import (
    _advance_phases,
    _append_conformer_orca_phase,
    _append_reaction_orca_phase,
    _append_reaction_xtb_phase,
    _checkpoint_advance_phase,
    _clear_xtb_handoff_phase,
    _finalize_advanced_workflow,
    _notify_crest_phase,
    _notify_xtb_phase,
    _orca_stage_count,
    _reaction_orca_ready,
    _run_advance_phase,
    _sync_crest_phase,
    _sync_orca_phase,
    _sync_xtb_phase,
)
from orca_auto.flow.orchestration.dep_context import (
    orchestration_context as _orchestration_context,
)
from orca_auto.flow.orchestration.dep_types import OrchestrationDeps
from orca_auto.flow.orchestration.workflow_cancellation import (
    _cancel_active_workflow_stages,
    _cancel_engine_target,
    _cancel_stage_activity,
    _cancel_stage_activity_outcome,
    _StageCancelOutcome,
    cancel_materialized_workflow,
)


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
    o = _orchestration_context(deps)
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = o.persistence.resolve_workflow_workspace(
        target=target,
        workflow_root=workflow_root_path,
    )
    with o.persistence.acquire_workflow_lock(workspace_dir):
        payload = o.persistence.load_workflow_payload(workspace_dir)
        sync_only = o.stages.workflow._workflow_sync_only(payload)
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


__all__ = [
    "_AdvanceContext",
    "_StageCancelOutcome",
    "_advance_phases",
    "_append_conformer_orca_phase",
    "_append_reaction_orca_phase",
    "_append_reaction_xtb_phase",
    "_cancel_active_workflow_stages",
    "_cancel_engine_target",
    "_cancel_stage_activity",
    "_cancel_stage_activity_outcome",
    "_checkpoint_advance_phase",
    "_clear_xtb_handoff_phase",
    "_finalize_advanced_workflow",
    "_notify_crest_phase",
    "_notify_xtb_phase",
    "_orca_stage_count",
    "_reaction_orca_ready",
    "_run_advance_phase",
    "_sync_crest_phase",
    "_sync_orca_phase",
    "_sync_xtb_phase",
    "advance_workflow",
    "cancel_materialized_workflow",
]
