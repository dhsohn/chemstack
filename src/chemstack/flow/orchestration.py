from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from chemstack.core.app_ids import CHEMSTACK_EXECUTABLE
from chemstack.core.utils import now_utc_iso, timestamped_token

from ._orchestration_builders import (
    create_conformer_screening_workflow_impl,
    create_reaction_ts_search_workflow_impl,
    new_crest_stage_impl,
    new_xtb_stage_impl,
)
from ._orchestration_lifecycle import (
    downstream_terminal_result_impl,
    effective_stage_status_impl,
    latest_child_stage_summary_impl,
    recompute_workflow_status_impl,
    stage_failure_is_recoverable_impl,
    workflow_has_active_children_impl,
    workflow_sync_only_impl,
)
from ._orchestration_stage_materialization import (
    append_crest_orca_stages_impl,
    append_reaction_orca_stages_impl,
    append_reaction_xtb_stages_impl,
)
from ._orchestration_stage_runtime import (
    append_unique_artifact_impl,
    completed_crest_roles_impl,
    completed_crest_stage_impl,
    completed_orca_stage_impl,
    ensure_crest_job_dir_impl,
    ensure_xtb_job_dir_impl,
    stage_has_xtb_candidates_impl,
    sync_crest_stage_impl,
    sync_orca_stage_impl,
    sync_xtb_stage_impl,
    write_xtb_path_job_impl,
    xtb_attempt_record_impl,
    xtb_attempt_rows_impl,
    xtb_current_attempt_number_impl,
    xtb_handoff_status_impl,
    xtb_path_retry_limit_impl,
    xtb_retry_recipe_impl,
)
from ._orchestration_support import (
    clear_reaction_xtb_handoff_error_if_recovering_impl,
    load_config_organized_root_impl,
    load_config_root_impl,
    reaction_orca_allows_next_candidate_impl,
    reaction_orca_source_candidate_path_impl,
    reaction_ts_guess_error_impl,
    stage_metadata_impl,
    submission_target_impl,
    task_payload_dict_impl,
)
from .adapters import (
    load_crest_artifact_contract,
    load_orca_artifact_contract,
    load_xtb_artifact_contract,
    select_crest_downstream_inputs,
    select_xtb_downstream_inputs,
)
from .contracts import CrestDownstreamPolicy, WorkflowArtifactRef, WorkflowPlan, WorkflowStage, WorkflowStageInput, WorkflowTask, WorkflowTemplateRequest, XtbDownstreamPolicy
from .registry import sync_workflow_registry
from .state import (
    acquire_workflow_lock,
    load_workflow_payload,
    resolve_workflow_workspace,
    workflow_has_active_downstream,
    write_workflow_payload,
)
from .submitters.common import normalize_text, sibling_allowed_root, sibling_runtime_paths
from .submitters.crest_auto import cancel_target as crest_cancel_target, submit_job_dir as submit_crest_job_dir
from .submitters.orca_auto import cancel_target as orca_cancel_target, submit_reaction_dir
from .submitters.xtb_auto import cancel_target as xtb_cancel_target, submit_job_dir as submit_xtb_job_dir
from .workflows.orca_stage_utils import build_materialized_orca_stage, safe_name
from .xyz_utils import choose_orca_geometry_frame, load_xyz_atom_sequence

# Keep these imports bound on the facade module so extracted private helpers can
# continue resolving them through ``from . import orchestration as o`` and tests
# can monkeypatch the pre-refactor surface.
_FACADE_COMPAT = (
    load_crest_artifact_contract,
    load_orca_artifact_contract,
    load_xtb_artifact_contract,
    select_crest_downstream_inputs,
    select_xtb_downstream_inputs,
    CrestDownstreamPolicy,
    WorkflowArtifactRef,
    WorkflowPlan,
    WorkflowStageInput,
    WorkflowTask,
    WorkflowTemplateRequest,
    XtbDownstreamPolicy,
    sibling_allowed_root,
    sibling_runtime_paths,
    submit_crest_job_dir,
    submit_reaction_dir,
    submit_xtb_job_dir,
    build_materialized_orca_stage,
    safe_name,
    choose_orca_geometry_frame,
)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_text(value)


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _workflow_id(prefix: str) -> str:
    return timestamped_token(prefix)


def _copy_input(source: str, target: Path) -> str:
    src = Path(source).expanduser().resolve()
    if not src.exists():
        raise FileNotFoundError(f"Input XYZ not found: {src}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, target)
    return str(target.resolve())


def _write_workflow_payload_side_effect(workspace_dir: Path, payload: dict[str, Any]) -> None:
    write_workflow_payload(workspace_dir, payload)


def _sync_workflow_registry_side_effect(
    workflow_root: Path,
    workspace_dir: Path,
    payload: dict[str, Any],
) -> None:
    sync_workflow_registry(workflow_root, workspace_dir, payload)


def _stage_dict(stage: WorkflowStage) -> dict[str, Any]:
    return stage.to_dict()


def _workflow_sync_only(payload: dict[str, Any]) -> bool:
    return workflow_sync_only_impl(payload, normalize_text_fn=_normalize_text)


def _workflow_has_active_children(payload: dict[str, Any]) -> bool:
    return workflow_has_active_children_impl(
        payload,
        normalize_text_fn=_normalize_text,
        workflow_has_active_downstream_fn=workflow_has_active_downstream,
    )


def _latest_child_stage_summary(stage_summaries: list[dict[str, Any]]) -> dict[str, Any]:
    return latest_child_stage_summary_impl(stage_summaries, normalize_text_fn=_normalize_text)


def _downstream_terminal_result(child_payload: dict[str, Any], child_summary: dict[str, Any]) -> dict[str, Any]:
    return downstream_terminal_result_impl(
        child_payload,
        child_summary,
        normalize_text_fn=_normalize_text,
    )


def _new_crest_stage(
    *,
    workflow_id: str,
    template_name: str,
    stage_id: str,
    source_path: str,
    input_role: str,
    mode: str,
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    manifest_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return new_crest_stage_impl(
        workflow_id=workflow_id,
        template_name=template_name,
        stage_id=stage_id,
        source_path=source_path,
        input_role=input_role,
        mode=mode,
        priority=priority,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        manifest_overrides=manifest_overrides,
    )


def _new_xtb_stage(
    *,
    workflow_id: str,
    stage_id: str,
    reaction_key: str,
    reactant_input: dict[str, Any],
    product_input: dict[str, Any],
    priority: int,
    max_cores: int,
    max_memory_gb: int,
    max_handoff_retries: int = 2,
    manifest_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return new_xtb_stage_impl(
        workflow_id=workflow_id,
        stage_id=stage_id,
        reaction_key=reaction_key,
        reactant_input=reactant_input,
        product_input=product_input,
        priority=priority,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        max_handoff_retries=max_handoff_retries,
        manifest_overrides=manifest_overrides,
    )


def create_reaction_ts_search_workflow(
    *,
    reactant_xyz: str,
    product_xyz: str,
    workflow_root: str | Path,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_crest_candidates: int = 3,
    max_xtb_stages: int = 3,
    max_xtb_handoff_retries: int = 2,
    max_orca_stages: int = 3,
    orca_route_line: str = "! r2scan-3c OptTS Freq TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
    xtb_job_manifest: dict[str, Any] | None = None,
    source_job_id: str = "",
    source_job_type: str = "",
) -> dict[str, Any]:
    normalized_crest_mode = _normalize_text(crest_mode).lower()
    if normalized_crest_mode not in {"standard", "nci"}:
        raise ValueError("reaction_ts_search only supports crest_mode 'standard' or 'nci'")
    return create_reaction_ts_search_workflow_impl(
        reactant_xyz=reactant_xyz,
        product_xyz=product_xyz,
        workflow_root=workflow_root,
        crest_mode=normalized_crest_mode,
        priority=priority,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        max_crest_candidates=max_crest_candidates,
        max_xtb_stages=max_xtb_stages,
        max_xtb_handoff_retries=max_xtb_handoff_retries,
        max_orca_stages=max_orca_stages,
        orca_route_line=orca_route_line,
        charge=charge,
        multiplicity=multiplicity,
        crest_job_manifest=crest_job_manifest,
        xtb_job_manifest=xtb_job_manifest,
        source_job_id=source_job_id,
        source_job_type=source_job_type,
        workflow_id_factory=_workflow_id,
        copy_input_fn=_copy_input,
        now_utc_iso_fn=now_utc_iso,
        load_xyz_atom_sequence_fn=load_xyz_atom_sequence,
        new_crest_stage_fn=_new_crest_stage,
        write_workflow_payload_fn=_write_workflow_payload_side_effect,
        sync_workflow_registry_fn=_sync_workflow_registry_side_effect,
    )


def create_conformer_screening_workflow(
    *,
    input_xyz: str,
    workflow_root: str | Path,
    crest_mode: str = "standard",
    priority: int = 10,
    max_cores: int = 8,
    max_memory_gb: int = 32,
    max_orca_stages: int = 3,
    orca_route_line: str = "! r2scan-3c Opt TightSCF",
    charge: int = 0,
    multiplicity: int = 1,
    crest_job_manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return create_conformer_screening_workflow_impl(
        input_xyz=input_xyz,
        workflow_root=workflow_root,
        crest_mode=crest_mode,
        priority=priority,
        max_cores=max_cores,
        max_memory_gb=max_memory_gb,
        max_orca_stages=max_orca_stages,
        orca_route_line=orca_route_line,
        charge=charge,
        multiplicity=multiplicity,
        crest_job_manifest=crest_job_manifest,
        workflow_id_factory=_workflow_id,
        copy_input_fn=_copy_input,
        now_utc_iso_fn=now_utc_iso,
        new_crest_stage_fn=_new_crest_stage,
        write_workflow_payload_fn=_write_workflow_payload_side_effect,
        sync_workflow_registry_fn=_sync_workflow_registry_side_effect,
    )

def _submission_target(stage: dict[str, Any]) -> str:
    return submission_target_impl(stage)


def _load_config_root(config_path: str | None, *, engine: str = "orca") -> Path | None:
    return load_config_root_impl(config_path, engine=engine)


def _load_config_organized_root(config_path: str | None, *, engine: str = "orca") -> Path | None:
    return load_config_organized_root_impl(config_path, engine=engine)


def _stage_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    return stage_metadata_impl(stage)


def _task_payload_dict(task: dict[str, Any]) -> dict[str, Any]:
    return task_payload_dict_impl(task)


def _xtb_attempt_rows(stage: dict[str, Any]) -> list[dict[str, Any]]:
    return xtb_attempt_rows_impl(stage)


def _xtb_attempt_record(stage: dict[str, Any], *, attempt_number: int) -> dict[str, Any]:
    return xtb_attempt_record_impl(stage, attempt_number=attempt_number)


def _xtb_retry_recipe(attempt_number: int) -> dict[str, Any]:
    return xtb_retry_recipe_impl(attempt_number)


def _xtb_path_retry_limit(stage: dict[str, Any]) -> int:
    return xtb_path_retry_limit_impl(stage)


def _xtb_current_attempt_number(stage: dict[str, Any]) -> int:
    return xtb_current_attempt_number_impl(stage)


def _write_xtb_path_job(
    stage: dict[str, Any],
    *,
    xtb_allowed_root: Path,
    workflow_id: str,
    attempt_number: int,
) -> str:
    return write_xtb_path_job_impl(
        stage,
        xtb_allowed_root=xtb_allowed_root,
        workflow_id=workflow_id,
        attempt_number=attempt_number,
    )


def _xtb_handoff_status(contract: Any) -> dict[str, str]:
    return xtb_handoff_status_impl(contract)


def _stage_has_xtb_candidates(stage: dict[str, Any]) -> bool:
    return stage_has_xtb_candidates_impl(stage)


def _stage_failure_is_recoverable(stage: dict[str, Any]) -> bool:
    return stage_failure_is_recoverable_impl(
        stage,
        normalize_text_fn=_normalize_text,
        stage_metadata_fn=_stage_metadata,
    )


def _effective_stage_status(stage: dict[str, Any]) -> str:
    return effective_stage_status_impl(
        stage,
        normalize_text_fn=_normalize_text,
        stage_failure_is_recoverable_fn=_stage_failure_is_recoverable,
    )


def _reaction_ts_guess_error(contract: Any) -> dict[str, str]:
    return reaction_ts_guess_error_impl(contract)


def _reaction_orca_source_candidate_path(stage: dict[str, Any]) -> str:
    return reaction_orca_source_candidate_path_impl(stage)


def _reaction_orca_allows_next_candidate(stage: dict[str, Any]) -> bool:
    return reaction_orca_allows_next_candidate_impl(stage)


def _clear_reaction_xtb_handoff_error_if_recovering(payload: dict[str, Any]) -> None:
    return clear_reaction_xtb_handoff_error_if_recovering_impl(payload)


def _append_unique_artifact(rows: list[dict[str, Any]], *, kind: str, path: str, selected: bool = False, metadata: dict[str, Any] | None = None) -> None:
    return append_unique_artifact_impl(
        rows,
        kind=kind,
        path=path,
        selected=selected,
        metadata=metadata,
    )


def _ensure_crest_job_dir(stage: dict[str, Any], *, crest_allowed_root: Path, workflow_id: str) -> str:
    return ensure_crest_job_dir_impl(
        stage,
        crest_allowed_root=crest_allowed_root,
        workflow_id=workflow_id,
    )


def _ensure_xtb_job_dir(stage: dict[str, Any], *, xtb_allowed_root: Path, workflow_id: str) -> str:
    return ensure_xtb_job_dir_impl(
        stage,
        xtb_allowed_root=xtb_allowed_root,
        workflow_id=workflow_id,
    )


def _sync_crest_stage(stage: dict[str, Any], *, crest_auto_config: str | None, crest_auto_executable: str, crest_auto_repo_root: str | None, submit_ready: bool, workflow_id: str) -> None:
    return sync_crest_stage_impl(
        stage,
        crest_auto_config=crest_auto_config,
        crest_auto_executable=crest_auto_executable,
        crest_auto_repo_root=crest_auto_repo_root,
        submit_ready=submit_ready,
        workflow_id=workflow_id,
    )


def _sync_xtb_stage(stage: dict[str, Any], *, xtb_auto_config: str | None, xtb_auto_executable: str, xtb_auto_repo_root: str | None, submit_ready: bool, workflow_id: str) -> None:
    return sync_xtb_stage_impl(
        stage,
        xtb_auto_config=xtb_auto_config,
        xtb_auto_executable=xtb_auto_executable,
        xtb_auto_repo_root=xtb_auto_repo_root,
        submit_ready=submit_ready,
        workflow_id=workflow_id,
    )


def _sync_orca_stage(stage: dict[str, Any], *, orca_auto_config: str | None, orca_auto_executable: str, orca_auto_repo_root: str | None, submit_ready: bool) -> None:
    return sync_orca_stage_impl(
        stage,
        orca_auto_config=orca_auto_config,
        orca_auto_executable=orca_auto_executable,
        orca_auto_repo_root=orca_auto_repo_root,
        submit_ready=submit_ready,
    )


def _completed_crest_roles(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return completed_crest_roles_impl(payload)


def _completed_crest_stage(stage: dict[str, Any], *, crest_auto_config: str | None) -> Any | None:
    return completed_crest_stage_impl(stage, crest_auto_config=crest_auto_config)


def _completed_orca_stage(stage: dict[str, Any], *, orca_auto_config: str | None) -> Any | None:
    return completed_orca_stage_impl(stage, orca_auto_config=orca_auto_config)


def _append_reaction_xtb_stages(payload: dict[str, Any], *, workspace_dir: Path, crest_auto_config: str | None) -> bool:
    return append_reaction_xtb_stages_impl(
        payload,
        workspace_dir=workspace_dir,
        crest_auto_config=crest_auto_config,
    )


def _append_reaction_orca_stages(payload: dict[str, Any], *, workspace_dir: Path, xtb_auto_config: str | None, orca_auto_config: str | None) -> bool:
    return append_reaction_orca_stages_impl(
        payload,
        workspace_dir=workspace_dir,
        xtb_auto_config=xtb_auto_config,
        orca_auto_config=orca_auto_config,
    )


def _append_crest_orca_stages(
    payload: dict[str, Any],
    *,
    template_name: str,
    crest_auto_config: str | None,
    orca_auto_config: str | None,
    stage_id_prefix: str,
    xyz_filename: str,
    inp_filename: str,
) -> bool:
    return append_crest_orca_stages_impl(
        payload,
        template_name=template_name,
        crest_auto_config=crest_auto_config,
        orca_auto_config=orca_auto_config,
        stage_id_prefix=stage_id_prefix,
        xyz_filename=xyz_filename,
        inp_filename=inp_filename,
    )

def _recompute_workflow_status(payload: dict[str, Any]) -> str:
    return recompute_workflow_status_impl(
        payload,
        normalize_text_fn=_normalize_text,
        effective_stage_status_fn=_effective_stage_status,
    )


def advance_workflow(
    *,
    target: str,
    workflow_root: str | Path,
    crest_auto_config: str | None = None,
    crest_auto_executable: str = "crest_auto",
    crest_auto_repo_root: str | None = None,
    xtb_auto_config: str | None = None,
    xtb_auto_executable: str = "xtb_auto",
    xtb_auto_repo_root: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
    submit_ready: bool = True,
) -> dict[str, Any]:
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = resolve_workflow_workspace(target=target, workflow_root=workflow_root_path)
    with acquire_workflow_lock(workspace_dir):
        payload = load_workflow_payload(workspace_dir)
        workflow_id = _normalize_text(payload.get("workflow_id"))
        template_name = _normalize_text(payload.get("template_name"))
        sync_only = _workflow_sync_only(payload)
        effective_submit_ready = bool(submit_ready) and not sync_only

        for stage in payload.get("stages", []):
            if not isinstance(stage, dict):
                continue
            _sync_crest_stage(stage, crest_auto_config=crest_auto_config, crest_auto_executable=crest_auto_executable, crest_auto_repo_root=crest_auto_repo_root, submit_ready=effective_submit_ready, workflow_id=workflow_id)

        if not sync_only and template_name == "reaction_ts_search":
            _append_reaction_xtb_stages(payload, workspace_dir=workspace_dir, crest_auto_config=crest_auto_config)

        for stage in payload.get("stages", []):
            if not isinstance(stage, dict):
                continue
            _sync_xtb_stage(stage, xtb_auto_config=xtb_auto_config, xtb_auto_executable=xtb_auto_executable, xtb_auto_repo_root=xtb_auto_repo_root, submit_ready=effective_submit_ready, workflow_id=workflow_id)

        _clear_reaction_xtb_handoff_error_if_recovering(payload)

        if not sync_only and template_name == "reaction_ts_search":
            _append_reaction_orca_stages(payload, workspace_dir=workspace_dir, xtb_auto_config=xtb_auto_config, orca_auto_config=orca_auto_config)
        elif not sync_only and template_name == "conformer_screening":
            _append_crest_orca_stages(
                payload,
                template_name="conformer_screening",
                crest_auto_config=crest_auto_config,
                orca_auto_config=orca_auto_config,
                stage_id_prefix="orca_conformer",
                xyz_filename="conformer_guess.xyz",
                inp_filename="conformer_opt.inp",
            )

        for stage in payload.get("stages", []):
            if not isinstance(stage, dict):
                continue
            _sync_orca_stage(stage, orca_auto_config=orca_auto_config, orca_auto_executable=orca_auto_executable, orca_auto_repo_root=orca_auto_repo_root, submit_ready=effective_submit_ready)

        payload["status"] = _recompute_workflow_status(payload)
        payload.setdefault("metadata", {})
        if isinstance(payload["metadata"], dict):
            payload["metadata"]["last_advanced_at"] = now_utc_iso()
            payload["metadata"]["sync_only"] = bool(sync_only)
            final_child_sync_pending = (
                _normalize_text(payload.get("status")).lower()
                in {"completed", "failed", "cancel_requested", "cancelled", "cancel_failed"}
                and _workflow_has_active_children(payload)
            )
            payload["metadata"]["final_child_sync_pending"] = final_child_sync_pending
            if final_child_sync_pending:
                payload["metadata"]["final_child_sync_completed_at"] = ""
            else:
                payload["metadata"]["final_child_sync_completed_at"] = now_utc_iso()
        write_workflow_payload(workspace_dir, payload)
        sync_workflow_registry(workflow_root_path, workspace_dir, payload)
        return payload


def cancel_materialized_workflow(
    *,
    target: str,
    workflow_root: str | Path,
    crest_auto_config: str | None = None,
    crest_auto_executable: str = "crest_auto",
    crest_auto_repo_root: str | None = None,
    xtb_auto_config: str | None = None,
    xtb_auto_executable: str = "xtb_auto",
    xtb_auto_repo_root: str | None = None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    workflow_root_path = Path(workflow_root).expanduser().resolve()
    workspace_dir = resolve_workflow_workspace(target=target, workflow_root=workflow_root_path)
    with acquire_workflow_lock(workspace_dir):
        payload = load_workflow_payload(workspace_dir)
        cancelled: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []

        for stage in payload.get("stages", []):
            if not isinstance(stage, dict):
                continue
            task = stage.get("task")
            if not isinstance(task, dict):
                continue
            stage_status = _normalize_text(stage.get("status")).lower()
            task_status = _normalize_text(task.get("status")).lower()
            if stage_status in {"completed", "failed", "cancelled"} or task_status in {"completed", "failed", "cancelled"}:
                continue
            engine = _normalize_text(task.get("engine"))
            cancel_target = _submission_target(stage)
            if not cancel_target:
                task["status"] = "cancelled"
                stage["status"] = "cancelled"
                cancelled.append({"stage_id": stage.get("stage_id", ""), "mode": "local"})
                continue
            if engine == "crest" and _normalize_text(crest_auto_config):
                result = crest_cancel_target(target=cancel_target, config_path=str(crest_auto_config), executable=crest_auto_executable, repo_root=crest_auto_repo_root)
            elif engine == "xtb" and _normalize_text(xtb_auto_config):
                result = xtb_cancel_target(target=cancel_target, config_path=str(xtb_auto_config), executable=xtb_auto_executable, repo_root=xtb_auto_repo_root)
            elif engine == "orca" and _normalize_text(orca_auto_config):
                result = orca_cancel_target(target=cancel_target, config_path=str(orca_auto_config), executable=orca_auto_executable, repo_root=orca_auto_repo_root)
            else:
                result = {"status": "failed", "reason": "missing_engine_config"}
            task["cancel_result"] = result
            if result.get("status") in {"cancelled", "cancel_requested"}:
                task["status"] = result["status"]
                stage["status"] = result["status"]
                cancelled.append({"stage_id": stage.get("stage_id", ""), "status": result["status"]})
            else:
                failed.append({"stage_id": stage.get("stage_id", ""), "reason": result.get("reason", "cancel_failed")})

        payload["status"] = "cancel_requested" if any(item.get("status") == "cancel_requested" for item in cancelled) else "cancelled"
        write_workflow_payload(workspace_dir, payload)
        sync_workflow_registry(workflow_root_path, workspace_dir, payload)
        return {
            "workflow_id": payload.get("workflow_id", ""),
            "workspace_dir": str(workspace_dir),
            "status": payload.get("status", ""),
            "cancelled": cancelled,
            "failed": failed,
        }


__all__ = [
    "advance_workflow",
    "cancel_materialized_workflow",
    "create_conformer_screening_workflow",
    "create_reaction_ts_search_workflow",
]
