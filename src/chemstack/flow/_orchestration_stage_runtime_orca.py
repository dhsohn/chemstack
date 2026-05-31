from __future__ import annotations

from typing import Any

from ._orchestration_deps import OrchestrationDeps
from ._orchestration_stage_runtime_shared import (
    _apply_contract_status,
    _apply_submission_result,
    _coerce_bool,
    _engine_stage_sync_context,
    _load_contract_or_none,
    _orchestration_context,
    _workflow_internal_organized_root,
)
from ._orchestration_stage_views import WorkflowStageView, WorkflowTaskView


def _orca_submission_resource_kwargs(o: Any, enqueue_payload: dict[str, Any]) -> dict[str, Any]:
    resource_kwargs: dict[str, Any] = {}
    max_cores = o.stages._safe_int(enqueue_payload.get("max_cores"), default=0)
    max_memory_gb = o.stages._safe_int(enqueue_payload.get("max_memory_gb"), default=0)
    if max_cores > 0:
        resource_kwargs["max_cores"] = max_cores
    if max_memory_gb > 0:
        resource_kwargs["max_memory_gb"] = max_memory_gb
    if _coerce_bool(enqueue_payload.get("force", False)):
        resource_kwargs["force"] = True
    return resource_kwargs


def _submit_orca_stage(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    enqueue_payload: dict[str, Any],
    stage_metadata: dict[str, Any],
    *,
    orca_config: str | None,
    orca_repo_root: str | None,
) -> None:
    submission = o.engines.submit_reaction_dir(
        reaction_dir=str(enqueue_payload.get("reaction_dir", "")),
        priority=int(enqueue_payload.get("priority", 10) or 10),
        config_path=str(orca_config),
        repo_root=orca_repo_root,
        **_orca_submission_resource_kwargs(o, enqueue_payload),
    )
    submission["submitted_at"] = o.persistence.now_utc_iso()
    WorkflowTaskView(task).set_submission_result(submission)
    _apply_submission_result(
        stage=stage,
        task=task,
        stage_metadata=stage_metadata,
        submission=submission,
        metadata_fields=(
            ("queue_id", "queue_id"),
            ("submission_status", "status"),
            ("submitted_at", "submitted_at"),
        ),
    )


def _load_orca_contract(
    o: Any,
    stage_metadata: dict[str, Any],
    *,
    reaction_dir_hint: str,
    orca_config: str | None,
) -> Any | None:
    allowed_root = o.stages._load_config_root(orca_config, engine="orca")
    organized_root = _workflow_internal_organized_root(
        reaction_dir_hint, engine="orca"
    ) or o.stages._load_config_organized_root(orca_config, engine="orca")
    target = (
        o.stages._normalize_text(stage_metadata.get("run_id"))
        or reaction_dir_hint
        or o.stages._normalize_text(stage_metadata.get("queue_id"))
    )
    if not target:
        return None
    return o.engines.load_orca_artifact_contract(
        target=target,
        orca_allowed_root=allowed_root,
        orca_organized_root=organized_root,
        queue_id=o.stages._normalize_text(stage_metadata.get("queue_id")),
        run_id=o.stages._normalize_text(stage_metadata.get("run_id")),
        reaction_dir=reaction_dir_hint,
    )


def _apply_orca_attempt_metadata(
    o: Any, task_payload: dict[str, Any], stage_metadata: dict[str, Any], contract: Any
) -> None:
    if contract.state_status in {"running", "retrying"}:
        stage_metadata["orca_current_attempt_number"] = max(0, contract.attempt_count)
    elif contract.attempts:
        stage_metadata["orca_current_attempt_number"] = contract.attempts[-1].get("attempt_number")
    else:
        stage_metadata.pop("orca_current_attempt_number", None)

    if contract.attempts:
        last_attempt = contract.attempts[-1]
        stage_metadata["orca_latest_attempt_number"] = last_attempt.get("attempt_number")
        stage_metadata["orca_latest_attempt_status"] = last_attempt.get("analyzer_status")
        task_payload["orca_latest_attempt_inp"] = o.stages._normalize_text(last_attempt.get("inp_path"))
        task_payload["orca_latest_attempt_out"] = o.stages._normalize_text(last_attempt.get("out_path"))
        return

    stage_metadata.pop("orca_latest_attempt_number", None)
    stage_metadata.pop("orca_latest_attempt_status", None)


def _apply_orca_contract(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    task_payload: dict[str, Any],
    stage_metadata: dict[str, Any],
    contract: Any,
) -> None:
    _apply_contract_status(stage, task, contract.status)

    task_payload["selected_inp"] = contract.selected_inp or o.stages._normalize_text(
        task_payload.get("selected_inp")
    )
    if contract.selected_input_xyz:
        task_payload["selected_input_xyz"] = contract.selected_input_xyz
    if contract.last_out_path:
        task_payload["last_out_path"] = contract.last_out_path
    if contract.optimized_xyz_path:
        task_payload["optimized_xyz_path"] = contract.optimized_xyz_path

    stage_metadata["queue_id"] = contract.queue_id or o.stages._normalize_text(
        stage_metadata.get("queue_id")
    )
    stage_metadata["run_id"] = contract.run_id or o.stages._normalize_text(stage_metadata.get("run_id"))
    stage_metadata["queue_status"] = contract.queue_status
    stage_metadata["cancel_requested"] = bool(contract.cancel_requested)
    stage_metadata["latest_known_path"] = contract.latest_known_path
    stage_metadata["organized_output_dir"] = contract.organized_output_dir
    stage_metadata["optimized_xyz_path"] = contract.optimized_xyz_path
    stage_metadata["analyzer_status"] = contract.analyzer_status
    stage_metadata["reason"] = contract.reason
    stage_metadata["completed_at"] = contract.completed_at
    stage_metadata["state_status"] = contract.state_status
    stage_metadata["attempt_count"] = contract.attempt_count
    stage_metadata["max_retries"] = contract.max_retries
    stage_metadata["orca_attempts"] = [dict(item) for item in contract.attempts]
    stage_metadata["orca_final_result"] = dict(contract.final_result)
    _apply_orca_attempt_metadata(o, task_payload, stage_metadata, contract)


def _orca_output_artifacts(o: Any, contract: Any) -> list[dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_selected_inp",
        path=contract.selected_inp,
        selected=True,
        metadata={"run_id": contract.run_id},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_selected_input_xyz",
        path=contract.selected_input_xyz,
        metadata={"run_id": contract.run_id},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_optimized_xyz",
        path=contract.optimized_xyz_path,
        selected=contract.status == "completed",
        metadata={"run_id": contract.run_id},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_last_out",
        path=contract.last_out_path,
        selected=contract.status == "completed",
        metadata={"analyzer_status": contract.analyzer_status},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_run_state",
        path=contract.run_state_path,
        metadata={"status": contract.status},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_report_json",
        path=contract.report_json_path,
        metadata={"status": contract.status},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_report_md",
        path=contract.report_md_path,
        metadata={"status": contract.status},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_output_dir",
        path=contract.latest_known_path,
        selected=contract.status in {"completed", "failed", "cancelled"},
        metadata={"organized": bool(contract.organized_output_dir)},
    )
    o.stages._append_unique_artifact(
        artifacts,
        kind="orca_organized_output_dir",
        path=contract.organized_output_dir,
        selected=bool(contract.organized_output_dir),
        metadata={"run_id": contract.run_id},
    )
    return artifacts


def sync_orca_stage_impl(
    stage: dict[str, Any],
    *,
    orca_config: str | None,
    orca_repo_root: str | None,
    submit_ready: bool,
    deps: OrchestrationDeps | None = None,
) -> None:
    context = _engine_stage_sync_context(stage, engine="orca", deps=deps)
    if context is None:
        return
    o = context.o
    task = context.task
    enqueue_payload = task.get("enqueue_payload")
    if not isinstance(enqueue_payload, dict):
        return
    reaction_dir_hint = o.stages._normalize_text(
        context.task_payload.get("reaction_dir") or enqueue_payload.get("reaction_dir")
    )
    if context.should_submit(submit_ready=submit_ready, config_path=orca_config):
        _submit_orca_stage(
            o,
            stage,
            task,
            enqueue_payload,
            context.stage_metadata,
            orca_config=orca_config,
            orca_repo_root=orca_repo_root,
        )
    contract = _load_orca_contract(
        o,
        context.stage_metadata,
        reaction_dir_hint=reaction_dir_hint,
        orca_config=orca_config,
    )
    if contract is None:
        return
    _apply_orca_contract(
        o,
        stage,
        task,
        context.task_payload,
        context.stage_metadata,
        contract,
    )
    WorkflowStageView(stage).set_output_artifacts(_orca_output_artifacts(o, contract))


def completed_orca_stage_impl(
    stage: dict[str, Any],
    *,
    orca_config: str | None,
    deps: OrchestrationDeps | None = None,
) -> Any | None:
    o = _orchestration_context(deps)
    task = stage.get("task")
    if not isinstance(task, dict):
        return None
    payload = o.stages._task_payload_dict(task)
    enqueue_payload = o.stages._coerce_mapping(task.get("enqueue_payload"))
    stage_metadata = o.stages._stage_metadata(stage)
    reaction_dir_hint = o.stages._normalize_text(
        payload.get("reaction_dir") or enqueue_payload.get("reaction_dir")
    )
    target = (
        o.stages._normalize_text(stage_metadata.get("run_id"))
        or reaction_dir_hint
        or o.stages._normalize_text(stage_metadata.get("queue_id"))
    )
    if not target:
        return None
    return _load_contract_or_none(
        o.engines.load_orca_artifact_contract,
        engine="orca",
        target=target,
        stage=stage,
        orca_allowed_root=o.stages._load_config_root(orca_config, engine="orca"),
        orca_organized_root=(
            _workflow_internal_organized_root(reaction_dir_hint, engine="orca")
            or o.stages._load_config_organized_root(orca_config, engine="orca")
        ),
        queue_id=o.stages._normalize_text(stage_metadata.get("queue_id")),
        run_id=o.stages._normalize_text(stage_metadata.get("run_id")),
        reaction_dir=reaction_dir_hint,
    )
