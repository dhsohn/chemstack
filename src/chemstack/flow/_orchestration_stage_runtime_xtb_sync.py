from __future__ import annotations

from pathlib import Path
from typing import Any

from ._orchestration_deps import OrchestrationDeps
from ._orchestration_stage_runtime_shared import (
    _call_engine_aware,
    _load_contract_or_none,
    _orchestration_context,
    _submission_is_deferred,
)
from ._orchestration_stage_runtime_xtb_handoff import (
    _empty_xtb_handoff,
    _update_xtb_handoff_metadata,
)
from ._orchestration_stage_runtime_xtb_submission import (
    _apply_xtb_submission_result,
    _record_xtb_submission_attempt,
    _submit_xtb_stage,
)
from .state import workflow_workspace_internal_engine_paths


def _load_xtb_contract(
    o: Any,
    stage: dict[str, Any],
    task_payload: dict[str, Any],
    *,
    xtb_runtime_paths: dict[str, Path],
    xtb_config: str | None,
) -> Any | None:
    job_dir_target = o.stages._normalize_text(task_payload.get("job_dir"))
    index_root = (
        xtb_runtime_paths["allowed_root"]
        or _call_engine_aware(o.stages._load_config_root, xtb_config, engine="xtb")
        or Path(job_dir_target or ".").resolve().parent
    )
    target = job_dir_target or o.stages._submission_target(stage)
    if not target:
        return None
    return _load_contract_or_none(
        o.engines.load_xtb_artifact_contract,
        engine="xtb",
        target=target,
        stage=stage,
        xtb_index_root=index_root,
    )


def _apply_xtb_contract(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    task_payload: dict[str, Any],
    stage_metadata: dict[str, Any],
    contract: Any,
) -> dict[str, str]:
    if contract.status != "unknown":
        task["status"] = contract.status
        stage["status"] = contract.status
    stage_metadata["child_job_id"] = contract.job_id
    stage_metadata["latest_known_path"] = contract.latest_known_path
    stage_metadata["organized_output_dir"] = contract.organized_output_dir
    task_payload["selected_input_xyz"] = contract.selected_input_xyz

    current_attempt = o.stages._xtb_current_attempt_number(stage)
    handoff = (
        o.stages._xtb_handoff_status(contract)
        if o.stages._normalize_text(task.get("task_kind")) == "path_search"
        else _empty_xtb_handoff()
    )
    attempt_record = o.stages._xtb_attempt_record(stage, attempt_number=current_attempt)
    attempt_record.update(
        {
            "job_id": contract.job_id,
            "status": contract.status,
            "reason": contract.reason,
            "latest_known_path": contract.latest_known_path,
            "organized_output_dir": contract.organized_output_dir,
            "candidate_count": len(contract.candidate_details),
            "selected_candidate_paths": list(contract.selected_candidate_paths),
            "analysis_summary": dict(contract.analysis_summary),
            "handoff_status": handoff["status"],
            "handoff_reason": handoff["reason"],
            "handoff_message": handoff["message"],
            "completed_at": o.stages._normalize_text(contract.analysis_summary.get("completed_at")),
        }
    )
    _update_xtb_handoff_metadata(stage_metadata, handoff)
    return handoff


def _maybe_retry_xtb_handoff(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    handoff: dict[str, str],
    *,
    xtb_runtime_paths: dict[str, Path],
    xtb_config: str | None,
    submit_ready: bool,
    workflow_id: str,
) -> bool:
    if not (
        submit_ready
        and o.stages._normalize_text(xtb_config)
        and o.stages._normalize_text(task.get("task_kind")) == "path_search"
        and handoff["status"] == "failed"
        and o.stages._normalize_text(stage.get("status")).lower() in {"completed", "failed"}
    ):
        return False

    retries_used = o.stages._safe_int(stage_metadata.get("xtb_handoff_retries_used"), default=0)
    retry_limit = o.stages._xtb_path_retry_limit(stage)
    if retries_used >= retry_limit:
        return False

    next_attempt = retries_used + 1
    retry_job_dir = o.stages._write_xtb_path_job(
        stage,
        xtb_allowed_root=xtb_runtime_paths["allowed_root"],
        workflow_id=workflow_id,
        attempt_number=next_attempt,
    )
    submission = o.engines.submit_xtb_job_dir(
        job_dir=retry_job_dir,
        priority=int(task["enqueue_payload"].get("priority", 10) or 10),
        config_path=str(xtb_config),
    )
    submission["submitted_at"] = o.persistence.now_utc_iso()
    task["submission_result"] = submission
    _record_xtb_submission_attempt(
        o,
        stage,
        submission,
        attempt_number=next_attempt,
        trigger_reason=handoff["reason"],
        trigger_message=handoff["message"],
    )
    _apply_xtb_submission_result(
        stage,
        task,
        stage_metadata,
        submission,
        deferred_handoff_status="waiting_for_slot",
        active_handoff_status="retrying",
    )
    stage_metadata["reaction_handoff_status"] = "retrying"
    stage_metadata["xtb_handoff_retry_limit"] = retry_limit
    if not _submission_is_deferred(submission):
        stage_metadata["xtb_handoff_retries_used"] = next_attempt
    return True


def _xtb_output_artifacts(contract: Any) -> list[dict[str, Any]]:
    return [
        {
            "kind": "xtb_candidate",
            "path": item.path,
            "selected": item.selected,
            "metadata": {
                "rank": item.rank,
                "kind": item.kind,
                "score": item.score,
                **dict(item.metadata),
            },
        }
        for item in contract.candidate_details
    ]


def sync_xtb_stage_impl(
    stage: dict[str, Any],
    *,
    xtb_config: str | None,
    submit_ready: bool,
    workflow_id: str,
    workspace_dir: Path,
    deps: OrchestrationDeps | None = None,
) -> None:
    o = _orchestration_context(deps)
    task = stage.get("task")
    if not isinstance(task, dict) or o.stages._normalize_text(task.get("engine")) != "xtb":
        return
    stage_metadata = o.stages._stage_metadata(stage)
    task_payload = o.stages._task_payload_dict(task)
    xtb_runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="xtb")
    if (
        o.stages._normalize_text(task.get("status")) == "planned"
        and submit_ready
        and o.stages._normalize_text(xtb_config)
    ):
        _submit_xtb_stage(
            o,
            stage,
            task,
            stage_metadata,
            xtb_runtime_paths=xtb_runtime_paths,
            xtb_config=xtb_config,
            workflow_id=workflow_id,
        )
    contract = _load_xtb_contract(
        o,
        stage,
        task_payload,
        xtb_runtime_paths=xtb_runtime_paths,
        xtb_config=xtb_config,
    )
    if contract is None:
        return
    handoff = _apply_xtb_contract(o, stage, task, task_payload, stage_metadata, contract)
    if _maybe_retry_xtb_handoff(
        o,
        stage,
        task,
        stage_metadata,
        handoff,
        xtb_runtime_paths=xtb_runtime_paths,
        xtb_config=xtb_config,
        submit_ready=submit_ready,
        workflow_id=workflow_id,
    ):
        return
    stage_metadata["xtb_handoff_retries_used"] = o.stages._safe_int(
        stage_metadata.get("xtb_handoff_retries_used"), default=0
    )
    stage_metadata["xtb_handoff_retry_limit"] = o.stages._xtb_path_retry_limit(stage)
    stage["output_artifacts"] = _xtb_output_artifacts(contract)


__all__ = [
    "_apply_xtb_contract",
    "_load_xtb_contract",
    "_maybe_retry_xtb_handoff",
    "_xtb_output_artifacts",
    "sync_xtb_stage_impl",
]
