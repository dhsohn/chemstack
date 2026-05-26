from __future__ import annotations

from pathlib import Path
from typing import Any

from ._orchestration_stage_runtime_shared import (
    _clear_submission_deferred_metadata,
    _mark_submission_deferred,
    _submission_is_deferred,
)


def _record_xtb_submission_attempt(
    o: Any,
    stage: dict[str, Any],
    submission: dict[str, Any],
    *,
    attempt_number: int,
    trigger_reason: str = "",
    trigger_message: str = "",
) -> None:
    attempt_record = o.stages._xtb_attempt_record(stage, attempt_number=attempt_number)
    attempt_record["submission_status"] = submission.get("status", "")
    attempt_record["submitted_at"] = submission.get("submitted_at", "")
    attempt_record["queue_id"] = submission.get("queue_id", "")
    if trigger_reason:
        attempt_record["trigger_reason"] = trigger_reason
    if trigger_message:
        attempt_record["trigger_message"] = trigger_message


def _apply_xtb_submission_result(
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    submission: dict[str, Any],
    *,
    deferred_handoff_status: str,
    active_handoff_status: str,
) -> None:
    if _submission_is_deferred(submission):
        _mark_submission_deferred(
            stage=stage,
            task=task,
            stage_metadata=stage_metadata,
            submission=submission,
        )
        stage_metadata["xtb_handoff_status"] = deferred_handoff_status
        return

    task["status"] = "submitted" if submission["status"] == "submitted" else "submission_failed"
    stage["status"] = "queued" if submission["status"] == "submitted" else "submission_failed"
    stage_metadata["queue_id"] = submission.get("queue_id", "")
    stage_metadata["xtb_handoff_status"] = active_handoff_status
    _clear_submission_deferred_metadata(stage_metadata)


def _submit_xtb_stage(
    o: Any,
    stage: dict[str, Any],
    task: dict[str, Any],
    stage_metadata: dict[str, Any],
    *,
    xtb_runtime_paths: dict[str, Path],
    xtb_config: str | None,
    xtb_executable: str,
    xtb_repo_root: str | None,
    workflow_id: str,
) -> None:
    job_dir = o.stages._ensure_xtb_job_dir(
        stage,
        xtb_allowed_root=xtb_runtime_paths["allowed_root"],
        workflow_id=workflow_id,
    )
    submission = o.engines.submit_xtb_job_dir(
        job_dir=job_dir,
        priority=int(task["enqueue_payload"].get("priority", 10) or 10),
        config_path=str(xtb_config),
        executable=xtb_executable,
        repo_root=xtb_repo_root,
    )
    submission["submitted_at"] = o.persistence.now_utc_iso()
    task["submission_result"] = submission
    current_attempt = o.stages._xtb_current_attempt_number(stage)
    _record_xtb_submission_attempt(o, stage, submission, attempt_number=current_attempt)
    _apply_xtb_submission_result(
        stage,
        task,
        stage_metadata,
        submission,
        deferred_handoff_status="waiting_for_slot",
        active_handoff_status="submitted",
    )
    if not _submission_is_deferred(submission):
        stage_metadata["child_job_id"] = submission.get("job_id", "")


__all__ = [
    "_apply_xtb_submission_result",
    "_record_xtb_submission_attempt",
    "_submit_xtb_stage",
]
