from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.app_ids import (
    CHEMSTACK_CLI_MODULE,
    CHEMSTACK_EXECUTABLE,
    CHEMSTACK_ORCA_INTERNAL_MODULE,
    ORCA_SUBMITTERS,
)
from chemstack.core.utils import now_utc_iso

from ..registry import sync_workflow_registry
from ..state import load_workflow_payload, resolve_workflow_workspace, write_workflow_payload
from .common import normalize_text as _normalize_text, parse_key_value_lines as _parse_key_value_lines, run_sibling_app

_SUBMIT_MODULE_NAME = CHEMSTACK_CLI_MODULE
_CANCEL_MODULE_NAME = CHEMSTACK_ORCA_INTERNAL_MODULE


def _mapping_payload(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _submission_tail_argv(
    *,
    reaction_dir: str,
    priority: int,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
) -> list[str]:
    argv = [
        "run-dir",
        reaction_dir,
        "--priority",
        str(int(priority)),
    ]
    if max_cores is not None and int(max_cores) > 0:
        argv.extend(["--max-cores", str(int(max_cores))])
    if max_memory_gb is not None and int(max_memory_gb) > 0:
        argv.extend(["--max-memory-gb", str(int(max_memory_gb))])
    return argv


def _cancel_tail_argv(*, target: str) -> list[str]:
    return [
        "queue",
        "cancel",
        target,
    ]


def _cancel_status_from_output(*, returncode: int, stdout: str) -> str:
    if returncode != 0:
        return "failed"
    text = stdout.strip()
    if text.startswith("Cancelled:"):
        return "cancelled"
    if "Cancel requested" in text:
        return "cancel_requested"
    return "cancelled"


def submit_reaction_dir(
    *,
    reaction_dir: str,
    priority: int,
    config_path: str,
    max_cores: int | None = None,
    max_memory_gb: int | None = None,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    result = run_sibling_app(
        executable=_normalize_text(executable) or CHEMSTACK_EXECUTABLE,
        config_path=_normalize_text(config_path),
        repo_root=_normalize_text(repo_root) or None,
        module_name=_SUBMIT_MODULE_NAME,
        tail_argv=_submission_tail_argv(
            reaction_dir=reaction_dir,
            priority=priority,
            max_cores=max_cores,
            max_memory_gb=max_memory_gb,
        ),
    )
    parsed = _parse_key_value_lines(result.stdout)
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": "submitted" if result.returncode == 0 and parsed.get("status") == "queued" else "failed",
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "reaction_dir": parsed.get("job_dir") or parsed.get("reaction_dir", reaction_dir),
        "priority": int(priority),
    }


def cancel_target(
    *,
    target: str,
    config_path: str,
    executable: str = CHEMSTACK_EXECUTABLE,
    repo_root: str | None = None,
) -> dict[str, Any]:
    result = run_sibling_app(
        executable=_normalize_text(executable) or CHEMSTACK_EXECUTABLE,
        config_path=_normalize_text(config_path),
        repo_root=_normalize_text(repo_root) or None,
        module_name=_CANCEL_MODULE_NAME,
        tail_argv=_cancel_tail_argv(target=target),
    )
    argv = list(result.args) if isinstance(result.args, (list, tuple)) else [str(result.args)]
    return {
        "status": _cancel_status_from_output(returncode=result.returncode, stdout=result.stdout),
        "returncode": int(result.returncode),
        "command_argv": argv,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def submit_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_auto_config: str,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
    skip_submitted: bool = True,
) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=workflow_target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    submitted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    stage_results: list[dict[str, Any]] = []

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        task = stage.get("task")
        if not isinstance(task, dict):
            continue
        enqueue_payload = task.get("enqueue_payload")
        if not isinstance(enqueue_payload, dict):
            continue
        metadata = task.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            task["metadata"] = metadata
        stage_metadata = stage.get("metadata")
        if not isinstance(stage_metadata, dict):
            stage_metadata = {}
            stage["metadata"] = stage_metadata

        existing_submission = task.get("submission_result")
        task_status = _normalize_text(task.get("status")).lower()
        stage_status = _normalize_text(stage.get("status")).lower()
        if skip_submitted and (
            (isinstance(existing_submission, dict) and existing_submission.get("status") == "submitted")
            or task_status == "submitted"
            or stage_status in {"submitted", "queued"}
        ):
            skip_record = {"stage_id": stage.get("stage_id", ""), "reason": "already_submitted"}
            skipped.append(skip_record)
            stage_results.append({"stage_id": stage.get("stage_id", ""), "status": "skipped", "reason": "already_submitted"})
            continue

        reaction_dir = _normalize_text(enqueue_payload.get("reaction_dir"))
        priority = int(enqueue_payload.get("priority", 10) or 10)
        if not reaction_dir:
            submission_record = {
                "status": "failed",
                "reason": "missing_reaction_dir",
                "submitted_at": now_utc_iso(),
            }
            task["status"] = "submission_failed"
            task["submission_result"] = submission_record
            stage["status"] = "submission_failed"
            stage_metadata["submission_status"] = "submission_failed"
            stage_metadata["submitted_at"] = submission_record["submitted_at"]
            fail_record = {"stage_id": stage.get("stage_id", ""), "reason": "missing_reaction_dir"}
            failed.append(fail_record)
            stage_results.append({"stage_id": stage.get("stage_id", ""), "status": "submission_failed", "reason": "missing_reaction_dir"})
            continue

        if _normalize_text(enqueue_payload.get("submitter")) not in {"", *ORCA_SUBMITTERS}:
            continue
        resource_kwargs: dict[str, Any] = {}
        max_cores = int(enqueue_payload.get("max_cores", 0) or 0)
        max_memory_gb = int(enqueue_payload.get("max_memory_gb", 0) or 0)
        if max_cores > 0:
            resource_kwargs["max_cores"] = max_cores
        if max_memory_gb > 0:
            resource_kwargs["max_memory_gb"] = max_memory_gb
        submission_record = submit_reaction_dir(
            reaction_dir=reaction_dir,
            priority=priority,
            config_path=_normalize_text(orca_auto_config),
            executable=_normalize_text(orca_auto_executable) or CHEMSTACK_EXECUTABLE,
            repo_root=_normalize_text(orca_auto_repo_root) or None,
            **resource_kwargs,
        )
        stdout_payload: dict[str, Any] = _mapping_payload(submission_record.get("parsed_stdout"))
        stderr_text = str(submission_record.get("stderr", "")).strip()
        submission_record["submitted_at"] = now_utc_iso()
        task["submission_result"] = submission_record
        task["status"] = "submitted" if submission_record["status"] == "submitted" else "submission_failed"

        if submission_record["status"] == "submitted":
            stage["status"] = "queued"
            stage_metadata["queue_id"] = stdout_payload.get("queue_id", "")
            stage_metadata["submission_status"] = "submitted"
            stage_metadata["submitted_at"] = submission_record["submitted_at"]
            submitted_record = {
                "stage_id": stage.get("stage_id", ""),
                "queue_id": stdout_payload.get("queue_id", ""),
                "reaction_dir": stdout_payload.get("job_dir") or stdout_payload.get("reaction_dir", reaction_dir),
            }
            submitted.append(submitted_record)
            stage_results.append(
                {
                    "stage_id": stage.get("stage_id", ""),
                    "status": "submitted",
                    "queue_id": stdout_payload.get("queue_id", ""),
                    "returncode": int(submission_record.get("returncode", 1)),
                }
            )
        else:
            stage["status"] = "submission_failed"
            stage_metadata["submission_status"] = "submission_failed"
            stage_metadata["submitted_at"] = submission_record["submitted_at"]
            failed_record = {
                "stage_id": stage.get("stage_id", ""),
                "returncode": int(submission_record.get("returncode", 1)),
                "stderr": stderr_text,
                "stdout": str(submission_record.get("stdout", "")).strip(),
            }
            failed.append(failed_record)
            stage_results.append(
                {
                    "stage_id": stage.get("stage_id", ""),
                    "status": "submission_failed",
                    "queue_id": stdout_payload.get("queue_id", ""),
                    "returncode": int(submission_record.get("returncode", 1)),
                }
            )

    if failed and submitted:
        payload["status"] = "partially_submitted"
    elif failed:
        payload["status"] = "submission_failed"
    elif submitted:
        payload["status"] = "queued"
    payload.setdefault("metadata", {})
    if isinstance(payload["metadata"], dict):
        payload["metadata"]["submission_summary"] = {
            "submitted_count": len(submitted),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "stage_results": stage_results,
            "updated_at": now_utc_iso(),
        }
    write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "submitted": submitted,
        "skipped": skipped,
        "failed": failed,
    }


def cancel_reaction_ts_search_workflow(
    *,
    workflow_target: str,
    workflow_root: str | Path | None,
    orca_auto_config: str | None = None,
    orca_auto_executable: str = CHEMSTACK_EXECUTABLE,
    orca_auto_repo_root: str | None = None,
) -> dict[str, Any]:
    workspace_dir = resolve_workflow_workspace(target=workflow_target, workflow_root=workflow_root)
    payload = load_workflow_payload(workspace_dir)
    cancelled: list[dict[str, Any]] = []
    requested: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    stage_results: list[dict[str, Any]] = []

    for stage in payload.get("stages", []):
        if not isinstance(stage, dict):
            continue
        stage_id = _normalize_text(stage.get("stage_id"))
        task = stage.get("task")
        if not isinstance(task, dict):
            continue
        metadata = task.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            task["metadata"] = metadata
        stage_metadata = stage.get("metadata")
        if not isinstance(stage_metadata, dict):
            stage_metadata = {}
            stage["metadata"] = stage_metadata

        current_task_status = _normalize_text(task.get("status")).lower()
        current_stage_status = _normalize_text(stage.get("status")).lower()
        enqueue_payload = task.get("enqueue_payload")
        if not isinstance(enqueue_payload, dict):
            enqueue_payload = {}

        if current_task_status in {"cancelled", "cancel_requested"} or current_stage_status in {"cancelled", "cancel_requested"}:
            skipped.append({"stage_id": stage_id, "reason": "already_cancelled"})
            stage_results.append({"stage_id": stage_id, "status": "skipped", "reason": "already_cancelled"})
            continue

        if current_task_status in {"completed", "failed"} or current_stage_status in {"completed", "failed"}:
            skipped.append({"stage_id": stage_id, "reason": "already_terminal"})
            stage_results.append({"stage_id": stage_id, "status": "skipped", "reason": "already_terminal"})
            continue

        queue_id = _normalize_text(stage_metadata.get("queue_id"))
        reaction_dir = _normalize_text(
            task.get("payload", {}).get("reaction_dir") if isinstance(task.get("payload"), dict) else ""
        ) or _normalize_text(enqueue_payload.get("reaction_dir"))
        submission_result = task.get("submission_result")
        if not isinstance(submission_result, dict):
            submission_result = {}

        needs_orca_cancel = bool(queue_id or current_task_status in {"submitted"} or current_stage_status in {"queued", "running"})
        if not needs_orca_cancel:
            cancel_record = {
                "status": "cancelled",
                "cancelled_at": now_utc_iso(),
                "mode": "local",
            }
            task["status"] = "cancelled"
            task["cancel_result"] = cancel_record
            stage["status"] = "cancelled"
            stage_metadata["cancel_status"] = "cancelled"
            stage_metadata["cancelled_at"] = cancel_record["cancelled_at"]
            cancelled.append({"stage_id": stage_id, "mode": "local"})
            stage_results.append({"stage_id": stage_id, "status": "cancelled", "mode": "local"})
            continue

        cancel_identifier = queue_id or reaction_dir
        if not cancel_identifier:
            task["cancel_result"] = {
                "status": "failed",
                "reason": "missing_cancel_target",
                "cancelled_at": now_utc_iso(),
            }
            failed.append({"stage_id": stage_id, "reason": "missing_cancel_target"})
            stage_results.append({"stage_id": stage_id, "status": "cancel_failed", "reason": "missing_cancel_target"})
            continue

        if not _normalize_text(orca_auto_config):
            fail_record = {
                "stage_id": stage_id,
                "reason": "orca_auto_config_required",
            }
            task["cancel_result"] = {
                "status": "failed",
                "reason": "orca_auto_config_required",
                "cancelled_at": now_utc_iso(),
            }
            failed.append(fail_record)
            stage_results.append({"stage_id": stage_id, "status": "cancel_failed", "reason": "orca_auto_config_required"})
            continue

        if _normalize_text(enqueue_payload.get("submitter")) not in {"", *ORCA_SUBMITTERS}:
            continue
        cancel_record = cancel_target(
            target=cancel_identifier,
            config_path=_normalize_text(orca_auto_config),
            executable=_normalize_text(orca_auto_executable) or CHEMSTACK_EXECUTABLE,
            repo_root=_normalize_text(orca_auto_repo_root) or None,
        )
        cancel_status = str(cancel_record.get("status", "failed"))
        cancel_record["cancelled_at"] = now_utc_iso()
        cancel_record["target"] = cancel_identifier
        task["cancel_result"] = cancel_record

        if cancel_status == "cancel_requested":
            task["status"] = "cancel_requested"
            stage["status"] = "cancel_requested"
            stage_metadata["cancel_status"] = "cancel_requested"
            stage_metadata["cancelled_at"] = cancel_record["cancelled_at"]
            requested.append({"stage_id": stage_id, "queue_id": queue_id, "reaction_dir": reaction_dir})
            stage_results.append({"stage_id": stage_id, "status": "cancel_requested"})
        elif cancel_status == "cancelled":
            task["status"] = "cancelled"
            stage["status"] = "cancelled"
            stage_metadata["cancel_status"] = "cancelled"
            stage_metadata["cancelled_at"] = cancel_record["cancelled_at"]
            cancelled.append({"stage_id": stage_id, "queue_id": queue_id, "reaction_dir": reaction_dir})
            stage_results.append({"stage_id": stage_id, "status": "cancelled"})
        else:
            failed.append(
                {
                    "stage_id": stage_id,
                    "queue_id": queue_id,
                    "reaction_dir": reaction_dir,
                    "returncode": int(cancel_record.get("returncode", 1)),
                }
            )
            stage_results.append({"stage_id": stage_id, "status": "cancel_failed", "returncode": int(cancel_record.get("returncode", 1))})

    if requested:
        payload["status"] = "cancel_requested"
    elif cancelled:
        payload["status"] = "cancelled"
    elif failed:
        payload["status"] = "cancel_failed"
    payload.setdefault("metadata", {})
    if isinstance(payload["metadata"], dict):
        payload["metadata"]["cancellation_summary"] = {
            "cancelled_count": len(cancelled),
            "requested_count": len(requested),
            "skipped_count": len(skipped),
            "failed_count": len(failed),
            "stage_results": stage_results,
            "updated_at": now_utc_iso(),
        }
    write_workflow_payload(workspace_dir, payload)
    if workflow_root is not None:
        sync_workflow_registry(workflow_root, workspace_dir, payload)
    return {
        "workflow_id": payload.get("workflow_id", ""),
        "workspace_dir": str(workspace_dir),
        "status": payload.get("status", ""),
        "cancelled": cancelled,
        "requested": requested,
        "skipped": skipped,
        "failed": failed,
    }


__all__ = [
    "cancel_target",
    "cancel_reaction_ts_search_workflow",
    "submit_reaction_dir",
    "submit_reaction_ts_search_workflow",
]
