from __future__ import annotations

from typing import Any

from chemstack.core.commands.run_dir import (
    EngineQueuedRecord,
    EngineRunDirSubmission,
    cmd_engine_run_dir_from_module_globals,
    print_queued_common,
    record_queued_common,
)
from chemstack.core.config.engines import load_xtb_config as load_config
from chemstack.core.notifications.engines import notify_xtb_job_queued as notify_job_queued
from chemstack.core.queue import enqueue

from .job_locations import index_root_for_path, upsert_job_record
from .state import write_state
from .job_inputs import (
    load_job_manifest,
    new_job_id,
    queued_state_payload,
    resolve_job_dir,
    resolve_job_inputs,
    resource_request_from_manifest,
)

__all__ = [
    "cmd_run_dir",
    "enqueue",
    "load_config",
    "load_job_manifest",
    "resolve_job_dir",
]


def _build_submission(
    cfg: Any,
    job_dir: Any,
    manifest: dict[str, Any],
    args: Any,
) -> EngineRunDirSubmission:
    job = resolve_job_inputs(job_dir, manifest)
    job_id = new_job_id()
    resource_request = resource_request_from_manifest(cfg, manifest)
    input_summary = dict(job["input_summary"])
    return EngineRunDirSubmission(
        queue_root=index_root_for_path(cfg, job_dir),
        app_name="chemstack_xtb",
        task_id=job_id,
        task_kind=f"xtb_{job['job_type']}",
        engine="xtb",
        priority=int(getattr(args, "priority", 10)),
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(job["selected_input_xyz"]),
            "secondary_input_xyz": str(job["secondary_input_xyz"] or ""),
            "job_type": str(job["job_type"]),
            "reaction_key": str(job["reaction_key"]),
            "input_summary": input_summary,
            "manifest_present": "true" if manifest else "false",
            "candidate_paths": list(input_summary.get("candidate_paths", [])),
            "resource_request": dict(resource_request),
            "resource_actual": dict(resource_request),
        },
        context={
            "job": job,
            "job_dir": job_dir,
            "input_summary": input_summary,
            "resource_request": resource_request,
        },
    )


def _queued_record(submission: EngineRunDirSubmission, _entry: Any) -> EngineQueuedRecord:
    job = submission.context["job"]
    job_dir = submission.context["job_dir"]
    input_summary = submission.context["input_summary"]
    resource_request = submission.context["resource_request"]
    return EngineQueuedRecord(
        state_payload=queued_state_payload(
            job_id=submission.task_id,
            job_dir=job_dir,
            selected_input_xyz=job["selected_input_xyz"],
            job_type=str(job["job_type"]),
            reaction_key=str(job["reaction_key"]),
            input_summary=input_summary,
            resource_request=resource_request,
        ),
        index_fields={
            "job_type": str(job["job_type"]),
            "selected_input_xyz": str(job["selected_input_xyz"]),
            "reaction_key": str(job["reaction_key"]),
            "resource_request": resource_request,
            "resource_actual": resource_request,
        },
        notification_fields={
            "job_type": str(job["job_type"]),
            "reaction_key": str(job["reaction_key"]),
            "selected_xyz": job["selected_input_xyz"],
        },
    )


def _record_queued(cfg: Any, submission: EngineRunDirSubmission, entry: Any) -> None:
    record_queued_common(
        cfg,
        submission,
        entry,
        build_record_fn=_queued_record,
        write_state_fn=write_state,
        upsert_job_record_fn=upsert_job_record,
        notify_job_queued_fn=notify_job_queued,
    )


def _print_queued(submission: EngineRunDirSubmission, entry: Any) -> None:
    job = submission.context["job"]
    job_dir = submission.context["job_dir"]
    extra_fields = [
        ("job_type", job["job_type"]),
        ("reaction_key", job["reaction_key"]),
        ("selected_input_xyz", job["selected_input_xyz"].name),
    ]
    if job["job_type"] == "ranking":
        extra_fields.append(("candidate_count", job["input_summary"].get("candidate_count", 0)))
    print_queued_common(submission, entry, job_dir=job_dir, extra_fields=extra_fields)


def cmd_run_dir(args: Any) -> int:
    return cmd_engine_run_dir_from_module_globals(args, globals())
