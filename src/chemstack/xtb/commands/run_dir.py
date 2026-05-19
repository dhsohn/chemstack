from __future__ import annotations

from typing import Any

from chemstack.core.commands.run_dir import (
    EngineRunDirSubmission,
    cmd_engine_run_dir,
    print_queued_common,
)
from chemstack.core.queue import enqueue

from ..config import load_config
from ..job_locations import index_root_for_path, upsert_job_record
from ..notifications import notify_job_queued
from ..state import write_state
from ._helpers import (
    load_job_manifest,
    new_job_id,
    queued_state_payload,
    resolve_job_dir,
    resolve_job_inputs,
    resource_request_from_manifest,
)


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
        app_name="xtb_auto",
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


def _record_queued(cfg: Any, submission: EngineRunDirSubmission, entry: Any) -> None:
    job = submission.context["job"]
    job_dir = submission.context["job_dir"]
    input_summary = submission.context["input_summary"]
    resource_request = submission.context["resource_request"]
    write_state(
        job_dir,
        queued_state_payload(
            job_id=submission.task_id,
            job_dir=job_dir,
            selected_input_xyz=job["selected_input_xyz"],
            job_type=str(job["job_type"]),
            reaction_key=str(job["reaction_key"]),
            input_summary=input_summary,
            resource_request=resource_request,
        )
    )
    upsert_job_record(
        cfg,
        job_id=submission.task_id,
        status="queued",
        job_dir=job_dir,
        job_type=str(job["job_type"]),
        selected_input_xyz=str(job["selected_input_xyz"]),
        reaction_key=str(job["reaction_key"]),
        resource_request=resource_request,
        resource_actual=resource_request,
    )
    notify_job_queued(
        cfg,
        job_id=submission.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        job_type=str(job["job_type"]),
        reaction_key=str(job["reaction_key"]),
        selected_xyz=job["selected_input_xyz"],
    )


def _print_queued(submission: EngineRunDirSubmission, entry: Any) -> None:
    job = submission.context["job"]
    job_dir = submission.context["job_dir"]
    print_queued_common(submission, entry, job_dir=job_dir)
    print(f"job_type: {job['job_type']}")
    print(f"reaction_key: {job['reaction_key']}")
    print(f"selected_input_xyz: {job['selected_input_xyz'].name}")
    if job["job_type"] == "ranking":
        print(f"candidate_count: {job['input_summary'].get('candidate_count', 0)}")


def cmd_run_dir(args: Any) -> int:
    return cmd_engine_run_dir(
        args,
        load_config_fn=load_config,
        resolve_job_dir_fn=resolve_job_dir,
        load_manifest_fn=load_job_manifest,
        build_submission_fn=_build_submission,
        record_queued_fn=_record_queued,
        print_queued_fn=_print_queued,
        enqueue_fn=enqueue,
    )
