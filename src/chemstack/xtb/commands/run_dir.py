from __future__ import annotations

from typing import Any

from chemstack.core.queue import DuplicateQueueEntryError, enqueue

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


def cmd_run_dir(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_job_dir = getattr(args, "path", None)
    if not isinstance(raw_job_dir, str) or not raw_job_dir.strip():
        raise ValueError("job directory path is required")

    job_dir = resolve_job_dir(cfg, raw_job_dir)
    manifest = load_job_manifest(job_dir)
    job = resolve_job_inputs(job_dir, manifest)
    job_id = new_job_id()
    resource_request = resource_request_from_manifest(cfg, manifest)
    queue_root = index_root_for_path(cfg, job_dir)

    try:
        entry = enqueue(
            queue_root,
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
                "input_summary": dict(job["input_summary"]),
                "manifest_present": "true" if manifest else "false",
                "candidate_paths": list(job["input_summary"].get("candidate_paths", []))
                if isinstance(job["input_summary"], dict)
                else [],
                "resource_request": dict(resource_request),
                "resource_actual": dict(resource_request),
            },
        )
    except DuplicateQueueEntryError as exc:
        print(f"error: {exc}")
        return 1

    write_state(
        job_dir,
        queued_state_payload(
            job_id=job_id,
            job_dir=job_dir,
            selected_input_xyz=job["selected_input_xyz"],
            job_type=str(job["job_type"]),
            reaction_key=str(job["reaction_key"]),
            input_summary=dict(job["input_summary"]),
            resource_request=resource_request,
        ),
    )
    upsert_job_record(
        cfg,
        job_id=job_id,
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
        job_id=job_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        job_type=str(job["job_type"]),
        reaction_key=str(job["reaction_key"]),
        selected_xyz=job["selected_input_xyz"],
    )

    print("status: queued")
    print(f"job_dir: {job_dir}")
    print(f"job_id: {job_id}")
    print(f"queue_id: {entry.queue_id}")
    print(f"priority: {entry.priority}")
    print(f"job_type: {job['job_type']}")
    print(f"reaction_key: {job['reaction_key']}")
    print(f"selected_input_xyz: {job['selected_input_xyz'].name}")
    if job["job_type"] == "ranking":
        print(f"candidate_count: {job['input_summary'].get('candidate_count', 0)}")
    return 0
