from __future__ import annotations

from typing import Any

from chemstack.core.commands.run_dir import (
    EngineQueuedRecord,
    EngineRunDirSubmission,
    EngineSubmissionSpec,
    build_engine_queued_record,
    build_engine_run_dir_submission_from_spec,
    record_engine_run_dir_queued,
)
from chemstack.core.config.engines import (
    load_xtb_config as load_config,
    resource_request_from_manifest,
)
from chemstack.core.notifications import engines as _notification_engines
from chemstack.core.queue import enqueue

from . import job_locations as _job_locations
from . import state as _state
from .job_locations import index_root_for_path
from .job_inputs import (
    load_job_manifest,
    new_job_id,
    queued_state_payload,
    resolve_job_dir,
    resolve_job_inputs,
)

notify_job_queued = _notification_engines.notify_xtb_job_queued
upsert_job_record = _job_locations.upsert_job_record
write_state = _state.write_state

__all__ = [
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
    input_summary = dict(job["input_summary"])
    return build_engine_run_dir_submission_from_spec(
        spec=EngineSubmissionSpec(
            queue_root=index_root_for_path(cfg, job_dir),
            app_name="chemstack_xtb",
            task_id=job_id,
            task_kind=f"xtb_{job['job_type']}",
            engine="xtb",
            metadata={
                "job_dir": str(job_dir),
                "selected_input_xyz": str(job["selected_input_xyz"]),
                "secondary_input_xyz": str(job["secondary_input_xyz"] or ""),
                "job_type": str(job["job_type"]),
                "reaction_key": str(job["reaction_key"]),
                "input_summary": input_summary,
                "candidate_paths": list(input_summary.get("candidate_paths", [])),
            },
            context={
                "job": job,
                "job_dir": job_dir,
                "input_summary": input_summary,
            },
        ),
        args=args,
        manifest=manifest,
        resource_request=resource_request_from_manifest(cfg, manifest),
    )


def _queued_record(submission: EngineRunDirSubmission, _entry: Any) -> EngineQueuedRecord:
    job = submission.context["job"]
    job_dir = submission.context["job_dir"]
    input_summary = submission.context["input_summary"]
    resource_request = submission.context["resource_request"]
    return build_engine_queued_record(
        submission=submission,
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
        },
        notification_fields={
            "job_type": str(job["job_type"]),
            "reaction_key": str(job["reaction_key"]),
            "selected_xyz": job["selected_input_xyz"],
        },
    )


def _record_queued(cfg: Any, submission: EngineRunDirSubmission, entry: Any) -> None:
    record_engine_run_dir_queued(
        cfg,
        submission,
        entry,
        namespace=globals(),
    )
