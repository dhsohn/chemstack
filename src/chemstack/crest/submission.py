from __future__ import annotations

from typing import Any

from chemstack.core.commands.run_dir import (
    EngineQueuedRecord,
    EngineRunDirSubmission,
    record_queued_common,
)
from chemstack.core.config.engines import (
    load_crest_config as load_config,
    resource_request_from_manifest,
)
from chemstack.core.notifications.engines import (
    notify_crest_job_queued as notify_job_queued,
)
from chemstack.core.queue import enqueue

from .job_locations import index_root_for_path, molecule_key_from_selected_xyz, upsert_job_record
from .state import write_state
from .job_inputs import (
    job_mode,
    load_job_manifest,
    new_job_id,
    queued_state_payload,
    resolve_job_dir,
    select_input_xyz,
)

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
    selected_xyz = select_input_xyz(job_dir, manifest)
    job_id = new_job_id()
    mode = job_mode(manifest)
    molecule_key = molecule_key_from_selected_xyz(str(selected_xyz), job_dir)
    resource_request = resource_request_from_manifest(cfg, manifest)
    return EngineRunDirSubmission(
        queue_root=index_root_for_path(cfg, job_dir),
        app_name="chemstack_crest",
        task_id=job_id,
        task_kind="crest_conformer_search",
        engine="crest",
        priority=int(getattr(args, "priority", 10)),
        metadata={
            "job_dir": str(job_dir),
            "selected_input_xyz": str(selected_xyz),
            "mode": mode,
            "molecule_key": molecule_key,
            "manifest_present": "true" if manifest else "false",
            "resource_request": dict(resource_request),
            "resource_actual": dict(resource_request),
        },
        context={
            "job_dir": job_dir,
            "selected_xyz": selected_xyz,
            "mode": mode,
            "molecule_key": molecule_key,
            "resource_request": resource_request,
        },
    )


def _queued_record(submission: EngineRunDirSubmission, _entry: Any) -> EngineQueuedRecord:
    job_dir = submission.context["job_dir"]
    selected_xyz = submission.context["selected_xyz"]
    mode = submission.context["mode"]
    molecule_key = submission.context["molecule_key"]
    resource_request = submission.context["resource_request"]
    return EngineQueuedRecord(
        state_payload=queued_state_payload(
            job_id=submission.task_id,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            mode=mode,
            molecule_key=molecule_key,
            resource_request=resource_request,
        ),
        index_fields={
            "mode": mode,
            "selected_input_xyz": str(selected_xyz),
            "molecule_key": molecule_key,
            "resource_request": resource_request,
            "resource_actual": resource_request,
        },
        notification_fields={
            "mode": mode,
            "selected_xyz": selected_xyz,
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
