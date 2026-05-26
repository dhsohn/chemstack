from __future__ import annotations

from typing import Any

from chemstack.core.commands.run_dir import (
    EngineRunDirSubmission,
    cmd_engine_run_dir,
    print_queued_common,
)
from chemstack.core.queue import enqueue

from ..config import load_config
from ..job_locations import index_root_for_path, molecule_key_from_selected_xyz, upsert_job_record
from ..notifications import notify_job_queued
from ..state import write_state
from ._helpers import (
    job_mode,
    load_job_manifest,
    new_job_id,
    queued_state_payload,
    resource_request_from_manifest,
    resolve_job_dir,
    select_input_xyz,
)


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


def _record_queued(cfg: Any, submission: EngineRunDirSubmission, entry: Any) -> None:
    job_dir = submission.context["job_dir"]
    selected_xyz = submission.context["selected_xyz"]
    mode = submission.context["mode"]
    molecule_key = submission.context["molecule_key"]
    resource_request = submission.context["resource_request"]
    write_state(
        job_dir,
        queued_state_payload(
            job_id=submission.task_id,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            mode=mode,
            molecule_key=molecule_key,
            resource_request=resource_request,
        )
    )
    upsert_job_record(
        cfg,
        job_id=submission.task_id,
        status="queued",
        job_dir=job_dir,
        mode=mode,
        selected_input_xyz=str(selected_xyz),
        molecule_key=molecule_key,
        resource_request=resource_request,
        resource_actual=resource_request,
    )
    notify_job_queued(
        cfg,
        job_id=submission.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        mode=mode,
        selected_xyz=selected_xyz,
    )


def _print_queued(submission: EngineRunDirSubmission, entry: Any) -> None:
    job_dir = submission.context["job_dir"]
    selected_xyz = submission.context["selected_xyz"]
    print_queued_common(submission, entry, job_dir=job_dir)
    print(f"selected_input_xyz: {selected_xyz.name}")


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
