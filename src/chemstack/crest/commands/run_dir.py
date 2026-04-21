from __future__ import annotations

from typing import Any

from chemstack.core.queue import DuplicateQueueEntryError, enqueue

from ..config import load_config
from ..job_locations import molecule_key_from_selected_xyz, upsert_job_record
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


def cmd_run_dir(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_job_dir = getattr(args, "path", None)
    if not isinstance(raw_job_dir, str) or not raw_job_dir.strip():
        raise ValueError("job directory path is required")

    job_dir = resolve_job_dir(cfg, raw_job_dir)
    manifest = load_job_manifest(job_dir)
    selected_xyz = select_input_xyz(job_dir, manifest)
    job_id = new_job_id()
    mode = job_mode(manifest)
    molecule_key = molecule_key_from_selected_xyz(str(selected_xyz), job_dir)
    resource_request = resource_request_from_manifest(cfg, manifest)

    try:
        entry = enqueue(
            cfg.runtime.allowed_root,
            app_name="crest_auto",
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
        )
    except DuplicateQueueEntryError as exc:
        print(f"error: {exc}")
        return 1

    write_state(
        job_dir,
        queued_state_payload(
            job_id=job_id,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            mode=mode,
            molecule_key=molecule_key,
            resource_request=resource_request,
        ),
    )
    upsert_job_record(
        cfg,
        job_id=job_id,
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
        job_id=job_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        mode=mode,
        selected_xyz=selected_xyz,
    )

    print("status: queued")
    print(f"job_dir: {job_dir}")
    print(f"job_id: {job_id}")
    print(f"queue_id: {entry.queue_id}")
    print(f"priority: {entry.priority}")
    print(f"selected_input_xyz: {selected_xyz.name}")
    return 0
