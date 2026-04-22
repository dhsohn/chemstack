from __future__ import annotations

import json
from typing import Any

from ..config import load_config
from ..tracking import load_job_artifacts_for_cfg, resolve_job_location_for_cfg


def cmd_summary(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: summary requires a job_id or job directory")
        return 1

    _root, record = resolve_job_location_for_cfg(cfg, target)
    job_dir, state, report, record = load_job_artifacts_for_cfg(cfg, target)
    if job_dir is None:
        print(f"error: job not found: {target}")
        return 1

    payload = {
        "target": target,
        "job_dir": str(job_dir),
        "index_record": {
            "job_id": record.job_id,
            "status": record.status,
            "job_type": record.job_type,
            "original_run_dir": record.original_run_dir,
            "molecule_key": record.molecule_key,
            "selected_input_xyz": record.selected_input_xyz,
            "organized_output_dir": record.organized_output_dir,
            "latest_known_path": record.latest_known_path,
            "resource_request": record.resource_request,
            "resource_actual": record.resource_actual,
        }
        if record is not None
        else None,
        "state": state,
        "report": report,
    }

    if bool(getattr(args, "json", False)):
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    state = state or {}
    report = report or {}
    print(f"job_dir: {job_dir}")
    if record is not None:
        print(f"job_id: {record.job_id}")
        print(f"latest_known_path: {record.latest_known_path}")
        print(f"molecule_key: {record.molecule_key or '-'}")
        print(f"selected_input_xyz: {record.selected_input_xyz or '-'}")
        if record.organized_output_dir:
            print(f"organized_output_dir: {record.organized_output_dir}")
        if record.resource_request:
            print(f"resource_request: {record.resource_request}")
        if record.resource_actual:
            print(f"resource_actual: {record.resource_actual}")
    print(f"status: {report.get('status') or state.get('status') or '-'}")
    print(f"reason: {report.get('reason') or state.get('reason') or '-'}")
    print(f"mode: {report.get('mode') or state.get('mode') or '-'}")
    print(f"molecule_key: {report.get('molecule_key') or state.get('molecule_key') or '-'}")
    print(f"selected_input_xyz: {report.get('selected_input_xyz') or state.get('selected_input_xyz') or '-'}")
    print(f"retained_conformer_count: {report.get('retained_conformer_count') or state.get('retained_conformer_count') or 0}")
    if report.get("resource_request") or state.get("resource_request"):
        print(f"resource_request: {report.get('resource_request') or state.get('resource_request')}")
    if report.get("resource_actual") or state.get("resource_actual"):
        print(f"resource_actual: {report.get('resource_actual') or state.get('resource_actual')}")
    print(f"stdout_log: {report.get('stdout_log') or '-'}")
    print(f"stderr_log: {report.get('stderr_log') or '-'}")
    return 0
