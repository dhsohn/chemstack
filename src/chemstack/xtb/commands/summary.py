from __future__ import annotations

import json
from typing import Any

from chemstack.core.indexing import resolve_job_location

from ..config import load_config
from ..tracking import index_root_for_cfg, load_job_artifacts


def cmd_summary(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: summary requires a job_id or job directory")
        return 1

    index_root = index_root_for_cfg(cfg)
    record = resolve_job_location(index_root, target)
    job_dir, state, report = load_job_artifacts(index_root, target)
    if job_dir is None:
        print(f"error: job not found: {target}")
        return 1

    reaction_key = record.molecule_key if record is not None else ""
    payload = {
        "target": target,
        "job_dir": str(job_dir),
        "index_record": {
            "job_id": record.job_id,
            "status": record.status,
            "job_type": record.job_type,
            "original_run_dir": record.original_run_dir,
            "reaction_key": reaction_key,
            "molecule_key": reaction_key,
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
        print(f"reaction_key: {reaction_key or '-'}")
        print(f"selected_input_xyz: {record.selected_input_xyz or '-'}")
        if record.organized_output_dir:
            print(f"organized_output_dir: {record.organized_output_dir}")
        if record.resource_request:
            print(f"resource_request: {record.resource_request}")
        if record.resource_actual:
            print(f"resource_actual: {record.resource_actual}")
    print(f"status: {report.get('status') or state.get('status') or '-'}")
    print(f"reason: {report.get('reason') or state.get('reason') or '-'}")
    print(f"job_type: {report.get('job_type') or state.get('job_type') or '-'}")
    print(f"reaction_key: {report.get('reaction_key') or state.get('reaction_key') or reaction_key or '-'}")
    print(f"selected_input_xyz: {report.get('selected_input_xyz') or state.get('selected_input_xyz') or '-'}")
    print(f"candidate_count: {report.get('candidate_count') or state.get('candidate_count') or 0}")
    if report.get("selected_candidate_paths") or state.get("selected_candidate_paths"):
        print(f"selected_candidate_paths: {report.get('selected_candidate_paths') or state.get('selected_candidate_paths')}")
    if report.get("analysis_summary") or state.get("analysis_summary"):
        print(f"analysis_summary: {report.get('analysis_summary') or state.get('analysis_summary')}")
    if report.get("resource_request") or state.get("resource_request"):
        print(f"resource_request: {report.get('resource_request') or state.get('resource_request')}")
    if report.get("resource_actual") or state.get("resource_actual"):
        print(f"resource_actual: {report.get('resource_actual') or state.get('resource_actual')}")
    print(f"stdout_log: {report.get('stdout_log') or '-'}")
    print(f"stderr_log: {report.get('stderr_log') or '-'}")
    return 0
