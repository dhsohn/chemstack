from __future__ import annotations

import json
from typing import Any


def reaction_ts_search_plan_payload(args: Any, build_plan: Any) -> dict[str, Any]:
    return build_plan(
        xtb_index_root=getattr(args, "xtb_index_root"),
        target=getattr(args, "target"),
        max_orca_stages=int(getattr(args, "max_orca_stages", 3) or 3),
        selected_only=not bool(getattr(args, "include_unselected", False)),
        workspace_root=getattr(args, "workspace_root", None),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        priority=int(getattr(args, "priority", 10) or 10),
    )


def conformer_screening_plan_payload(args: Any, build_plan: Any) -> dict[str, Any]:
    return build_plan(
        crest_index_root=getattr(args, "crest_index_root"),
        target=getattr(args, "target"),
        max_orca_stages=int(getattr(args, "max_orca_stages", 20) or 20),
        workspace_root=getattr(args, "workspace_root", None),
        charge=int(getattr(args, "charge", 0) or 0),
        multiplicity=int(getattr(args, "multiplicity", 1) or 1),
        max_cores=int(getattr(args, "max_cores", 8) or 8),
        max_memory_gb=int(getattr(args, "max_memory_gb", 32) or 32),
        orca_route_line=str(getattr(args, "orca_route_line", "") or ""),
        priority=int(getattr(args, "priority", 10) or 10),
    )


def emit_workflow_plan(payload: dict[str, Any], *, json_mode: bool, show_enqueue: bool) -> int:
    if json_mode:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0

    print(f"workflow_id: {payload['workflow_id']}")
    print(f"template_name: {payload['template_name']}")
    print(f"status: {payload['status']}")
    print(f"source_job_id: {payload['source_job_id']}")
    print(f"reaction_key: {payload['reaction_key']}")
    workspace_dir = str((payload.get("metadata") or {}).get("workspace_dir", "")).strip()
    print(f"workspace_dir: {workspace_dir or '-'}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    for stage in payload.get("stages", []):
        _emit_stage_line(stage, show_enqueue=show_enqueue)
    return 0


def _emit_stage_line(stage: dict[str, Any], *, show_enqueue: bool) -> None:
    task = stage.get("task") or {}
    task_payload = task.get("payload", {})
    enqueue_payload = task.get("enqueue_payload") or {}
    print(
        f"- {stage.get('stage_id')} {task.get('engine', '-')}/{task.get('task_kind', '-')}"
        f" input={task_payload.get('selected_input_xyz', '-')}"
    )
    if task_payload.get("reaction_dir"):
        print(f"  reaction_dir={task_payload.get('reaction_dir')}")
    if not show_enqueue:
        return
    if enqueue_payload.get("command"):
        print(f"  enqueue_command={enqueue_payload.get('command')}")
    elif task_payload.get("suggested_command"):
        print(f"  suggested_command={task_payload.get('suggested_command')}")
