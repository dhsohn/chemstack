from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from chemstack.core.app_ids import is_orca_submitter
from chemstack.core.utils import now_utc_iso

from ._orchestration_builders import (
    _REACTION_TS_SEARCH_CREST_MANIFEST_DEFAULTS,
    _merge_manifest_defaults,
)
from .registry import append_workflow_journal_event, sync_workflow_registry
from .state import acquire_workflow_lock, load_workflow_payload, workflow_summary, write_workflow_payload
from .workflow_status import WORKFLOW_FAILED_STATUSES

_RESTARTABLE_WORKFLOW_STATUSES = frozenset({*WORKFLOW_FAILED_STATUSES, "cancelled"})
_RESTARTABLE_STAGE_STATUSES = frozenset(
    {
        "failed",
        "cancelled",
        "cancel_failed",
        "submission_failed",
    }
)
_ACTIVE_STAGE_STATUSES = frozenset(
    {
        "queued",
        "running",
        "submitted",
        "cancel_requested",
    }
)
_STALE_STAGE_METADATA_KEYS = frozenset(
    {
        "analyzer_status",
        "cancel_requested",
        "child_job_id",
        "completed_at",
        "latest_known_path",
        "orca_attempts",
        "orca_current_attempt_number",
        "orca_final_result",
        "orca_latest_attempt_number",
        "orca_latest_attempt_status",
        "optimized_xyz_path",
        "organized_output_dir",
        "queue_id",
        "queue_status",
        "reason",
        "run_id",
        "state_status",
        "submission_status",
        "submitted_at",
    }
)
_STALE_TASK_PAYLOAD_KEYS = frozenset(
    {
        "last_out_path",
        "optimized_xyz_path",
        "orca_latest_attempt_inp",
        "orca_latest_attempt_out",
    }
)
_REMATERIALIZED_ENGINES = frozenset({"crest", "xtb"})
_REMATERIALIZED_TASK_PAYLOAD_KEYS = frozenset(
    {"job_dir", "selected_input_xyz", "secondary_input_xyz"}
)
_FLOW_MANIFEST_FILENAMES = ("flow.yaml",)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _stage_task(stage: dict[str, Any]) -> dict[str, Any]:
    task = stage.get("task")
    if isinstance(task, dict):
        return task
    task = {}
    stage["task"] = task
    return task


def _stage_metadata(stage: dict[str, Any]) -> dict[str, Any]:
    metadata = stage.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    metadata = {}
    stage["metadata"] = metadata
    return metadata


def _task_metadata(task: dict[str, Any]) -> dict[str, Any]:
    metadata = task.get("metadata")
    if isinstance(metadata, dict):
        return metadata
    metadata = {}
    task["metadata"] = metadata
    return metadata


def _task_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        return payload
    payload = {}
    task["payload"] = payload
    return payload


def _enqueue_payload(task: dict[str, Any]) -> dict[str, Any]:
    payload = task.get("enqueue_payload")
    if isinstance(payload, dict):
        return payload
    payload = {}
    task["enqueue_payload"] = payload
    return payload


def _task_is_orca(task: dict[str, Any]) -> bool:
    engine = _normalize_text(task.get("engine")).lower()
    if engine == "orca":
        return True
    enqueue_payload = _coerce_mapping(task.get("enqueue_payload"))
    return is_orca_submitter(enqueue_payload.get("submitter"))


def _task_engine(task: dict[str, Any]) -> str:
    return _normalize_text(task.get("engine")).lower()


def _stage_needs_restart(stage: dict[str, Any]) -> bool:
    task = _coerce_mapping(stage.get("task"))
    stage_status = _normalize_text(stage.get("status")).lower()
    task_status = _normalize_text(task.get("status")).lower()
    if stage_status == "completed" and task_status == "completed":
        return False
    return stage_status in _RESTARTABLE_STAGE_STATUSES or task_status in _RESTARTABLE_STAGE_STATUSES


def _active_stage_rows(payload: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for raw_stage in payload.get("stages", []):
        if not isinstance(raw_stage, dict):
            continue
        task = _coerce_mapping(raw_stage.get("task"))
        stage_status = _normalize_text(raw_stage.get("status")).lower()
        task_status = _normalize_text(task.get("status")).lower()
        if stage_status not in _ACTIVE_STAGE_STATUSES and task_status not in _ACTIVE_STAGE_STATUSES:
            continue
        rows.append(
            {
                "stage_id": _normalize_text(raw_stage.get("stage_id")),
                "status": stage_status,
                "task_status": task_status,
                "engine": _normalize_text(task.get("engine")),
            }
        )
    return rows


def _active_restart_error(workflow_id: str, rows: list[dict[str, str]]) -> ValueError:
    shown = []
    for row in rows[:5]:
        stage_id = row.get("stage_id") or "stage"
        status = row.get("status") or "-"
        task_status = row.get("task_status") or "-"
        shown.append(f"{stage_id}(status={status}, task_status={task_status})")
    suffix = f"; active_stages={', '.join(shown)}" if shown else ""
    if len(rows) > len(shown):
        suffix += f"; remaining_active_count={len(rows) - len(shown)}"
    return ValueError(
        f"workflow still has active stages; wait for cancellation/sync to finish before restart: "
        f"{workflow_id}{suffix}"
    )


def _clear_phase_notification_state(metadata: dict[str, Any], restarted_stages: list[dict[str, str]]) -> None:
    phase_notifications = metadata.get("phase_notifications")
    if not isinstance(phase_notifications, dict):
        return

    engines = {
        _normalize_text(stage.get("engine")).lower()
        for stage in restarted_stages
        if _normalize_text(stage.get("engine"))
    }
    for engine in engines:
        phase_notifications.pop(f"{engine}_summary", None)
    if not phase_notifications:
        metadata.pop("phase_notifications", None)


def _manifest_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {str(key): item for key, item in value.items() if _normalize_text(key)}


def _load_flow_manifest(workspace: Path) -> dict[str, Any]:
    for name in _FLOW_MANIFEST_FILENAMES:
        candidate = workspace / name
        if not candidate.is_file():
            continue
        parsed = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
        if not isinstance(parsed, dict):
            raise ValueError(f"Workflow manifest must contain a mapping: {candidate}")
        return dict(parsed)
    return {}


def _resolve_manifest_file_value(workspace: Path, value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = workspace / candidate
    return str(candidate.resolve())


def _resolve_engine_manifest(
    workspace: Path,
    manifest: dict[str, Any],
    key: str,
) -> tuple[bool, dict[str, Any]]:
    if not isinstance(manifest.get(key), dict):
        return False, {}
    resolved = _manifest_mapping(manifest.get(key))
    if "xcontrol_file" in resolved:
        resolved["xcontrol_file"] = _resolve_manifest_file_value(
            workspace,
            resolved.get("xcontrol_file"),
        )
    return True, resolved


def _resolve_endpoint_pairing_manifest(
    manifest: dict[str, Any],
    xtb_manifest: dict[str, Any],
) -> dict[str, Any]:
    xtb_section = _manifest_mapping(xtb_manifest.pop("endpoint_pairing", None))
    top_level = _manifest_mapping(manifest.get("endpoint_pairing"))
    legacy_top_level = _manifest_mapping(manifest.get("xtb_endpoint_pairing"))
    resolved = dict(xtb_section)
    resolved.update(legacy_top_level)
    resolved.update(top_level)
    return resolved


def _positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolved_resource_request(manifest: dict[str, Any]) -> dict[str, int]:
    resources = _manifest_mapping(manifest.get("resources"))
    resolved: dict[str, int] = {}
    max_cores = _positive_int(resources.get("max_cores")) or _positive_int(
        manifest.get("max_cores")
    )
    max_memory_gb = _positive_int(resources.get("max_memory_gb")) or _positive_int(
        manifest.get("max_memory_gb")
    )
    if max_cores is not None:
        resolved["max_cores"] = max_cores
    if max_memory_gb is not None:
        resolved["max_memory_gb"] = max_memory_gb
    return resolved


def _flow_crest_mode(manifest: dict[str, Any], crest_manifest: dict[str, Any]) -> str:
    top_level = _normalize_text(manifest.get("crest_mode")).lower()
    if top_level:
        return "nci" if top_level == "nci" else "standard"
    section_mode = _normalize_text(crest_manifest.get("mode")).lower()
    if section_mode:
        return "nci" if section_mode == "nci" else "standard"
    return ""


def _workflow_template_name(payload: dict[str, Any], manifest: dict[str, Any]) -> str:
    return _normalize_text(payload.get("template_name") or manifest.get("workflow_type")).lower()


def _crest_manifest_with_defaults(
    *,
    template_name: str,
    crest_manifest: dict[str, Any],
) -> dict[str, Any]:
    defaults = (
        _REACTION_TS_SEARCH_CREST_MANIFEST_DEFAULTS
        if template_name == "reaction_ts_search"
        else {}
    )
    return _merge_manifest_defaults(defaults, crest_manifest)


def _set_mapping_field(parent: dict[str, Any], key: str, value: dict[str, Any]) -> None:
    if value:
        parent[key] = dict(value)
    else:
        parent.pop(key, None)


def _set_stage_manifest_overrides(stage: dict[str, Any], overrides: dict[str, Any]) -> None:
    task = _stage_task(stage)
    _set_mapping_field(_task_payload(task), "job_manifest_overrides", overrides)
    _set_mapping_field(_task_metadata(task), "job_manifest_overrides", overrides)
    _set_mapping_field(_stage_metadata(stage), "job_manifest_overrides", overrides)


def _apply_resource_request(task: dict[str, Any], resources: dict[str, int]) -> None:
    if not resources:
        return
    current = _coerce_mapping(task.get("resource_request"))
    task["resource_request"] = {**current, **resources}


def _apply_priority(task: dict[str, Any], priority: int | None) -> None:
    if priority is None:
        return
    enqueue_payload = _enqueue_payload(task)
    enqueue_payload["priority"] = priority
    argv = enqueue_payload.get("command_argv")
    if isinstance(argv, list) and "--priority" in argv:
        index = argv.index("--priority")
        if index + 1 < len(argv):
            argv[index + 1] = str(priority)


def _update_request_parameters(
    payload: dict[str, Any],
    *,
    manifest: dict[str, Any],
    resources: dict[str, int],
    priority: int | None,
    crest_mode: str,
    crest_present: bool,
    crest_overrides: dict[str, Any],
    xtb_present: bool,
    xtb_overrides: dict[str, Any],
    endpoint_pairing: dict[str, Any],
) -> None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
        payload["metadata"] = metadata
    request = metadata.get("request")
    if not isinstance(request, dict):
        return
    params = request.get("parameters")
    if not isinstance(params, dict):
        params = {}
        request["parameters"] = params

    params.update(resources)
    if priority is not None:
        params["priority"] = priority
    if crest_mode:
        params["crest_mode"] = crest_mode
    if crest_present:
        _set_mapping_field(params, "crest_job_manifest", crest_overrides)
    if xtb_present:
        _set_mapping_field(params, "xtb_job_manifest", xtb_overrides)
    _set_mapping_field(params, "endpoint_pairing", endpoint_pairing)

    for key in (
        "max_crest_candidates",
        "max_xtb_stages",
        "max_xtb_handoff_retries",
        "max_orca_stages",
    ):
        parsed = _positive_int(manifest.get(key))
        if parsed is not None:
            params[key] = parsed

    orca_manifest = _manifest_mapping(manifest.get("orca"))
    route_line = _normalize_text(manifest.get("orca_route_line") or orca_manifest.get("route_line"))
    if route_line:
        params["orca_route_line"] = route_line
    charge = _optional_int(
        manifest.get("charge") if "charge" in manifest else orca_manifest.get("charge")
    )
    if charge is not None:
        params["charge"] = charge
    raw_multiplicity = (
        manifest.get("multiplicity")
        if "multiplicity" in manifest
        else orca_manifest.get("multiplicity")
    )
    multiplicity = _positive_int(raw_multiplicity)
    if multiplicity is not None:
        params["multiplicity"] = multiplicity


def _flow_restart_settings(workspace: Path, payload: dict[str, Any]) -> dict[str, Any]:
    manifest = _load_flow_manifest(workspace)
    if not manifest:
        return {"applied": False}
    template_name = _workflow_template_name(payload, manifest)
    crest_present, crest_manifest = _resolve_engine_manifest(workspace, manifest, "crest")
    xtb_present, xtb_manifest = _resolve_engine_manifest(workspace, manifest, "xtb")
    endpoint_pairing = _resolve_endpoint_pairing_manifest(manifest, xtb_manifest)
    crest_overrides = _crest_manifest_with_defaults(
        template_name=template_name,
        crest_manifest=crest_manifest,
    )
    priority = _positive_int(manifest.get("priority"))
    resources = _resolved_resource_request(manifest)
    crest_mode = _flow_crest_mode(manifest, crest_manifest)
    _update_request_parameters(
        payload,
        manifest=manifest,
        resources=resources,
        priority=priority,
        crest_mode=crest_mode,
        crest_present=crest_present,
        crest_overrides=crest_overrides,
        xtb_present=xtb_present,
        xtb_overrides=xtb_manifest,
        endpoint_pairing=endpoint_pairing,
    )
    return {
        "applied": True,
        "resources": resources,
        "priority": priority,
        "crest_present": crest_present,
        "crest_mode": crest_mode,
        "crest_overrides": crest_overrides,
        "xtb_present": xtb_present,
        "xtb_overrides": xtb_manifest,
        "endpoint_pairing": endpoint_pairing,
    }


def _apply_flow_restart_settings(stage: dict[str, Any], settings: dict[str, Any]) -> None:
    if not settings.get("applied"):
        return
    task = _stage_task(stage)
    engine = _task_engine(task)
    _apply_resource_request(task, _coerce_mapping(settings.get("resources")))
    _apply_priority(
        task,
        settings.get("priority") if isinstance(settings.get("priority"), int) else None,
    )

    if engine == "crest":
        crest_mode = _normalize_text(settings.get("crest_mode"))
        if crest_mode:
            _task_payload(task)["mode"] = crest_mode
            _task_metadata(task)["mode"] = crest_mode
            _stage_metadata(stage)["mode"] = crest_mode
        if bool(settings.get("crest_present")):
            _set_stage_manifest_overrides(stage, _coerce_mapping(settings.get("crest_overrides")))
    elif engine == "xtb" and bool(settings.get("xtb_present")):
        _set_stage_manifest_overrides(stage, _coerce_mapping(settings.get("xtb_overrides")))


def _stage_should_rematerialize(stage: dict[str, Any], settings: dict[str, Any]) -> bool:
    if not settings.get("applied"):
        return False
    task = _stage_task(stage)
    engine = _task_engine(task)
    if engine not in _REMATERIALIZED_ENGINES:
        return False
    has_common_updates = bool(_coerce_mapping(settings.get("resources"))) or isinstance(
        settings.get("priority"),
        int,
    )
    if engine == "crest":
        return (
            has_common_updates
            or bool(settings.get("crest_present"))
            or bool(_normalize_text(settings.get("crest_mode")))
        )
    if engine == "xtb":
        return has_common_updates or bool(settings.get("xtb_present"))
    return False


def _reset_stage_for_restart(
    stage: dict[str, Any],
    *,
    rematerialize: bool = False,
) -> dict[str, str]:
    task = _stage_task(stage)
    metadata = _stage_metadata(stage)
    task_payload = _task_payload(task)
    enqueue_payload = _enqueue_payload(task)
    engine = _task_engine(task)

    previous = {
        "stage_id": _normalize_text(stage.get("stage_id")),
        "previous_status": _normalize_text(stage.get("status")),
        "previous_task_status": _normalize_text(task.get("status")),
        "engine": _normalize_text(task.get("engine")),
    }

    stage["status"] = "planned"
    task["status"] = "planned"
    stage["output_artifacts"] = []
    task.pop("submission_result", None)
    task.pop("cancel_result", None)

    for key in _STALE_STAGE_METADATA_KEYS:
        metadata.pop(key, None)
    for key in _STALE_TASK_PAYLOAD_KEYS:
        task_payload.pop(key, None)

    if rematerialize and engine in _REMATERIALIZED_ENGINES:
        for key in _REMATERIALIZED_TASK_PAYLOAD_KEYS:
            if key in task_payload:
                task_payload[key] = ""
        if "job_dir" in enqueue_payload:
            enqueue_payload["job_dir"] = ""

    if _task_is_orca(task):
        enqueue_payload["force"] = True

    return previous


def restart_failed_workflow(
    *,
    workspace_dir: str | Path,
    workflow_root: str | Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    workspace = Path(workspace_dir).expanduser().resolve()
    root = (
        Path(workflow_root).expanduser().resolve()
        if workflow_root is not None
        else workspace.parent
    )

    with acquire_workflow_lock(workspace):
        payload = load_workflow_payload(workspace)
        previous_status = _normalize_text(payload.get("status")).lower()
        force_restart = bool(force)
        if previous_status not in _RESTARTABLE_WORKFLOW_STATUSES and not force_restart:
            raise ValueError(
                f"workflow is not failed or cancelled: {payload.get('workflow_id', workspace.name)} "
                f"(status={previous_status or 'unknown'})"
            )
        workflow_id = _normalize_text(payload.get("workflow_id")) or workspace.name

        active_stages = _active_stage_rows(payload)
        if active_stages:
            raise _active_restart_error(workflow_id, active_stages)

        flow_settings = _flow_restart_settings(workspace, payload)
        restarted_stages: list[dict[str, str]] = []
        for raw_stage in payload.get("stages", []):
            if not isinstance(raw_stage, dict) or not _stage_needs_restart(raw_stage):
                continue
            _apply_flow_restart_settings(raw_stage, flow_settings)
            restarted_stages.append(
                _reset_stage_for_restart(
                    raw_stage,
                    rematerialize=_stage_should_rematerialize(raw_stage, flow_settings),
                )
            )

        if not restarted_stages:
            raise ValueError(
                f"workflow has no failed or cancelled stages to restart: "
                f"{workflow_id}"
            )

        restarted_at = now_utc_iso()
        payload["status"] = "planned"
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            metadata = {}
            payload["metadata"] = metadata
        metadata.pop("workflow_error", None)
        _clear_phase_notification_state(metadata, restarted_stages)
        metadata["final_child_sync_pending"] = False
        metadata["final_child_sync_completed_at"] = ""
        metadata["last_restarted_at"] = restarted_at
        metadata["restart_summary"] = {
            "status": "restarted",
            "previous_status": previous_status,
            "restarted_at": restarted_at,
            "restarted_count": len(restarted_stages),
            "flow_manifest_applied": bool(flow_settings.get("applied")),
            "stages": restarted_stages,
        }

        write_workflow_payload(workspace, payload)
        sync_workflow_registry(root, workspace, payload)
        summary = workflow_summary(workspace, payload)

    append_workflow_journal_event(
        root,
        event_type="workflow_restarted",
        workflow_id=_normalize_text(payload.get("workflow_id")),
        template_name=_normalize_text(payload.get("template_name")),
        previous_status=previous_status,
        status="planned",
        reason="run_dir_restart",
        metadata={
            "workspace_dir": str(workspace),
            "restarted_count": len(restarted_stages),
            "flow_manifest_applied": bool(flow_settings.get("applied")),
            "stages": restarted_stages,
        },
    )
    return {
        "workflow_id": _normalize_text(payload.get("workflow_id")),
        "template_name": _normalize_text(payload.get("template_name")),
        "workspace_dir": str(workspace),
        "workflow_root": str(root),
        "status": "restarted",
        "workflow_status": "planned",
        "previous_status": previous_status,
        "restarted_count": len(restarted_stages),
        "restarted_stages": restarted_stages,
        "summary": summary,
    }


__all__ = ["restart_failed_workflow"]
