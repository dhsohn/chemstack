from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any


def emit_json(payload: dict[str, Any], *, pretty: bool) -> None:
    indent = 2 if pretty else None
    print(json.dumps(payload, ensure_ascii=True, indent=indent))


def _emit_json_when_requested(
    payload: dict[str, Any], *, json_mode: bool, pretty: bool = True
) -> bool:
    if not json_mode:
        return False
    emit_json(payload, pretty=pretty)
    return True


def emit_error(message: Any) -> None:
    print(f"error: {message}")


def emit_worker_lock_error(message: Any) -> None:
    print(f"worker_lock_error: {message}")


def _emit_stage_result_group(
    payload: dict[str, Any],
    key: str,
    *,
    count_label: str,
    item_label: str,
    fields: tuple[str, ...],
    always: bool = False,
) -> None:
    items = payload.get(key, [])
    if not always and not items:
        return
    print(f"{count_label}: {len(items)}")
    for item in items:
        details = " ".join(f"{field}={item.get(field, '-')}" for field in fields)
        print(f"- {item_label} {item.get('stage_id', '-')}" + (f" {details}" if details else ""))


def emit_workflow_advance(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    return 0


def emit_created_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"template_name: {payload.get('template_name', '-')}")
    print(f"workspace_dir: {(payload.get('metadata') or {}).get('workspace_dir', '-')}")
    print(f"stage_count: {len(payload.get('stages', []))}")
    return 0


def emit_restarted_workflow(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"status: {payload.get('status', '-')}")
    print(f"workflow_status: {payload.get('workflow_status', '-')}")
    print(f"previous_status: {payload.get('previous_status', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"restarted_count: {payload.get('restarted_count', 0)}")
    for item in payload.get("restarted_stages", []):
        print(
            f"- restarted {item.get('stage_id', '-')}"
            f" previous_status={item.get('previous_status', '-')}"
            f" previous_task_status={item.get('previous_task_status', '-')}"
        )
    return 0


def emit_worker_payload(payload: dict[str, Any], *, json_mode: bool, single_cycle: bool) -> None:
    if _emit_json_when_requested(payload, json_mode=json_mode, pretty=single_cycle):
        return

    print(
        f"cycle_started_at: {payload.get('cycle_started_at', '-')}"
        f" worker_session_id={payload.get('worker_session_id', '-')}"
        f" discovered={payload.get('discovered_count', 0)}"
        f" advanced={payload.get('advanced_count', 0)}"
        f" skipped={payload.get('skipped_count', 0)}"
        f" failed={payload.get('failed_count', 0)}"
    )
    for item in payload.get("workflow_results", []):
        print(
            f"- {item.get('workflow_id', '-')} template={item.get('template_name', '-')}"
            f" previous={item.get('previous_status', '-')}"
            f" status={item.get('status', '-')}"
            f" advanced={'yes' if item.get('advanced') else 'no'}"
        )
        if item.get("reason"):
            print(f"  reason={item.get('reason')}")


def emit_workflow_runtime_status(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    state = payload["worker_state"] or {}
    print(f"worker_session_id: {state.get('worker_session_id', '-')}")
    print(f"status: {state.get('status', '-')}")
    print(f"pid: {state.get('pid', '-')}")
    print(f"hostname: {state.get('hostname', '-')}")
    print(f"last_heartbeat_at: {state.get('last_heartbeat_at', '-')}")
    print(f"lease_expires_at: {state.get('lease_expires_at', '-')}")
    print(f"last_cycle_started_at: {state.get('last_cycle_started_at', '-')}")
    print(f"last_cycle_finished_at: {state.get('last_cycle_finished_at', '-')}")
    return 0


def emit_workflow_journal(payload: dict[str, Any], *, json_mode: bool) -> int:
    events = payload["events"]
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"event_count: {len(events)}")
    for item in events:
        print(
            f"- {item.get('occurred_at', '-')} {item.get('event_type', '-')}"
            f" workflow_id={item.get('workflow_id', '-') or '-'}"
            f" status={item.get('status', '-') or '-'}"
        )
        if item.get("reason"):
            print(f"  reason={item.get('reason')}")
    return 0


def emit_workflow_telemetry(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_root: {payload.get('workflow_root', '-')}")
    worker_state = payload.get("worker_state") or {}
    print(f"worker_status: {worker_state.get('status', '-')}")
    print(f"worker_session_id: {worker_state.get('worker_session_id', '-')}")
    print(f"registry_count: {payload.get('registry_count', 0)}")
    print(f"journal_event_count: {payload.get('journal_event_count', 0)}")
    print(f"workflow_status_counts: {payload.get('workflow_status_counts', {})}")
    print(f"template_counts: {payload.get('template_counts', {})}")
    print(f"journal_event_type_counts: {payload.get('journal_event_type_counts', {})}")
    _emit_recent_failures(payload)
    _emit_recent_status_changes(payload)
    return 0


def emit_workflow_submit_reaction_ts_search(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"status: {payload.get('status', '-')}")
    _emit_stage_result_group(
        payload,
        "submitted",
        count_label="submitted_count",
        item_label="submitted",
        fields=("queue_id",),
        always=True,
    )
    _emit_stage_result_group(
        payload,
        "skipped",
        count_label="skipped_count",
        item_label="skipped",
        fields=("reason",),
    )
    _emit_stage_result_group(
        payload,
        "failed",
        count_label="failed_count",
        item_label="failed",
        fields=("returncode",),
    )
    return 0


def emit_workflow_list(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_count: {payload.get('count', 0)}")
    for item in payload.get("workflows", []):
        submission_summary = item.get("submission_summary") or {}
        submitted_count = int(submission_summary.get("submitted_count", 0) or 0)
        failed_count = int(submission_summary.get("failed_count", 0) or 0)
        print(
            f"- {item.get('workflow_id', '-')} template={item.get('template_name', '-')}"
            f" status={item.get('status', '-')}"
            f" stages={item.get('stage_count', 0)}"
            f" submitted={submitted_count}"
            f" failed={failed_count}"
        )
    return 0


def emit_workflow_get(response: dict[str, Any], *, json_mode: bool) -> int:
    summary = response["summary"]
    if _emit_json_when_requested(response, json_mode=json_mode):
        return 0

    print(f"workflow_id: {summary.get('workflow_id', '-')}")
    print(f"template_name: {summary.get('template_name', '-')}")
    print(f"status: {summary.get('status', '-')}")
    print(f"source_job_id: {summary.get('source_job_id', '-')}")
    print(f"reaction_key: {summary.get('reaction_key', '-')}")
    print(f"workspace_dir: {summary.get('workspace_dir', '-')}")
    print(f"stage_count: {summary.get('stage_count', 0)}")
    downstream = summary.get("downstream_reaction_workflow") or {}
    if downstream:
        print(
            f"downstream_reaction: {downstream.get('workflow_id', '-')} "
            f"status={downstream.get('status', '-')}"
        )
    submission_summary = summary.get("submission_summary") or {}
    if submission_summary:
        print(
            f"submission_summary: submitted={submission_summary.get('submitted_count', 0)} "
            f"skipped={submission_summary.get('skipped_count', 0)} "
            f"failed={submission_summary.get('failed_count', 0)}"
        )
    for stage in summary.get("stage_summaries", []):
        print(
            f"- {stage.get('stage_id', '-')} {stage.get('engine', '-')}/{stage.get('task_kind', '-')}"
            f" stage_status={stage.get('status', '-')}"
            f" task_status={stage.get('task_status', '-')}"
        )
        if stage.get("queue_id"):
            print(f"  queue_id={stage.get('queue_id')}")
        if stage.get("selected_input_xyz"):
            print(f"  selected_input_xyz={stage.get('selected_input_xyz')}")
        if stage.get("selected_inp"):
            print(f"  selected_inp={stage.get('selected_inp')}")
    return 0


def emit_workflow_artifacts(response: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(response, json_mode=json_mode):
        return 0

    print(f"workflow_id: {response.get('workflow_id', '-')}")
    print(f"workspace_dir: {response.get('workspace_dir', '-')}")
    print(f"artifact_count: {response.get('artifact_count', 0)}")
    for item in response.get("artifacts", []):
        print(
            f"- {item.get('kind', '-')}"
            f" stage={item.get('stage_id', '-') or '-'}"
            f" exists={'yes' if item.get('exists') else 'no'}"
            f" selected={'yes' if item.get('selected') else 'no'}"
        )
        print(f"  path={item.get('path', '-')}")
    return 0


def emit_workflow_cancel(payload: dict[str, Any], *, json_mode: bool) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_id: {payload.get('workflow_id', '-')}")
    print(f"workspace_dir: {payload.get('workspace_dir', '-')}")
    print(f"status: {payload.get('status', '-')}")
    _emit_stage_result_group(
        payload,
        "cancelled",
        count_label="cancelled_count",
        item_label="cancelled",
        fields=("queue_id",),
        always=True,
    )
    _emit_stage_result_group(
        payload,
        "requested",
        count_label="requested_count",
        item_label="cancel_requested",
        fields=("queue_id",),
    )
    _emit_stage_result_group(
        payload,
        "skipped",
        count_label="skipped_count",
        item_label="skipped",
        fields=("reason",),
    )
    _emit_stage_result_group(
        payload,
        "failed",
        count_label="failed_count",
        item_label="failed",
        fields=("reason",),
    )
    return 0


def emit_workflow_reindex(
    payload: dict[str, Any], *, records: Sequence[Any], json_mode: bool
) -> int:
    if _emit_json_when_requested(payload, json_mode=json_mode):
        return 0

    print(f"workflow_count: {len(records)}")
    for record in records:
        print(f"- {record.workflow_id} status={record.status} template={record.template_name}")
    return 0


def _emit_recent_failures(payload: dict[str, Any]) -> None:
    recent_failures = payload.get("recent_failures") or []
    if not recent_failures:
        return
    print("recent_failures:")
    for item in recent_failures:
        print(
            f"- {item.get('occurred_at', '-')} workflow={item.get('workflow_id', '-') or '-'}"
            f" reason={item.get('reason', '-') or '-'}"
        )


def _emit_recent_status_changes(payload: dict[str, Any]) -> None:
    recent_status_changes = payload.get("recent_status_changes") or []
    if not recent_status_changes:
        return
    print("recent_status_changes:")
    for item in recent_status_changes:
        print(
            f"- {item.get('occurred_at', '-')} workflow={item.get('workflow_id', '-') or '-'}"
            f" {item.get('previous_status', '-') or '-'}->{item.get('status', '-') or '-'}"
        )


__all__ = [
    "emit_created_workflow",
    "emit_error",
    "emit_json",
    "emit_restarted_workflow",
    "emit_worker_lock_error",
    "emit_worker_payload",
    "emit_workflow_advance",
    "emit_workflow_artifacts",
    "emit_workflow_cancel",
    "emit_workflow_get",
    "emit_workflow_journal",
    "emit_workflow_list",
    "emit_workflow_reindex",
    "emit_workflow_runtime_status",
    "emit_workflow_telemetry",
    "emit_workflow_submit_reaction_ts_search",
]
