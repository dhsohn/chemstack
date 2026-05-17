from __future__ import annotations

import json
from typing import Any


def emit_json(payload: dict[str, Any], *, pretty: bool) -> None:
    indent = 2 if pretty else None
    print(json.dumps(payload, ensure_ascii=True, indent=indent))


def emit_worker_payload(
    payload: dict[str, Any], *, json_mode: bool, single_cycle: bool
) -> None:
    if json_mode:
        emit_json(payload, pretty=single_cycle)
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
    if json_mode:
        emit_json(payload, pretty=True)
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
    if json_mode:
        emit_json(payload, pretty=True)
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
    if json_mode:
        emit_json(payload, pretty=True)
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
    "emit_json",
    "emit_worker_payload",
    "emit_workflow_journal",
    "emit_workflow_runtime_status",
    "emit_workflow_telemetry",
]
