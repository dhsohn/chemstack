from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .state_store import now_utc_iso

EVT_RUN_STARTED = "run_started"
EVT_ATTEMPT_COMPLETED = "attempt_completed"
EVT_RUN_COMPLETED = "run_completed"
EVT_RUN_FAILED = "run_failed"
EVT_RUN_INTERRUPTED = "run_interrupted"
EVT_DISK_THRESHOLD = "disk_threshold"
EVT_DISK_RECOVERED = "disk_recovered"


def _make_common(
    event_type: str,
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
) -> Dict[str, Any]:
    return {
        "event_type": event_type,
        "run_id": run_id,
        "reaction_dir": reaction_dir,
        "selected_inp": selected_inp,
        "timestamp": now_utc_iso(),
    }


def make_event_id(run_id: str, event_type: str, suffix: str = "") -> str:
    if suffix:
        return f"{run_id}:{event_type}:{suffix}"
    return f"{run_id}:{event_type}"


def event_run_started(
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
) -> Dict[str, Any]:
    evt = _make_common(EVT_RUN_STARTED, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, EVT_RUN_STARTED)
    return evt


def event_attempt_completed(
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
    *,
    attempt_index: int,
    analyzer_status: str,
    analyzer_reason: str,
) -> Dict[str, Any]:
    evt = _make_common(EVT_ATTEMPT_COMPLETED, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, EVT_ATTEMPT_COMPLETED, str(attempt_index))
    evt["attempt_index"] = attempt_index
    evt["analyzer_status"] = analyzer_status
    evt["analyzer_reason"] = analyzer_reason
    return evt


def event_run_terminal(
    event_type: str,
    run_id: str,
    reaction_dir: str,
    selected_inp: str,
    *,
    status: str,
    reason: str,
    attempt_count: int,
) -> Dict[str, Any]:
    evt = _make_common(event_type, run_id, reaction_dir, selected_inp)
    evt["event_id"] = make_event_id(run_id, event_type)
    evt["status"] = status
    evt["reason"] = reason
    evt["attempt_count"] = attempt_count
    return evt


def event_disk_threshold(
    *,
    combined_gb: float,
    threshold_gb: float,
    allowed_root: str,
    organized_root: str,
) -> Dict[str, Any]:
    return {
        "event_type": EVT_DISK_THRESHOLD,
        "event_id": f"disk:{EVT_DISK_THRESHOLD}",
        "combined_gb": round(combined_gb, 2),
        "threshold_gb": threshold_gb,
        "allowed_root": allowed_root,
        "organized_root": organized_root,
        "timestamp": now_utc_iso(),
    }


def event_disk_recovered(
    *,
    combined_gb: float,
    threshold_gb: float,
    allowed_root: str,
    organized_root: str,
) -> Dict[str, Any]:
    return {
        "event_type": EVT_DISK_RECOVERED,
        "event_id": f"disk:{EVT_DISK_RECOVERED}",
        "combined_gb": round(combined_gb, 2),
        "threshold_gb": threshold_gb,
        "allowed_root": allowed_root,
        "organized_root": organized_root,
        "timestamp": now_utc_iso(),
    }


def render_message(event: Dict[str, Any], *, mask_paths: bool = False) -> str:
    etype = event.get("event_type", "unknown")
    run_id = event.get("run_id", "?")
    reaction_dir = event.get("reaction_dir", "?")
    if mask_paths:
        reaction_dir = Path(reaction_dir).name if reaction_dir != "?" else "?"

    if etype == EVT_RUN_STARTED:
        return f"[orca_auto] started | run_id={run_id} | dir={reaction_dir}"

    if etype == EVT_ATTEMPT_COMPLETED:
        idx = event.get("attempt_index", "?")
        astatus = event.get("analyzer_status", "?")
        return f"[orca_auto] attempt {idx} done | run_id={run_id} | status={astatus}"

    if etype == EVT_RUN_COMPLETED:
        count = event.get("attempt_count", "?")
        reason = event.get("reason", "?")
        return f"[orca_auto] completed | run_id={run_id} | attempts={count} | reason={reason}"

    if etype in (EVT_RUN_FAILED, EVT_RUN_INTERRUPTED):
        status = event.get("status", "?")
        reason = event.get("reason", "?")
        label = "failed" if etype == EVT_RUN_FAILED else "interrupted"
        return f"[orca_auto] {label} | run_id={run_id} | status={status} | reason={reason}"

    if etype == EVT_DISK_THRESHOLD:
        combined = event.get("combined_gb", "?")
        threshold = event.get("threshold_gb", "?")
        return f"[orca_auto] disk threshold exceeded | combined={combined} GB >= {threshold} GB"

    if etype == EVT_DISK_RECOVERED:
        combined = event.get("combined_gb", "?")
        threshold = event.get("threshold_gb", "?")
        return f"[orca_auto] disk recovered | combined={combined} GB < {threshold} GB"

    return f"[orca_auto] {etype} | run_id={run_id}"
