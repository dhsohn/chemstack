from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import activate_reserved_slot, reconcile_stale_slots, release_slot, reserve_slot
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
    request_cancel,
)
from chemstack.core.queue.worker import (
    ManagedProcess as _ManagedProcess,
    dequeue_next_across_roots,
    fill_worker_slots,
    install_shutdown_signal_handlers,
    pid_is_alive as worker_pid_is_alive,
    pop_completed_worker_jobs,
    reserve_queue_worker_slot,
    resolve_admission_limit,
    resolve_admission_root,
    terminate_process_group,
)
from chemstack.core.utils import now_utc_iso

from ..config import default_config_path, load_config
from ..job_locations import reaction_key_from_job_dir, resource_dict, runtime_roots_for_cfg, upsert_job_record
from ..notifications import notify_job_finished, notify_job_started
from ..runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from ..state import (
    is_recovery_pending,
    load_organized_ref,
    load_report_json,
    load_state,
    mark_recovery_pending,
    state_matches_job,
    write_report_json,
    write_report_md_lines,
    write_state,
)
from .organize import organize_job_dir

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1
WORKER_CANCEL_SIGNAL = getattr(signal, "SIGUSR1", signal.SIGTERM)
WORKER_SHUTDOWN_EXIT_CODE = 190
WORKER_JOB_MODULE = "chemstack.xtb.worker_job"


@dataclass(frozen=True)
class QueueExecutionOutcome:
    result: XtbRunResult
    organized_output_dir: str = ""


@dataclass
class _RunningJob:
    queue_root: Path
    entry: Any
    process: _ManagedProcess
    admission_token: str
    cancel_requested: bool = False
    started_at: float = field(default_factory=time.monotonic)


@dataclass(frozen=True)
class _TerminalSummary:
    queue_id: str
    job_id: str
    status: str
    reason: str
    organized_output_dir: str = ""
    metadata_update: dict[str, Any] = field(default_factory=dict)


def _display_status(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    normalized = str(status_value).strip() or "unknown"
    if getattr(entry, "cancel_requested", False) and normalized == "running":
        return "cancel_requested"
    return normalized


def _find_entry_by_target(entries: list[Any], target: str) -> Any | None:
    for entry in entries:
        if entry.queue_id == target or entry.task_id == target:
            return entry
    return None


def _queue_roots(cfg: Any) -> tuple[Path, ...]:
    try:
        return tuple(runtime_roots_for_cfg(cfg))
    except Exception:
        return (Path(cfg.runtime.allowed_root).expanduser().resolve(),)


def _queue_entries_with_roots(cfg: Any) -> list[tuple[Path, Any]]:
    rows: list[tuple[Path, Any]] = []
    for root in _queue_roots(cfg):
        for entry in list_queue(root):
            rows.append((root, entry))
    return rows


def _dequeue_next_entry(cfg: Any) -> tuple[Path, Any] | None:
    return dequeue_next_across_roots(
        _queue_roots(cfg),
        list_queue_fn=list_queue,
        dequeue_next_fn=dequeue_next,
    )


def _queue_entry_by_id(queue_root: Path | str, queue_id: str) -> Any | None:
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def _job_dir(entry: Any) -> Path:
    return Path(str(entry.metadata.get("job_dir", ""))).expanduser().resolve()


def _selected_xyz(entry: Any) -> Path:
    return Path(str(entry.metadata.get("selected_input_xyz", ""))).expanduser().resolve()


def _admission_root(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _admission_limit(cfg: Any) -> int:
    return resolve_admission_limit(cfg)


def _pid_is_alive(pid: int) -> bool:
    return worker_pid_is_alive(pid)


def cmd_queue_cancel(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    target = str(getattr(args, "target", "")).strip()
    if not target:
        print("error: queue cancel requires a queue_id or job_id")
        return 1

    entry_with_root = None
    for queue_root, entry in _queue_entries_with_roots(cfg):
        if entry.queue_id == target or entry.task_id == target:
            entry_with_root = (queue_root, entry)
            break
    if entry_with_root is None:
        print(f"error: queue target not found: {target}")
        return 1
    queue_root, entry = entry_with_root

    updated = request_cancel(queue_root, entry.queue_id)
    if updated is None:
        print(f"error: queue target already terminal: {target}")
        return 1

    print(f"status: {_display_status(updated)}")
    print(f"queue_id: {updated.queue_id}")
    print(f"job_id: {updated.task_id}")
    return 0


def _resource_caps(cfg: Any) -> dict[str, int]:
    return resource_dict(cfg.resources.max_cores_per_task, cfg.resources.max_memory_gb_per_task)


def _coerce_resource_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    result: dict[str, int] = {}
    for key, raw in value.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        try:
            parsed = int(raw)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            result[key_text] = parsed
    return result


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _coerce_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _matching_state(
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    job_type: str,
    reaction_key: str,
) -> dict[str, Any]:
    state = load_state(job_dir) or {}
    if not state_matches_job(
        state,
        selected_input_xyz=str(selected_xyz),
        job_type=job_type,
        reaction_key=reaction_key,
    ):
        return {}
    return state


def _entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _coerce_resource_dict(entry.metadata.get("resource_request")) or _resource_caps(cfg)


def _job_type(entry: Any) -> str:
    value = str(entry.metadata.get("job_type", "")).strip().lower()
    return value or "path_search"


def _reaction_key(entry: Any, job_dir: Path) -> str:
    value = str(entry.metadata.get("reaction_key", "")).strip()
    return value or reaction_key_from_job_dir(job_dir)


def _input_summary(entry: Any) -> dict[str, Any]:
    payload = entry.metadata.get("input_summary", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _build_state_payload(entry: Any, result: XtbRunResult, *, previous_state: dict[str, Any] | None = None, resumed: bool = False) -> dict[str, Any]:
    base_state = _coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    recovery_reason = str(base_state.get("recovery_reason") or base_state.get("reason") or "").strip()
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(entry.metadata.get("job_dir", "")).strip(),
        "selected_input_xyz": result.selected_input_xyz,
        "job_type": result.job_type,
        "reaction_key": result.reaction_key,
        "input_summary": dict(result.input_summary),
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        "candidate_count": result.candidate_count,
        "candidate_paths": candidate_paths,
        "selected_candidate_paths": list(result.selected_candidate_paths),
        "candidate_details": [dict(item) for item in result.candidate_details],
        "analysis_summary": dict(result.analysis_summary),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": str(base_state.get("created_at", "")).strip(),
        "recovery_pending": False,
        "recovery_count": int(base_state.get("recovery_count", 0) or 0),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def _build_report_payload(entry: Any, result: XtbRunResult, *, previous_state: dict[str, Any] | None = None, resumed: bool = False) -> dict[str, Any]:
    base_state = _coerce_mapping(previous_state)
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    recovery_reason = str(base_state.get("recovery_reason") or base_state.get("reason") or "").strip()
    payload = {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        "job_type": result.job_type,
        "reaction_key": result.reaction_key,
        "selected_input_xyz": result.selected_input_xyz,
        "input_summary": dict(result.input_summary),
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "candidate_count": result.candidate_count,
        "candidate_paths": candidate_paths,
        "selected_candidate_paths": list(result.selected_candidate_paths),
        "candidate_details": [dict(item) for item in result.candidate_details],
        "analysis_summary": dict(result.analysis_summary),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": str(base_state.get("created_at", "")).strip(),
        "recovery_count": int(base_state.get("recovery_count", 0) or 0),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def _write_execution_artifacts(
    entry: Any,
    result: XtbRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    write_state(job_dir, _build_state_payload(entry, result, previous_state=previous_state, resumed=resumed))
    write_report_json(job_dir, _build_report_payload(entry, result, previous_state=previous_state, resumed=resumed))
    lines = [
        "# xtb_auto Report",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        f"- Job Type: `{result.job_type}`",
        f"- Reaction Key: `{result.reaction_key}`",
        f"- Selected Input XYZ: `{Path(result.selected_input_xyz).name}`",
        f"- Exit Code: `{result.exit_code}`",
        f"- Candidate Count: `{result.candidate_count}`",
        f"- Input Summary: `{result.input_summary}`",
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]
    if result.selected_candidate_paths:
        lines.append("- Selected Candidate Paths:")
        for path in result.selected_candidate_paths:
            lines.append(f"  - `{path}`")
    if result.job_type == "ranking" and result.analysis_summary:
        if result.analysis_summary.get("best_candidate_path"):
            lines.append(f"- Best Candidate Path: `{result.analysis_summary.get('best_candidate_path')}`")
        if result.analysis_summary.get("best_total_energy") is not None:
            lines.append(f"- Best Total Energy: `{result.analysis_summary.get('best_total_energy')}`")
    if result.analysis_summary:
        lines.append(f"- Analysis Summary: `{result.analysis_summary}`")
    write_report_md_lines(job_dir, lines)


def _write_running_state(
    cfg: Any,
    entry: Any,
    *,
    worker_job_pid: int | None = None,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    input_summary = _input_summary(entry)
    resource_request = _entry_resource_request(cfg, entry)
    base_state = _coerce_mapping(previous_state)
    recovery_reason = str(base_state.get("recovery_reason") or base_state.get("reason") or "").strip()
    started_at = entry.started_at or now_utc_iso()
    updated_at = now_utc_iso()
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
        "job_type": _job_type(entry),
        "reaction_key": _reaction_key(entry, job_dir),
        "input_summary": input_summary,
        "status": "running",
        "reason": recovery_reason if resumed else "",
        "started_at": started_at,
        "updated_at": updated_at,
        "candidate_count": int(input_summary.get("candidate_count", 0) or 0),
        "candidate_paths": list(input_summary.get("candidate_paths", [])),
        "selected_candidate_paths": [],
        "candidate_details": [],
        "analysis_summary": {},
        "resource_request": resource_request,
        "resource_actual": dict(resource_request),
        "created_at": str(base_state.get("created_at", "")).strip() or started_at,
        "recovery_pending": False,
        "recovery_count": int(base_state.get("recovery_count", 0) or 0),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    if worker_job_pid is not None and worker_job_pid > 0:
        payload["worker_job_pid"] = int(worker_job_pid)
    write_state(job_dir, payload)


def _mark_recovery_pending_state(cfg: Any, entry: Any, *, reason: str) -> None:
    job_dir = _job_dir(entry)
    selected_xyz = _selected_xyz(entry)
    job_type = _job_type(entry)
    reaction_key = _reaction_key(entry, job_dir)
    input_summary = _input_summary(entry)
    resource_request = _entry_resource_request(cfg, entry)
    mark_recovery_pending(
        job_dir,
        job_id=str(entry.task_id),
        selected_input_xyz=str(selected_xyz),
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        resource_request=resource_request,
        resource_actual=resource_request,
        reason=reason,
    )
    upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status="pending",
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=str(selected_xyz),
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_request,
    )


def _terminate_process(proc: _ManagedProcess) -> None:
    terminate_process_group(proc)


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    return reserve_queue_worker_slot(
        cfg,
        source="chemstack.xtb.queue_worker",
        app_name="xtb_auto",
        reserve_slot_fn=reserve_slot,
    )


def _build_terminal_result(
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    job_type: str,
    reaction_key: str,
    input_summary: dict[str, Any],
    resource_request: dict[str, int],
    status: str,
    reason: str,
    exit_code: int = 1,
    command: tuple[str, ...] = (),
) -> XtbRunResult:
    terminal_time = now_utc_iso()
    manifest_path = (job_dir / "xtb_job.yaml").resolve()
    return XtbRunResult(
        status=status,
        reason=reason,
        command=command,
        exit_code=exit_code,
        started_at=entry.started_at or terminal_time,
        finished_at=terminal_time,
        stdout_log=str((job_dir / "xtb.stdout.log").resolve()),
        stderr_log=str((job_dir / "xtb.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        job_type=job_type,
        reaction_key=reaction_key,
        input_summary=input_summary,
        candidate_count=0,
        selected_candidate_paths=(),
        candidate_details=(),
        analysis_summary={},
        manifest_path=str(manifest_path) if manifest_path.exists() else "",
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )


def _print_terminal_summary(summary: _TerminalSummary) -> None:
    if summary.organized_output_dir:
        print(f"organized_output_dir: {summary.organized_output_dir}")
    print(f"queue_id: {summary.queue_id}")
    print(f"job_id: {summary.job_id}")
    print(f"status: {summary.status}")
    print(f"reason: {summary.reason}")


def _load_terminal_summary(queue_root: Path, entry: Any, *, rc: int | None = None) -> _TerminalSummary:
    job_dir = _job_dir(entry)
    state = load_state(job_dir) or {}
    report = load_report_json(job_dir) or {}
    organized_ref = load_organized_ref(job_dir) or {}
    refreshed = _queue_entry_by_id(queue_root, entry.queue_id)

    queue_status_value = getattr(getattr(refreshed, "status", None), "value", None) if refreshed is not None else None
    queue_status = str(queue_status_value).strip().lower()
    status = str(report.get("status") or state.get("status") or queue_status).strip().lower()
    if not status:
        status = "completed" if rc == 0 else "failed"
    elif status not in {"completed", "failed", "cancelled"} and rc is not None:
        status = "completed" if rc == 0 else "failed"

    reason = str(report.get("reason") or state.get("reason") or getattr(refreshed, "error", "")).strip()
    if not reason:
        if status == "completed":
            reason = "completed"
        elif status == "cancelled":
            reason = "cancel_requested"
        elif rc is not None:
            reason = f"worker_exit_code_{rc}"
        else:
            reason = "unknown"

    organized_output_dir = str(
        organized_ref.get("organized_output_dir")
        or report.get("organized_output_dir")
        or state.get("organized_output_dir")
        or ""
    ).strip()

    metadata_update: dict[str, Any] = {}
    job_type = str(report.get("job_type") or state.get("job_type") or entry.metadata.get("job_type", "")).strip()
    if job_type:
        metadata_update["job_type"] = job_type
    candidate_count_raw = report.get("candidate_count")
    if candidate_count_raw is None:
        candidate_count_raw = state.get("candidate_count")
    if candidate_count_raw is not None:
        try:
            metadata_update["candidate_count"] = int(candidate_count_raw)
        except (TypeError, ValueError):
            pass

    return _TerminalSummary(
        queue_id=entry.queue_id,
        job_id=entry.task_id,
        status=status,
        reason=reason,
        organized_output_dir=organized_output_dir,
        metadata_update=metadata_update,
    )


def _ensure_terminal_queue_status(queue_root: Path, entry: Any, summary: _TerminalSummary) -> None:
    refreshed = _queue_entry_by_id(queue_root, entry.queue_id)
    current_status = str(getattr(getattr(refreshed, "status", None), "value", "")).strip().lower()
    if current_status in {"completed", "failed", "cancelled"}:
        return

    metadata_update = summary.metadata_update or None
    if summary.status == "completed":
        mark_completed(str(queue_root), entry.queue_id, metadata_update=metadata_update)
    elif summary.status == "cancelled":
        mark_cancelled(str(queue_root), entry.queue_id, error=summary.reason, metadata_update=metadata_update)
    else:
        mark_failed(str(queue_root), entry.queue_id, error=summary.reason, metadata_update=metadata_update)


def _finalize_execution_result(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    result: XtbRunResult,
    auto_organize: bool,
    emit_output: bool,
    previous_state: dict[str, Any] | None = None,
    resumed: bool = False,
) -> QueueExecutionOutcome:
    _write_execution_artifacts(entry, result, previous_state=previous_state, resumed=resumed)
    final_selected_xyz = Path(str(result.selected_input_xyz)).expanduser().resolve() if str(result.selected_input_xyz).strip() else _selected_xyz(entry)

    metadata_update = {
        "candidate_count": result.candidate_count,
        "job_type": result.job_type,
    }
    if result.status == "completed":
        mark_completed(str(queue_root), entry.queue_id, metadata_update=metadata_update)
    elif result.status == "cancelled":
        mark_cancelled(str(queue_root), entry.queue_id, error=result.reason, metadata_update=metadata_update)
    else:
        mark_failed(str(queue_root), entry.queue_id, error=result.reason, metadata_update=metadata_update)

    upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status=result.status,
        job_dir=_job_dir(entry),
        job_type=result.job_type,
        selected_input_xyz=str(final_selected_xyz),
        reaction_key=result.reaction_key,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )

    organized_target = ""
    if auto_organize:
        try:
            organize_result = organize_job_dir(cfg, _job_dir(entry), notify_summary=False)
        except Exception as exc:
            organize_result = {"action": "failed", "reason": f"auto_organize_error:{exc}"}
        if organize_result.get("action") == "organized":
            organized_target = str(organize_result.get("target_dir", "")).strip()

    notify_job_finished(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        status=result.status,
        reason=result.reason,
        job_type=result.job_type,
        reaction_key=result.reaction_key,
        job_dir=_job_dir(entry),
        selected_xyz=final_selected_xyz,
        candidate_count=result.candidate_count,
        organized_output_dir=Path(organized_target) if organized_target else None,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )

    outcome = QueueExecutionOutcome(result=result, organized_output_dir=organized_target)
    if emit_output:
        _print_terminal_summary(
            _TerminalSummary(
                queue_id=entry.queue_id,
                job_id=entry.task_id,
                status=result.status,
                reason=result.reason,
                organized_output_dir=organized_target,
            )
        )
    return outcome


def _execute_queue_entry(
    cfg: Any,
    *,
    queue_root: Path,
    entry: Any,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
    worker_job_pid: int | None = None,
    emit_output: bool = False,
) -> QueueExecutionOutcome:
    job_dir = _job_dir(entry)
    selected_xyz = _selected_xyz(entry)
    job_type = _job_type(entry)
    reaction_key = _reaction_key(entry, job_dir)
    input_summary = _input_summary(entry)
    resource_request = _entry_resource_request(cfg, entry)
    previous_state = _matching_state(
        entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        job_type=job_type,
        reaction_key=reaction_key,
    )
    resumed = is_recovery_pending(previous_state) or str(previous_state.get("status", "")).strip().lower() == "running"

    _write_running_state(
        cfg,
        entry,
        worker_job_pid=worker_job_pid,
        previous_state=previous_state,
        resumed=resumed,
    )
    upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status="running",
        job_dir=job_dir,
        job_type=job_type,
        selected_input_xyz=str(selected_xyz),
        reaction_key=reaction_key,
        resource_request=resource_request,
        resource_actual=resource_request,
    )
    notify_job_started(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        job_type=job_type,
        reaction_key=reaction_key,
        selected_xyz=selected_xyz,
    )

    try:
        if should_cancel is not None and should_cancel():
            result = _build_terminal_result(
                entry,
                job_dir=job_dir,
                selected_xyz=selected_xyz,
                job_type=job_type,
                reaction_key=reaction_key,
                input_summary=input_summary,
                resource_request=resource_request,
                status="cancelled",
                reason="cancel_requested",
                exit_code=1,
            )
        elif job_type == "ranking":
            result = run_xtb_ranking_job(
                cfg,
                job_dir=job_dir,
                should_cancel=should_cancel,
                on_running_job=register_running_job,
                terminate_process=_terminate_process,
            )
        else:
            running = start_xtb_job(cfg, job_dir=job_dir, selected_input_xyz=selected_xyz)
            if register_running_job is not None:
                register_running_job(running)
            try:
                while True:
                    if should_cancel is not None and should_cancel():
                        _terminate_process(running.process)
                        result = finalize_xtb_job(
                            running,
                            forced_status="cancelled",
                            forced_reason="cancel_requested",
                        )
                        break
                    if running.process.poll() is not None:
                        result = finalize_xtb_job(running)
                        break
                    time.sleep(CANCEL_CHECK_INTERVAL_SECONDS)
            finally:
                if register_running_job is not None:
                    register_running_job(None)
    except Exception as exc:
        result = _build_terminal_result(
            entry,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            job_type=job_type,
            reaction_key=reaction_key,
            input_summary=input_summary,
            resource_request=resource_request,
            status="failed",
            reason=f"runner_error:{exc}",
            exit_code=1,
        )

    return _finalize_execution_result(
        cfg,
        queue_root=queue_root,
        entry=entry,
        result=result,
        auto_organize=auto_organize,
        emit_output=emit_output,
        previous_state=previous_state,
        resumed=resumed,
    )


def _build_background_worker_command(
    *,
    config_path: str,
    queue_root: Path,
    queue_id: str,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
) -> list[str]:
    command = [
        sys.executable,
        "-m",
        WORKER_JOB_MODULE,
        "--config",
        config_path,
        "--queue-root",
        str(queue_root),
        "--queue-id",
        queue_id,
        "--admission-root",
        str(admission_root),
        "--admission-token",
        admission_token,
    ]
    if auto_organize:
        command.append("--auto-organize")
    return command


def _start_background_job_process(
    *,
    config_path: str,
    queue_root: Path,
    entry: Any,
    admission_root: str,
    admission_token: str,
    auto_organize: bool,
) -> subprocess.Popen[str]:
    return subprocess.Popen(
        _build_background_worker_command(
            config_path=config_path,
            queue_root=queue_root,
            queue_id=entry.queue_id,
            admission_root=admission_root,
            admission_token=admission_token,
            auto_organize=auto_organize,
        ),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
        text=True,
    )


def _request_job_cancellation(proc: _ManagedProcess) -> None:
    try:
        send_signal = getattr(proc, "send_signal", None)
        if callable(send_signal):
            send_signal(WORKER_CANCEL_SIGNAL)
        else:
            os.kill(proc.pid, WORKER_CANCEL_SIGNAL)
    except (OSError, ProcessLookupError, PermissionError):
        _terminate_process(proc)


def _resolve_worker_auto_organize(cfg: Any, args: Any) -> bool:
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False
    return auto_organize


def _config_path_for_worker(args: Any) -> str:
    configured = str(getattr(args, "config", "") or "").strip()
    return configured or default_config_path()


class QueueWorker:
    def __init__(
        self,
        cfg: Any,
        *,
        config_path: str,
        auto_organize: bool,
        max_concurrent: int | None = None,
    ) -> None:
        self.cfg = cfg
        self.config_path = config_path
        self.auto_organize = bool(auto_organize)
        configured_max = cfg.runtime.max_concurrent if max_concurrent is None else max_concurrent
        self.max_concurrent = max(1, int(configured_max))
        self.admission_root = _admission_root(cfg)
        self._running: dict[str, _RunningJob] = {}
        self._shutdown_requested = False

    def run(self) -> int:
        self._install_signal_handlers()
        self._reconcile_worker_state()
        try:
            while not self._shutdown_requested:
                self._run_iteration()
        except KeyboardInterrupt:
            self._shutdown_requested = True
        finally:
            self._shutdown_all()
        return 0

    def run_once(self) -> int:
        self._install_signal_handlers()
        self._reconcile_worker_state()
        try:
            outcome = self._fill_slots(max_new_jobs=1)
            if outcome == "idle":
                print("No pending jobs.")
                return 0
            if outcome == "blocked":
                print("status: waiting_for_slot")
                return 0

            while self._running and not self._shutdown_requested:
                self._check_completed_jobs()
                self._check_cancel_requests()
                if self._running:
                    time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            self._shutdown_requested = True
        finally:
            self._shutdown_all()
        return 0

    def _run_iteration(self) -> None:
        self._check_completed_jobs()
        self._check_cancel_requests()
        self._fill_slots()
        time.sleep(POLL_INTERVAL_SECONDS)

    def _reserve_next_entry(self) -> tuple[str, tuple[Path, Any, str] | None]:
        slot_token = _try_reserve_admission_slot(self.cfg)
        if slot_token is None:
            return "blocked", None

        dequeued = _dequeue_next_entry(self.cfg)
        if dequeued is None:
            release_slot(self.admission_root, slot_token)
            return "idle", None
        queue_root, entry = dequeued
        return "processed", (queue_root, entry, slot_token)

    def _fill_slots(self, *, max_new_jobs: int | None = None) -> str:
        def start_reserved(reserved: tuple[Path, Any, str]) -> None:
            queue_root, entry, slot_token = reserved
            self._start_job(queue_root, entry, admission_token=slot_token)

        result = fill_worker_slots(
            running_count=lambda: len(self._running),
            max_concurrent=self.max_concurrent,
            reserve_next=self._reserve_next_entry,
            start_reserved=start_reserved,
            max_new_jobs=max_new_jobs,
        )
        return result.status

    def _start_job(self, queue_root: Path, entry: Any, *, admission_token: str) -> None:
        try:
            proc = _start_background_job_process(
                config_path=self.config_path,
                queue_root=queue_root,
                entry=entry,
                admission_root=self.admission_root,
                admission_token=admission_token,
                auto_organize=self.auto_organize,
            )
        except OSError as exc:
            release_slot(self.admission_root, admission_token)
            failure = _build_terminal_result(
                entry,
                job_dir=_job_dir(entry),
                selected_xyz=_selected_xyz(entry),
                job_type=_job_type(entry),
                reaction_key=_reaction_key(entry, _job_dir(entry)),
                input_summary=_input_summary(entry),
                resource_request=_entry_resource_request(self.cfg, entry),
                status="failed",
                reason=f"worker_start_error:{exc}",
            )
            _finalize_execution_result(
                self.cfg,
                queue_root=queue_root,
                entry=entry,
                result=failure,
                auto_organize=self.auto_organize,
                emit_output=True,
            )
            return

        self._running[entry.queue_id] = _RunningJob(
            queue_root=queue_root,
            entry=entry,
            process=proc,
            admission_token=admission_token,
        )

    def _check_completed_jobs(self) -> None:
        def finalize_finished(_queue_id: str, job: _RunningJob, rc: int) -> None:
            summary = _load_terminal_summary(job.queue_root, job.entry, rc=rc)
            _ensure_terminal_queue_status(job.queue_root, job.entry, summary)
            _print_terminal_summary(summary)
            release_slot(self.admission_root, job.admission_token)

        pop_completed_worker_jobs(
            self._running,
            poll_job=lambda job: job.process.poll(),
            finalize_finished=finalize_finished,
        )

    def _check_cancel_requests(self) -> None:
        for job in self._running.values():
            if job.cancel_requested:
                continue
            if get_cancel_requested(str(job.queue_root), job.entry.queue_id):
                _request_job_cancellation(job.process)
                job.cancel_requested = True

    def _shutdown_all(self) -> None:
        if not self._running:
            return
        for queue_id, job in list(self._running.items()):
            _terminate_process(job.process)
            _mark_recovery_pending_state(self.cfg, job.entry, reason="worker_shutdown")
            requeue_running_entry(str(job.queue_root), queue_id)
            release_slot(self.admission_root, job.admission_token)
            del self._running[queue_id]

    def _install_signal_handlers(self) -> None:
        def request_shutdown() -> None:
            self._shutdown_requested = True

        install_shutdown_signal_handlers(request_shutdown)

    def _reconcile_worker_state(self) -> None:
        reconcile_stale_slots(self.admission_root)
        for queue_root, entry in _queue_entries_with_roots(self.cfg):
            status = str(getattr(getattr(entry, "status", None), "value", "")).strip().lower()
            if status != "running":
                continue
            summary = _load_terminal_summary(queue_root, entry)
            if summary.status in {"completed", "failed", "cancelled"}:
                _ensure_terminal_queue_status(queue_root, entry, summary)
                continue

            state = load_state(_job_dir(entry)) or {}
            worker_job_pid = int(state.get("worker_job_pid", 0) or 0)
            if worker_job_pid and _pid_is_alive(worker_job_pid):
                continue
            requeue_running_entry(str(queue_root), entry.queue_id)
            _mark_recovery_pending_state(self.cfg, entry, reason="crashed_recovery")


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    slot_token = _try_reserve_admission_slot(cfg)
    if slot_token is None:
        return "blocked"

    try:
        dequeued = _dequeue_next_entry(cfg)
        if dequeued is None:
            return "idle"
        queue_root, entry = dequeued
        _execute_queue_entry(
            cfg,
            queue_root=queue_root,
            entry=entry,
            auto_organize=auto_organize,
            emit_output=True,
        )
        return "processed"
    finally:
        release_slot(_admission_root(cfg), slot_token)


def run_worker_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    admission_root: str,
    admission_token: str | None,
    auto_organize: bool,
    should_cancel: Callable[[], bool] | None = None,
    register_running_job: Callable[[Any | None], None] | None = None,
) -> int:
    cfg = load_config(config_path)
    resolved_queue_root = Path(queue_root).expanduser().resolve()
    entry = _queue_entry_by_id(resolved_queue_root, queue_id)
    if entry is None:
        return 1

    if admission_token:
        activated = activate_reserved_slot(
            admission_root,
            admission_token,
            work_dir=_job_dir(entry),
            queue_id=entry.queue_id,
            source="chemstack.xtb.worker_job",
        )
        if activated is None:
            return 1

    try:
        outcome = _execute_queue_entry(
            cfg,
            queue_root=resolved_queue_root,
            entry=entry,
            auto_organize=auto_organize,
            should_cancel=should_cancel,
            register_running_job=register_running_job,
            emit_output=False,
            worker_job_pid=os.getpid(),
        )
        return 0 if outcome.result.status in {"completed", "cancelled"} else 1
    finally:
        if admission_token:
            release_slot(admission_root, admission_token)


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    auto_organize = _resolve_worker_auto_organize(cfg, args)
    worker = QueueWorker(
        cfg,
        config_path=_config_path_for_worker(args),
        auto_organize=auto_organize,
    )
    return worker.run()
