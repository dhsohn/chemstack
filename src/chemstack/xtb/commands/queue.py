from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Any

from chemstack.core.admission import release_slot, reserve_slot
from chemstack.core.queue import (
    dequeue_next,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    request_cancel,
)
from chemstack.core.utils import now_utc_iso

from ..config import load_config
from ..job_locations import reaction_key_from_job_dir, resource_dict, runtime_roots_for_cfg, upsert_job_record
from ..notifications import notify_job_finished, notify_job_started
from ..runner import XtbRunResult, finalize_xtb_job, run_xtb_ranking_job, start_xtb_job
from ..state import write_report_json, write_report_md_lines, write_state
from .organize import organize_job_dir

POLL_INTERVAL_SECONDS = 5
CANCEL_CHECK_INTERVAL_SECONDS = 1


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
    roots = _queue_roots(cfg)
    if len(roots) == 1:
        entry = dequeue_next(roots[0])
        if entry is None:
            return None
        return roots[0], entry

    selected_root: Path | None = None
    selected_key: tuple[int, str, int, str] | None = None

    for root_index, root in enumerate(roots):
        for entry in list_queue(root):
            status_value = getattr(getattr(entry, "status", None), "value", None)
            status = str(status_value).strip().lower()
            if status != "pending" or getattr(entry, "cancel_requested", False):
                continue
            key = (
                int(getattr(entry, "priority", 10) or 10),
                str(getattr(entry, "enqueued_at", "")),
                root_index,
                str(getattr(entry, "queue_id", "")),
            )
            if selected_key is None or key < selected_key:
                selected_key = key
                selected_root = root

    if selected_root is None:
        return None

    entry = dequeue_next(selected_root)
    if entry is None:
        return None
    return selected_root, entry


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


def _build_state_payload(entry: Any, result: XtbRunResult) -> dict[str, Any]:
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    return {
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
    }


def _build_report_payload(entry: Any, result: XtbRunResult) -> dict[str, Any]:
    candidate_paths = list(result.analysis_summary.get("candidate_paths", []))
    if not candidate_paths and isinstance(result.input_summary, dict):
        candidate_paths = list(result.input_summary.get("candidate_paths", []))
    return {
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
    }


def _write_execution_artifacts(entry: Any, result: XtbRunResult) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    write_state(job_dir, _build_state_payload(entry, result))
    write_report_json(job_dir, _build_report_payload(entry, result))
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


def _write_running_state(cfg: Any, entry: Any) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    input_summary = _input_summary(entry)
    resource_request = _entry_resource_request(cfg, entry)
    write_state(
        job_dir,
        {
            "job_id": entry.task_id,
            "job_dir": str(job_dir),
            "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
            "job_type": _job_type(entry),
            "reaction_key": _reaction_key(entry, job_dir),
            "input_summary": input_summary,
            "status": "running",
            "reason": "",
            "started_at": entry.started_at or now_utc_iso(),
            "updated_at": now_utc_iso(),
            "candidate_count": int(input_summary.get("candidate_count", 0) or 0),
            "candidate_paths": list(input_summary.get("candidate_paths", [])),
            "selected_candidate_paths": [],
            "candidate_details": [],
            "analysis_summary": {},
            "resource_request": resource_request,
            "resource_actual": dict(resource_request),
        },
    )


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return

    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        try:
            proc.terminate()
        except Exception:
            pass

    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except Exception:
                pass
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass


def _try_reserve_admission_slot(cfg: Any) -> str | None:
    admission_root = getattr(cfg.runtime, "resolved_admission_root", None) or getattr(cfg.runtime, "admission_root", "") or cfg.runtime.allowed_root
    admission_limit = getattr(cfg.runtime, "resolved_admission_limit", None) or getattr(cfg.runtime, "admission_limit", 0) or cfg.runtime.max_concurrent
    return reserve_slot(
        admission_root,
        admission_limit,
        source="chemstack.xtb.queue_worker",
        app_name="xtb_auto",
    )


def _process_one(cfg: Any, *, auto_organize: bool) -> str:
    slot_token = _try_reserve_admission_slot(cfg)
    if slot_token is None:
        return "blocked"

    try:
        dequeued = _dequeue_next_entry(cfg)
        if dequeued is None:
            return "idle"
        queue_root, entry = dequeued

        job_dir = Path(str(entry.metadata.get("job_dir", ""))).expanduser().resolve()
        selected_xyz = Path(str(entry.metadata.get("selected_input_xyz", ""))).expanduser().resolve()
        job_type = _job_type(entry)
        reaction_key = _reaction_key(entry, job_dir)
        input_summary = _input_summary(entry)
        resource_request = _entry_resource_request(cfg, entry)

        _write_running_state(cfg, entry)
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
            if job_type == "ranking":
                result = run_xtb_ranking_job(cfg, job_dir=job_dir)
            else:
                running = start_xtb_job(cfg, job_dir=job_dir, selected_input_xyz=selected_xyz)
                while True:
                    if running.process.poll() is not None:
                        result = finalize_xtb_job(running)
                        break
                    if get_cancel_requested(str(queue_root), entry.queue_id):
                        _terminate_process(running.process)
                        result = finalize_xtb_job(
                            running,
                            forced_status="cancelled",
                            forced_reason="cancel_requested",
                        )
                        break
                    time.sleep(CANCEL_CHECK_INTERVAL_SECONDS)
        except Exception as exc:
            failure_time = now_utc_iso()
            result = XtbRunResult(
                status="failed",
                reason=f"runner_error:{exc}",
                command=(),
                exit_code=1,
                started_at=entry.started_at or failure_time,
                finished_at=failure_time,
                stdout_log=str((job_dir / "xtb.stdout.log").resolve()),
                stderr_log=str((job_dir / "xtb.stderr.log").resolve()),
                selected_input_xyz=str(selected_xyz),
                job_type=job_type,
                reaction_key=reaction_key,
                input_summary=input_summary,
                candidate_count=0,
                selected_candidate_paths=(),
                candidate_details=(),
                analysis_summary={},
                manifest_path=str((job_dir / "xtb_job.yaml").resolve()) if (job_dir / "xtb_job.yaml").exists() else "",
                resource_request=resource_request,
                resource_actual=dict(resource_request),
            )

        _write_execution_artifacts(entry, result)
        final_selected_xyz = Path(str(result.selected_input_xyz)).expanduser().resolve() if str(result.selected_input_xyz).strip() else selected_xyz

        if result.status == "completed":
            mark_completed(
                str(queue_root),
                entry.queue_id,
                metadata_update={
                    "candidate_count": result.candidate_count,
                    "job_type": result.job_type,
                },
            )
        elif result.status == "cancelled":
            mark_cancelled(
                str(queue_root),
                entry.queue_id,
                error=result.reason,
                metadata_update={
                    "candidate_count": result.candidate_count,
                    "job_type": result.job_type,
                },
            )
        else:
            mark_failed(
                str(queue_root),
                entry.queue_id,
                error=result.reason,
                metadata_update={
                    "candidate_count": result.candidate_count,
                    "job_type": result.job_type,
                },
            )

        upsert_job_record(
            cfg,
            job_id=entry.task_id,
            status=result.status,
            job_dir=job_dir,
            job_type=result.job_type,
            selected_input_xyz=str(final_selected_xyz),
            reaction_key=reaction_key,
            resource_request=result.resource_request,
            resource_actual=result.resource_actual,
        )

        organized_target: str = ""
        if auto_organize:
            try:
                organize_result = organize_job_dir(cfg, job_dir, notify_summary=False)
            except Exception as exc:
                organize_result = {"action": "failed", "reason": f"auto_organize_error:{exc}"}

            if organize_result.get("action") == "organized":
                organized_target = str(organize_result.get("target_dir", "")).strip()
                if organized_target:
                    print(f"organized_output_dir: {organized_target}")

        notify_job_finished(
            cfg,
            job_id=entry.task_id,
            queue_id=entry.queue_id,
            status=result.status,
            reason=result.reason,
            job_type=result.job_type,
            reaction_key=reaction_key,
            job_dir=job_dir,
            selected_xyz=final_selected_xyz,
            candidate_count=result.candidate_count,
            organized_output_dir=Path(organized_target) if organized_target else None,
            resource_request=result.resource_request,
            resource_actual=result.resource_actual,
        )

        print(f"queue_id: {entry.queue_id}")
        print(f"job_id: {entry.task_id}")
        print(f"status: {result.status}")
        print(f"reason: {result.reason}")
        return "processed"
    finally:
        admission_root = getattr(cfg.runtime, "resolved_admission_root", None) or getattr(cfg.runtime, "admission_root", "") or cfg.runtime.allowed_root
        release_slot(admission_root, slot_token)


def cmd_queue_worker(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    run_once = bool(getattr(args, "once", False))
    auto_organize = bool(cfg.behavior.auto_organize_on_terminal)
    if bool(getattr(args, "auto_organize", False)):
        auto_organize = True
    elif bool(getattr(args, "no_auto_organize", False)):
        auto_organize = False

    if run_once:
        outcome = _process_one(cfg, auto_organize=auto_organize)
        if outcome == "idle":
            print("No pending jobs.")
        elif outcome == "blocked":
            print("status: waiting_for_slot")
        return 0

    try:
        while True:
            outcome = _process_one(cfg, auto_organize=auto_organize)
            if outcome != "processed":
                time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        return 0
