from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .runner import CrestRunResult
from .state import write_report_json, write_report_md_lines, write_state

CANCEL_CHECK_INTERVAL_SECONDS = 1


@dataclass(frozen=True)
class ExecutionContext:
    entry: Any
    job_dir: Path
    selected_xyz: Path
    molecule_key: str
    mode: str
    resource_request: dict[str, int]


@dataclass(frozen=True)
class WorkerExecutionOutcome:
    result: CrestRunResult
    job_dir: Path
    selected_xyz: Path
    molecule_key: str
    organized_output_dir: Path | None


@dataclass(frozen=True)
class WorkerExecutionDependencies:
    now_utc_iso: Callable[[], str]
    get_cancel_requested: Callable[[str, str], bool]
    start_crest_job: Callable[..., Any]
    finalize_crest_job: Callable[..., CrestRunResult]
    terminate_process: Callable[[subprocess.Popen[str]], None]
    write_running_state: Callable[[Any, Any], None]
    write_execution_artifacts: Callable[[Any, CrestRunResult], None]
    mark_completed: Callable[..., Any]
    mark_cancelled: Callable[..., Any]
    mark_failed: Callable[..., Any]
    upsert_job_record: Callable[..., Any]
    notify_job_started: Callable[..., bool]
    notify_job_finished: Callable[..., bool]
    organize_job_dir: Callable[..., dict[str, str]]


def _build_state_payload(entry: Any, result: CrestRunResult) -> dict[str, Any]:
    return {
        "job_id": entry.task_id,
        "job_dir": str(entry.metadata.get("job_dir", "")).strip(),
        "selected_input_xyz": result.selected_input_xyz,
        "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        "mode": result.mode,
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        "retained_conformer_count": result.retained_conformer_count,
        "retained_conformer_paths": list(result.retained_conformer_paths),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
    }


def _build_report_payload(entry: Any, result: CrestRunResult) -> dict[str, Any]:
    return {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        "mode": result.mode,
        "selected_input_xyz": result.selected_input_xyz,
        "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        "retained_conformer_count": result.retained_conformer_count,
        "retained_conformer_paths": list(result.retained_conformer_paths),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
    }


def _write_execution_artifacts(entry: Any, result: CrestRunResult) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return

    job_dir = Path(job_dir_text).expanduser().resolve()
    write_state(job_dir, _build_state_payload(entry, result))
    write_report_json(job_dir, _build_report_payload(entry, result))
    lines = [
        "# crest_auto Report",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        f"- Mode: `{result.mode}`",
        f"- Selected XYZ: `{Path(result.selected_input_xyz).name}`",
        f"- Molecule Key: `{str(entry.metadata.get('molecule_key', '')).strip() or '-'}`",
        f"- Exit Code: `{result.exit_code}`",
        f"- Retained Conformers: `{result.retained_conformer_count}`",
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]
    if result.retained_conformer_paths:
        lines.append("- Retained Files:")
        for path in result.retained_conformer_paths:
            lines.append(f"  - `{path}`")
    write_report_md_lines(job_dir, lines)


def _write_running_state(cfg: Any, entry: Any) -> None:
    job_dir_text = str(entry.metadata.get("job_dir", "")).strip()
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    resource_request = _entry_resource_request(cfg, entry)
    write_state(
        job_dir,
        {
            "job_id": entry.task_id,
            "job_dir": str(job_dir),
            "selected_input_xyz": str(entry.metadata.get("selected_input_xyz", "")).strip(),
            "molecule_key": str(entry.metadata.get("molecule_key", "")).strip(),
            "mode": str(entry.metadata.get("mode", "standard")).strip(),
            "status": "running",
            "reason": "",
            "started_at": entry.started_at or depsafe_now_utc_iso(),
            "updated_at": depsafe_now_utc_iso(),
            "resource_request": resource_request,
            "resource_actual": dict(resource_request),
        },
    )


def depsafe_now_utc_iso() -> str:
    from chemstack.core.utils import now_utc_iso

    return now_utc_iso()


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


def _resource_caps(cfg: Any) -> dict[str, int]:
    from .job_locations import resource_dict

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
    metadata = getattr(entry, "metadata", {})
    return _coerce_resource_dict(metadata.get("resource_request")) or _resource_caps(cfg)


def _molecule_key(entry: Any, selected_xyz: Path, job_dir: Path) -> str:
    from .job_locations import molecule_key_from_selected_xyz

    raw = str(entry.metadata.get("molecule_key", "")).strip()
    if raw:
        return raw
    return molecule_key_from_selected_xyz(str(selected_xyz), job_dir)


def _build_execution_context(
    cfg: Any,
    entry: Any,
    *,
    resource_caps: Callable[[Any], dict[str, int]],
    molecule_key_resolver: Callable[[Any, Path, Path], str],
) -> ExecutionContext:
    job_dir = Path(str(entry.metadata.get("job_dir", ""))).expanduser().resolve()
    selected_xyz = Path(str(entry.metadata.get("selected_input_xyz", ""))).expanduser().resolve()
    return ExecutionContext(
        entry=entry,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        molecule_key=molecule_key_resolver(entry, selected_xyz, job_dir),
        mode=str(entry.metadata.get("mode", "standard")),
        resource_request=_entry_resource_request(cfg, entry),
    )


def _mark_queue_terminal(
    queue_root: str | Path,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    dependencies: WorkerExecutionDependencies,
) -> None:
    metadata_update = {
        "retained_conformer_count": result.retained_conformer_count,
        "mode": result.mode,
    }
    if result.status == "completed":
        dependencies.mark_completed(
            str(queue_root),
            context.entry.queue_id,
            metadata_update=metadata_update,
        )
        return
    if result.status == "cancelled":
        dependencies.mark_cancelled(
            str(queue_root),
            context.entry.queue_id,
            error=result.reason,
            metadata_update=metadata_update,
        )
        return
    dependencies.mark_failed(
        str(queue_root),
        context.entry.queue_id,
        error=result.reason,
        metadata_update=metadata_update,
    )


def _sync_job_tracking(
    cfg: Any,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    auto_organize: bool,
    dependencies: WorkerExecutionDependencies,
) -> Path | None:
    dependencies.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status=result.status,
        job_dir=context.job_dir,
        mode=result.mode,
        selected_input_xyz=str(context.selected_xyz),
        molecule_key=context.molecule_key,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )

    if not auto_organize:
        return None

    try:
        organize_result = dependencies.organize_job_dir(cfg, context.job_dir, notify_summary=False)
    except Exception as exc:
        organize_result = {"action": "failed", "reason": f"auto_organize_error:{exc}"}

    if organize_result.get("action") != "organized":
        return None

    organized_output_dir = Path(str(organize_result.get("target_dir", "")).strip())
    if not organized_output_dir:
        return None

    print(f"organized_output_dir: {organized_output_dir}")
    dependencies.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status=result.status,
        job_dir=context.job_dir,
        mode=result.mode,
        selected_input_xyz=str(context.selected_xyz),
        organized_output_dir=organized_output_dir,
        molecule_key=context.molecule_key,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )
    dependencies.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status=result.status,
        job_dir=organized_output_dir,
        mode=result.mode,
        selected_input_xyz=str(context.selected_xyz),
        organized_output_dir=organized_output_dir,
        molecule_key=context.molecule_key,
        resource_request=result.resource_request,
        resource_actual=result.resource_actual,
    )
    return organized_output_dir


def process_dequeued_entry(
    cfg: Any,
    entry: Any,
    *,
    queue_root: Path | None = None,
    auto_organize: bool,
    resource_caps: Callable[[Any], dict[str, int]],
    molecule_key_resolver: Callable[[Any, Path, Path], str],
    dependencies: WorkerExecutionDependencies,
) -> WorkerExecutionOutcome:
    active_queue_root = queue_root or Path(str(cfg.runtime.allowed_root)).expanduser().resolve()
    context = _build_execution_context(
        cfg,
        entry,
        resource_caps=resource_caps,
        molecule_key_resolver=molecule_key_resolver,
    )
    dependencies.write_running_state(cfg, entry)
    dependencies.upsert_job_record(
        cfg,
        job_id=entry.task_id,
        status="running",
        job_dir=context.job_dir,
        mode=context.mode,
        selected_input_xyz=str(context.selected_xyz),
        molecule_key=context.molecule_key,
        resource_request=context.resource_request,
        resource_actual=context.resource_request,
    )
    dependencies.notify_job_started(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        job_dir=context.job_dir,
        mode=context.mode,
        selected_xyz=context.selected_xyz,
    )

    try:
        running = dependencies.start_crest_job(
            cfg,
            job_dir=context.job_dir,
            selected_xyz=context.selected_xyz,
        )
        while True:
            if running.process.poll() is not None:
                result = dependencies.finalize_crest_job(running)
                break

            if dependencies.get_cancel_requested(str(active_queue_root), entry.queue_id):
                dependencies.terminate_process(running.process)
                result = dependencies.finalize_crest_job(
                    running,
                    forced_status="cancelled",
                    forced_reason="cancel_requested",
                )
                break

            time.sleep(CANCEL_CHECK_INTERVAL_SECONDS)
    except Exception as exc:
        failure_time = dependencies.now_utc_iso()
        resource_request = context.resource_request
        result = CrestRunResult(
            status="failed",
            reason=f"runner_error:{exc}",
            command=(),
            exit_code=1,
            started_at=entry.started_at or failure_time,
            finished_at=failure_time,
            stdout_log=str((context.job_dir / "crest.stdout.log").resolve()),
            stderr_log=str((context.job_dir / "crest.stderr.log").resolve()),
            selected_input_xyz=str(context.selected_xyz),
            mode=context.mode,
            retained_conformer_count=0,
            retained_conformer_paths=(),
            manifest_path=(
                str((context.job_dir / "crest_job.yaml").resolve())
                if (context.job_dir / "crest_job.yaml").exists()
                else ""
            ),
            resource_request=resource_request,
            resource_actual=dict(resource_request),
        )

    dependencies.write_execution_artifacts(entry, result)
    _mark_queue_terminal(active_queue_root, context, result, dependencies=dependencies)
    organized_output_dir = _sync_job_tracking(
        cfg,
        context,
        result,
        auto_organize=auto_organize,
        dependencies=dependencies,
    )
    dependencies.notify_job_finished(
        cfg,
        job_id=entry.task_id,
        queue_id=entry.queue_id,
        status=result.status,
        reason=result.reason,
        mode=result.mode,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        retained_conformer_count=result.retained_conformer_count,
        organized_output_dir=organized_output_dir,
        resource_request=context.resource_request,
        resource_actual=result.resource_actual,
    )
    return WorkerExecutionOutcome(
        result=result,
        job_dir=context.job_dir,
        selected_xyz=context.selected_xyz,
        molecule_key=context.molecule_key,
        organized_output_dir=organized_output_dir,
    )
