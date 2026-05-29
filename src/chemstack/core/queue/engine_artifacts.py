from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chemstack.core.queue import execution as _queue_execution


@dataclass(frozen=True)
class TerminalArtifactPayloads:
    state: dict[str, Any]
    report: dict[str, Any]


@dataclass(frozen=True)
class EngineArtifactFields:
    selected_input_xyz: str
    engine_fields: Mapping[str, Any] | None = None
    detail_fields: Mapping[str, Any] | None = None

    def engine_payload(self) -> dict[str, Any]:
        return dict(self.engine_fields or {})

    def detail_payload(self) -> dict[str, Any]:
        return dict(self.detail_fields or {})


@dataclass(frozen=True)
class TerminalArtifactWriters:
    write_state: Callable[..., Any]
    write_report_json: Callable[..., Any]
    write_report_md_lines: Callable[..., Any]


def is_resumed_state(
    previous_state: dict[str, Any],
    *,
    is_recovery_pending_fn: Callable[[dict[str, Any]], bool],
) -> bool:
    return (
        is_recovery_pending_fn(previous_state)
        or str(previous_state.get("status", "")).strip().lower() == "running"
    )


def build_running_state_payload(
    entry: Any,
    *,
    job_dir: Path,
    selected_input_xyz: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": selected_input_xyz,
        **dict(engine_fields or {}),
        "status": "running",
        "reason": recovery_reason if resumed else "",
        "started_at": started_at,
        "updated_at": updated_at,
        **dict(detail_fields or {}),
        "resource_request": resource_request,
        "resource_actual": dict(resource_request),
        "created_at": _queue_execution.created_at(previous_state) or started_at,
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def write_running_state_artifact(
    entry: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    write_state_fn: Callable[..., Any],
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
    worker_job_pid: int | None = None,
) -> None:
    if not job_dir_text:
        return
    job_dir = Path(job_dir_text).expanduser().resolve()
    payload = build_running_state_payload(
        entry,
        job_dir=job_dir,
        selected_input_xyz=selected_input_xyz,
        started_at=started_at,
        updated_at=updated_at,
        previous_state=previous_state,
        resumed=resumed,
        resource_request=resource_request,
        engine_fields=engine_fields,
        detail_fields=detail_fields,
    )
    if worker_job_pid is not None and worker_job_pid > 0:
        payload["worker_job_pid"] = int(worker_job_pid)
    write_state_fn(job_dir, payload)


def write_running_engine_state_artifact(
    entry: Any,
    *,
    job_dir_text: str,
    started_at: str,
    updated_at: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    resource_request: dict[str, int],
    artifact_fields: EngineArtifactFields,
    write_state_fn: Callable[..., Any],
    worker_job_pid: int | None = None,
) -> None:
    write_running_state_artifact(
        entry,
        job_dir_text=job_dir_text,
        selected_input_xyz=artifact_fields.selected_input_xyz,
        started_at=started_at,
        updated_at=updated_at,
        previous_state=previous_state,
        resumed=resumed,
        resource_request=resource_request,
        write_state_fn=write_state_fn,
        engine_fields=artifact_fields.engine_payload(),
        detail_fields=artifact_fields.detail_payload(),
        worker_job_pid=worker_job_pid,
    )


def build_terminal_state_payload(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "job_dir": job_dir_text,
        "selected_input_xyz": selected_input_xyz,
        **dict(engine_fields or {}),
        "status": result.status,
        "reason": result.reason,
        "started_at": result.started_at,
        "updated_at": result.finished_at,
        **dict(detail_fields or {}),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(previous_state),
        "recovery_pending": False,
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_terminal_report_payload(
    entry: Any,
    result: Any,
    *,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    recovery_reason = _queue_execution.recovery_reason(previous_state)
    payload = {
        "job_id": entry.task_id,
        "queue_id": entry.queue_id,
        "status": result.status,
        "reason": result.reason,
        **dict(engine_fields or {}),
        "selected_input_xyz": selected_input_xyz,
        "command": list(result.command),
        "exit_code": result.exit_code,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "stdout_log": result.stdout_log,
        "stderr_log": result.stderr_log,
        **dict(detail_fields or {}),
        "manifest_path": result.manifest_path,
        "resource_request": dict(result.resource_request),
        "resource_actual": dict(result.resource_actual),
        "created_at": _queue_execution.created_at(previous_state),
        "recovery_count": _queue_execution.recovery_count(previous_state),
        "resumed": bool(resumed),
    }
    if recovery_reason:
        payload["recovery_reason"] = recovery_reason
    return payload


def build_terminal_artifact_payloads(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> TerminalArtifactPayloads:
    return TerminalArtifactPayloads(
        state=build_terminal_state_payload(
            entry,
            result,
            job_dir_text=job_dir_text,
            selected_input_xyz=selected_input_xyz,
            previous_state=previous_state,
            resumed=resumed,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
        report=build_terminal_report_payload(
            entry,
            result,
            selected_input_xyz=selected_input_xyz,
            previous_state=previous_state,
            resumed=resumed,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
    )


def write_terminal_execution_artifacts(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    selected_input_xyz: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    engine_fields: dict[str, Any] | None,
    detail_fields: dict[str, Any] | None,
    report_lines: list[str],
    write_state_fn: Callable[..., Any],
    write_report_json_fn: Callable[..., Any],
    write_report_md_lines_fn: Callable[..., Any],
) -> None:
    write_terminal_engine_artifacts(
        entry,
        result,
        job_dir_text=job_dir_text,
        previous_state=previous_state,
        resumed=resumed,
        artifact_fields=EngineArtifactFields(
            selected_input_xyz=selected_input_xyz,
            engine_fields=engine_fields,
            detail_fields=detail_fields,
        ),
        report_lines=report_lines,
        writers=TerminalArtifactWriters(
            write_state=write_state_fn,
            write_report_json=write_report_json_fn,
            write_report_md_lines=write_report_md_lines_fn,
        ),
    )


def write_terminal_engine_artifacts(
    entry: Any,
    result: Any,
    *,
    job_dir_text: str,
    previous_state: dict[str, Any] | None,
    resumed: bool,
    artifact_fields: EngineArtifactFields,
    report_lines: list[str],
    writers: TerminalArtifactWriters,
) -> None:
    if not job_dir_text:
        return
    payloads = build_terminal_artifact_payloads(
        entry,
        result,
        job_dir_text=job_dir_text,
        selected_input_xyz=artifact_fields.selected_input_xyz,
        previous_state=previous_state,
        resumed=resumed,
        engine_fields=artifact_fields.engine_payload(),
        detail_fields=artifact_fields.detail_payload(),
    )
    _queue_execution.write_result_artifacts(
        job_dir_text,
        state_payload=payloads.state,
        report_payload=payloads.report,
        report_lines=report_lines,
        write_state_fn=writers.write_state,
        write_report_json_fn=writers.write_report_json,
        write_report_md_lines_fn=writers.write_report_md_lines,
    )


def terminal_report_lines(
    entry: Any,
    result: Any,
    *,
    title: str,
    selected_input_label: str,
    selected_input_xyz: str,
    engine_lines: list[str] | None = None,
    detail_lines: list[str] | None = None,
) -> list[str]:
    return [
        f"# {title}",
        "",
        f"- Job ID: `{entry.task_id}`",
        f"- Queue ID: `{entry.queue_id}`",
        f"- Status: `{result.status}`",
        f"- Reason: `{result.reason}`",
        *list(engine_lines or []),
        f"- {selected_input_label}: `{Path(selected_input_xyz).name}`",
        f"- Exit Code: `{result.exit_code}`",
        *list(detail_lines or []),
        f"- Resource Request: `{result.resource_request}`",
        f"- Resource Actual: `{result.resource_actual}`",
        f"- Stdout Log: `{result.stdout_log}`",
        f"- Stderr Log: `{result.stderr_log}`",
    ]


def build_terminal_result(
    result_cls: type,
    entry: Any,
    *,
    job_dir: Path,
    selected_xyz: Path,
    log_prefix: str,
    manifest_filename: str,
    resource_request: dict[str, int],
    status: str,
    reason: str,
    now_utc_iso_fn: Callable[[], str],
    command: tuple[str, ...] = (),
    exit_code: int = 1,
    engine_fields: dict[str, Any] | None = None,
    detail_fields: dict[str, Any] | None = None,
) -> Any:
    terminal_time = now_utc_iso_fn()
    manifest_path = (job_dir / manifest_filename).resolve()
    return result_cls(
        status=status,
        reason=reason,
        command=command,
        exit_code=exit_code,
        started_at=entry.started_at or terminal_time,
        finished_at=terminal_time,
        stdout_log=str((job_dir / f"{log_prefix}.stdout.log").resolve()),
        stderr_log=str((job_dir / f"{log_prefix}.stderr.log").resolve()),
        selected_input_xyz=str(selected_xyz.resolve()),
        **dict(engine_fields or {}),
        **dict(detail_fields or {}),
        manifest_path=str(manifest_path) if manifest_path.exists() else "",
        resource_request=resource_request,
        resource_actual=dict(resource_request),
    )


__all__ = [
    "EngineArtifactFields",
    "TerminalArtifactPayloads",
    "TerminalArtifactWriters",
    "build_running_state_payload",
    "build_terminal_artifact_payloads",
    "build_terminal_report_payload",
    "build_terminal_result",
    "build_terminal_state_payload",
    "is_resumed_state",
    "terminal_report_lines",
    "write_running_engine_state_artifact",
    "write_running_state_artifact",
    "write_terminal_engine_artifacts",
    "write_terminal_execution_artifacts",
]
