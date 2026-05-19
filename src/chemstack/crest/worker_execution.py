from __future__ import annotations

import argparse
import os
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from chemstack.core.admission import release_slot
from chemstack.core.queue import (
    execution as _queue_execution,
    get_cancel_requested,
    list_queue,
    mark_cancelled,
    mark_completed,
    mark_failed,
    requeue_running_entry,
)
from chemstack.core.queue.types import QueueStatus
from chemstack.core.queue.engine_execution import (
    EngineWorkerLifecycle,
    run_engine_worker_lifecycle,
)
from chemstack.core.queue.worker import (
    build_background_worker_command,
    install_shutdown_signal_handlers,
    resolve_admission_root,
    terminate_process_group,
)
from chemstack.core.utils import now_utc_iso

from . import queue_artifacts as _queue_artifacts
from .config import load_config
from .job_locations import upsert_job_record
from .notifications import notify_job_finished, notify_job_started
from .runner import CrestRunResult, finalize_crest_job, start_crest_job
from .state import mark_recovery_pending

CANCEL_CHECK_INTERVAL_SECONDS = 1
is_recovery_pending = _queue_artifacts.is_recovery_pending
load_state = _queue_artifacts.load_state
state_matches_job = _queue_artifacts.state_matches_job
write_report_json = _queue_artifacts.write_report_json
write_report_md_lines = _queue_artifacts.write_report_md_lines
write_state = _queue_artifacts.write_state


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


def build_worker_execution_dependencies(
    *,
    now_utc_iso_fn: Callable[[], str],
    get_cancel_requested_fn: Callable[[str, str], bool],
    start_crest_job_fn: Callable[..., Any],
    finalize_crest_job_fn: Callable[..., CrestRunResult],
    terminate_process_fn: Callable[[subprocess.Popen[str]], None],
    write_running_state_fn: Callable[[Any, Any], None],
    write_execution_artifacts_fn: Callable[[Any, CrestRunResult], None],
    mark_completed_fn: Callable[..., Any],
    mark_cancelled_fn: Callable[..., Any],
    mark_failed_fn: Callable[..., Any],
    upsert_job_record_fn: Callable[..., Any],
    notify_job_started_fn: Callable[..., bool],
    notify_job_finished_fn: Callable[..., bool],
) -> WorkerExecutionDependencies:
    return WorkerExecutionDependencies(
        now_utc_iso=now_utc_iso_fn,
        get_cancel_requested=get_cancel_requested_fn,
        start_crest_job=start_crest_job_fn,
        finalize_crest_job=finalize_crest_job_fn,
        terminate_process=terminate_process_fn,
        write_running_state=write_running_state_fn,
        write_execution_artifacts=write_execution_artifacts_fn,
        mark_completed=mark_completed_fn,
        mark_cancelled=mark_cancelled_fn,
        mark_failed=mark_failed_fn,
        upsert_job_record=upsert_job_record_fn,
        notify_job_started=notify_job_started_fn,
        notify_job_finished=notify_job_finished_fn,
    )


class WorkerShutdownRequested(RuntimeError):
    def __init__(self, context: ExecutionContext):
        super().__init__("worker_shutdown")
        self.context = context


@dataclass
class _ShutdownController:
    requested: bool = False

    def request(self) -> None:
        self.requested = True

    def is_requested(self) -> bool:
        return self.requested


def default_worker_execution_dependencies() -> WorkerExecutionDependencies:
    return build_worker_execution_dependencies(
        now_utc_iso_fn=now_utc_iso,
        get_cancel_requested_fn=get_cancel_requested,
        start_crest_job_fn=start_crest_job,
        finalize_crest_job_fn=finalize_crest_job,
        terminate_process_fn=_terminate_process,
        write_running_state_fn=_write_running_state,
        write_execution_artifacts_fn=_write_execution_artifacts,
        mark_completed_fn=mark_completed,
        mark_cancelled_fn=mark_cancelled,
        mark_failed_fn=mark_failed,
        upsert_job_record_fn=upsert_job_record,
        notify_job_started_fn=notify_job_started,
        notify_job_finished_fn=notify_job_finished,
    )


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return _queue_artifacts.coerce_mapping(value)


def build_worker_child_command(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    auto_organize: bool = False,
    admission_token: str | None = None,
) -> list[str]:
    del auto_organize
    return build_background_worker_command(
        config_path=config_path,
        queue_root=Path(queue_root),
        queue_id=queue_id,
        worker_job_module="chemstack.crest.worker_execution",
        admission_token=admission_token,
        auto_organize=False,
        include_admission_root=False,
        auto_organize_before_admission_token=True,
    )


def _matching_result_state(entry: Any, result: CrestRunResult, job_dir: Path) -> dict[str, Any]:
    return _queue_artifacts.matching_result_state(
        entry,
        result,
        job_dir,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
    )


def _build_state_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _queue_artifacts.build_state_payload(entry, result, previous_state=previous_state)


def _build_report_payload(
    entry: Any,
    result: CrestRunResult,
    *,
    previous_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _queue_artifacts.build_report_payload(entry, result, previous_state=previous_state)


def _write_execution_artifacts(entry: Any, result: CrestRunResult) -> None:
    _queue_artifacts.write_execution_artifacts(
        entry,
        result,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        write_state_fn=write_state,
        write_report_json_fn=write_report_json,
        write_report_md_lines_fn=write_report_md_lines,
    )


def _write_running_state(cfg: Any, entry: Any) -> None:
    _queue_artifacts.write_running_state(
        cfg,
        entry,
        load_state_fn=load_state,
        state_matches_job_fn=state_matches_job,
        is_recovery_pending_fn=is_recovery_pending,
        write_state_fn=write_state,
        now_utc_iso_fn=depsafe_now_utc_iso,
    )


def depsafe_now_utc_iso() -> str:
    return _queue_artifacts.depsafe_now_utc_iso()


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    terminate_process_group(
        proc,
        killpg_fn=os.killpg,
        sigterm=signal.SIGTERM,
        sigkill=signal.SIGKILL,
    )


def _resource_caps(cfg: Any) -> dict[str, int]:
    return _queue_artifacts.resource_caps(cfg)


def _coerce_resource_dict(value: Any) -> dict[str, int]:
    return _queue_artifacts.coerce_resource_dict(value)


def _entry_resource_request(cfg: Any, entry: Any) -> dict[str, int]:
    return _queue_artifacts.entry_resource_request(cfg, entry)


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


def _mark_recovery_pending_context(cfg: Any, context: ExecutionContext, *, reason: str) -> None:
    mark_recovery_pending(
        context.job_dir,
        job_id=str(context.entry.task_id),
        selected_input_xyz=str(context.selected_xyz),
        mode=context.mode,
        molecule_key=context.molecule_key,
        resource_request=context.resource_request,
        resource_actual=context.resource_request,
        reason=reason,
    )
    upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
        status="pending",
        job_dir=context.job_dir,
        mode=context.mode,
        selected_input_xyz=str(context.selected_xyz),
        molecule_key=context.molecule_key,
        resource_request=context.resource_request,
        resource_actual=dict(context.resource_request),
    )


def _mark_recovery_pending_entry(cfg: Any, entry: Any, *, reason: str) -> None:
    context = _build_execution_context(
        cfg,
        entry,
        resource_caps=_resource_caps,
        molecule_key_resolver=_molecule_key,
    )
    _mark_recovery_pending_context(cfg, context, reason=reason)


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
    _queue_execution.mark_terminal_status(
        queue_root,
        context.entry.queue_id,
        status=result.status,
        reason=result.reason,
        metadata_update=metadata_update,
        mark_completed_fn=dependencies.mark_completed,
        mark_cancelled_fn=dependencies.mark_cancelled,
        mark_failed_fn=dependencies.mark_failed,
    )


def _sync_job_tracking(
    cfg: Any,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    auto_organize: bool,
    dependencies: WorkerExecutionDependencies,
) -> Path | None:
    del auto_organize
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

    return None


def _raise_if_shutdown_requested(
    context: ExecutionContext,
    shutdown_requested: Callable[[], bool] | None,
) -> None:
    if shutdown_requested is not None and shutdown_requested():
        raise WorkerShutdownRequested(context)


def _mark_job_running(
    cfg: Any,
    context: ExecutionContext,
    *,
    dependencies: WorkerExecutionDependencies,
) -> None:
    dependencies.write_running_state(cfg, context.entry)
    dependencies.upsert_job_record(
        cfg,
        job_id=context.entry.task_id,
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
        job_id=context.entry.task_id,
        queue_id=context.entry.queue_id,
        job_dir=context.job_dir,
        mode=context.mode,
        selected_xyz=context.selected_xyz,
    )


def _failed_result_from_exception(
    context: ExecutionContext,
    *,
    exc: Exception,
    failure_time: str,
) -> CrestRunResult:
    resource_request = context.resource_request
    return CrestRunResult(
        status="failed",
        reason=f"runner_error:{exc}",
        command=(),
        exit_code=1,
        started_at=context.entry.started_at or failure_time,
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


def _run_crest_job_for_entry(
    cfg: Any,
    context: ExecutionContext,
    *,
    queue_root: Path,
    dependencies: WorkerExecutionDependencies,
    shutdown_requested: Callable[[], bool] | None,
) -> CrestRunResult:
    try:
        running = dependencies.start_crest_job(
            cfg,
            job_dir=context.job_dir,
            selected_xyz=context.selected_xyz,
        )

        def raise_shutdown(_running: Any) -> None:
            raise WorkerShutdownRequested(context)

        return _queue_execution.wait_for_cancellable_process(
            running,
            finalize_fn=dependencies.finalize_crest_job,
            terminate_process_fn=dependencies.terminate_process,
            should_cancel=lambda: dependencies.get_cancel_requested(
                str(queue_root),
                context.entry.queue_id,
            ),
            shutdown_requested=shutdown_requested,
            on_shutdown=raise_shutdown,
            sleep_fn=time.sleep,
            poll_interval_seconds=CANCEL_CHECK_INTERVAL_SECONDS,
        )
    except Exception as exc:
        if isinstance(exc, WorkerShutdownRequested):
            raise
        return _failed_result_from_exception(
            context,
            exc=exc,
            failure_time=dependencies.now_utc_iso(),
        )


def _finalize_processed_entry(
    cfg: Any,
    context: ExecutionContext,
    result: CrestRunResult,
    *,
    queue_root: Path,
    auto_organize: bool,
    dependencies: WorkerExecutionDependencies,
) -> Path | None:
    dependencies.write_execution_artifacts(context.entry, result)
    _mark_queue_terminal(queue_root, context, result, dependencies=dependencies)
    organized_output_dir = _sync_job_tracking(
        cfg,
        context,
        result,
        auto_organize=auto_organize,
        dependencies=dependencies,
    )
    dependencies.notify_job_finished(
        cfg,
        job_id=context.entry.task_id,
        queue_id=context.entry.queue_id,
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
    shutdown_requested: Callable[[], bool] | None = None,
) -> WorkerExecutionOutcome:
    del auto_organize
    lifecycle = EngineWorkerLifecycle(
        build_context=lambda cfg_obj, entry_obj: _build_execution_context(
            cfg_obj,
            entry_obj,
            resource_caps=resource_caps,
            molecule_key_resolver=molecule_key_resolver,
        ),
        check_shutdown=lambda context: _raise_if_shutdown_requested(
            context,
            shutdown_requested,
        ),
        mark_running=lambda cfg_obj, context: _mark_job_running(
            cfg_obj,
            context,
            dependencies=dependencies,
        ),
        run_job=lambda cfg_obj, context, active_queue_root: _run_crest_job_for_entry(
            cfg_obj,
            context,
            queue_root=active_queue_root,
            dependencies=dependencies,
            shutdown_requested=shutdown_requested,
        ),
        finalize_entry=lambda cfg_obj, context, result, active_queue_root, should_organize: (
            _finalize_processed_entry(
                cfg_obj,
                context,
                result,
                queue_root=active_queue_root,
                auto_organize=should_organize,
                dependencies=dependencies,
            )
        ),
        build_outcome=lambda context, result, organized_output_dir: WorkerExecutionOutcome(
            result=result,
            job_dir=context.job_dir,
            selected_xyz=context.selected_xyz,
            molecule_key=context.molecule_key,
            organized_output_dir=organized_output_dir,
        ),
    )
    return run_engine_worker_lifecycle(
        cfg,
        entry,
        queue_root=queue_root,
        auto_organize=False,
        lifecycle=lifecycle,
    )


def _admission_root_for_cfg(cfg: Any) -> str:
    return resolve_admission_root(cfg)


def _find_queue_entry(queue_root: Path, queue_id: str) -> Any | None:
    for entry in list_queue(queue_root):
        if entry.queue_id == queue_id:
            return entry
    return None


def _install_shutdown_signal_handlers(controller: _ShutdownController) -> None:
    def request_shutdown() -> None:
        controller.request()

    install_shutdown_signal_handlers(request_shutdown)


def run_worker_child_job(
    *,
    config_path: str,
    queue_root: str | Path,
    queue_id: str,
    auto_organize: bool = False,
    admission_token: str | None = None,
) -> int:
    del auto_organize
    cfg = load_config(config_path)
    queue_root_path = Path(queue_root).expanduser().resolve()
    entry = _find_queue_entry(queue_root_path, queue_id)
    if entry is None or getattr(entry, "status", None) != QueueStatus.RUNNING:
        if admission_token:
            release_slot(_admission_root_for_cfg(cfg), admission_token)
        return 1

    controller = _ShutdownController()
    _install_shutdown_signal_handlers(controller)

    try:
        process_dequeued_entry(
            cfg,
            entry,
            queue_root=queue_root_path,
            auto_organize=False,
            resource_caps=_resource_caps,
            molecule_key_resolver=_molecule_key,
            dependencies=default_worker_execution_dependencies(),
            shutdown_requested=controller.is_requested,
        )
        return 0
    except WorkerShutdownRequested as exc:
        requeue_running_entry(queue_root_path, queue_id)
        _mark_recovery_pending_context(cfg, exc.context, reason="worker_shutdown")
        return 0
    finally:
        if admission_token:
            release_slot(_admission_root_for_cfg(cfg), admission_token)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m chemstack.crest.worker_execution")
    parser.add_argument("--config", required=True)
    parser.add_argument("--queue-root", required=True)
    parser.add_argument("--queue-id", required=True)
    parser.add_argument("--admission-token", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_worker_child_job(
        config_path=args.config,
        queue_root=args.queue_root,
        queue_id=args.queue_id,
        auto_organize=False,
        admission_token=str(args.admission_token).strip() or None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
