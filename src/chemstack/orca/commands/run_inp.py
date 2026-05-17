from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Type

from chemstack.core.app_ids import CHEMSTACK_ORCA_APP_NAME

from .. import queue_store as _queue_store
from ..admission_store import (
    ADMISSION_APP_NAME_ENV_VAR,
    ADMISSION_TASK_ID_ENV_VAR,
    ADMISSION_TOKEN_ENV_VAR,
    AdmissionLimitReachedError,
    activate_reserved_slot,
    acquire_direct_slot,
    release_slot,
)
from ..attempt_engine import _exit_with_result, run_attempts
from ..completion_rules import detect_completion_mode
from ..config import load_config
from ..inp_rewriter import ensure_submission_resource_request, read_resource_request_from_input
from ..lock_utils import is_process_alive, parse_lock_info, process_start_ticks
from ..orca_runner import OrcaRunner
from ..out_analyzer import analyze_output
from ..runtime.run_lock import LOCK_FILE_NAME, acquire_run_lock
from ..state_machine import RESUMABLE_RUN_STATUSES, load_or_create_state
from ..state_store import load_state, save_state
from ..statuses import AnalyzerStatus, QueueStatus, RunStatus
from ..telegram_notifier import (
    notify_queue_enqueued_event,
    notify_retry_event,
    notify_run_finished_event,
    notify_run_started_event,
)
from ..types import (
    QueueEnqueuedNotification,
    QueueEntry,
    RetryNotification,
    RunFinishedNotification,
    RunStartedNotification,
)
from ._helpers import (
    ORCA_GENERATED_INP_RE,
    RETRY_INP_RE,
    _emit,
    _to_resolved_local,
    _validate_reaction_dir,
)
from . import run_inp_execution as _run_inp_execution
from . import run_inp_submission as _run_inp_submission

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _RunInpStatusDeps:
    AdmissionLimitReachedError: Any
    AnalyzerStatus: Any
    RunStatus: Any


@dataclass(frozen=True)
class _RunInpExecutionDeps:
    acquire_run_lock: Any
    load_or_create_state: Any
    release_slot: Any
    run_attempts: Any
    save_state: Any
    _admission_context: Any
    _emit: Any
    _execute_locked_run: Any
    _existing_completed_exit: Any
    _existing_completed_out: Any
    _exit_with_result: Any
    _notification_callbacks: Any
    _recover_crashed_state: Any
    _release_reservation_if_needed: Any
    _resolve_execution_context: Any
    _retry_inp_path: Any
    _run_with_state: Any
    _to_resolved_local: Any


@dataclass(frozen=True)
class _RunInpNotificationDeps:
    notify_queue_enqueued_event: Any
    notify_retry_event: Any
    notify_run_finished_event: Any
    notify_run_started_event: Any


@dataclass(frozen=True)
class _RunInpSubmissionDeps:
    ensure_submission_resource_request: Any
    read_resource_request_from_input: Any
    _build_queue_enqueued_notification: Any
    _build_queue_metadata: Any
    _emit_queued_submission: Any
    _queue_store: Any
    _resource_request_from_selected_inp: Any
    _select_latest_inp: Any
    _submit_as_queued: Any
    _upsert_queued_job_record: Any
    _warn_ignored_resource_override_flags: Any
    _worker_status_for_submission: Any


@dataclass(frozen=True)
class _RunInpDeps:
    statuses: _RunInpStatusDeps
    execution: _RunInpExecutionDeps
    notifications: _RunInpNotificationDeps
    submission: _RunInpSubmissionDeps

    def __getattr__(self, name: str) -> Any:
        for group in (self.statuses, self.execution, self.notifications, self.submission):
            if hasattr(group, name):
                return getattr(group, name)
        raise AttributeError(name)


def _run_inp_deps() -> _RunInpDeps:
    return _RunInpDeps(
        statuses=_RunInpStatusDeps(
            AdmissionLimitReachedError=AdmissionLimitReachedError,
            AnalyzerStatus=AnalyzerStatus,
            RunStatus=RunStatus,
        ),
        execution=_RunInpExecutionDeps(
            acquire_run_lock=acquire_run_lock,
            load_or_create_state=load_or_create_state,
            release_slot=release_slot,
            run_attempts=run_attempts,
            save_state=save_state,
            _admission_context=_admission_context,
            _emit=_emit,
            _execute_locked_run=_execute_locked_run,
            _existing_completed_exit=_existing_completed_exit,
            _existing_completed_out=_existing_completed_out,
            _exit_with_result=_exit_with_result,
            _notification_callbacks=_notification_callbacks,
            _recover_crashed_state=_recover_crashed_state,
            _release_reservation_if_needed=_release_reservation_if_needed,
            _resolve_execution_context=_resolve_execution_context,
            _retry_inp_path=_retry_inp_path,
            _run_with_state=_run_with_state,
            _to_resolved_local=_to_resolved_local,
        ),
        notifications=_RunInpNotificationDeps(
            notify_queue_enqueued_event=notify_queue_enqueued_event,
            notify_retry_event=notify_retry_event,
            notify_run_finished_event=notify_run_finished_event,
            notify_run_started_event=notify_run_started_event,
        ),
        submission=_RunInpSubmissionDeps(
            ensure_submission_resource_request=ensure_submission_resource_request,
            read_resource_request_from_input=read_resource_request_from_input,
            _build_queue_enqueued_notification=_build_queue_enqueued_notification,
            _build_queue_metadata=_build_queue_metadata,
            _emit_queued_submission=_emit_queued_submission,
            _queue_store=_queue_store,
            _resource_request_from_selected_inp=_resource_request_from_selected_inp,
            _select_latest_inp=_select_latest_inp,
            _submit_as_queued=_submit_as_queued,
            _upsert_queued_job_record=_upsert_queued_job_record,
            _warn_ignored_resource_override_flags=_warn_ignored_resource_override_flags,
            _worker_status_for_submission=_worker_status_for_submission,
        ),
    )


@dataclass(frozen=True)
class ResolvedRunTarget:
    reaction_dir: Path
    selected_inp: Path


@dataclass(frozen=True)
class RunExecutionContext:
    cfg: Any
    reaction_dir: Path
    selected_inp: Path
    allowed_root: Path
    admission_root: Path
    max_retries: int
    max_concurrent: int
    admission_limit: int
    reservation_token: str | None
    admission_app_name: str | None
    admission_task_id: str | None


@dataclass(frozen=True)
class WorkerStatusInfo:
    status: str | None = None
    pid: int | None = None
    log_file: str | Path | None = None
    detail: str | None = None


@dataclass(frozen=True)
class RunSubmissionContext:
    cfg: Any
    reaction_dir: Path
    selected_inp: Path
    allowed_root: Path


def _select_latest_inp(reaction_dir: Path) -> Path:
    all_candidates = list(reaction_dir.glob("*.inp"))
    if not all_candidates:
        raise ValueError(f"No .inp file found in: {reaction_dir}")
    # Prefer user-authored base inputs over generated retry/intermediate files.
    candidates = [
        p
        for p in all_candidates
        if not RETRY_INP_RE.search(p.stem) and not ORCA_GENERATED_INP_RE.search(p.stem)
    ]
    if not candidates:
        candidates = all_candidates
    candidates.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)
    return candidates[0]


def _retry_inp_path(selected_inp: Path, retry_number: int) -> Path:
    base_stem = RETRY_INP_RE.sub("", selected_inp.stem)
    if not base_stem:
        base_stem = selected_inp.stem
    return selected_inp.with_name(f"{base_stem}.retry{retry_number:02d}.inp")


def _existing_completed_out(selected_inp: Path) -> Dict[str, Any] | None:
    base_stem = RETRY_INP_RE.sub("", selected_inp.stem)
    if not base_stem:
        base_stem = selected_inp.stem

    out_candidates = list(selected_inp.parent.glob(f"{base_stem}.out"))
    out_candidates.extend(selected_inp.parent.glob(f"{base_stem}.retry*.out"))
    out_candidates.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)

    seen: set[Path] = set()
    for out_path in out_candidates:
        resolved = out_path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)

        mode_inp = out_path.with_suffix(".inp")
        if not mode_inp.exists():
            mode_inp = selected_inp
        mode = detect_completion_mode(mode_inp)
        analysis = analyze_output(out_path, mode)
        if analysis.status != AnalyzerStatus.COMPLETED:
            continue
        return {
            "out_path": str(out_path),
            "analysis": analysis,
        }
    return None


def _recover_crashed_state(reaction_dir: Path) -> bool:
    """Detect and recover from a crashed run (status=running/retrying but no active lock)."""
    state = load_state(reaction_dir)
    if not state:
        return False

    status = str(state.get("status", "")).strip()
    if status not in RESUMABLE_RUN_STATUSES:
        return False

    lock_path = reaction_dir / LOCK_FILE_NAME
    if lock_path.exists():
        lock_info = parse_lock_info(lock_path)
        lock_pid = lock_info.get("pid")
        if isinstance(lock_pid, int) and is_process_alive(lock_pid):
            return False

    logger.warning(
        "Detected crashed run in %s (status=%s, no active lock). Recovering state.",
        reaction_dir,
        status,
    )
    state["status"] = RunStatus.FAILED.value
    state["final_result"] = {
        "status": RunStatus.FAILED.value,
        "reason": "crashed_recovery",
        "analyzer_status": AnalyzerStatus.INCOMPLETE.value,
    }
    save_state(reaction_dir, state)
    return True


def _configured_max_concurrent(cfg: Any) -> int:
    raw = getattr(cfg.runtime, "max_concurrent", 4)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 4
    return max(1, value)


def _configured_admission_root(cfg: Any) -> Path:
    raw = (
        getattr(cfg.runtime, "resolved_admission_root", None)
        or getattr(cfg.runtime, "admission_root", "")
        or getattr(cfg.runtime, "allowed_root", "")
    )
    return Path(str(raw)).expanduser().resolve()


def _configured_admission_limit(cfg: Any) -> int:
    raw: object | None = getattr(cfg.runtime, "admission_limit", None)
    if raw in {None, ""}:
        return _configured_max_concurrent(cfg)
    try:
        if isinstance(raw, bool):
            value = int(raw)
        elif isinstance(raw, (int, float, str)):
            value = int(raw)
        else:
            raise TypeError("Unsupported admission_limit type")
    except (TypeError, ValueError):
        value = _configured_max_concurrent(cfg)
    return max(1, value)


def _resolve_run_target(cfg: Any, reaction_dir_raw: str) -> ResolvedRunTarget:
    reaction_dir = _validate_reaction_dir(cfg, reaction_dir_raw)
    return ResolvedRunTarget(
        reaction_dir=reaction_dir,
        selected_inp=_select_latest_inp(reaction_dir),
    )


def _resolve_run_target_or_log(cfg: Any, reaction_dir_raw: str) -> ResolvedRunTarget | None:
    try:
        return _resolve_run_target(cfg, reaction_dir_raw)
    except ValueError as exc:
        logger.error("%s", exc)
        return None


def _active_direct_run_error(reaction_dir: Path) -> str | None:
    lock_info = parse_lock_info(reaction_dir / LOCK_FILE_NAME)
    lock_pid = lock_info.get("pid")
    if not isinstance(lock_pid, int) or not is_process_alive(lock_pid):
        return None

    expected_ticks = lock_info.get("process_start_ticks")
    if isinstance(expected_ticks, int) and expected_ticks > 0:
        observed_ticks = process_start_ticks(lock_pid)
        if observed_ticks is None or observed_ticks != expected_ticks:
            logger.info(
                "Ignoring stale run.lock due to PID reuse: reaction_dir=%s pid=%d expected=%d observed=%s",
                reaction_dir,
                lock_pid,
                expected_ticks,
                observed_ticks,
            )
            return None

    started_at = lock_info.get("started_at")
    started = started_at if isinstance(started_at, str) and started_at else "unknown"
    return (
        "Another chemstack instance is already running in this directory "
        f"(pid={lock_pid}, started_at={started}). Lock file: {reaction_dir / LOCK_FILE_NAME}"
    )


def _active_queue_entry(allowed_root: Path, reaction_dir: Path) -> QueueEntry | None:
    helper = getattr(_queue_store, "get_active_entry_for_reaction_dir", None)
    if callable(helper):
        return helper(allowed_root, str(reaction_dir))

    resolved = str(reaction_dir.expanduser().resolve())
    for entry in _queue_store.list_queue(allowed_root):
        if _queue_store.queue_entry_reaction_dir(entry) != resolved:
            continue
        if _queue_store.queue_entry_status(entry) in {
            QueueStatus.PENDING.value,
            QueueStatus.RUNNING.value,
        }:
            return entry
    return None


def _find_submission_conflict(allowed_root: Path, reaction_dir: Path) -> str | None:
    active_entry = _active_queue_entry(allowed_root, reaction_dir)
    if active_entry is not None:
        return (
            "Job directory already queued: "
            f"{reaction_dir} (queue_id={_queue_store.queue_entry_id(active_entry)}, "
            f"status={_queue_store.queue_entry_status(active_entry)})"
        )
    return _active_direct_run_error(reaction_dir)


def _emit_queued_submission(
    reaction_dir: Path,
    entry: QueueEntry,
    *,
    worker_status: str | None,
    worker_pid: int | None,
    worker_log: str | Path | None,
    worker_detail: str | None = None,
) -> None:
    print("status: queued")
    print(f"job_dir: {reaction_dir}")
    print(f"queue_id: {_queue_store.queue_entry_id(entry)}")
    task_id = _queue_store.queue_entry_task_id(entry)
    if task_id:
        print(f"job_id: {task_id}")
    print(f"priority: {_queue_store.queue_entry_priority(entry)}")
    if _queue_store.queue_entry_force(entry):
        print("force: true")
    if worker_status:
        print(f"worker: {worker_status}")
    if worker_pid is not None:
        print(f"worker_pid: {worker_pid}")
    if worker_log:
        print(f"worker_log: {worker_log}")
    if worker_detail:
        print(f"worker_detail: {worker_detail}")


def _worker_status_for_submission(allowed_root: Path) -> WorkerStatusInfo:
    from ..queue_worker import read_worker_pid

    pid = read_worker_pid(allowed_root)
    if pid is None:
        return WorkerStatusInfo(status="inactive")
    return WorkerStatusInfo(status="running", pid=pid)


def _existing_completed_exit(
    *,
    reaction_dir: Path,
    selected_inp: Path,
    admission_root: Path,
    reservation_token: str | None,
    max_retries: int,
) -> int | None:
    return _run_inp_execution.existing_completed_exit(
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        admission_root=admission_root,
        reservation_token=reservation_token,
        max_retries=max_retries,
        deps=_run_inp_deps(),
    )


def _submission_flag_error(args: Any) -> str | None:
    if getattr(args, "require_slot", False):
        return "Immediate execution has been removed; run-dir now always submits to the queue."
    return None


def _reaction_dir_arg(args: Any) -> str | None:
    raw = getattr(args, "path", None) or getattr(args, "reaction_dir", None)
    if not isinstance(raw, str) or not raw.strip():
        return None
    return raw


def _resolve_submission_context(
    args: Any,
    *,
    cfg: Any | None = None,
) -> RunSubmissionContext | None:
    if cfg is None:
        cfg = load_config(args.config)
    reaction_dir_raw = _reaction_dir_arg(args)
    if reaction_dir_raw is None:
        logger.error("job directory path is required")
        return None
    target = _resolve_run_target_or_log(cfg, reaction_dir_raw)
    if target is None:
        return None
    return RunSubmissionContext(
        cfg=cfg,
        reaction_dir=target.reaction_dir,
        selected_inp=target.selected_inp,
        allowed_root=Path(cfg.runtime.allowed_root).expanduser().resolve(),
    )


def _resolve_execution_context(
    args: Any,
    *,
    cfg: Any | None = None,
    reaction_dir: Path | None = None,
    selected_inp: Path | None = None,
    reservation_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
) -> RunExecutionContext | None:
    if cfg is None:
        cfg = load_config(args.config)
    if reaction_dir is None or selected_inp is None:
        reaction_dir_raw = _reaction_dir_arg(args)
        if reaction_dir_raw is None:
            logger.error("job directory path is required")
            return None
        target = _resolve_run_target_or_log(cfg, reaction_dir_raw)
        if target is None:
            return None
        reaction_dir = target.reaction_dir
        selected_inp = target.selected_inp

    return RunExecutionContext(
        cfg=cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        allowed_root=Path(cfg.runtime.allowed_root).expanduser().resolve(),
        admission_root=_configured_admission_root(cfg),
        max_retries=max(0, int(cfg.runtime.default_max_retries)),
        max_concurrent=_configured_max_concurrent(cfg),
        admission_limit=_configured_admission_limit(cfg),
        reservation_token=reservation_token
        if reservation_token is not None
        else (os.getenv(ADMISSION_TOKEN_ENV_VAR, "").strip() or None),
        admission_app_name=admission_app_name
        if admission_app_name is not None
        else (os.getenv(ADMISSION_APP_NAME_ENV_VAR, "").strip() or None),
        admission_task_id=admission_task_id
        if admission_task_id is not None
        else (os.getenv(ADMISSION_TASK_ID_ENV_VAR, "").strip() or None),
    )


def _admission_context(
    *,
    admission_root: Path,
    reaction_dir: Path,
    admission_limit: int,
    reservation_token: str | None,
    admission_app_name: str | None,
    admission_task_id: str | None,
) -> AbstractContextManager[str]:
    if reservation_token is not None:
        return activate_reserved_slot(
            admission_root,
            reservation_token,
            reaction_dir=str(reaction_dir),
            source="queue_run",
            app_name=admission_app_name,
            task_id=admission_task_id,
        )
    return acquire_direct_slot(
        admission_root,
        max_concurrent=admission_limit,
        reaction_dir=str(reaction_dir),
        app_name=admission_app_name or CHEMSTACK_ORCA_APP_NAME,
        task_id=admission_task_id,
    )


def _release_reservation_if_needed(admission_root: Path, reservation_token: str | None) -> None:
    if reservation_token is not None:
        release_slot(admission_root, reservation_token)


def _notification_callbacks(
    cfg: Any,
) -> tuple[
    Callable[[RunStartedNotification], None] | None,
    Callable[[RunFinishedNotification], None] | None,
    Callable[[RetryNotification], None] | None,
]:
    return _run_inp_execution.notification_callbacks(cfg, deps=_run_inp_deps())


def _run_with_state(
    *,
    cfg: Any,
    reaction_dir: Path,
    selected_inp: Path,
    runner_cls: Type[OrcaRunner],
    max_retries: int,
    resumed: bool,
    state: Any,
) -> int:
    return _run_inp_execution.run_with_state(
        cfg=cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        runner_cls=runner_cls,
        max_retries=max_retries,
        resumed=resumed,
        state=state,
        deps=_run_inp_deps(),
    )


def _build_queue_enqueued_notification(entry: QueueEntry) -> QueueEnqueuedNotification:
    return _run_inp_submission.build_queue_enqueued_notification(
        entry,
        deps=_run_inp_deps(),
    )


def _resource_request_from_selected_inp(cfg: Any, selected_inp: Path | None) -> dict[str, int]:
    return _run_inp_submission.resource_request_from_selected_inp(
        cfg,
        selected_inp,
        deps=_run_inp_deps(),
        logger=logger,
    )


def _warn_ignored_resource_override_flags(args: Any) -> None:
    _run_inp_submission.warn_ignored_resource_override_flags(args, logger=logger)


def _build_queue_metadata(
    cfg: Any,
    *,
    reaction_dir: Path,
    selected_inp: Path | None,
    args: Any | None = None,
) -> dict[str, Any]:
    return _run_inp_submission.build_queue_metadata(
        cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        args=args,
        deps=_run_inp_deps(),
    )


def _upsert_queued_job_record(
    cfg: Any,
    *,
    reaction_dir: Path,
    selected_inp: Path | None,
    job_id: str,
    queue_metadata: dict[str, Any] | None = None,
) -> None:
    _run_inp_submission.upsert_queued_job_record(
        cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        job_id=job_id,
        queue_metadata=queue_metadata,
        deps=_run_inp_deps(),
    )


def _submit_as_queued(
    cfg: Any,
    args: Any,
    reaction_dir: Path,
    *,
    selected_inp: Path | None = None,
) -> int:
    return _run_inp_submission.submit_as_queued(
        cfg,
        args,
        reaction_dir,
        selected_inp=selected_inp,
        deps=_run_inp_deps(),
        logger=logger,
    )


def _existing_completed_submit_exit(
    args: Any,
    context: RunSubmissionContext,
    *,
    runner_cls: Type[OrcaRunner],
) -> int | None:
    if getattr(args, "force", False) or not _existing_completed_out(context.selected_inp):
        return None
    return _cmd_run_inp_execute(
        args,
        runner_cls=runner_cls,
        cfg=context.cfg,
        reaction_dir=context.reaction_dir,
        selected_inp=context.selected_inp,
    )


def _execute_locked_run(
    args: Any,
    context: RunExecutionContext,
    *,
    runner_cls: Type[OrcaRunner],
) -> int:
    return _run_inp_execution.execute_locked_run(
        args,
        context,
        runner_cls=runner_cls,
        deps=_run_inp_deps(),
    )


def _cmd_run_inp_execute(
    args: Any,
    *,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
    cfg: Any | None = None,
    reaction_dir: Path | None = None,
    selected_inp: Path | None = None,
    reservation_token: str | None = None,
    admission_app_name: str | None = None,
    admission_task_id: str | None = None,
) -> int:
    return _run_inp_execution.cmd_run_inp_execute(
        args,
        runner_cls=runner_cls,
        cfg=cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        reservation_token=reservation_token,
        admission_app_name=admission_app_name,
        admission_task_id=admission_task_id,
        deps=_run_inp_deps(),
        logger=logger,
    )


def _cmd_run_inp_submit(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    flag_error = _submission_flag_error(args)
    if flag_error is not None:
        logger.error("%s", flag_error)
        return 1

    context = _resolve_submission_context(args)
    if context is None:
        return 1

    conflict_error = _find_submission_conflict(context.allowed_root, context.reaction_dir)
    if conflict_error is not None:
        logger.error("%s", conflict_error)
        return 1

    existing_completed_exit = _existing_completed_submit_exit(args, context, runner_cls=runner_cls)
    if existing_completed_exit is not None:
        return existing_completed_exit

    return _submit_as_queued(
        context.cfg,
        args,
        context.reaction_dir,
        selected_inp=context.selected_inp,
    )


def cmd_run_inp(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    return _cmd_run_inp_submit(args, runner_cls=runner_cls)
