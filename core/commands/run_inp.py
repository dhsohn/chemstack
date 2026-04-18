from __future__ import annotations

import logging
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Type

from .. import queue_store as _queue_store
from ..admission_store import (
    ADMISSION_TOKEN_ENV_VAR,
    AdmissionLimitReachedError,
    activate_reserved_slot,
    acquire_direct_slot,
    release_slot,
)
from ..attempt_engine import _exit_with_result, run_attempts
from ..completion_rules import detect_completion_mode
from ..config import load_config
from ..lock_utils import is_process_alive, parse_lock_info, process_start_ticks
from ..orca_runner import OrcaRunner
from ..out_analyzer import analyze_output
from ..state_machine import RESUMABLE_RUN_STATUSES, load_or_create_state
from ..state_store import LOCK_FILE_NAME, acquire_run_lock, load_state, save_state
from ..statuses import AnalyzerStatus, QueueStatus, RunStatus
from ..telegram_notifier import (
    notify_queue_enqueued_event,
    notify_retry_event,
    notify_run_finished_event,
    notify_run_started_event,
)
from ..types import QueueEnqueuedNotification, QueueEntry, RetryNotification, RunFinishedNotification, RunStartedNotification
from ._helpers import ORCA_GENERATED_INP_RE, RETRY_INP_RE, _emit, _to_resolved_local, _validate_reaction_dir

logger = logging.getLogger(__name__)


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
    admission_max_concurrent: int
    reservation_token: str | None


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
        p for p in all_candidates
        if not RETRY_INP_RE.search(p.stem)
        and not ORCA_GENERATED_INP_RE.search(p.stem)
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
    raw = getattr(cfg.runtime, "admission_root", "") or getattr(cfg.runtime, "allowed_root", "")
    return Path(str(raw)).expanduser().resolve()


def _configured_admission_max_concurrent(cfg: Any) -> int:
    raw = getattr(cfg.runtime, "admission_max_concurrent", None)
    if raw in {None, ""}:
        return _configured_max_concurrent(cfg)
    try:
        value = int(raw)
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
        "Another orca_auto instance is already running in this directory "
        f"(pid={lock_pid}, started_at={started}). Lock file: {reaction_dir / LOCK_FILE_NAME}"
    )


def _active_queue_entry(allowed_root: Path, reaction_dir: Path) -> QueueEntry | None:
    helper = getattr(_queue_store, "get_active_entry_for_reaction_dir", None)
    if callable(helper):
        return helper(allowed_root, str(reaction_dir))

    resolved = str(reaction_dir.expanduser().resolve())
    for entry in _queue_store.list_queue(allowed_root):
        if entry.get("reaction_dir") != resolved:
            continue
        if entry.get("status") in {QueueStatus.PENDING.value, QueueStatus.RUNNING.value}:
            return entry
    return None


def _find_submission_conflict(allowed_root: Path, reaction_dir: Path) -> str | None:
    active_entry = _active_queue_entry(allowed_root, reaction_dir)
    if active_entry is not None:
        return (
            "Reaction directory already queued: "
            f"{reaction_dir} (queue_id={active_entry.get('queue_id')}, status={active_entry.get('status')})"
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
    print(f"reaction_dir: {reaction_dir}")
    print(f"queue_id: {entry.get('queue_id')}")
    print(f"priority: {entry.get('priority')}")
    if entry.get("force"):
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
    done = _existing_completed_out(selected_inp)
    if done is None:
        return None

    if reservation_token is not None:
        release_slot(admission_root, reservation_token)
    state, resumed = load_or_create_state(
        reaction_dir,
        selected_inp,
        max_retries=max_retries,
        to_resolved_local=_to_resolved_local,
    )
    return _exit_with_result(
        reaction_dir,
        state,
        selected_inp,
        status=RunStatus.COMPLETED,
        analyzer_status=AnalyzerStatus.COMPLETED,
        reason="existing_out_completed",
        last_out_path=done["out_path"],
        resumed=True if resumed else None,
        exit_code=0,
        emit=_emit,
        extra={"skipped_execution": True},
    )


def _submission_flag_error(args: Any) -> str | None:
    if getattr(args, "require_slot", False):
        return "Immediate execution has been removed; run-inp now always submits to the queue."
    return None


def _resolve_submission_context(
    args: Any,
    *,
    cfg: Any | None = None,
) -> RunSubmissionContext | None:
    if cfg is None:
        cfg = load_config(args.config)
    target = _resolve_run_target_or_log(cfg, args.reaction_dir)
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
) -> RunExecutionContext | None:
    if cfg is None:
        cfg = load_config(args.config)
    if reaction_dir is None or selected_inp is None:
        target = _resolve_run_target_or_log(cfg, args.reaction_dir)
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
        admission_max_concurrent=_configured_admission_max_concurrent(cfg),
        reservation_token=os.getenv(ADMISSION_TOKEN_ENV_VAR, "").strip() or None,
    )


def _admission_context(
    *,
    admission_root: Path,
    reaction_dir: Path,
    admission_max_concurrent: int,
    reservation_token: str | None,
) -> AbstractContextManager[str]:
    if reservation_token is not None:
        return activate_reserved_slot(
            admission_root,
            reservation_token,
            reaction_dir=str(reaction_dir),
            source="queue_run",
        )
    return acquire_direct_slot(
        admission_root,
        max_concurrent=admission_max_concurrent,
        reaction_dir=str(reaction_dir),
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
    if not cfg.telegram.enabled:
        return None, None, None

    def _notify_started(event: RunStartedNotification) -> None:
        notify_run_started_event(cfg.telegram, event)

    def _notify_finished(event: RunFinishedNotification) -> None:
        notify_run_finished_event(cfg.telegram, event)

    def _notify_retry(event: RetryNotification) -> None:
        notify_retry_event(cfg.telegram, event)

    return _notify_started, _notify_finished, _notify_retry


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
    notify_started, notify_finished, notify_retry = _notification_callbacks(cfg)
    runner = runner_cls(cfg.paths.orca_executable)
    return run_attempts(
        reaction_dir,
        selected_inp,
        state,
        resumed=resumed,
        runner=runner,
        max_retries=max_retries,
        retry_inp_path=_retry_inp_path,
        to_resolved_local=_to_resolved_local,
        emit=_emit,
        notify_started=notify_started,
        notify_finished=notify_finished,
        notify_retry=notify_retry,
    )


def _build_queue_enqueued_notification(entry: QueueEntry) -> QueueEnqueuedNotification:
    return {
        "queue_id": entry["queue_id"],
        "reaction_dir": entry["reaction_dir"],
        "priority": entry["priority"],
        "force": entry.get("force", False),
        "enqueued_at": entry.get("enqueued_at", ""),
    }


def _submit_as_queued(cfg: Any, args: Any, reaction_dir: Path) -> int:
    from ..queue_store import DuplicateEntryError, enqueue

    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    try:
        entry = enqueue(
            allowed_root,
            str(reaction_dir),
            priority=int(getattr(args, "priority", 10)),
            force=bool(getattr(args, "force", False)),
        )
    except DuplicateEntryError as exc:
        logger.error("%s", exc)
        return 1

    notification = _build_queue_enqueued_notification(entry)
    notify_queue_enqueued_event(cfg.telegram, notification)

    worker_info = _worker_status_for_submission(allowed_root)

    _emit_queued_submission(
        reaction_dir,
        entry,
        worker_status=worker_info.status,
        worker_pid=worker_info.pid,
        worker_log=worker_info.log_file,
        worker_detail=worker_info.detail,
    )
    return 0


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
    with acquire_run_lock(context.reaction_dir):
        if not getattr(args, "force", False):
            existing_exit = _existing_completed_exit(
                reaction_dir=context.reaction_dir,
                selected_inp=context.selected_inp,
                admission_root=context.admission_root,
                reservation_token=context.reservation_token,
                max_retries=context.max_retries,
            )
            if existing_exit is not None:
                return existing_exit

        with _admission_context(
            admission_root=context.admission_root,
            reaction_dir=context.reaction_dir,
            admission_max_concurrent=context.admission_max_concurrent,
            reservation_token=context.reservation_token,
        ):
            state, resumed = load_or_create_state(
                context.reaction_dir,
                context.selected_inp,
                max_retries=context.max_retries,
                to_resolved_local=_to_resolved_local,
            )
            return _run_with_state(
                cfg=context.cfg,
                reaction_dir=context.reaction_dir,
                selected_inp=context.selected_inp,
                runner_cls=runner_cls,
                max_retries=context.max_retries,
                resumed=resumed,
                state=state,
            )


def _cmd_run_inp_execute(
    args: Any,
    *,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
    cfg: Any | None = None,
    reaction_dir: Path | None = None,
    selected_inp: Path | None = None,
) -> int:
    context = _resolve_execution_context(
        args,
        cfg=cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
    )
    if context is None:
        return 1

    logger.info("Selected input: %s", context.selected_inp)

    _recover_crashed_state(context.reaction_dir)

    try:
        return _execute_locked_run(args, context, runner_cls=runner_cls)
    except AdmissionLimitReachedError as exc:
        _release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.error("%s", exc)
        return 1
    except RuntimeError as exc:
        _release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        _release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.exception("Unexpected error while running input: %s", exc)
        return 1


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

    return _submit_as_queued(context.cfg, args, context.reaction_dir)


def cmd_run_inp(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    return _cmd_run_inp_submit(args, runner_cls=runner_cls)
