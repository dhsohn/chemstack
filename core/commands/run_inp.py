from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Type

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


def _resolve_reaction_and_input(cfg: Any, reaction_dir_raw: str) -> tuple[Path, Path]:
    reaction_dir = _validate_reaction_dir(cfg, reaction_dir_raw)
    selected_inp = _select_latest_inp(reaction_dir)
    return reaction_dir, selected_inp


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


def _has_pending_entries(allowed_root: Path) -> bool:
    helper = getattr(_queue_store, "has_pending_entries", None)
    if callable(helper):
        return bool(helper(allowed_root))
    return any(
        entry.get("status") == QueueStatus.PENDING.value
        for entry in _queue_store.list_queue(allowed_root)
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


def _ensure_worker_for_submission(config_path: str, allowed_root: Path) -> Any:
    from ..queue_worker import ensure_worker_running

    return ensure_worker_running(config_path, allowed_root)


def _worker_result_field(result: Any, field: str) -> Any:
    if result is None:
        return None
    if isinstance(result, dict):
        return result.get(field)
    return getattr(result, field, None)


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

    notification: QueueEnqueuedNotification = {
        "queue_id": entry["queue_id"],
        "reaction_dir": entry["reaction_dir"],
        "priority": entry["priority"],
        "force": entry.get("force", False),
        "enqueued_at": entry.get("enqueued_at", ""),
    }
    notify_queue_enqueued_event(cfg.telegram, notification)

    worker_status = None
    worker_pid = None
    worker_log = None
    worker_detail = None
    try:
        worker_result = _ensure_worker_for_submission(args.config, allowed_root)
        worker_status = _worker_result_field(worker_result, "status")
        worker_pid = _worker_result_field(worker_result, "pid")
        worker_log = _worker_result_field(worker_result, "log_file")
        worker_detail = _worker_result_field(worker_result, "detail")
        if worker_status == "failed":
            if worker_detail:
                logger.warning("Queue worker autostart failed for queued submission %s: %s", reaction_dir, worker_detail)
            else:
                logger.warning("Queue worker autostart failed for queued submission: %s", reaction_dir)
    except Exception as exc:
        worker_status = "failed"
        worker_detail = str(exc)
        logger.warning("Queue worker autostart failed for queued submission %s: %s", reaction_dir, exc)

    _emit_queued_submission(
        reaction_dir,
        entry,
        worker_status=worker_status,
        worker_pid=worker_pid,
        worker_log=worker_log,
        worker_detail=worker_detail,
    )
    return 0


def _cmd_run_inp_execute(
    args: Any,
    *,
    runner_cls: Type[OrcaRunner] = OrcaRunner,
    cfg: Any | None = None,
    reaction_dir: Path | None = None,
    selected_inp: Path | None = None,
    raise_on_admission_limit: bool = False,
) -> int:
    if cfg is None:
        cfg = load_config(args.config)
    try:
        if reaction_dir is None or selected_inp is None:
            reaction_dir, selected_inp = _resolve_reaction_and_input(cfg, args.reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    logger.info("Selected input: %s", selected_inp)

    max_retries = max(0, int(cfg.runtime.default_max_retries))
    max_concurrent = _configured_max_concurrent(cfg)
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    reservation_token = os.getenv(ADMISSION_TOKEN_ENV_VAR, "").strip() or None

    _recover_crashed_state(reaction_dir)

    try:
        with acquire_run_lock(reaction_dir):
            if not getattr(args, "force", False):
                done = _existing_completed_out(selected_inp)
                if done:
                    if reservation_token is not None:
                        release_slot(allowed_root, reservation_token)
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

            if reservation_token is not None:
                admission_cm = activate_reserved_slot(
                    allowed_root,
                    reservation_token,
                    reaction_dir=str(reaction_dir),
                    source="queue_run",
                )
            else:
                admission_cm = acquire_direct_slot(
                    allowed_root,
                    max_concurrent=max_concurrent,
                    reaction_dir=str(reaction_dir),
                )

            with admission_cm:
                state, resumed = load_or_create_state(
                    reaction_dir,
                    selected_inp,
                    max_retries=max_retries,
                    to_resolved_local=_to_resolved_local,
                )

                notify_started = None
                notify_finished = None
                notify_retry = None
                if cfg.telegram.enabled:

                    def _notify_started(event: RunStartedNotification) -> None:
                        notify_run_started_event(cfg.telegram, event)

                    def _notify_finished(event: RunFinishedNotification) -> None:
                        notify_run_finished_event(cfg.telegram, event)

                    def _notify_retry(event: RetryNotification) -> None:
                        notify_retry_event(cfg.telegram, event)

                    notify_started = _notify_started
                    notify_finished = _notify_finished
                    notify_retry = _notify_retry

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
    except AdmissionLimitReachedError as exc:
        if reservation_token is not None:
            release_slot(allowed_root, reservation_token)
        if raise_on_admission_limit:
            raise
        logger.error("%s", exc)
        return 1
    except RuntimeError as exc:
        if reservation_token is not None:
            release_slot(allowed_root, reservation_token)
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        if reservation_token is not None:
            release_slot(allowed_root, reservation_token)
        logger.exception("Unexpected error while running input: %s", exc)
        return 1


def _cmd_run_inp_submit(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    if getattr(args, "queue_only", False) and getattr(args, "require_slot", False):
        logger.error("--queue-only and --require-slot cannot be used together.")
        return 1

    cfg = load_config(args.config)
    try:
        reaction_dir, selected_inp = _resolve_reaction_and_input(cfg, args.reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()

    conflict_error = _find_submission_conflict(allowed_root, reaction_dir)
    if conflict_error is not None:
        logger.error("%s", conflict_error)
        return 1

    if not getattr(args, "force", False) and _existing_completed_out(selected_inp):
        return _cmd_run_inp_execute(
            args,
            runner_cls=runner_cls,
            cfg=cfg,
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
        )

    if getattr(args, "queue_only", False):
        return _submit_as_queued(cfg, args, reaction_dir)

    if _has_pending_entries(allowed_root):
        if getattr(args, "require_slot", False):
            logger.error("Immediate execution unavailable: pending queue backlog exists under %s", allowed_root)
            return 1
        return _submit_as_queued(cfg, args, reaction_dir)

    try:
        return _cmd_run_inp_execute(
            args,
            runner_cls=runner_cls,
            cfg=cfg,
            reaction_dir=reaction_dir,
            selected_inp=selected_inp,
            raise_on_admission_limit=True,
        )
    except AdmissionLimitReachedError as exc:
        if getattr(args, "require_slot", False):
            logger.error("%s", exc)
            return 1
        logger.info("Immediate execution unavailable, enqueueing instead: %s", exc)
        return _submit_as_queued(cfg, args, reaction_dir)


def cmd_run_inp(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    if getattr(args, "execute_now", False):
        return _cmd_run_inp_execute(args, runner_cls=runner_cls)
    return _cmd_run_inp_submit(args, runner_cls=runner_cls)
