from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Type

from ..attempt_engine import _exit_with_result, run_attempts
from ..completion_rules import detect_completion_mode
from ..config import load_config
from ..lock_utils import is_process_alive, parse_lock_info
from ..orca_runner import OrcaRunner
from ..out_analyzer import analyze_output
from ..state_machine import RESUMABLE_RUN_STATUSES, load_or_create_state
from ..state_store import LOCK_FILE_NAME, acquire_run_lock, load_state, save_state
from ..statuses import AnalyzerStatus, RunStatus
from ..telegram_notifier import (
    notify_retry_event,
    notify_run_finished_event,
    notify_run_started_event,
)
from ..types import RetryNotification, RunFinishedNotification, RunStartedNotification
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
    """Detect and recover from a crashed run (status=running/retrying but no active lock).

    If the previous process crashed, we mark the state as failed with reason
    'crashed_recovery' so that the next load_or_create_state treats it as
    resumable and continues from where it left off.

    Returns True if recovery was performed.
    """
    state = load_state(reaction_dir)
    if not state:
        return False

    status = str(state.get("status", "")).strip()
    if status not in RESUMABLE_RUN_STATUSES:
        return False

    # Check if the lock is held by an active process
    lock_path = reaction_dir / LOCK_FILE_NAME
    if lock_path.exists():
        lock_info = parse_lock_info(lock_path)
        lock_pid = lock_info.get("pid")
        if isinstance(lock_pid, int) and is_process_alive(lock_pid):
            return False  # Another process is actually running

    # State says running/retrying but no active lock → crashed
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


def cmd_run_inp(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    cfg = load_config(args.config)
    try:
        reaction_dir = _validate_reaction_dir(cfg, args.reaction_dir)
        selected_inp = _select_latest_inp(reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    logger.info("Selected input: %s", selected_inp)

    max_retries = max(0, int(cfg.runtime.default_max_retries))

    # Recover from crashes before attempting to acquire the lock
    _recover_crashed_state(reaction_dir)

    try:
        with acquire_run_lock(reaction_dir):
            if not args.force:
                done = _existing_completed_out(selected_inp)
                if done:
                    state, resumed = load_or_create_state(
                        reaction_dir,
                        selected_inp,
                        max_retries=max_retries,
                        to_resolved_local=_to_resolved_local,
                    )
                    return _exit_with_result(
                        reaction_dir, state, selected_inp,
                        status=RunStatus.COMPLETED,
                        analyzer_status=AnalyzerStatus.COMPLETED,
                        reason="existing_out_completed",
                        last_out_path=done["out_path"],
                        resumed=True if resumed else None, exit_code=0, emit=_emit,
                        extra={"skipped_execution": True},
                    )

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
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        logger.exception("Unexpected error while running input: %s", exc)
        return 1
