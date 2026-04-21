from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict

from .inp_rewriter import rewrite_for_retry
from .state_machine import decide_attempt_outcome
from .statuses import AnalyzerStatus
from .types import AttemptRecord, RunFinishedNotification, RunState

logger = logging.getLogger(__name__)


def _ensure_patch_actions_list(attempt: AttemptRecord) -> list[str]:
    existing = attempt.get("patch_actions")
    if isinstance(existing, list):
        return existing
    attempt["patch_actions"] = []
    return attempt["patch_actions"]


def _as_non_empty_text(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def recover_missing_retry_input(
    *,
    reaction_dir: Path,
    state: RunState,
    selected_inp: Path,
    current_inp: Path,
    retries_used: int,
    retry_recipe_step: Callable[[int], int],
    to_resolved_local: Callable[[str], Path],
    save_state: Callable[[Path, RunState], Path],
) -> tuple[bool, str]:
    attempts = state.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return False, "resume_attempts_missing"

    last_attempt = attempts[-1]
    if not isinstance(last_attempt, dict):
        return False, "resume_last_attempt_invalid"

    source_inp_text = last_attempt.get("inp_path")
    if not isinstance(source_inp_text, str) or not source_inp_text.strip():
        return False, "resume_source_input_missing"

    source_inp = to_resolved_local(source_inp_text)
    if source_inp.resolve() == current_inp.resolve():
        source_inp = selected_inp.resolve()
        if not source_inp.exists():
            return False, "resume_fallback_source_missing"
    elif not source_inp.exists():
        return False, "resume_source_input_not_found"

    patch_actions = rewrite_for_retry(
        source_inp=source_inp,
        target_inp=current_inp,
        reaction_dir=reaction_dir,
        step=retry_recipe_step(retries_used),
    )
    actions = _ensure_patch_actions_list(last_attempt)
    actions.append(f"resume_recreated_missing_input:{current_inp.name}")
    actions.extend([f"resume_{action}" for action in patch_actions])
    save_state(reaction_dir, state)
    return True, "resume_recovered"


def resolve_execution_input(
    *,
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    execution_index: int,
    retries_used: int,
    retry_inp_path: Callable[[Path, int], Path],
    retry_recipe_step: Callable[[int], int],
    to_resolved_local: Callable[[str], Path],
    save_state: Callable[[Path, RunState], Path],
) -> tuple[Path | None, str | None]:
    current_inp = selected_inp if execution_index == 1 else retry_inp_path(selected_inp, retries_used)
    if current_inp.exists():
        return current_inp, None

    reason = f"missing_input_for_attempt_{execution_index}"
    if execution_index == 1:
        return None, reason

    try:
        recovered, recovery_reason = recover_missing_retry_input(
            reaction_dir=reaction_dir,
            state=state,
            selected_inp=selected_inp,
            current_inp=current_inp,
            retries_used=retries_used,
            retry_recipe_step=retry_recipe_step,
            to_resolved_local=to_resolved_local,
            save_state=save_state,
        )
    except Exception:
        logger.warning(
            "Failed while recovering missing retry input for attempt %d",
            execution_index,
            exc_info=True,
        )
        recovered = False
        recovery_reason = "resume_recovery_exception"

    if recovered and not current_inp.exists():
        recovered = False
        recovery_reason = "resume_recovery_no_output"
    if not recovered:
        return None, f"{reason}:{recovery_reason}"
    return current_inp, None


def resume_terminal_decision(
    *,
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    resumed: bool,
    max_retries: int,
    last_out_path_from_state: Callable[[RunState], str | None],
    exit_with_result: Callable[..., int],
    emit: Callable[[Dict[str, Any]], None],
    notify_finished: Callable[[RunFinishedNotification], None] | None = None,
) -> int | None:
    if not resumed:
        return None

    attempts = state.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return None
    last_attempt = attempts[-1]
    if not isinstance(last_attempt, dict):
        return None

    retries_used = len(attempts) - 1
    analyzer_status = _as_non_empty_text(last_attempt.get("analyzer_status")) or AnalyzerStatus.INCOMPLETE.value
    analyzer_reason = _as_non_empty_text(last_attempt.get("analyzer_reason")) or "resume_last_attempt"
    decision = decide_attempt_outcome(
        analyzer_status=analyzer_status,
        analyzer_reason=analyzer_reason,
        retries_used=retries_used,
        max_retries=max_retries,
    )
    if decision is None:
        return None

    logger.info(
        "Resume detected terminal previous attempt: analyzer_status=%s, reason=%s",
        analyzer_status,
        decision.reason,
    )
    last_out_path = _as_non_empty_text(last_attempt.get("out_path")) or last_out_path_from_state(state)
    return exit_with_result(
        reaction_dir,
        state,
        selected_inp,
        status=decision.run_status,
        analyzer_status=analyzer_status,
        reason=decision.reason,
        last_out_path=last_out_path,
        resumed=resumed,
        exit_code=decision.exit_code,
        emit=emit,
        notify_finished=notify_finished,
    )
