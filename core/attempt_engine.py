from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Protocol

from .completion_rules import detect_completion_mode
from .inp_rewriter import rewrite_for_retry
from .out_analyzer import analyze_output
from .state_machine import MAX_RETRY_RECIPES, decide_attempt_outcome
from .state_store import finalize_state, now_utc_iso, save_state, state_path, write_report_files
from .statuses import AnalyzerStatus, RunStatus
from .types import AttemptRecord, RunFinalResult, RunState

logger = logging.getLogger(__name__)


class RunResultLike(Protocol):
    out_path: str
    return_code: int


class RunnerLike(Protocol):
    def run(self, inp_path: Path) -> RunResultLike: ...


def _last_out_path_from_state(state: RunState) -> str | None:
    attempts = state.get("attempts")
    if not isinstance(attempts, list) or not attempts:
        return None
    last = attempts[-1]
    if not isinstance(last, dict):
        return None
    out_path = last.get("out_path")
    if isinstance(out_path, str) and out_path.strip():
        return out_path
    return None


def build_final_result(
    *,
    status: RunStatus | str,
    analyzer_status: AnalyzerStatus | str,
    reason: str,
    last_out_path: str | None,
    resumed: bool | None = None,
    extra: Mapping[str, object] | None = None,
) -> RunFinalResult:
    status_text = status.value if isinstance(status, RunStatus) else str(status)
    analyzer_status_text = analyzer_status.value if isinstance(analyzer_status, AnalyzerStatus) else str(analyzer_status)
    result: RunFinalResult = {
        "status": status_text,
        "analyzer_status": analyzer_status_text,
        "reason": reason,
        "completed_at": now_utc_iso(),
        "last_out_path": last_out_path,
    }
    if resumed is not None:
        result["resumed"] = resumed
    if extra is not None:
        skipped_execution = extra.get("skipped_execution")
        if isinstance(skipped_execution, bool):
            result["skipped_execution"] = skipped_execution
        runner_error = extra.get("runner_error")
        if isinstance(runner_error, str) and runner_error:
            result["runner_error"] = runner_error
    return result


def _run_status_text(status: RunStatus | str) -> str:
    return status.value if isinstance(status, RunStatus) else str(status)


def _retry_recipe_step(retry_number: int) -> int:
    """Map retry number to available recipe steps.

    With two recipes, retries beyond step 2 re-use step 2.
    """
    retry_number = max(1, int(retry_number))
    return min(retry_number, MAX_RETRY_RECIPES)


def _build_run_payload(
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    *,
    status: RunStatus | str,
    reason: str,
    reports: Dict[str, str],
) -> Dict[str, Any]:
    status_text = _run_status_text(status)
    attempts = state.get("attempts")
    attempt_count = len(attempts) if isinstance(attempts, list) else 0
    return {
        "status": status_text,
        "reason": reason,
        "reaction_dir": str(reaction_dir),
        "selected_inp": str(selected_inp),
        "attempt_count": attempt_count,
        "run_state": str(state_path(reaction_dir)),
        **reports,
    }


def finalize_and_emit(
    reaction_dir: Path,
    state: RunState,
    selected_inp: Path,
    *,
    status: RunStatus | str,
    reason: str,
    final_result: RunFinalResult,
    as_json: bool,
    exit_code: int,
    emit: Callable[[Dict[str, Any], bool], None],
) -> int:
    status_text = _run_status_text(status)
    finalize_state(reaction_dir, state, status=status_text, final_result=final_result)

    reports = write_report_files(reaction_dir, state)
    payload = _build_run_payload(
        reaction_dir,
        selected_inp,
        state,
        status=status,
        reason=reason,
        reports=reports,
    )
    emit(payload, as_json)
    return exit_code


def _exit_with_result(
    reaction_dir: Path,
    state: RunState,
    selected_inp: Path,
    *,
    status: RunStatus | str,
    analyzer_status: AnalyzerStatus | str,
    reason: str,
    last_out_path: str | None,
    resumed: bool | None,
    as_json: bool,
    exit_code: int,
    emit: Callable[[Dict[str, Any], bool], None],
    extra: Mapping[str, object] | None = None,
) -> int:
    final = build_final_result(
        status=status,
        analyzer_status=analyzer_status,
        reason=reason,
        last_out_path=last_out_path,
        resumed=resumed,
        extra=extra,
    )
    return finalize_and_emit(
        reaction_dir,
        state,
        selected_inp,
        status=status,
        reason=reason,
        final_result=final,
        as_json=as_json,
        exit_code=exit_code,
        emit=emit,
    )


def _ensure_patch_actions_list(attempt: AttemptRecord) -> list[str]:
    existing = attempt.get("patch_actions")
    if isinstance(existing, list):
        return existing
    attempt["patch_actions"] = []
    return attempt["patch_actions"]


def _recover_missing_retry_input(
    *,
    reaction_dir: Path,
    state: RunState,
    selected_inp: Path,
    current_inp: Path,
    retries_used: int,
    to_resolved_local: Callable[[str], Path],
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
    if not source_inp.exists():
        return False, "resume_source_input_not_found"

    if source_inp.resolve() == current_inp.resolve():
        source_inp = selected_inp.resolve()
        if not source_inp.exists():
            return False, "resume_fallback_source_missing"

    patch_step = _retry_recipe_step(retries_used)
    patch_actions = rewrite_for_retry(
        source_inp=source_inp,
        target_inp=current_inp,
        reaction_dir=reaction_dir,
        step=patch_step,
    )

    actions = _ensure_patch_actions_list(last_attempt)
    actions.append(f"resume_recreated_missing_input:{current_inp.name}")
    actions.extend([f"resume_{action}" for action in patch_actions])
    save_state(reaction_dir, state)
    return True, "resume_recovered"


def _as_non_empty_text(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return stripped
    return None


def _resume_terminal_decision(
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    *,
    resumed: bool,
    max_retries: int,
    as_json: bool,
    emit: Callable[[Dict[str, Any], bool], None],
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
    last_out_path = _as_non_empty_text(last_attempt.get("out_path")) or _last_out_path_from_state(state)
    return _exit_with_result(
        reaction_dir, state, selected_inp,
        status=decision.run_status,
        analyzer_status=analyzer_status,
        reason=decision.reason,
        last_out_path=last_out_path,
        resumed=resumed, as_json=as_json, exit_code=decision.exit_code, emit=emit,
    )


def run_attempts(
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    *,
    resumed: bool,
    runner: RunnerLike,
    max_retries: int,
    as_json: bool,
    retry_inp_path: Callable[[Path, int], Path],
    to_resolved_local: Callable[[str], Path],
    emit: Callable[[Dict[str, Any], bool], None],
) -> int:
    resumed_exit = _resume_terminal_decision(
        reaction_dir,
        selected_inp,
        state,
        resumed=resumed,
        max_retries=max_retries,
        as_json=as_json,
        emit=emit,
    )
    if resumed_exit is not None:
        return resumed_exit

    execution_index = len(state["attempts"]) + 1
    while True:
        retries_used = execution_index - 1
        if retries_used > max_retries:
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=RunStatus.FAILED,
                analyzer_status=AnalyzerStatus.INCOMPLETE,
                reason="retry_limit_reached",
                last_out_path=_last_out_path_from_state(state),
                resumed=resumed, as_json=as_json, exit_code=1, emit=emit,
            )

        current_inp = selected_inp if execution_index == 1 else retry_inp_path(selected_inp, retries_used)
        if not current_inp.exists():
            reason = f"missing_input_for_attempt_{execution_index}"
            if execution_index > 1:
                try:
                    recovered, recovery_reason = _recover_missing_retry_input(
                        reaction_dir=reaction_dir,
                        state=state,
                        selected_inp=selected_inp,
                        current_inp=current_inp,
                        retries_used=retries_used,
                        to_resolved_local=to_resolved_local,
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
                    reason = f"{reason}:{recovery_reason}"
            if not current_inp.exists():
                return _exit_with_result(
                    reaction_dir, state, selected_inp,
                    status=RunStatus.FAILED,
                    analyzer_status=AnalyzerStatus.INCOMPLETE,
                    reason=reason, last_out_path=None,
                    resumed=resumed, as_json=as_json, exit_code=1, emit=emit,
                )

        state["status"] = RunStatus.RUNNING.value if retries_used == 0 else RunStatus.RETRYING.value
        save_state(reaction_dir, state)

        started_at = now_utc_iso()
        logger.info("Attempt %d starting: %s", execution_index, current_inp)
        try:
            run_result = runner.run(current_inp)
        except KeyboardInterrupt:
            logger.warning("Interrupted by user during attempt %d", execution_index)
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=RunStatus.FAILED,
                analyzer_status=AnalyzerStatus.INCOMPLETE,
                reason="interrupted_by_user",
                last_out_path=str(current_inp.with_suffix(".out")),
                resumed=resumed, as_json=as_json, exit_code=130, emit=emit,
            )
        except Exception as exc:
            logger.exception("ORCA runner crashed during attempt %d: %s", execution_index, exc)
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=RunStatus.FAILED,
                analyzer_status=AnalyzerStatus.INCOMPLETE,
                reason="runner_exception",
                last_out_path=str(current_inp.with_suffix(".out")),
                resumed=resumed, as_json=as_json, exit_code=1, emit=emit,
                extra={"runner_error": str(exc)},
            )
        out_path = Path(run_result.out_path)

        mode = detect_completion_mode(current_inp)
        analysis = analyze_output(out_path, mode)
        attempt: AttemptRecord = {
            "index": execution_index,
            "inp_path": str(current_inp),
            "out_path": str(out_path),
            "return_code": run_result.return_code,
            "analyzer_status": analysis.status,
            "analyzer_reason": analysis.reason,
            "markers": analysis.markers,
            "patch_actions": [],
            "started_at": started_at,
            "ended_at": now_utc_iso(),
        }
        state["attempts"].append(attempt)
        save_state(reaction_dir, state)

        logger.info("Attempt %d finished: return_code=%d, status=%s", execution_index, run_result.return_code, analysis.status)
        decision = decide_attempt_outcome(
            analyzer_status=analysis.status,
            analyzer_reason=analysis.reason,
            retries_used=retries_used,
            max_retries=max_retries,
        )
        if decision is not None:
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=decision.run_status,
                analyzer_status=analysis.status,
                reason=decision.reason,
                last_out_path=str(out_path),
                resumed=resumed, as_json=as_json,
                exit_code=decision.exit_code, emit=emit,
            )

        next_retry_number = retries_used + 1
        next_inp = retry_inp_path(selected_inp, next_retry_number)
        patch_step = _retry_recipe_step(next_retry_number)
        try:
            patch_actions = rewrite_for_retry(
                source_inp=current_inp,
                target_inp=next_inp,
                reaction_dir=reaction_dir,
                step=patch_step,
            )
        except Exception as exc:
            state["attempts"][-1]["patch_actions"] = [f"rewrite_failed:{exc}"]
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=RunStatus.FAILED,
                analyzer_status=analysis.status,
                reason="rewrite_failed",
                last_out_path=str(out_path),
                resumed=resumed, as_json=as_json, exit_code=1, emit=emit,
            )

        state["attempts"][-1]["patch_actions"] = patch_actions
        save_state(reaction_dir, state)
        execution_index += 1
