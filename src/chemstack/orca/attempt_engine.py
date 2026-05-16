from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Protocol

from .attempt_reporting import (
    build_retry_notification,
    build_run_started_notification,
    exit_with_result as _exit_with_result,
    last_out_path_from_state as _last_out_path_from_state,
)
from .attempt_resume import resolve_execution_input, resume_terminal_decision
from .completion_rules import detect_completion_mode
from .inp_rewriter import rewrite_for_retry
from .orca_runner import WorkerShutdownInterrupt
from .out_analyzer import OutAnalysis, analyze_output
from .state_machine import MAX_RETRY_RECIPES, decide_attempt_outcome
from .state_store import now_utc_iso, save_state
from .statuses import AnalyzerStatus, RunStatus
from .types import (
    AttemptRecord,
    RetryNotification,
    RunFinishedNotification,
    RunStartedNotification,
    RunState,
)

logger = logging.getLogger(__name__)


class RunResultLike(Protocol):
    out_path: str
    return_code: int


class RunnerLike(Protocol):
    def run(self, inp_path: Path) -> RunResultLike: ...


@dataclass(frozen=True)
class AttemptRunContext:
    reaction_dir: Path
    selected_inp: Path
    state: RunState
    resumed: bool
    runner: RunnerLike
    max_retries: int
    retry_inp_path: Callable[[Path, int], Path]
    to_resolved_local: Callable[[str], Path]
    emit: Callable[[Dict[str, Any]], None]
    notify_started: Callable[[RunStartedNotification], None] | None
    notify_finished: Callable[[RunFinishedNotification], None] | None
    notify_retry: Callable[[RetryNotification], None] | None


@dataclass(frozen=True)
class AttemptStartNotificationContext:
    run: AttemptRunContext
    current_inp: Path
    execution_index: int
    first_execution_index: int
    current_status: RunStatus
    started_at: str


@dataclass(frozen=True)
class RetryPreparationContext:
    run: AttemptRunContext
    current_inp: Path
    out_path: Path
    execution_index: int
    retries_used: int
    analysis: OutAnalysis


@dataclass
class AttemptLoopState:
    execution_index: int
    first_execution_index: int

    @property
    def retries_used(self) -> int:
        return self.execution_index - 1

    def advance(self) -> None:
        self.execution_index += 1


def _retry_recipe_step(retry_number: int) -> int:
    """Map retry number to available recipe steps.

    With two recipes, retries beyond step 2 re-use step 2.
    """
    retry_number = max(1, int(retry_number))
    return min(retry_number, MAX_RETRY_RECIPES)


def _mark_attempt_started(
    reaction_dir: Path,
    state: RunState,
    *,
    retries_used: int,
) -> tuple[str, RunStatus]:
    started_at = now_utc_iso()
    current_status = RunStatus.RUNNING if retries_used == 0 else RunStatus.RETRYING
    state["status"] = current_status.value
    save_state(reaction_dir, state)
    return started_at, current_status


def _notify_attempt_started(
    *,
    reaction_dir: Path,
    selected_inp: Path,
    current_inp: Path,
    state: RunState,
    execution_index: int,
    first_execution_index: int,
    max_retries: int,
    current_status: RunStatus,
    started_at: str,
    resumed: bool,
    notify_started: Callable[[RunStartedNotification], None] | None,
) -> None:
    _notify_attempt_started_from_context(
        AttemptStartNotificationContext(
            run=AttemptRunContext(
                reaction_dir=reaction_dir,
                selected_inp=selected_inp,
                state=state,
                resumed=resumed,
                runner=_NoRunnerForNotification(),
                max_retries=max_retries,
                retry_inp_path=_missing_retry_inp_path,
                to_resolved_local=_missing_to_resolved_local,
                emit=_missing_emit,
                notify_started=notify_started,
                notify_finished=None,
                notify_retry=None,
            ),
            current_inp=current_inp,
            execution_index=execution_index,
            first_execution_index=first_execution_index,
            current_status=current_status,
            started_at=started_at,
        )
    )


class _NoRunnerForNotification:
    def run(self, inp_path: Path) -> RunResultLike:
        raise RuntimeError("notification-only attempt context cannot run ORCA")


def _missing_retry_inp_path(selected_inp: Path, retry_number: int) -> Path:
    raise RuntimeError("notification-only attempt context cannot resolve retry input")


def _missing_to_resolved_local(path_text: str) -> Path:
    raise RuntimeError("notification-only attempt context cannot resolve local paths")


def _missing_emit(payload: Dict[str, Any]) -> None:
    raise RuntimeError("notification-only attempt context cannot emit")


def _notify_attempt_started_from_context(ctx: AttemptStartNotificationContext) -> None:
    should_notify_started = ctx.execution_index == ctx.first_execution_index and (
        ctx.execution_index == 1 or ctx.run.resumed
    )
    if not should_notify_started or ctx.run.notify_started is None:
        return

    started_notification = build_run_started_notification(
        reaction_dir=ctx.run.reaction_dir,
        selected_inp=ctx.run.selected_inp,
        current_inp=ctx.current_inp,
        state=ctx.run.state,
        execution_index=ctx.execution_index,
        max_retries=ctx.run.max_retries,
        status=ctx.current_status,
        attempt_started_at=ctx.started_at,
        resumed=ctx.run.resumed,
    )
    try:
        ctx.run.notify_started(started_notification)
    except Exception:
        logger.warning(
            "Started notification callback failed for attempt %d",
            ctx.execution_index,
            exc_info=True,
        )


def _run_and_record_attempt(
    reaction_dir: Path,
    state: RunState,
    *,
    current_inp: Path,
    execution_index: int,
    started_at: str,
    runner: RunnerLike,
) -> tuple[Path, OutAnalysis]:
    logger.info("Attempt %d starting: %s", execution_index, current_inp)
    run_result = runner.run(current_inp)
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

    logger.info(
        "Attempt %d finished: return_code=%d, status=%s",
        execution_index,
        run_result.return_code,
        analysis.status,
    )
    return out_path, analysis


def _finish_attempt(
    ctx: AttemptRunContext,
    *,
    status: RunStatus,
    analyzer_status: AnalyzerStatus | str,
    reason: str,
    last_out_path: str | None,
    exit_code: int,
    extra: Dict[str, Any] | None = None,
) -> int:
    return _exit_with_result(
        ctx.reaction_dir,
        ctx.state,
        ctx.selected_inp,
        status=status,
        analyzer_status=analyzer_status,
        reason=reason,
        last_out_path=last_out_path,
        resumed=ctx.resumed,
        exit_code=exit_code,
        emit=ctx.emit,
        extra=extra,
        notify_finished=ctx.notify_finished,
    )


def _resume_attempts_if_terminal(ctx: AttemptRunContext) -> int | None:
    return resume_terminal_decision(
        reaction_dir=ctx.reaction_dir,
        selected_inp=ctx.selected_inp,
        state=ctx.state,
        resumed=ctx.resumed,
        max_retries=ctx.max_retries,
        last_out_path_from_state=_last_out_path_from_state,
        exit_with_result=_exit_with_result,
        emit=ctx.emit,
        notify_finished=ctx.notify_finished,
    )


def _resolve_current_attempt_input(
    ctx: AttemptRunContext,
    loop: AttemptLoopState,
) -> tuple[Path | None, str | None]:
    return resolve_execution_input(
        reaction_dir=ctx.reaction_dir,
        selected_inp=ctx.selected_inp,
        state=ctx.state,
        execution_index=loop.execution_index,
        retries_used=loop.retries_used,
        retry_inp_path=ctx.retry_inp_path,
        retry_recipe_step=_retry_recipe_step,
        to_resolved_local=ctx.to_resolved_local,
        save_state=save_state,
    )


def _finish_missing_attempt_input(
    ctx: AttemptRunContext,
    loop: AttemptLoopState,
    *,
    missing_reason: str | None,
) -> int:
    return _finish_attempt(
        ctx,
        status=RunStatus.FAILED,
        analyzer_status=AnalyzerStatus.INCOMPLETE,
        reason=missing_reason or f"missing_input_for_attempt_{loop.execution_index}",
        last_out_path=None,
        exit_code=1,
    )


def _finish_attempt_exception(
    ctx: AttemptRunContext,
    loop: AttemptLoopState,
    current_inp: Path,
    exc: BaseException,
) -> int:
    if isinstance(exc, WorkerShutdownInterrupt):
        logger.warning("Interrupted by worker shutdown during attempt %d", loop.execution_index)
        return _finish_attempt(
            ctx,
            status=RunStatus.FAILED,
            analyzer_status=AnalyzerStatus.INCOMPLETE,
            reason="worker_shutdown",
            last_out_path=str(current_inp.with_suffix(".out")),
            exit_code=143,
        )
    if isinstance(exc, KeyboardInterrupt):
        logger.warning("Interrupted by user during attempt %d", loop.execution_index)
        return _finish_attempt(
            ctx,
            status=RunStatus.FAILED,
            analyzer_status=AnalyzerStatus.INCOMPLETE,
            reason="interrupted_by_user",
            last_out_path=str(current_inp.with_suffix(".out")),
            exit_code=130,
        )

    logger.exception("ORCA runner crashed during attempt %d: %s", loop.execution_index, exc)
    return _finish_attempt(
        ctx,
        status=RunStatus.FAILED,
        analyzer_status=AnalyzerStatus.INCOMPLETE,
        reason="runner_exception",
        last_out_path=str(current_inp.with_suffix(".out")),
        exit_code=1,
        extra={"runner_error": str(exc)},
    )


def _mark_and_notify_attempt_started(
    ctx: AttemptRunContext,
    loop: AttemptLoopState,
    current_inp: Path,
) -> str:
    started_at, current_status = _mark_attempt_started(
        ctx.reaction_dir,
        ctx.state,
        retries_used=loop.retries_used,
    )
    _notify_attempt_started_from_context(
        AttemptStartNotificationContext(
            run=ctx,
            current_inp=current_inp,
            execution_index=loop.execution_index,
            first_execution_index=loop.first_execution_index,
            current_status=current_status,
            started_at=started_at,
        )
    )
    return started_at


def _run_attempt_cycle(ctx: AttemptRunContext, loop: AttemptLoopState) -> int | None:
    if loop.retries_used > ctx.max_retries:
        return _finish_attempt(
            ctx,
            status=RunStatus.FAILED,
            analyzer_status=AnalyzerStatus.INCOMPLETE,
            reason="retry_limit_reached",
            last_out_path=_last_out_path_from_state(ctx.state),
            exit_code=1,
        )

    current_inp, missing_reason = _resolve_current_attempt_input(ctx, loop)
    if current_inp is None:
        return _finish_missing_attempt_input(ctx, loop, missing_reason=missing_reason)

    started_at = _mark_and_notify_attempt_started(ctx, loop, current_inp)
    try:
        out_path, analysis = _run_and_record_attempt(
            ctx.reaction_dir,
            ctx.state,
            current_inp=current_inp,
            execution_index=loop.execution_index,
            started_at=started_at,
            runner=ctx.runner,
        )
    except (WorkerShutdownInterrupt, KeyboardInterrupt, Exception) as exc:
        return _finish_attempt_exception(ctx, loop, current_inp, exc)

    decision = decide_attempt_outcome(
        analyzer_status=analysis.status,
        analyzer_reason=analysis.reason,
        retries_used=loop.retries_used,
        max_retries=ctx.max_retries,
    )
    if decision is not None:
        return _finish_attempt(
            ctx,
            status=decision.run_status,
            analyzer_status=analysis.status,
            reason=decision.reason,
            last_out_path=str(out_path),
            exit_code=decision.exit_code,
        )

    retry_exit = _prepare_retry_attempt_from_context(
        RetryPreparationContext(
            run=ctx,
            current_inp=current_inp,
            out_path=out_path,
            execution_index=loop.execution_index,
            retries_used=loop.retries_used,
            analysis=analysis,
        )
    )
    if retry_exit is not None:
        return retry_exit
    loop.advance()
    return None


def _prepare_retry_attempt(
    reaction_dir: Path,
    state: RunState,
    selected_inp: Path,
    *,
    current_inp: Path,
    out_path: Path,
    execution_index: int,
    retries_used: int,
    max_retries: int,
    resumed: bool,
    analysis: OutAnalysis,
    retry_inp_path: Callable[[Path, int], Path],
    emit: Callable[[Dict[str, Any]], None],
    notify_finished: Callable[[RunFinishedNotification], None] | None,
    notify_retry: Callable[[RetryNotification], None] | None,
) -> int | None:
    return _prepare_retry_attempt_from_context(
        RetryPreparationContext(
            run=AttemptRunContext(
                reaction_dir=reaction_dir,
                selected_inp=selected_inp,
                state=state,
                resumed=resumed,
                runner=_NoRunnerForNotification(),
                max_retries=max_retries,
                retry_inp_path=retry_inp_path,
                to_resolved_local=_missing_to_resolved_local,
                emit=emit,
                notify_started=None,
                notify_finished=notify_finished,
                notify_retry=notify_retry,
            ),
            current_inp=current_inp,
            out_path=out_path,
            execution_index=execution_index,
            retries_used=retries_used,
            analysis=analysis,
        )
    )


def _prepare_retry_attempt_from_context(ctx: RetryPreparationContext) -> int | None:
    next_retry_number = ctx.retries_used + 1
    next_inp = ctx.run.retry_inp_path(ctx.run.selected_inp, next_retry_number)
    patch_step = _retry_recipe_step(next_retry_number)
    try:
        patch_actions = rewrite_for_retry(
            source_inp=ctx.current_inp,
            target_inp=next_inp,
            reaction_dir=ctx.run.reaction_dir,
            step=patch_step,
        )
    except Exception as exc:
        ctx.run.state["attempts"][-1]["patch_actions"] = [f"rewrite_failed:{exc}"]
        return _exit_with_result(
            ctx.run.reaction_dir,
            ctx.run.state,
            ctx.run.selected_inp,
            status=RunStatus.FAILED,
            analyzer_status=ctx.analysis.status,
            reason="rewrite_failed",
            last_out_path=str(ctx.out_path),
            resumed=ctx.run.resumed,
            exit_code=1,
            emit=ctx.run.emit,
            notify_finished=ctx.run.notify_finished,
        )

    ctx.run.state["attempts"][-1]["patch_actions"] = patch_actions
    save_state(ctx.run.reaction_dir, ctx.run.state)
    if ctx.run.notify_retry is None:
        return None

    retry_notification = build_retry_notification(
        reaction_dir=ctx.run.reaction_dir,
        selected_inp=ctx.run.selected_inp,
        current_inp=ctx.current_inp,
        out_path=ctx.out_path,
        next_inp=next_inp,
        execution_index=ctx.execution_index,
        next_retry_number=next_retry_number,
        max_retries=ctx.run.max_retries,
        analysis_status=ctx.analysis.status,
        analysis_reason=ctx.analysis.reason,
        patch_actions=patch_actions,
        resumed=ctx.run.resumed,
    )
    try:
        ctx.run.notify_retry(retry_notification)
    except Exception:
        logger.warning(
            "Retry notification callback failed for attempt %d",
            ctx.execution_index,
            exc_info=True,
        )
    return None


def run_attempts(
    reaction_dir: Path,
    selected_inp: Path,
    state: RunState,
    *,
    resumed: bool,
    runner: RunnerLike,
    max_retries: int,
    retry_inp_path: Callable[[Path, int], Path],
    to_resolved_local: Callable[[str], Path],
    emit: Callable[[Dict[str, Any]], None],
    notify_started: Callable[[RunStartedNotification], None] | None = None,
    notify_finished: Callable[[RunFinishedNotification], None] | None = None,
    notify_retry: Callable[[RetryNotification], None] | None = None,
) -> int:
    ctx = AttemptRunContext(
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        state=state,
        resumed=resumed,
        runner=runner,
        max_retries=max_retries,
        retry_inp_path=retry_inp_path,
        to_resolved_local=to_resolved_local,
        emit=emit,
        notify_started=notify_started,
        notify_finished=notify_finished,
        notify_retry=notify_retry,
    )
    resumed_exit = _resume_attempts_if_terminal(ctx)
    if resumed_exit is not None:
        return resumed_exit

    loop = AttemptLoopState(
        execution_index=len(ctx.state["attempts"]) + 1,
        first_execution_index=len(ctx.state["attempts"]) + 1,
    )
    while True:
        cycle_exit = _run_attempt_cycle(ctx, loop)
        if cycle_exit is not None:
            return cycle_exit
