from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Type


def notification_callbacks(cfg: Any, *, deps: Any) -> tuple[Any, Any, Any]:
    if not cfg.telegram.enabled:
        return None, None, None
    notifications = deps.notifications

    def notify_started(event: Any) -> None:
        notifications.notify_run_started_event(cfg.telegram, event)

    def notify_finished(event: Any) -> None:
        notifications.notify_run_finished_event(cfg.telegram, event)

    def notify_retry(event: Any) -> None:
        notifications.notify_retry_event(cfg.telegram, event)

    return notify_started, notify_finished, notify_retry


def run_with_state(
    *,
    cfg: Any,
    reaction_dir: Path,
    selected_inp: Path,
    runner_cls: Type[Any],
    max_retries: int,
    resumed: bool,
    state: Any,
    deps: Any,
) -> int:
    execution = deps.execution
    notify_started, notify_finished, notify_retry = execution._notification_callbacks(cfg)
    runner = runner_cls(cfg.paths.orca_executable)
    return execution.run_attempts(
        reaction_dir,
        selected_inp,
        state,
        resumed=resumed,
        runner=runner,
        max_retries=max_retries,
        retry_inp_path=execution._retry_inp_path,
        to_resolved_local=execution._to_resolved_local,
        emit=execution._emit,
        notify_started=notify_started,
        notify_finished=notify_finished,
        notify_retry=notify_retry,
    )


def existing_completed_exit(
    *,
    reaction_dir: Path,
    selected_inp: Path,
    admission_root: Path,
    reservation_token: str | None,
    max_retries: int,
    deps: Any,
) -> int | None:
    execution = deps.execution
    statuses = deps.statuses
    done = execution._existing_completed_out(selected_inp)
    if done is None:
        return None

    if reservation_token is not None:
        execution.release_slot(admission_root, reservation_token)
    state, resumed = execution.load_or_create_state(
        reaction_dir,
        selected_inp,
        max_retries=max_retries,
        to_resolved_local=execution._to_resolved_local,
    )
    return execution._exit_with_result(
        reaction_dir,
        state,
        selected_inp,
        status=statuses.RunStatus.COMPLETED,
        analyzer_status=statuses.AnalyzerStatus.COMPLETED,
        reason="existing_out_completed",
        last_out_path=done["out_path"],
        resumed=True if resumed else None,
        exit_code=0,
        emit=execution._emit,
        extra={"skipped_execution": True},
    )


def execute_locked_run(
    args: Any,
    context: Any,
    *,
    runner_cls: Type[Any],
    deps: Any,
) -> int:
    execution = deps.execution
    with execution.acquire_run_lock(context.reaction_dir):
        if not getattr(args, "force", False):
            existing_exit = execution._existing_completed_exit(
                reaction_dir=context.reaction_dir,
                selected_inp=context.selected_inp,
                admission_root=context.admission_root,
                reservation_token=context.reservation_token,
                max_retries=context.max_retries,
            )
            if existing_exit is not None:
                return existing_exit

        with execution._admission_context(
            admission_root=context.admission_root,
            reaction_dir=context.reaction_dir,
            admission_limit=context.admission_limit,
            reservation_token=context.reservation_token,
            admission_app_name=context.admission_app_name,
            admission_task_id=context.admission_task_id,
        ):
            state, resumed = execution.load_or_create_state(
                context.reaction_dir,
                context.selected_inp,
                max_retries=context.max_retries,
                to_resolved_local=execution._to_resolved_local,
            )
            if context.admission_task_id and state.get("job_id") != context.admission_task_id:
                state["job_id"] = context.admission_task_id
                execution.save_state(context.reaction_dir, state)
            return execution._run_with_state(
                cfg=context.cfg,
                reaction_dir=context.reaction_dir,
                selected_inp=context.selected_inp,
                runner_cls=runner_cls,
                max_retries=context.max_retries,
                resumed=resumed,
                state=state,
            )


def cmd_run_inp_execute(
    args: Any,
    *,
    runner_cls: Type[Any],
    cfg: Any | None,
    reaction_dir: Path | None,
    selected_inp: Path | None,
    reservation_token: str | None,
    admission_app_name: str | None,
    admission_task_id: str | None,
    deps: Any,
    logger: logging.Logger,
) -> int:
    execution = deps.execution
    statuses = deps.statuses
    context = execution._resolve_execution_context(
        args,
        cfg=cfg,
        reaction_dir=reaction_dir,
        selected_inp=selected_inp,
        reservation_token=reservation_token,
        admission_app_name=admission_app_name,
        admission_task_id=admission_task_id,
    )
    if context is None:
        return 1

    logger.info("Selected input: %s", context.selected_inp)
    execution._recover_crashed_state(context.reaction_dir)

    try:
        return execution._execute_locked_run(args, context, runner_cls=runner_cls)
    except statuses.AdmissionLimitReachedError as exc:
        execution._release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.error("%s", exc)
        return 1
    except RuntimeError as exc:
        execution._release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        execution._release_reservation_if_needed(context.admission_root, context.reservation_token)
        logger.exception("Unexpected error while running input: %s", exc)
        return 1
