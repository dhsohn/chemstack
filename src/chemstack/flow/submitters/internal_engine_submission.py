from __future__ import annotations

from argparse import Namespace
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from chemstack.core.queue import DuplicateQueueEntryError
from chemstack.core.statuses import (
    STATUS_ADMISSION_LIMIT_REACHED,
    STATUS_BLOCKED,
    STATUS_FAILED,
    STATUS_QUEUED,
    STATUS_SUBMITTED,
    STATUS_WAITING_FOR_SLOT,
    SUBMISSION_DEFERRED_STATUSES,
)
from chemstack.core.utils import normalize_text

from .internal_engine_models import (
    InternalEngineCommandResult,
    InternalEngineSubmitterDeps,
    InternalEngineSubmitterSpec,
    _key_value_stdout,
    _stderr_with_exception,
    _text_fields,
    internal_call_argv,
)


@dataclass(frozen=True)
class _InternalEngineSubmissionRequest:
    command_trace: list[str]
    args: Namespace
    job_dir: str
    priority: int


@dataclass
class _InternalEngineSubmissionState:
    resolved_job_dir: Any
    submission: Any | None = None
    entry: Any | None = None


def transient_submission_block_reason(
    *, parsed_stdout: dict[str, str], stdout: str, stderr: str
) -> str:
    parsed_status = normalize_text(parsed_stdout.get("status")).lower()
    if parsed_status in SUBMISSION_DEFERRED_STATUSES:
        return parsed_status

    combined = f"{stdout}\n{stderr}".lower()
    if parsed_status == STATUS_BLOCKED and any(
        token in combined for token in ("admission", "slot", "limit")
    ):
        return STATUS_WAITING_FOR_SLOT

    patterns = (
        ("admission limit reached", STATUS_ADMISSION_LIMIT_REACHED),
        ("admission slots are full", STATUS_ADMISSION_LIMIT_REACHED),
        (STATUS_WAITING_FOR_SLOT, STATUS_WAITING_FOR_SLOT),
        ("waiting for slot", STATUS_WAITING_FOR_SLOT),
        ("no admission slot", STATUS_WAITING_FOR_SLOT),
        ("active simulation limit", STATUS_ADMISSION_LIMIT_REACHED),
        ("max_active_simulations", STATUS_ADMISSION_LIMIT_REACHED),
    )
    for pattern, reason in patterns:
        if pattern in combined:
            return reason
    return ""


def queue_submission_status(
    *,
    returncode: int,
    parsed_stdout: dict[str, str],
    stdout: str,
    stderr: str,
) -> tuple[str, str]:
    if (
        int(returncode) == 0
        and normalize_text(parsed_stdout.get("status")).lower() == STATUS_QUEUED
    ):
        return STATUS_SUBMITTED, ""
    blocked_reason = transient_submission_block_reason(
        parsed_stdout=parsed_stdout,
        stdout=stdout,
        stderr=stderr,
    )
    if blocked_reason:
        return STATUS_BLOCKED, blocked_reason
    return STATUS_FAILED, ""


def _submission_failure_payload(
    *,
    command_trace: list[str],
    job_dir: str,
    stderr: str,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    parsed: dict[str, str] = {}
    status, reason = queue_submission_status(
        returncode=1,
        parsed_stdout=parsed,
        stdout="",
        stderr=stderr,
    )
    return InternalEngineCommandResult(
        status=status,
        reason=reason,
        returncode=1,
        command_argv=command_trace,
        stderr=stderr,
        parsed_stdout=parsed,
        job_dir=job_dir,
        extra_fields=dict(extra_fields or {}),
    ).to_payload()


def _submission_success_payload(
    *,
    command_trace: list[str],
    parsed: dict[str, str],
    job_dir: str,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return InternalEngineCommandResult(
        status=STATUS_SUBMITTED,
        returncode=0,
        command_argv=command_trace,
        stdout=_key_value_stdout(parsed),
        parsed_stdout=parsed,
        job_id=parsed.get("job_id", ""),
        queue_id=parsed.get("queue_id", ""),
        job_dir=parsed.get("job_dir", job_dir),
        extra_fields=dict(extra_fields or {}),
    ).to_payload()


def _internal_engine_submission_request(
    *,
    api_name: str,
    job_dir: str,
    priority: int,
    config_path: str,
) -> _InternalEngineSubmissionRequest:
    priority_value = int(priority)
    return _InternalEngineSubmissionRequest(
        command_trace=internal_call_argv(
            api_name=api_name,
            config_path=config_path,
            kwargs={"job_dir": job_dir, "priority": priority_value},
        ),
        args=Namespace(config=config_path, path=job_dir, priority=priority_value),
        job_dir=job_dir,
        priority=priority_value,
    )


def _submission_extras(
    state: _InternalEngineSubmissionState,
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None,
) -> dict[str, Any]:
    return extra_fields_fn(state.submission, state.entry) if extra_fields_fn is not None else {}


def _resolved_job_dir_text(
    state: _InternalEngineSubmissionState,
    fallback_job_dir: str,
) -> str:
    return normalize_text(state.resolved_job_dir) or fallback_job_dir


def _submission_failure_for_state(
    *,
    request: _InternalEngineSubmissionRequest,
    state: _InternalEngineSubmissionState,
    stderr: str,
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None,
) -> dict[str, Any]:
    return _submission_failure_payload(
        command_trace=request.command_trace,
        job_dir=_resolved_job_dir_text(state, request.job_dir),
        stderr=stderr,
        extra_fields=_submission_extras(state, extra_fields_fn),
    )


def _enqueue_internal_engine_submission(
    *,
    request: _InternalEngineSubmissionRequest,
    state: _InternalEngineSubmissionState,
    load_config_fn: Callable[[Any], Any],
    resolve_job_dir_fn: Callable[[Any, str], Any],
    load_manifest_fn: Callable[[Any], dict[str, Any]],
    build_submission_fn: Callable[[Any, Any, dict[str, Any], Any], Any],
    record_queued_fn: Callable[[Any, Any, Any], Any],
    enqueue_fn: Callable[..., Any],
) -> None:
    cfg = load_config_fn(request.args.config)
    state.resolved_job_dir = resolve_job_dir_fn(cfg, request.job_dir)
    manifest = load_manifest_fn(state.resolved_job_dir)
    state.submission = build_submission_fn(cfg, state.resolved_job_dir, manifest, request.args)
    state.entry = enqueue_fn(
        state.submission.queue_root,
        app_name=state.submission.app_name,
        task_id=state.submission.task_id,
        task_kind=state.submission.task_kind,
        engine=state.submission.engine,
        priority=state.submission.priority,
        metadata=dict(state.submission.metadata),
    )
    record_queued_fn(cfg, state.submission, state.entry)


def _queued_submission_fields(
    *,
    state: _InternalEngineSubmissionState,
    extra_fields: dict[str, Any],
) -> dict[str, str]:
    submission = state.submission
    entry = state.entry
    if submission is None or entry is None:
        raise RuntimeError("internal engine submission did not produce a queue entry")
    return _text_fields(
        {
            "status": STATUS_QUEUED,
            "job_dir": state.resolved_job_dir,
            "job_id": getattr(entry, "task_id", "") or submission.task_id,
            "queue_id": getattr(entry, "queue_id", ""),
            "priority": getattr(entry, "priority", submission.priority),
            **extra_fields,
        }
    )


def submit_internal_engine_job_dir(
    *,
    load_config_fn: Callable[[Any], Any],
    resolve_job_dir_fn: Callable[[Any, str], Any],
    load_manifest_fn: Callable[[Any], dict[str, Any]],
    build_submission_fn: Callable[[Any, Any, dict[str, Any], Any], Any],
    record_queued_fn: Callable[[Any, Any, Any], Any],
    enqueue_fn: Callable[..., Any],
    api_name: str,
    job_dir: str,
    priority: int,
    config_path: str,
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    request = _internal_engine_submission_request(
        api_name=api_name,
        job_dir=job_dir,
        priority=priority,
        config_path=config_path,
    )
    state = _InternalEngineSubmissionState(resolved_job_dir=job_dir)
    try:
        _enqueue_internal_engine_submission(
            request=request,
            state=state,
            load_config_fn=load_config_fn,
            resolve_job_dir_fn=resolve_job_dir_fn,
            load_manifest_fn=load_manifest_fn,
            build_submission_fn=build_submission_fn,
            record_queued_fn=record_queued_fn,
            enqueue_fn=enqueue_fn,
        )
    except DuplicateQueueEntryError as exc:
        return _submission_failure_for_state(
            request=request,
            state=state,
            stderr=_stderr_with_exception("", exc),
            extra_fields_fn=extra_fields_fn,
        )
    except Exception as exc:  # noqa: BLE001
        return _submission_failure_for_state(
            request=request,
            state=state,
            stderr=_stderr_with_exception("", exc),
            extra_fields_fn=extra_fields_fn,
        )

    extras = _submission_extras(state, extra_fields_fn)
    parsed = _queued_submission_fields(state=state, extra_fields=extras)
    return _submission_success_payload(
        command_trace=request.command_trace,
        parsed=parsed,
        job_dir=_resolved_job_dir_text(state, request.job_dir),
        extra_fields=extras,
    )


def submit_engine_job_dir(
    *,
    spec: InternalEngineSubmitterSpec,
    deps: InternalEngineSubmitterDeps,
    job_dir: str,
    priority: int,
    config_path: str,
) -> dict[str, Any]:
    return submit_internal_engine_job_dir(
        load_config_fn=deps.load_config_fn,
        resolve_job_dir_fn=deps.resolve_job_dir_fn,
        load_manifest_fn=deps.load_manifest_fn,
        build_submission_fn=deps.build_submission_fn,
        record_queued_fn=deps.record_queued_fn,
        enqueue_fn=deps.enqueue_fn,
        api_name=spec.run_dir_api_name,
        config_path=config_path,
        job_dir=job_dir,
        priority=priority,
        extra_fields_fn=spec.extra_fields_fn,
    )
