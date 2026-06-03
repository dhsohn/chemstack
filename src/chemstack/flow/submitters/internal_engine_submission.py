from __future__ import annotations

from argparse import Namespace
from collections.abc import Callable
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
    command_trace = internal_call_argv(
        api_name=api_name,
        config_path=config_path,
        kwargs={"job_dir": job_dir, "priority": int(priority)},
    )
    args = Namespace(config=config_path, path=job_dir, priority=int(priority))
    submission = None
    entry = None
    resolved_job_dir = job_dir
    try:
        cfg = load_config_fn(config_path)
        resolved_job_dir = resolve_job_dir_fn(cfg, job_dir)
        manifest = load_manifest_fn(resolved_job_dir)
        submission = build_submission_fn(cfg, resolved_job_dir, manifest, args)
        entry = enqueue_fn(
            submission.queue_root,
            app_name=submission.app_name,
            task_id=submission.task_id,
            task_kind=submission.task_kind,
            engine=submission.engine,
            priority=submission.priority,
            metadata=dict(submission.metadata),
        )
        record_queued_fn(cfg, submission, entry)
    except DuplicateQueueEntryError as exc:
        extras = extra_fields_fn(submission, entry) if extra_fields_fn is not None else {}
        return _submission_failure_payload(
            command_trace=command_trace,
            job_dir=normalize_text(resolved_job_dir) or job_dir,
            stderr=_stderr_with_exception("", exc),
            extra_fields=extras,
        )
    except Exception as exc:  # noqa: BLE001
        extras = extra_fields_fn(submission, entry) if extra_fields_fn is not None else {}
        return _submission_failure_payload(
            command_trace=command_trace,
            job_dir=normalize_text(resolved_job_dir) or job_dir,
            stderr=_stderr_with_exception("", exc),
            extra_fields=extras,
        )

    extras = extra_fields_fn(submission, entry) if extra_fields_fn is not None else {}
    parsed = _text_fields(
        {
            "status": STATUS_QUEUED,
            "job_dir": resolved_job_dir,
            "job_id": getattr(entry, "task_id", "") or submission.task_id,
            "queue_id": getattr(entry, "queue_id", ""),
            "priority": getattr(entry, "priority", submission.priority),
            **extras,
        }
    )
    return _submission_success_payload(
        command_trace=command_trace,
        parsed=parsed,
        job_dir=normalize_text(resolved_job_dir) or job_dir,
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
