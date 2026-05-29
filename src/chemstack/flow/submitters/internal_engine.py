from __future__ import annotations

from argparse import Namespace
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from chemstack.core.queue import DuplicateQueueEntryError

from chemstack.core.utils import normalize_text


@dataclass(frozen=True)
class InternalEngineSubmitterSpec:
    run_dir_api_name: str
    cancel_api_name: str
    extra_fields_fn: Callable[[Any | None, Any | None], dict[str, Any]] | None = None


@dataclass(frozen=True)
class InternalEngineSubmitterDeps:
    load_config_fn: Callable[[Any], Any]
    resolve_job_dir_fn: Callable[[Any, str], Any]
    load_manifest_fn: Callable[[Any], dict[str, Any]]
    build_submission_fn: Callable[[Any, Any, dict[str, Any], Any], Any]
    record_queued_fn: Callable[[Any, Any, Any], Any]
    enqueue_fn: Callable[..., Any]
    load_queue_config_fn: Callable[[Any], Any]
    queue_entries_with_roots_fn: Callable[[Any], list[tuple[Any, Any]]]
    request_cancel_fn: Callable[[Any, str], Any | None]
    display_status_fn: Callable[[Any], str]


def internal_call_argv(
    *,
    api_name: str,
    config_path: str,
    kwargs: dict[str, Any],
) -> list[str]:
    return [
        api_name,
        f"config={config_path}",
        *[f"{key}={value}" for key, value in kwargs.items()],
    ]


def _stderr_with_exception(stderr: str, exc: Exception) -> str:
    pieces = [stderr] if stderr else []
    if pieces and not pieces[-1].endswith("\n"):
        pieces[-1] += "\n"
    pieces.append(f"{exc.__class__.__name__}: {exc}\n")
    return "".join(pieces)


def transient_submission_block_reason(
    *, parsed_stdout: dict[str, str], stdout: str, stderr: str
) -> str:
    parsed_status = normalize_text(parsed_stdout.get("status")).lower()
    if parsed_status in {"waiting_for_slot", "admission_blocked", "admission_limit_reached"}:
        return parsed_status

    combined = f"{stdout}\n{stderr}".lower()
    if parsed_status == "blocked" and any(
        token in combined for token in ("admission", "slot", "limit")
    ):
        return "waiting_for_slot"

    patterns = (
        ("admission limit reached", "admission_limit_reached"),
        ("admission slots are full", "admission_limit_reached"),
        ("waiting_for_slot", "waiting_for_slot"),
        ("waiting for slot", "waiting_for_slot"),
        ("no admission slot", "waiting_for_slot"),
        ("active simulation limit", "admission_limit_reached"),
        ("max_active_simulations", "admission_limit_reached"),
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
    if int(returncode) == 0 and normalize_text(parsed_stdout.get("status")).lower() == "queued":
        return "submitted", ""
    blocked_reason = transient_submission_block_reason(
        parsed_stdout=parsed_stdout,
        stdout=stdout,
        stderr=stderr,
    )
    if blocked_reason:
        return "blocked", blocked_reason
    return "failed", ""


def _key_value_stdout(fields: dict[str, str]) -> str:
    return "\n".join(f"{key}: {value}" for key, value in fields.items() if value)


def _text_fields(fields: dict[str, Any]) -> dict[str, str]:
    return {key: text for key, value in fields.items() if (text := normalize_text(value))}


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
    payload = {
        "status": status,
        "reason": reason,
        "returncode": 1,
        "command_argv": command_trace,
        "stdout": "",
        "stderr": stderr,
        "parsed_stdout": parsed,
        "job_id": "",
        "queue_id": "",
        "job_dir": job_dir,
    }
    if extra_fields:
        payload.update(extra_fields)
    return payload


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
    except Exception as exc:
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
            "status": "queued",
            "job_dir": resolved_job_dir,
            "job_id": getattr(entry, "task_id", "") or submission.task_id,
            "queue_id": getattr(entry, "queue_id", ""),
            "priority": getattr(entry, "priority", submission.priority),
            **extras,
        }
    )
    return {
        "status": "submitted",
        "reason": "",
        "returncode": 0,
        "command_argv": command_trace,
        "stdout": _key_value_stdout(parsed),
        "stderr": "",
        "parsed_stdout": parsed,
        "job_id": parsed.get("job_id", ""),
        "queue_id": parsed.get("queue_id", ""),
        "job_dir": parsed.get("job_dir", normalize_text(resolved_job_dir) or job_dir),
        **extras,
    }


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


def _queue_entry_status_text(entry: Any) -> str:
    status_value = getattr(getattr(entry, "status", None), "value", None)
    return normalize_text(status_value or getattr(entry, "status", ""))


def _direct_cancel_status(entry: Any, displayed_status: str) -> str:
    normalized_display = normalize_text(displayed_status).lower()
    if normalized_display == "cancel_requested":
        return "cancel_requested"
    if normalized_display == "cancelled":
        return "cancelled"
    if _queue_entry_status_text(entry).lower() == "running" and getattr(
        entry, "cancel_requested", False
    ):
        return "cancel_requested"
    return "cancelled"


def _cancel_failure_payload(
    *,
    command_trace: list[str],
    stderr: str,
) -> dict[str, Any]:
    return {
        "status": "failed",
        "reason": "cancel_command_failed",
        "returncode": 1,
        "command_argv": command_trace,
        "stdout": "",
        "stderr": stderr,
        "parsed_stdout": {},
        "queue_id": "",
        "job_id": "",
    }


def cancel_internal_engine_target(
    *,
    load_config_fn: Callable[[Any], Any],
    queue_entries_with_roots_fn: Callable[[Any], list[tuple[Any, Any]]],
    request_cancel_fn: Callable[[Any, str], Any | None],
    display_status_fn: Callable[[Any], str],
    api_name: str,
    target: str,
    config_path: str,
) -> dict[str, Any]:
    command_trace = internal_call_argv(
        api_name=api_name,
        config_path=config_path,
        kwargs={"target": target},
    )
    normalized_target = normalize_text(target)
    if not normalized_target:
        return _cancel_failure_payload(
            command_trace=command_trace,
            stderr="queue cancel requires a queue_id or job_id\n",
        )

    try:
        cfg = load_config_fn(config_path)
        entry_with_root = None
        for queue_root, entry in queue_entries_with_roots_fn(cfg):
            if entry.queue_id == normalized_target or entry.task_id == normalized_target:
                entry_with_root = (queue_root, entry)
                break
        if entry_with_root is None:
            return _cancel_failure_payload(
                command_trace=command_trace,
                stderr=f"queue target not found: {normalized_target}\n",
            )

        queue_root, entry = entry_with_root
        updated = request_cancel_fn(queue_root, entry.queue_id)
        if updated is None:
            return _cancel_failure_payload(
                command_trace=command_trace,
                stderr=f"queue target already terminal: {normalized_target}\n",
            )
        status = _direct_cancel_status(updated, display_status_fn(updated))
    except Exception as exc:
        return _cancel_failure_payload(
            command_trace=command_trace,
            stderr=_stderr_with_exception("", exc),
        )

    parsed = _text_fields(
        {
            "status": status,
            "queue_id": getattr(updated, "queue_id", ""),
            "job_id": getattr(updated, "task_id", ""),
        }
    )
    return {
        "status": status,
        "reason": "",
        "returncode": 0,
        "command_argv": command_trace,
        "stdout": _key_value_stdout(parsed),
        "stderr": "",
        "parsed_stdout": parsed,
        "queue_id": parsed.get("queue_id", ""),
        "job_id": parsed.get("job_id", ""),
    }


def cancel_engine_target(
    *,
    spec: InternalEngineSubmitterSpec,
    deps: InternalEngineSubmitterDeps,
    target: str,
    config_path: str,
) -> dict[str, Any]:
    return cancel_internal_engine_target(
        load_config_fn=deps.load_queue_config_fn,
        queue_entries_with_roots_fn=deps.queue_entries_with_roots_fn,
        request_cancel_fn=deps.request_cancel_fn,
        display_status_fn=deps.display_status_fn,
        api_name=spec.cancel_api_name,
        config_path=config_path,
        target=target,
    )
