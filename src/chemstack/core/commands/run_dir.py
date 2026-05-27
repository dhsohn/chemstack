from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.paths import validate_job_dir
from chemstack.core.paths.workflow import workflow_workspace_internal_engine_paths_from_path
from chemstack.core.queue import DuplicateQueueEntryError, enqueue


@dataclass(frozen=True)
class EngineRunDirSubmission:
    queue_root: Path
    app_name: str
    task_id: str
    task_kind: str
    engine: str
    priority: int
    metadata: dict[str, Any]
    context: dict[str, Any]


@dataclass(frozen=True)
class EngineQueuedRecord:
    state_payload: dict[str, Any]
    index_fields: dict[str, Any]
    notification_fields: dict[str, Any]


def load_yaml_job_manifest(
    job_dir: Path,
    filename: str,
    *,
    missing_message: str | None = None,
    invalid_message: str,
) -> dict[str, Any]:
    path = job_dir / filename
    if not path.exists():
        if missing_message is None:
            return {}
        raise ValueError(missing_message.format(path=path))

    with path.open("r", encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle) or {}
    if not isinstance(parsed, dict):
        raise ValueError(invalid_message.format(path=path))
    return parsed


def resolve_engine_job_dir(
    cfg: Any,
    raw_job_dir: str,
    *,
    engine: str,
    workflow_error_message: str,
    validate_job_dir_fn: Any = validate_job_dir,
    workflow_paths_from_path_fn: Any = workflow_workspace_internal_engine_paths_from_path,
) -> Path:
    candidate = Path(raw_job_dir).expanduser().resolve()
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if workflow_root:
        runtime_paths = workflow_paths_from_path_fn(
            candidate,
            workflow_root=workflow_root,
            engine=engine,
        )
        if runtime_paths is None:
            raise ValueError(workflow_error_message)
        return validate_job_dir_fn(
            raw_job_dir,
            str(runtime_paths["allowed_root"]),
            label="Job directory",
        )
    return validate_job_dir_fn(raw_job_dir, cfg.runtime.allowed_root, label="Job directory")


def print_queued_common(
    submission: EngineRunDirSubmission,
    entry: Any,
    *,
    job_dir: Path,
    extra_fields: Iterable[tuple[str, Any]] | None = None,
) -> None:
    print("status: queued")
    print(f"job_dir: {job_dir}")
    print(f"job_id: {submission.task_id}")
    print(f"queue_id: {entry.queue_id}")
    print(f"priority: {entry.priority}")
    for key, value in extra_fields or ():
        print(f"{key}: {value}")


def record_queued_common(
    cfg: Any,
    submission: EngineRunDirSubmission,
    entry: Any,
    *,
    build_record_fn: Callable[[EngineRunDirSubmission, Any], EngineQueuedRecord],
    write_state_fn: Callable[[Path, dict[str, Any]], Any],
    upsert_job_record_fn: Callable[..., Any],
    notify_job_queued_fn: Callable[..., Any],
) -> None:
    job_dir = submission.context["job_dir"]
    record = build_record_fn(submission, entry)
    write_state_fn(job_dir, record.state_payload)
    upsert_job_record_fn(
        cfg,
        job_id=submission.task_id,
        status="queued",
        job_dir=job_dir,
        **record.index_fields,
    )
    notify_job_queued_fn(
        cfg,
        job_id=submission.task_id,
        queue_id=entry.queue_id,
        job_dir=job_dir,
        **record.notification_fields,
    )


def cmd_engine_run_dir_from_module_globals(args: Any, module_globals: Mapping[str, Any]) -> int:
    return cmd_engine_run_dir(
        args,
        load_config_fn=module_globals["load_config"],
        resolve_job_dir_fn=module_globals["resolve_job_dir"],
        load_manifest_fn=module_globals["load_job_manifest"],
        build_submission_fn=module_globals["_build_submission"],
        record_queued_fn=module_globals["_record_queued"],
        print_queued_fn=module_globals["_print_queued"],
        enqueue_fn=module_globals["enqueue"],
    )


def cmd_engine_run_dir(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    resolve_job_dir_fn: Callable[[Any, str], Path],
    load_manifest_fn: Callable[[Path], dict[str, Any]],
    build_submission_fn: Callable[[Any, Path, dict[str, Any], Any], EngineRunDirSubmission],
    record_queued_fn: Callable[[Any, EngineRunDirSubmission, Any], None],
    print_queued_fn: Callable[[EngineRunDirSubmission, Any], None],
    enqueue_fn: Callable[..., Any] = enqueue,
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    raw_job_dir = getattr(args, "path", None)
    if not isinstance(raw_job_dir, str) or not raw_job_dir.strip():
        raise ValueError("job directory path is required")

    job_dir = resolve_job_dir_fn(cfg, raw_job_dir)
    manifest = load_manifest_fn(job_dir)
    submission = build_submission_fn(cfg, job_dir, manifest, args)
    try:
        entry = enqueue_fn(
            submission.queue_root,
            app_name=submission.app_name,
            task_id=submission.task_id,
            task_kind=submission.task_kind,
            engine=submission.engine,
            priority=submission.priority,
            metadata=dict(submission.metadata),
        )
    except DuplicateQueueEntryError as exc:
        print(f"error: {exc}")
        return 1

    record_queued_fn(cfg, submission, entry)
    print_queued_fn(submission, entry)
    return 0
