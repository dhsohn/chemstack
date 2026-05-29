from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.paths import validate_job_dir
from chemstack.core.paths.workflow import workflow_workspace_internal_engine_paths_from_path


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


def engine_resource_fields(resource_request: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    payload = dict(resource_request or {})
    return {
        "resource_request": payload,
        "resource_actual": dict(payload),
    }


def manifest_present_text(manifest: dict[str, Any]) -> str:
    return "true" if manifest else "false"


def build_engine_run_dir_submission(
    *,
    queue_root: Path,
    app_name: str,
    task_id: str,
    task_kind: str,
    engine: str,
    args: Any,
    metadata: dict[str, Any],
    context: dict[str, Any],
) -> EngineRunDirSubmission:
    return EngineRunDirSubmission(
        queue_root=queue_root,
        app_name=app_name,
        task_id=task_id,
        task_kind=task_kind,
        engine=engine,
        priority=int(getattr(args, "priority", 10)),
        metadata=metadata,
        context=context,
    )


def record_queued_common_from_namespace(
    cfg: Any,
    submission: EngineRunDirSubmission,
    entry: Any,
    *,
    namespace: Mapping[str, Any],
    build_record_name: str = "_queued_record",
    write_state_name: str = "write_state",
    upsert_job_record_name: str = "upsert_job_record",
    notify_job_queued_name: str = "notify_job_queued",
) -> None:
    record_queued_common(
        cfg,
        submission,
        entry,
        build_record_fn=namespace[build_record_name],
        write_state_fn=namespace[write_state_name],
        upsert_job_record_fn=namespace[upsert_job_record_name],
        notify_job_queued_fn=namespace[notify_job_queued_name],
    )


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
