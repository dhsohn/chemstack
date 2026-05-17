from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "xtb_auto"
_ENGINE = "xtb"


def _is_workflow_child(job_dir: Path) -> bool:
    return _engine_notifications.is_workflow_child(job_dir, engine=_ENGINE)


_send = _engine_notifications.telegram_line_sender(lambda: build_telegram_transport)
_NOTIFIER = _engine_notifications.build_engine_notifier(
    label=_LABEL,
    engine=_ENGINE,
    send_fn=_send,
)
_SELECTED_XYZ_FIELD = "selected_input_xyz"


def notify_job_queued(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    job_type: str,
    reaction_key: str,
    selected_xyz: Path,
) -> bool:
    return _engine_notifications.send_lifecycle_event(
        _NOTIFIER,
        cfg,
        headline="Job queued",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        selected_field_name=_SELECTED_XYZ_FIELD,
        detail_fields=[("job_type", job_type), ("reaction_key", reaction_key)],
    )


def notify_job_started(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    job_type: str,
    reaction_key: str,
    selected_xyz: Path,
) -> bool:
    return _engine_notifications.send_lifecycle_event(
        _NOTIFIER,
        cfg,
        headline="Job started",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        selected_field_name=_SELECTED_XYZ_FIELD,
        detail_fields=[("job_type", job_type), ("reaction_key", reaction_key)],
    )


def notify_job_terminal(
    cfg: AppConfig,
    *,
    headline: str,
    job_id: str,
    queue_id: str,
    status: str,
    reason: str,
    job_type: str,
    reaction_key: str,
    job_dir: Path,
    selected_xyz: Path,
    candidate_count: int,
    extra_lines: list[str] | None = None,
) -> bool:
    return _engine_notifications.send_terminal_event(
        _NOTIFIER,
        cfg,
        headline=headline,
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        selected_field_name=_SELECTED_XYZ_FIELD,
        detail_fields=[("job_type", job_type), ("reaction_key", reaction_key)],
        count_field=("candidate_count", candidate_count),
        extra_lines=extra_lines,
    )


def notify_job_finished(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    status: str,
    reason: str,
    job_type: str,
    reaction_key: str,
    job_dir: Path,
    selected_xyz: Path,
    candidate_count: int,
    organized_output_dir: Path | None = None,
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> bool:
    extra_lines = _engine_notifications.optional_terminal_lines(
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
    return notify_job_terminal(
        cfg,
        headline=_engine_notifications.terminal_headline(status),
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_type=job_type,
        reaction_key=reaction_key,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        candidate_count=candidate_count,
        extra_lines=extra_lines or None,
    )


def notify_organize_summary(
    cfg: AppConfig,
    *,
    organized_count: int,
    skipped_count: int,
    root: Path,
) -> bool:
    return _NOTIFIER.send_organize_summary(
        cfg,
        organized_count=organized_count,
        skipped_count=skipped_count,
        root=root,
    )
