from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "crest_auto"
_ENGINE = "crest"


def _is_workflow_child(job_dir: Path) -> bool:
    return _engine_notifications.is_workflow_child(job_dir, engine=_ENGINE)


def _send(cfg: AppConfig, lines: list[str]) -> bool:
    return _engine_notifications.send_lines(cfg, lines, build_transport=build_telegram_transport)


def notify_job_queued(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    return _engine_notifications.send_job_event(
        cfg,
        label=_LABEL,
        engine=_ENGINE,
        job_dir=job_dir,
        headline="Job queued",
        fields=[
            ("job_id", job_id),
            ("queue_id", queue_id),
            ("mode", mode),
            ("job_dir", job_dir.name),
            ("selected_xyz", selected_xyz.name),
        ],
        send_fn=_send,
    )


def notify_job_started(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    return _engine_notifications.send_job_event(
        cfg,
        label=_LABEL,
        engine=_ENGINE,
        job_dir=job_dir,
        headline="Job started",
        fields=[
            ("job_id", job_id),
            ("queue_id", queue_id),
            ("mode", mode),
            ("job_dir", job_dir.name),
            ("selected_xyz", selected_xyz.name),
        ],
        send_fn=_send,
    )


def notify_job_terminal(
    cfg: AppConfig,
    *,
    headline: str,
    job_id: str,
    queue_id: str,
    status: str,
    reason: str,
    mode: str,
    job_dir: Path,
    selected_xyz: Path,
    retained_conformer_count: int,
    extra_lines: list[str] | None = None,
) -> bool:
    return _engine_notifications.send_job_event(
        cfg,
        label=_LABEL,
        engine=_ENGINE,
        job_dir=job_dir,
        headline=headline,
        fields=[
            ("job_id", job_id),
            ("queue_id", queue_id),
            ("status", status),
            ("reason", reason),
            ("mode", mode),
            ("job_dir", job_dir.name),
            ("selected_xyz", selected_xyz.name),
            ("retained_conformer_count", retained_conformer_count),
        ],
        send_fn=_send,
        extra_lines=extra_lines,
    )


def notify_job_finished(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    status: str,
    reason: str,
    mode: str,
    job_dir: Path,
    selected_xyz: Path,
    retained_conformer_count: int,
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
        mode=mode,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        retained_conformer_count=retained_conformer_count,
        extra_lines=extra_lines or None,
    )


def notify_organize_summary(
    cfg: AppConfig,
    *,
    organized_count: int,
    skipped_count: int,
    root: Path,
) -> bool:
    return _engine_notifications.send_organize_summary(
        cfg,
        label=_LABEL,
        organized_count=organized_count,
        skipped_count=skipped_count,
        root=root,
        send_fn=_send,
    )
