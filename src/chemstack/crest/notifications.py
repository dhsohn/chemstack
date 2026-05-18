from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "crest_auto"
_ENGINE = "crest"


_send = _engine_notifications.telegram_line_sender(lambda: build_telegram_transport)
_NOTIFICATIONS = _engine_notifications.build_engine_notification_module(
    label=_LABEL,
    engine=_ENGINE,
    selected_field_name="selected_xyz",
    detail_field_names=("mode",),
    terminal_count_field="retained_conformer_count",
    send_fn=_send,
)


def _detail_values(mode: str) -> dict[str, object]:
    return {"mode": mode}


def notify_job_queued(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    return _NOTIFICATIONS.notify_lifecycle(
        cfg,
        headline="Job queued",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        detail_values=_detail_values(mode),
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
    return _NOTIFICATIONS.notify_lifecycle(
        cfg,
        headline="Job started",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        detail_values=_detail_values(mode),
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
    return _NOTIFICATIONS.notify_terminal(
        cfg,
        headline=headline,
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        count_value=retained_conformer_count,
        detail_values=_detail_values(mode),
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
    return _NOTIFICATIONS.notify_finished(
        cfg,
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        count_value=retained_conformer_count,
        detail_values=_detail_values(mode),
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
