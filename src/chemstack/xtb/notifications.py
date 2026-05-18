from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "xtb_auto"
_ENGINE = "xtb"


_send = _engine_notifications.telegram_line_sender(lambda: build_telegram_transport)
_NOTIFICATIONS = _engine_notifications.build_engine_notification_module(
    label=_LABEL,
    engine=_ENGINE,
    selected_field_name="selected_input_xyz",
    detail_field_names=("job_type", "reaction_key"),
    terminal_count_field="candidate_count",
    send_fn=_send,
)


def _detail_values(job_type: str, reaction_key: str) -> dict[str, object]:
    return {"job_type": job_type, "reaction_key": reaction_key}


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
    return _NOTIFICATIONS.notify_lifecycle(
        cfg,
        headline="Job queued",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        detail_values=_detail_values(job_type, reaction_key),
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
    return _NOTIFICATIONS.notify_lifecycle(
        cfg,
        headline="Job started",
        job_id=job_id,
        queue_id=queue_id,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        detail_values=_detail_values(job_type, reaction_key),
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
    return _NOTIFICATIONS.notify_terminal(
        cfg,
        headline=headline,
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        count_value=candidate_count,
        detail_values=_detail_values(job_type, reaction_key),
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
    return _NOTIFICATIONS.notify_finished(
        cfg,
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        count_value=candidate_count,
        detail_values=_detail_values(job_type, reaction_key),
        organized_output_dir=organized_output_dir,
        resource_request=resource_request,
        resource_actual=resource_actual,
    )
