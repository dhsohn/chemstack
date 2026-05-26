from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "chemstack_crest"
_ENGINE = "crest"


_send = _engine_notifications.telegram_line_sender(lambda: build_telegram_transport)
_JOB_NOTIFICATIONS = _engine_notifications.build_engine_job_notifications(
    label=_LABEL,
    engine=_ENGINE,
    selected_field_name="selected_xyz",
    detail_field_names=("mode",),
    terminal_count_field="retained_conformer_count",
    send_fn=_send,
)


def notify_job_queued(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_queued(cfg, locals())


def notify_job_started(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_started(cfg, locals())


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
    return _JOB_NOTIFICATIONS.notify_job_terminal(cfg, locals())


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
    return _JOB_NOTIFICATIONS.notify_job_finished(cfg, locals())
