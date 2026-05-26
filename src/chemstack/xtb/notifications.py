from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "xtb_auto"
_ENGINE = "xtb"


_send = _engine_notifications.telegram_line_sender(lambda: build_telegram_transport)
_JOB_NOTIFICATIONS = _engine_notifications.build_engine_job_notifications(
    label=_LABEL,
    engine=_ENGINE,
    selected_field_name="selected_input_xyz",
    detail_field_names=("job_type", "reaction_key"),
    terminal_count_field="candidate_count",
    send_fn=_send,
)


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
    return _JOB_NOTIFICATIONS.notify_job_queued(cfg, locals())


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
    return _JOB_NOTIFICATIONS.notify_job_started(cfg, locals())


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
    return _JOB_NOTIFICATIONS.notify_job_terminal(cfg, locals())


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
    return _JOB_NOTIFICATIONS.notify_job_finished(cfg, locals())
