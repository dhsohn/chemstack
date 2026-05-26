from __future__ import annotations

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

from .config import AppConfig

_LABEL = "chemstack_xtb"
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
    **values: object,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_queued(cfg, values)


def notify_job_started(
    cfg: AppConfig,
    **values: object,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_started(cfg, values)


def notify_job_terminal(
    cfg: AppConfig,
    **values: object,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_terminal(cfg, values)


def notify_job_finished(
    cfg: AppConfig,
    **values: object,
) -> bool:
    return _JOB_NOTIFICATIONS.notify_job_finished(cfg, values)
