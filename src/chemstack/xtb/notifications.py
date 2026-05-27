from __future__ import annotations

from chemstack.core.notifications import build_telegram_transport
from chemstack.core.notifications import engines as _engine_notifications

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

notify_job_queued = _JOB_NOTIFICATIONS.notify_job_queued
notify_job_started = _JOB_NOTIFICATIONS.notify_job_started
notify_job_terminal = _JOB_NOTIFICATIONS.notify_job_terminal
notify_job_finished = _JOB_NOTIFICATIONS.notify_job_finished
