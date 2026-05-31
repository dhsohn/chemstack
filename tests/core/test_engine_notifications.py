from __future__ import annotations

from typing import Any

from chemstack.core.notifications.engine_module import (
    build_engine_job_notifications,
    build_engine_notification_module,
)


def _send_ok(_cfg: Any, _lines: list[str]) -> bool:
    return True


def test_engine_notification_module_reuses_delivery_helper() -> None:
    notifications = build_engine_notification_module(
        label="xTB",
        engine="xtb",
        selected_field_name="selected_xyz",
        detail_field_names=("mode", "molecule_key"),
        terminal_count_field="attempt_count",
        send_fn=_send_ok,
    )

    assert notifications.delivery is notifications.delivery


def test_engine_job_notifications_reuses_request_factory() -> None:
    notifications = build_engine_job_notifications(
        label="CREST",
        engine="crest",
        selected_field_name="selected_xyz",
        detail_field_names=("mode", "molecule_key"),
        terminal_count_field="attempt_count",
        send_fn=_send_ok,
    )

    assert notifications.request_factory is notifications.request_factory
