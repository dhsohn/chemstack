from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .telegram import build_telegram_transport, split_telegram_message


def is_workflow_child(job_dir: Path, *, engine: str) -> bool:
    parts = tuple(part for part in job_dir.parts if part)
    return any(part.endswith(f"_{engine}") for part in parts)


def send_lines(
    cfg: Any,
    lines: list[str],
    *,
    build_transport: Callable[[Any], Any] = build_telegram_transport,
) -> bool:
    transport = build_transport(cfg.telegram)
    chunks = split_telegram_message("\n".join(lines))
    if not chunks:
        return False
    for chunk in chunks:
        result = transport.send_text(chunk)
        if not bool(result.sent or result.skipped):
            return False
    return True


def telegram_line_sender(
    build_transport_getter: Callable[[], Callable[[Any], Any]],
) -> Callable[[Any, list[str]], bool]:
    def send(cfg: Any, lines: list[str]) -> bool:
        return send_lines(cfg, lines, build_transport=build_transport_getter())

    return send


@dataclass(frozen=True)
class EngineNotifier:
    label: str
    engine: str
    send_fn: Callable[[Any, list[str]], bool]

    def send_job_event(
        self,
        cfg: Any,
        *,
        job_dir: Path,
        headline: str,
        fields: list[tuple[str, object]],
        extra_lines: list[str] | None = None,
    ) -> bool:
        return send_job_event(
            cfg,
            label=self.label,
            engine=self.engine,
            job_dir=job_dir,
            headline=headline,
            fields=fields,
            send_fn=self.send_fn,
            extra_lines=extra_lines,
        )

    def send_organize_summary(
        self,
        cfg: Any,
        *,
        organized_count: int,
        skipped_count: int,
        root: Path,
    ) -> bool:
        return send_organize_summary(
            cfg,
            label=self.label,
            organized_count=organized_count,
            skipped_count=skipped_count,
            root=root,
            send_fn=self.send_fn,
        )


def build_engine_notifier(
    *,
    label: str,
    engine: str,
    send_fn: Callable[[Any, list[str]], bool],
) -> EngineNotifier:
    return EngineNotifier(label=label, engine=engine, send_fn=send_fn)


@dataclass(frozen=True)
class EngineNotificationModule:
    notifier: EngineNotifier
    selected_field_name: str
    detail_field_names: tuple[str, ...]
    terminal_count_field: str

    def detail_fields(self, values: dict[str, object]) -> list[tuple[str, object]]:
        return [
            (field_name, values[field_name])
            for field_name in self.detail_field_names
            if field_name in values
        ]

    def notify_lifecycle(
        self,
        cfg: Any,
        *,
        headline: str,
        job_id: str,
        queue_id: str,
        job_dir: Path,
        selected_xyz: Path,
        detail_values: dict[str, object],
    ) -> bool:
        return send_lifecycle_event(
            self.notifier,
            cfg,
            headline=headline,
            job_id=job_id,
            queue_id=queue_id,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            selected_field_name=self.selected_field_name,
            detail_fields=self.detail_fields(detail_values),
        )

    def notify_terminal(
        self,
        cfg: Any,
        *,
        headline: str,
        job_id: str,
        queue_id: str,
        status: str,
        reason: str,
        job_dir: Path,
        selected_xyz: Path,
        count_value: int,
        detail_values: dict[str, object],
        extra_lines: list[str] | None = None,
    ) -> bool:
        return send_terminal_event(
            self.notifier,
            cfg,
            headline=headline,
            job_id=job_id,
            queue_id=queue_id,
            status=status,
            reason=reason,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            selected_field_name=self.selected_field_name,
            detail_fields=self.detail_fields(detail_values),
            count_field=(self.terminal_count_field, count_value),
            extra_lines=extra_lines,
        )

    def notify_finished(
        self,
        cfg: Any,
        *,
        job_id: str,
        queue_id: str,
        status: str,
        reason: str,
        job_dir: Path,
        selected_xyz: Path,
        count_value: int,
        detail_values: dict[str, object],
        organized_output_dir: Path | None = None,
        resource_request: dict[str, int] | None = None,
        resource_actual: dict[str, int] | None = None,
    ) -> bool:
        extra_lines = optional_terminal_lines(
            organized_output_dir=organized_output_dir,
            resource_request=resource_request,
            resource_actual=resource_actual,
        )
        return self.notify_terminal(
            cfg,
            headline=terminal_headline(status),
            job_id=job_id,
            queue_id=queue_id,
            status=status,
            reason=reason,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            count_value=count_value,
            detail_values=detail_values,
            extra_lines=extra_lines or None,
        )


def build_engine_notification_module(
    *,
    label: str,
    engine: str,
    selected_field_name: str,
    detail_field_names: tuple[str, ...],
    terminal_count_field: str,
    send_fn: Callable[[Any, list[str]], bool],
) -> EngineNotificationModule:
    return EngineNotificationModule(
        notifier=build_engine_notifier(label=label, engine=engine, send_fn=send_fn),
        selected_field_name=selected_field_name,
        detail_field_names=detail_field_names,
        terminal_count_field=terminal_count_field,
    )


@dataclass(frozen=True)
class EngineJobNotifications:
    notifications: EngineNotificationModule
    terminal_count_param_name: str | None = None

    @property
    def _terminal_count_param(self) -> str:
        return self.terminal_count_param_name or self.notifications.terminal_count_field

    def _detail_values(self, values: Mapping[str, object]) -> dict[str, object]:
        return {
            field_name: values[field_name]
            for field_name in self.notifications.detail_field_names
            if field_name in values
        }

    def _notify_lifecycle(
        self,
        cfg: Any,
        values: Mapping[str, object],
        *,
        headline: str,
    ) -> bool:
        return self.notifications.notify_lifecycle(
            cfg,
            headline=headline,
            job_id=cast(str, values["job_id"]),
            queue_id=cast(str, values["queue_id"]),
            job_dir=cast(Path, values["job_dir"]),
            selected_xyz=cast(Path, values["selected_xyz"]),
            detail_values=self._detail_values(values),
        )

    def notify_job_queued(self, cfg: Any, **values: object) -> bool:
        return self._notify_lifecycle(cfg, values, headline="Job queued")

    def notify_job_started(self, cfg: Any, **values: object) -> bool:
        return self._notify_lifecycle(cfg, values, headline="Job started")

    def notify_job_terminal(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_terminal(
            cfg,
            headline=cast(str, values["headline"]),
            job_id=cast(str, values["job_id"]),
            queue_id=cast(str, values["queue_id"]),
            status=cast(str, values["status"]),
            reason=cast(str, values["reason"]),
            job_dir=cast(Path, values["job_dir"]),
            selected_xyz=cast(Path, values["selected_xyz"]),
            count_value=cast(int, values[self._terminal_count_param]),
            detail_values=self._detail_values(values),
            extra_lines=cast(list[str] | None, values.get("extra_lines")),
        )

    def notify_job_finished(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_finished(
            cfg,
            job_id=cast(str, values["job_id"]),
            queue_id=cast(str, values["queue_id"]),
            status=cast(str, values["status"]),
            reason=cast(str, values["reason"]),
            job_dir=cast(Path, values["job_dir"]),
            selected_xyz=cast(Path, values["selected_xyz"]),
            count_value=cast(int, values[self._terminal_count_param]),
            detail_values=self._detail_values(values),
            organized_output_dir=cast(Path | None, values.get("organized_output_dir")),
            resource_request=cast(dict[str, int] | None, values.get("resource_request")),
            resource_actual=cast(dict[str, int] | None, values.get("resource_actual")),
        )


def build_engine_job_notifications(
    *,
    label: str,
    engine: str,
    selected_field_name: str,
    detail_field_names: tuple[str, ...],
    terminal_count_field: str,
    send_fn: Callable[[Any, list[str]], bool],
    terminal_count_param_name: str | None = None,
) -> EngineJobNotifications:
    return EngineJobNotifications(
        notifications=build_engine_notification_module(
            label=label,
            engine=engine,
            selected_field_name=selected_field_name,
            detail_field_names=detail_field_names,
            terminal_count_field=terminal_count_field,
            send_fn=send_fn,
        ),
        terminal_count_param_name=terminal_count_param_name,
    )


_ENGINE_LINE_SENDER = telegram_line_sender(lambda: build_telegram_transport)

_XTB_JOB_NOTIFICATIONS = build_engine_job_notifications(
    label="chemstack_xtb",
    engine="xtb",
    selected_field_name="selected_input_xyz",
    detail_field_names=("job_type", "reaction_key"),
    terminal_count_field="candidate_count",
    send_fn=_ENGINE_LINE_SENDER,
)

notify_xtb_job_queued = _XTB_JOB_NOTIFICATIONS.notify_job_queued
notify_xtb_job_started = _XTB_JOB_NOTIFICATIONS.notify_job_started
notify_xtb_job_terminal = _XTB_JOB_NOTIFICATIONS.notify_job_terminal
notify_xtb_job_finished = _XTB_JOB_NOTIFICATIONS.notify_job_finished

_CREST_JOB_NOTIFICATIONS = build_engine_job_notifications(
    label="chemstack_crest",
    engine="crest",
    selected_field_name="selected_xyz",
    detail_field_names=("mode",),
    terminal_count_field="retained_conformer_count",
    send_fn=_ENGINE_LINE_SENDER,
)

notify_crest_job_queued = _CREST_JOB_NOTIFICATIONS.notify_job_queued
notify_crest_job_started = _CREST_JOB_NOTIFICATIONS.notify_job_started
notify_crest_job_terminal = _CREST_JOB_NOTIFICATIONS.notify_job_terminal
notify_crest_job_finished = _CREST_JOB_NOTIFICATIONS.notify_job_finished


def terminal_headline(status: str) -> str:
    return {
        "completed": "Job finished",
        "failed": "Job failed",
        "cancelled": "Job cancelled",
    }.get(status, "Job finished")


def optional_terminal_lines(
    *,
    organized_output_dir: Path | None = None,
    resource_request: dict[str, int] | None = None,
    resource_actual: dict[str, int] | None = None,
) -> list[str]:
    lines: list[str] = []
    if organized_output_dir is not None:
        lines.append(f"organized_output_dir: {organized_output_dir}")
    if resource_request is not None:
        lines.append(f"resource_request: {resource_request}")
    if resource_actual is not None:
        lines.append(f"resource_actual: {resource_actual}")
    return lines


def job_event_fields(
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    selected_xyz: Path,
    selected_field_name: str,
    detail_fields: list[tuple[str, object]] | None = None,
    status: str | None = None,
    reason: str | None = None,
    count_field: tuple[str, object] | None = None,
) -> list[tuple[str, object]]:
    fields: list[tuple[str, object]] = [
        ("job_id", job_id),
        ("queue_id", queue_id),
    ]
    if status is not None:
        fields.append(("status", status))
    if reason is not None:
        fields.append(("reason", reason))
    fields.extend(detail_fields or [])
    fields.extend(
        [
            ("job_dir", job_dir.name),
            (selected_field_name, selected_xyz.name),
        ]
    )
    if count_field is not None:
        fields.append(count_field)
    return fields


def send_lifecycle_event(
    notifier: EngineNotifier,
    cfg: Any,
    *,
    headline: str,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    selected_xyz: Path,
    selected_field_name: str,
    detail_fields: list[tuple[str, object]] | None = None,
) -> bool:
    return notifier.send_job_event(
        cfg,
        job_dir=job_dir,
        headline=headline,
        fields=job_event_fields(
            job_id=job_id,
            queue_id=queue_id,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            selected_field_name=selected_field_name,
            detail_fields=detail_fields,
        ),
    )


def send_terminal_event(
    notifier: EngineNotifier,
    cfg: Any,
    *,
    headline: str,
    job_id: str,
    queue_id: str,
    status: str,
    reason: str,
    job_dir: Path,
    selected_xyz: Path,
    selected_field_name: str,
    count_field: tuple[str, object],
    detail_fields: list[tuple[str, object]] | None = None,
    extra_lines: list[str] | None = None,
) -> bool:
    return notifier.send_job_event(
        cfg,
        job_dir=job_dir,
        headline=headline,
        fields=job_event_fields(
            job_id=job_id,
            queue_id=queue_id,
            status=status,
            reason=reason,
            job_dir=job_dir,
            selected_xyz=selected_xyz,
            selected_field_name=selected_field_name,
            detail_fields=detail_fields,
            count_field=count_field,
        ),
        extra_lines=extra_lines,
    )


def event_lines(
    *,
    label: str,
    headline: str,
    fields: list[tuple[str, object]],
    extra_lines: list[str] | None = None,
) -> list[str]:
    lines = [f"[{label}] {headline}"]
    lines.extend(f"{key}: {value}" for key, value in fields)
    if extra_lines:
        lines.extend(extra_lines)
    return lines


def send_job_event(
    cfg: Any,
    *,
    label: str,
    engine: str,
    job_dir: Path,
    headline: str,
    fields: list[tuple[str, object]],
    send_fn: Callable[[Any, list[str]], bool],
    extra_lines: list[str] | None = None,
) -> bool:
    if is_workflow_child(job_dir, engine=engine):
        return True
    return send_fn(
        cfg,
        event_lines(
            label=label,
            headline=headline,
            fields=fields,
            extra_lines=extra_lines,
        ),
    )


def organize_summary_lines(
    *,
    label: str,
    organized_count: int,
    skipped_count: int,
    root: Path,
) -> list[str]:
    return [
        f"[{label}] Organize summary",
        f"root: {root}",
        f"organized: {organized_count}",
        f"skipped: {skipped_count}",
    ]


def send_organize_summary(
    cfg: Any,
    *,
    label: str,
    organized_count: int,
    skipped_count: int,
    root: Path,
    send_fn: Callable[[Any, list[str]], bool],
) -> bool:
    return send_fn(
        cfg,
        organize_summary_lines(
            label=label,
            organized_count=organized_count,
            skipped_count=skipped_count,
            root=root,
        ),
    )
