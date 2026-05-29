from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ._engine_delivery import send_job_event, send_organize_summary
from ._engine_rendering import (
    EngineEventField,
    job_event_fields,
    optional_terminal_lines,
    terminal_headline,
)
from .engine_requests import (
    EngineJobFinishedRequest,
    EngineJobLifecycleRequest,
    EngineJobTerminalRequest,
)
from .engine_validation import (
    _optional_int_dict,
    _optional_lines,
    _optional_path,
    _required_int,
    _required_path,
    _required_str,
)


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

    def detail_fields(self, values: Mapping[str, object]) -> list[EngineEventField]:
        return [
            (field_name, values[field_name])
            for field_name in self.detail_field_names
            if field_name in values
        ]

    def notify_lifecycle_request(
        self,
        cfg: Any,
        request: EngineJobLifecycleRequest,
    ) -> bool:
        return send_lifecycle_event(
            self.notifier,
            cfg,
            headline=request.headline,
            job_id=request.job_id,
            queue_id=request.queue_id,
            job_dir=request.job_dir,
            selected_xyz=request.selected_xyz,
            selected_field_name=self.selected_field_name,
            detail_fields=self.detail_fields(request.detail_values),
        )

    def notify_lifecycle(
        self,
        cfg: Any,
        *,
        headline: str,
        job_id: str,
        queue_id: str,
        job_dir: Path,
        selected_xyz: Path,
        detail_values: Mapping[str, object],
    ) -> bool:
        return self.notify_lifecycle_request(
            cfg,
            EngineJobLifecycleRequest(
                headline=headline,
                job_id=job_id,
                queue_id=queue_id,
                job_dir=job_dir,
                selected_xyz=selected_xyz,
                detail_values=detail_values,
            ),
        )

    def notify_terminal_request(
        self,
        cfg: Any,
        request: EngineJobTerminalRequest,
    ) -> bool:
        return send_terminal_event(
            self.notifier,
            cfg,
            headline=request.headline,
            job_id=request.job_id,
            queue_id=request.queue_id,
            status=request.status,
            reason=request.reason,
            job_dir=request.job_dir,
            selected_xyz=request.selected_xyz,
            selected_field_name=self.selected_field_name,
            detail_fields=self.detail_fields(request.detail_values),
            count_field=(self.terminal_count_field, request.count_value),
            extra_lines=request.extra_lines,
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
        detail_values: Mapping[str, object],
        extra_lines: list[str] | None = None,
    ) -> bool:
        return self.notify_terminal_request(
            cfg,
            EngineJobTerminalRequest(
                headline=headline,
                job_id=job_id,
                queue_id=queue_id,
                status=status,
                reason=reason,
                job_dir=job_dir,
                selected_xyz=selected_xyz,
                count_value=count_value,
                detail_values=detail_values,
                extra_lines=extra_lines,
            ),
        )

    def notify_finished_request(
        self,
        cfg: Any,
        request: EngineJobFinishedRequest,
    ) -> bool:
        extra_lines = optional_terminal_lines(
            organized_output_dir=request.organized_output_dir,
            resource_request=request.resource_request,
            resource_actual=request.resource_actual,
        )
        return self.notify_terminal_request(
            cfg,
            EngineJobTerminalRequest(
                headline=terminal_headline(request.status),
                job_id=request.job_id,
                queue_id=request.queue_id,
                status=request.status,
                reason=request.reason,
                job_dir=request.job_dir,
                selected_xyz=request.selected_xyz,
                count_value=request.count_value,
                detail_values=request.detail_values,
                extra_lines=extra_lines or None,
            ),
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
        detail_values: Mapping[str, object],
        organized_output_dir: Path | None = None,
        resource_request: dict[str, int] | None = None,
        resource_actual: dict[str, int] | None = None,
    ) -> bool:
        return self.notify_finished_request(
            cfg,
            EngineJobFinishedRequest(
                job_id=job_id,
                queue_id=queue_id,
                status=status,
                reason=reason,
                job_dir=job_dir,
                selected_xyz=selected_xyz,
                count_value=count_value,
                detail_values=detail_values,
                organized_output_dir=organized_output_dir,
                resource_request=resource_request,
                resource_actual=resource_actual,
            ),
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

    def _lifecycle_request(
        self,
        values: Mapping[str, object],
        headline: str,
    ) -> EngineJobLifecycleRequest:
        return EngineJobLifecycleRequest(
            headline=headline,
            job_id=_required_str(values, "job_id"),
            queue_id=_required_str(values, "queue_id"),
            job_dir=_required_path(values, "job_dir"),
            selected_xyz=_required_path(values, "selected_xyz"),
            detail_values=self._detail_values(values),
        )

    def _terminal_request(
        self,
        values: Mapping[str, object],
    ) -> EngineJobTerminalRequest:
        return EngineJobTerminalRequest(
            headline=_required_str(values, "headline"),
            job_id=_required_str(values, "job_id"),
            queue_id=_required_str(values, "queue_id"),
            status=_required_str(values, "status"),
            reason=_required_str(values, "reason"),
            job_dir=_required_path(values, "job_dir"),
            selected_xyz=_required_path(values, "selected_xyz"),
            count_value=_required_int(values, self._terminal_count_param),
            detail_values=self._detail_values(values),
            extra_lines=_optional_lines(values, "extra_lines"),
        )

    def _finished_request(
        self,
        values: Mapping[str, object],
    ) -> EngineJobFinishedRequest:
        return EngineJobFinishedRequest(
            job_id=_required_str(values, "job_id"),
            queue_id=_required_str(values, "queue_id"),
            status=_required_str(values, "status"),
            reason=_required_str(values, "reason"),
            job_dir=_required_path(values, "job_dir"),
            selected_xyz=_required_path(values, "selected_xyz"),
            count_value=_required_int(values, self._terminal_count_param),
            detail_values=self._detail_values(values),
            organized_output_dir=_optional_path(values, "organized_output_dir"),
            resource_request=_optional_int_dict(values, "resource_request"),
            resource_actual=_optional_int_dict(values, "resource_actual"),
        )

    def notify_job_queued(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_lifecycle_request(
            cfg,
            self._lifecycle_request(values, "Job queued"),
        )

    def notify_job_started(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_lifecycle_request(
            cfg,
            self._lifecycle_request(values, "Job started"),
        )

    def notify_job_terminal(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_terminal_request(cfg, self._terminal_request(values))

    def notify_job_finished(self, cfg: Any, **values: object) -> bool:
        return self.notifications.notify_finished_request(cfg, self._finished_request(values))


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


__all__ = [
    "EngineJobNotifications",
    "EngineNotificationModule",
    "EngineNotifier",
    "build_engine_job_notifications",
    "build_engine_notification_module",
    "build_engine_notifier",
    "send_lifecycle_event",
    "send_terminal_event",
]
