from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .telegram import build_telegram_transport, split_telegram_message


def is_workflow_child(job_dir: Path, *, engine: str) -> bool:
    parts = tuple(part for part in job_dir.parts if part)
    if "workflow_jobs" in parts:
        return True
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
