from __future__ import annotations

from collections.abc import Callable
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
