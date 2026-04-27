from __future__ import annotations

from pathlib import Path

from chemstack.core.notifications import build_telegram_transport

from .config import AppConfig


def _is_workflow_child(job_dir: Path) -> bool:
    parts = tuple(part for part in job_dir.parts if part)
    if "workflow_jobs" in parts:
        return True
    return any(
        parts[index : index + 3] == ("internal", "crest", "runs")
        for index in range(max(0, len(parts) - 2))
    )


def _send(cfg: AppConfig, lines: list[str]) -> bool:
    result = build_telegram_transport(cfg.telegram).send_text("\n".join(lines))
    return bool(result.sent or result.skipped)


def notify_job_queued(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    if _is_workflow_child(job_dir):
        return True
    return _send(
        cfg,
        [
            "[crest_auto] Job queued",
            f"job_id: {job_id}",
            f"queue_id: {queue_id}",
            f"mode: {mode}",
            f"job_dir: {job_dir.name}",
            f"selected_xyz: {selected_xyz.name}",
        ],
    )


def notify_job_started(
    cfg: AppConfig,
    *,
    job_id: str,
    queue_id: str,
    job_dir: Path,
    mode: str,
    selected_xyz: Path,
) -> bool:
    if _is_workflow_child(job_dir):
        return True
    return _send(
        cfg,
        [
            "[crest_auto] Job started",
            f"job_id: {job_id}",
            f"queue_id: {queue_id}",
            f"mode: {mode}",
            f"job_dir: {job_dir.name}",
            f"selected_xyz: {selected_xyz.name}",
        ],
    )


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
    if _is_workflow_child(job_dir):
        return True
    lines = [
        f"[crest_auto] {headline}",
        f"job_id: {job_id}",
        f"queue_id: {queue_id}",
        f"status: {status}",
        f"reason: {reason}",
        f"mode: {mode}",
        f"job_dir: {job_dir.name}",
        f"selected_xyz: {selected_xyz.name}",
        f"retained_conformer_count: {retained_conformer_count}",
    ]
    if extra_lines:
        lines.extend(extra_lines)
    return _send(cfg, lines)


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
    extra_lines: list[str] = []
    if organized_output_dir is not None:
        extra_lines.append(f"organized_output_dir: {organized_output_dir}")
    if resource_request is not None:
        extra_lines.append(f"resource_request: {resource_request}")
    if resource_actual is not None:
        extra_lines.append(f"resource_actual: {resource_actual}")
    return notify_job_terminal(
        cfg,
        headline={
            "completed": "Job finished",
            "failed": "Job failed",
            "cancelled": "Job cancelled",
        }.get(status, "Job finished"),
        job_id=job_id,
        queue_id=queue_id,
        status=status,
        reason=reason,
        mode=mode,
        job_dir=job_dir,
        selected_xyz=selected_xyz,
        retained_conformer_count=retained_conformer_count,
        extra_lines=extra_lines or None,
    )


def notify_organize_summary(
    cfg: AppConfig,
    *,
    organized_count: int,
    skipped_count: int,
    root: Path,
) -> bool:
    return _send(
        cfg,
        [
            "[crest_auto] Organize summary",
            f"root: {root}",
            f"organized: {organized_count}",
            f"skipped: {skipped_count}",
        ],
    )
