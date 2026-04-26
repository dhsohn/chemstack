from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from chemstack.activity_view import count_global_active_simulations
from chemstack.core.config.files import shared_workflow_root_from_config
from chemstack.flow.workflow_status import (
    WORKFLOW_STATUS_ORDER,
    normalize_workflow_status,
    select_current_stage,
    workflow_status_is_active,
    workflow_status_needs_attention,
)
from chemstack.flow.operations import list_activities
from chemstack.flow.state import list_workflow_summaries
from chemstack.orca.commands import summary as orca_summary
from chemstack.orca.config import AppConfig, load_config
from chemstack.orca.telegram_notifier import escape_html, send_message

logger = logging.getLogger(__name__)

_WORKFLOW_SHOW_LIMIT = 6


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _workflow_status_icon(status: str) -> str:
    return {
        "created": "🆕",
        "planned": "⏳",
        "pending": "⏳",
        "queued": "⏳",
        "submitted": "📤",
        "running": "▶",
        "retrying": "🔄",
        "cancel_requested": "⏹",
        "completed": "✅",
        "failed": "❌",
        "cancel_failed": "❌",
        "submission_failed": "❌",
        "cancelled": "⛔",
    }.get(_normalize_text(status).lower(), "•")


def _workflow_template_label(template_name: Any) -> str:
    normalized = _normalize_text(template_name).lower()
    return {
        "reaction_ts_search": "ts_search",
        "conformer_screening": "conformer_search",
    }.get(normalized, _normalize_text(template_name) or "workflow")


def _workflow_summary_rows(config_path: str | None) -> tuple[str | None, list[dict[str, Any]]]:
    workflow_root = shared_workflow_root_from_config(config_path)
    if not workflow_root:
        return None, []
    return workflow_root, list_workflow_summaries(workflow_root)


def _activity_rows(config_path: str | None, workflow_root: str | None) -> list[dict[str, Any]]:
    if not config_path:
        return []
    payload = list_activities(
        workflow_root=workflow_root,
        crest_auto_config=config_path,
        xtb_auto_config=config_path,
        orca_auto_config=config_path,
    )
    activities = payload.get("activities")
    return [item for item in activities if isinstance(item, dict)] if isinstance(activities, list) else []


def _format_overview_section(
    *,
    active_runs: list[orca_summary.RunSnapshot],
    failed_runs: list[orca_summary.RunSnapshot],
    other_runs: list[orca_summary.RunSnapshot],
    active_simulations: int,
    workflow_summaries: list[dict[str, Any]],
    workflow_root: str | None,
    orca_proc_count: int | None = None,
) -> str:
    running_count = sum(1 for snapshot in active_runs if snapshot.status == "running")
    retrying_count = sum(1 for snapshot in active_runs if snapshot.status == "retrying")

    orca_parts: list[str] = []
    for status, count in [
        ("running", running_count),
        ("retrying", retrying_count),
        ("failed", len(failed_runs)),
    ]:
        if count > 0:
            orca_parts.append(f"{orca_summary.status_icon(status)} {status} {count}")
    if other_runs:
        orca_parts.append(f"❓ other {len(other_runs)}")

    workflow_counts = Counter(normalize_workflow_status(item.get("status")) or "unknown" for item in workflow_summaries)
    workflow_parts = [
        f"{_workflow_status_icon(status)} {status} {workflow_counts[status]}"
        for status in WORKFLOW_STATUS_ORDER
        if workflow_counts.get(status)
    ]

    lines = [
        "📊 <b>Current State</b>",
        " | ".join(orca_parts) if orca_parts else "No active or attention-needed ORCA runs",
        f"🔗 Active simulations: {active_simulations}",
    ]
    if workflow_root:
        if workflow_parts:
            lines.append(f"🧭 Workflows: {' | '.join(workflow_parts)}")
        else:
            lines.append("🧭 Workflows: none")
    return "\n".join(lines)


def _workflow_detail_block(summary: dict[str, Any]) -> str:
    workflow_id = _normalize_text(summary.get("workflow_id")) or "-"
    template = _workflow_template_label(summary.get("template_name"))
    status = normalize_workflow_status(summary.get("status")) or "unknown"
    current_stage = select_current_stage(summary.get("stage_summaries") or [])
    current_engine = _normalize_text(current_stage.get("engine")) or "workflow"
    current_task = _normalize_text(current_stage.get("task_kind")) or _normalize_text(current_stage.get("stage_kind")) or "-"
    current_stage_id = _normalize_text(current_stage.get("stage_id"))
    reaction_key = _normalize_text(summary.get("reaction_key"))
    stage_count = int(summary.get("stage_count", 0) or 0)
    raw_submission_summary = summary.get("submission_summary")
    submission_summary: dict[str, Any] = raw_submission_summary if isinstance(raw_submission_summary, dict) else {}
    submitted = int(submission_summary.get("submitted_count", 0) or 0)
    failed = int(submission_summary.get("failed_count", 0) or 0)
    skipped = int(submission_summary.get("skipped_count", 0) or 0)

    lines = [
        f"{_workflow_status_icon(status)} <b>{escape_html(workflow_id)}</b>",
        f"   🧩 {escape_html(template)}",
        f"   📍 <code>{escape_html(status)}</code> · {escape_html(current_engine)}/{escape_html(current_task)}",
        f"   🧱 stages={stage_count}",
    ]
    if current_stage_id:
        lines.append(f"   🪜 {escape_html(current_stage_id)}")
    if reaction_key:
        lines.append(f"   🧬 {escape_html(reaction_key)}")
    if submitted or failed or skipped:
        lines.append(
            "   📥 "
            f"submitted={submitted} skipped={skipped} failed={failed}"
        )
    return "\n".join(lines)


def _format_active_workflows_section(workflow_summaries: list[dict[str, Any]]) -> str | None:
    active = [item for item in workflow_summaries if workflow_status_is_active(item.get("status"))]
    if not active:
        return None

    shown = active[:_WORKFLOW_SHOW_LIMIT]
    header = f"🧭 <b>Active Workflows</b>  ({len(active)})"
    if len(active) > len(shown):
        header += f"  showing {len(shown)}/{len(active)}"
    return header + "\n\n" + "\n\n".join(_workflow_detail_block(item) for item in shown)


def _format_attention_workflows_section(workflow_summaries: list[dict[str, Any]]) -> str | None:
    attention = [item for item in workflow_summaries if workflow_status_needs_attention(item.get("status"))]
    if not attention:
        return None

    shown = attention[:_WORKFLOW_SHOW_LIMIT]
    header = f"⚠️ <b>Workflow Attention</b>  ({len(attention)})"
    if len(attention) > len(shown):
        header += f"  showing {len(shown)}/{len(attention)}"
    return header + "\n\n" + "\n\n".join(_workflow_detail_block(item) for item in shown)


def _build_summary_message(cfg: AppConfig, *, config_path: str | None) -> str:
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    snapshots = orca_summary.collect_run_snapshots(allowed_root)
    process_counts = orca_summary._scan_cwd_process_counts(allowed_root)

    active_runs = orca_summary.sort_snapshots_by_started(
        snapshot for snapshot in snapshots if snapshot.status in {"running", "retrying"}
    )
    failed_runs = orca_summary.sort_snapshots_by_completed(
        snapshot for snapshot in snapshots if snapshot.status == "failed"
    )
    other_runs = [
        snapshot
        for snapshot in snapshots
        if snapshot.status not in {"running", "retrying", "completed", "failed"}
    ]

    workflow_root, workflow_summaries = _workflow_summary_rows(config_path)
    activity_rows = _activity_rows(config_path, workflow_root)
    active_simulations = count_global_active_simulations(activity_rows, config_path=config_path) if activity_rows else len(active_runs)

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = f"📊 <b>chemstack summary</b>  <code>{escape_html(now)}</code>"
    divider = "─" * 28
    scope = (
        "🔎 <b>Scope</b>\n"
        "Current-state digest across ORCA runs and workflows. "
        "Active work, current blockers, and automation-relevant status are shown; completed history is omitted."
    )

    sections: list[str] = [header, divider, scope]
    sections.append(
        _format_overview_section(
            active_runs=active_runs,
            failed_runs=failed_runs,
            other_runs=other_runs,
            active_simulations=active_simulations,
            workflow_summaries=workflow_summaries,
            workflow_root=workflow_root,
        )
    )

    running = orca_summary._format_running_section(active_runs, process_counts)
    if running:
        sections.append(running)

    workflow_active = _format_active_workflows_section(workflow_summaries)
    if workflow_active:
        sections.append(workflow_active)

    attention = orca_summary._format_attention_section(failed_runs, other_runs)
    workflow_attention = _format_attention_workflows_section(workflow_summaries)
    if attention:
        sections.append(attention)
    if workflow_attention:
        sections.append(workflow_attention)

    sections.append(divider)
    return "\n\n".join(sections)


def _run_summary(cfg: AppConfig, *, config_path: str | None, send: bool = True) -> int:
    summary_message = _build_summary_message(cfg, config_path=config_path)
    print(orca_summary._html_to_plain_text(summary_message))

    if not send:
        return 0

    if not cfg.telegram.enabled:
        logger.error("Telegram is not configured.")
        return 1

    if send_message(cfg.telegram, summary_message):
        logger.info("Telegram combined summary sent successfully")
        return 0

    logger.error("Failed to send Telegram combined summary")
    return 1


def cmd_summary(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_summary(
        cfg,
        config_path=args.config,
        send=not getattr(args, "no_send", False),
    )


__all__ = ["cmd_summary"]
