"""monitor command — detect new events and send concise Telegram alerts.

Runs hourly via cron to report only newly observed run lifecycle events and
newly detected DFT calculation results.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import AppConfig, load_config
from ..dft_index import DFTIndex
from ..dft_monitor import DFTMonitor, MonitorResult, ScanReport
from ..run_snapshot import RunSnapshot, collect_run_snapshots, parse_iso_utc, status_icon
from ..telegram_notifier import escape_html, send_message
from ._helpers import _to_resolved_local

logger = logging.getLogger(__name__)

_STATE_FILE = ".dft_monitor_state.json"
_MONITOR_STATE_FILE = ".monitor_state.json"
_DFT_DB = "dft.db"
_MONITOR_STATE_VERSION = 1


@dataclass(frozen=True)
class RunEvent:
    kind: str
    snapshot: RunSnapshot


def _monitor_state_path(allowed_root: Path) -> Path:
    return allowed_root / _MONITOR_STATE_FILE


def _load_monitor_state(path: Path) -> tuple[dict[str, dict[str, object]], bool]:
    if not path.exists():
        return {}, False
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}, False
    if not isinstance(raw, dict):
        return {}, False
    runs = raw.get("runs")
    if not isinstance(runs, dict):
        return {}, False
    normalized: dict[str, dict[str, object]] = {}
    for key, value in runs.items():
        if isinstance(key, str) and isinstance(value, dict):
            normalized[key] = value
    return normalized, True


def _build_monitor_state(snapshots: list[RunSnapshot]) -> dict[str, dict[str, object]]:
    state: dict[str, dict[str, object]] = {}
    for snapshot in snapshots:
        state[snapshot.key] = {
            "status": snapshot.status,
            "attempts": snapshot.attempts,
            "updated_at": snapshot.updated_at,
        }
    return state


def _save_monitor_state(path: Path, runs: dict[str, dict[str, object]]) -> None:
    payload = {
        "version": _MONITOR_STATE_VERSION,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "runs": runs,
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def _event_sort_key(event: RunEvent) -> datetime:
    snapshot = event.snapshot
    parsed = (
        parse_iso_utc(snapshot.completed_at)
        or parse_iso_utc(snapshot.updated_at)
        or parse_iso_utc(snapshot.started_at)
    )
    return parsed or datetime.min.replace(tzinfo=timezone.utc)


def _coerce_int(value: object) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                return int(stripped)
            except ValueError:
                return 0
    return 0


def _detect_run_events(
    previous: dict[str, dict[str, object]],
    snapshots: list[RunSnapshot],
    *,
    has_baseline: bool,
) -> list[RunEvent]:
    if not has_baseline:
        return []

    events: list[RunEvent] = []
    for snapshot in snapshots:
        prior = previous.get(snapshot.key)
        if prior is None:
            continue

        prior_status = str(prior.get("status", "")).strip().lower()
        prior_attempts = _coerce_int(prior.get("attempts", 0))

        if snapshot.status == "completed" and prior_status != "completed":
            events.append(RunEvent(kind="completed", snapshot=snapshot))
        elif snapshot.status == "failed" and prior_status != "failed":
            events.append(RunEvent(kind="failed", snapshot=snapshot))
        elif snapshot.status == "retrying" and (
            prior_status != "retrying" or snapshot.attempts > prior_attempts
        ):
            events.append(RunEvent(kind="retrying", snapshot=snapshot))

    events.sort(key=_event_sort_key, reverse=True)
    return events


def _format_run_event_section(events: list[RunEvent], kind: str, title: str) -> str | None:
    matched = [event for event in events if event.kind == kind]
    if not matched:
        return None

    lines: list[str] = []
    for event in matched[:8]:
        snapshot = event.snapshot
        timestamp = snapshot.completed_at or snapshot.updated_at or snapshot.started_at
        time_text = parse_iso_utc(timestamp)
        local_text = (
            time_text.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
            if time_text is not None
            else "n/a"
        )
        attempt_suffix = ""
        if kind == "retrying":
            attempt_suffix = f" (attempt #{snapshot.attempts})"
        reason_suffix = f"\n   \U0001f4cc {escape_html(snapshot.final_reason)}" if snapshot.final_reason else ""
        lines.append(
            f"{status_icon(snapshot.status)} <b>{escape_html(snapshot.name)}</b>{attempt_suffix}\n"
            f"   \U0001f4c5 {escape_html(local_text)}"
            f"{reason_suffix}"
        )

    return f"{title}  ({len(matched)})\n\n" + "\n\n".join(lines)


def _notifiable_dft_results(report: ScanReport) -> list[MonitorResult]:
    return [
        result
        for result in report.new_results
        if str(result.status).strip().lower() != "running"
    ]


def _format_dft_section(report: ScanReport) -> str | None:
    results = _notifiable_dft_results(report)
    if not results:
        return None

    lines: list[str] = []
    for result in results:
        icon = status_icon(result.status)
        calc_label = result.calc_type.upper() if result.calc_type else "-"
        note = f"\n   \u26a0\ufe0f {escape_html(result.note.strip('() '))}" if result.note else ""
        lines.append(
            f"{icon} <b>{escape_html(result.formula)}</b>  [{escape_html(calc_label)}]\n"
            f"   \U0001f9ec {escape_html(result.method_basis)}\n"
            f"   \u26a1 {escape_html(result.energy)}\n"
            f"   \U0001f4c2 <code>{escape_html(result.path)}</code>"
            f"{note}"
        )

    header = f"\U0001f9ea <b>New Calculations Detected</b>  ({len(results)})"
    return header + "\n\n" + "\n\n".join(lines)


def _format_failure_section(report: ScanReport) -> str | None:
    if not report.failures:
        return None

    lines: list[str] = []
    for failure in report.failures[:5]:
        lines.append(
            f"\u274c <code>{escape_html(failure.path)}</code>\n"
            f"   {escape_html(failure.error_type)}: {escape_html(failure.error)}"
        )

    count = len(report.failures)
    header = f"\u26a0\ufe0f <b>Parse Failures</b>  ({count})"
    body = "\n\n".join(lines)
    if count > 5:
        body += f"\n\n   ... and {count - 5} more"
    return header + "\n\n" + body


def _format_overview_line(snapshots: list[RunSnapshot]) -> str:
    counts: dict[str, int] = {}
    for snapshot in snapshots:
        counts[snapshot.status] = counts.get(snapshot.status, 0) + 1

    parts: list[str] = []
    for status in ("running", "retrying", "completed", "failed", "created"):
        count = counts.get(status, 0)
        if count > 0:
            parts.append(f"{status_icon(status)} {status} {count}")

    summary = " | ".join(parts) if parts else "No runs"
    return f"\U0001f4ca <b>Overview</b>\n{summary}"


def _build_message(
    snapshots: list[RunSnapshot],
    run_events: list[RunEvent],
    report: ScanReport,
) -> str:
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = f"\u2699\ufe0f <b>orca_auto monitor</b>  <code>{now}</code>"
    divider = "\u2500" * 28

    sections: list[str] = [header, divider]

    for kind, title in [
        ("completed", "\u2705 <b>Completed</b>"),
        ("failed", "\u274c <b>Failed</b>"),
        ("retrying", "\U0001f504 <b>Retries Started</b>"),
    ]:
        section = _format_run_event_section(run_events, kind, title)
        if section:
            sections.append(section)

    dft = _format_dft_section(report)
    if dft:
        sections.append(dft)

    fail = _format_failure_section(report)
    if fail:
        sections.append(fail)

    sections.append(divider)
    sections.append(_format_overview_line(snapshots))
    return "\n\n".join(sections)


def _run_monitor(cfg: AppConfig) -> int:
    tg = cfg.telegram
    if not tg.enabled:
        logger.error("Telegram is not configured.")
        return 1

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    snapshots = collect_run_snapshots(allowed_root)
    previous_state, has_baseline = _load_monitor_state(_monitor_state_path(allowed_root))
    current_state = _build_monitor_state(snapshots)
    run_events = _detect_run_events(previous_state, snapshots, has_baseline=has_baseline)

    state_file = str(allowed_root / _STATE_FILE)
    db_path = str(allowed_root / _DFT_DB)
    dft_index = DFTIndex()
    dft_index.initialize(db_path)
    monitor = DFTMonitor(
        dft_index=dft_index,
        kb_dirs=[str(allowed_root)],
        state_file=state_file,
    )
    report = monitor.scan()
    notifiable_dft_results = _notifiable_dft_results(report)

    should_send = bool(run_events or notifiable_dft_results or report.failures)
    if not should_send:
        _save_monitor_state(_monitor_state_path(allowed_root), current_state)
        logger.info("No new monitor events to send.")
        return 0

    message = _build_message(snapshots, run_events, report)
    success = send_message(tg, message)
    if not success:
        logger.error("Failed to send Telegram notification")
        return 1

    _save_monitor_state(_monitor_state_path(allowed_root), current_state)
    logger.info("Telegram notification sent successfully")
    return 0


def cmd_monitor(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_monitor(cfg)
