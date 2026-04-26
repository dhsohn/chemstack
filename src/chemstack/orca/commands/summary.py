from __future__ import annotations

import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

from ..config import AppConfig, load_config
from ..orca_parser import parse_opt_progress
from ..pathing import is_subpath
from ..run_snapshot import (
    RunSnapshot,
    collect_run_snapshots,
    parse_iso_utc,
    sort_snapshots_by_completed,
    sort_snapshots_by_started,
    status_icon,
)
from ..runtime.run_lock import LOCK_FILE_NAME
from ..telegram_notifier import escape_html, send_message

logger = logging.getLogger(__name__)

_ENERGY_RE = re.compile(r"FINAL SINGLE POINT ENERGY\s+([-\d.]+)")
_MAX_CYCLES_RE = re.compile(r"Max\.\s+no of cycles\s+MaxIter\s+\.\.\.\.\s+(\d+)", re.IGNORECASE)
_MAX_PROGRESS_FILE_BYTES = 128 * 1024 * 1024
_RUNNING_SHOW_LIMIT = 8
_ATTENTION_SHOW_LIMIT = 8
_HTML_TAG_RE = re.compile(r"</?(?:b|code|pre)>")


@dataclass
class ProgressSnapshot:
    cycle: int | None
    energy_hartree: float | None
    out_name: str
    out_size_text: str
    updated_text: str
    proc_count: int | None
    eta_text: str
    tail_text: str


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _human_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    return f"{minutes}m"


def _elapsed_from_started(value: Any) -> str:
    started = parse_iso_utc(value)
    if started is None:
        return "n/a"
    return _human_duration((_utc_now() - started).total_seconds())


def _updated_ago_text(path: Path) -> str:
    try:
        updated = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return "n/a"

    seconds = max(0, int((_utc_now() - updated).total_seconds()))
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        hours, rem = divmod(seconds, 3600)
        return f"{hours}h {rem // 60:02d}m"
    days, rem = divmod(seconds, 86400)
    return f"{days}d {rem // 3600}h"


def _human_bytes(size_bytes: int) -> str:
    value = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{size_bytes} B"


def _scan_cwd_process_counts(allowed_root: Path, proc_root: Path | None = None) -> dict[Path, int]:
    counts: dict[Path, int] = {}
    proc_root = proc_root or Path("/proc")
    if not proc_root.is_dir():
        return counts

    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cwd = Path(os.readlink(entry / "cwd")).resolve()
        except Exception:
            continue
        if not is_subpath(cwd, allowed_root):
            continue
        counts[cwd] = counts.get(cwd, 0) + 1
    return counts


def _read_tail_text(path: Path, max_bytes: int = 16384) -> str:
    try:
        with path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - max_bytes))
            raw = handle.read()
    except OSError:
        return ""

    if not raw:
        return ""

    for encoding in ("utf-8", "utf-8-sig", "utf-16", "latin-1"):
        try:
            return raw.decode(encoding, errors="replace")
        except LookupError:
            continue
    return raw.decode("utf-8", errors="replace")


def _last_non_empty_line(path: Path) -> str:
    text = _read_tail_text(path)
    for line in reversed(text.splitlines()):
        normalized = " ".join(line.strip().split())
        if normalized:
            return normalized[:200]
    return "(tail line not found)"


def _extract_geometry_maxiter(out_path: Path) -> int | None:
    try:
        with out_path.open("r", encoding="utf-8", errors="ignore") as handle:
            maxiter: int | None = None
            for line in handle:
                match = _MAX_CYCLES_RE.search(line)
                if match is not None:
                    maxiter = int(match.group(1))
            return maxiter
    except OSError:
        return None


def _eta_summary(
    *,
    cycle: int | None,
    maxiter: int | None,
    started_at: str,
) -> str:
    if cycle is None or cycle <= 0 or maxiter is None or maxiter <= cycle:
        return "n/a"

    started = parse_iso_utc(started_at)
    if started is None:
        return "n/a"

    elapsed_hours = (_utc_now() - started).total_seconds() / 3600.0
    if elapsed_hours <= 0:
        return "n/a"

    rate = cycle / elapsed_hours
    if rate <= 0:
        return "n/a"

    remaining_hours = max(0.0, (maxiter - cycle) / rate)
    remaining_minutes = max(0, int(round(remaining_hours * 60)))
    days, rem_minutes = divmod(remaining_minutes, 1440)
    hours, minutes = divmod(rem_minutes, 60)

    if days > 0:
        eta_label = f"{days}d {hours}h"
    elif hours > 0:
        eta_label = f"{hours}h {minutes}m"
    else:
        eta_label = f"{minutes}m"

    return f"{eta_label} (maxiter={maxiter}, rate={rate:.2f} cyc/h)"


def _build_progress_snapshot(
    run: RunSnapshot,
    process_counts: dict[Path, int],
) -> ProgressSnapshot:
    out_path = run.latest_out_path
    if out_path is None:
        return ProgressSnapshot(
            cycle=None,
            energy_hartree=None,
            out_name="n/a",
            out_size_text="0.0 B",
            updated_text="n/a ago",
            proc_count=process_counts.get(run.reaction_dir.resolve()),
            eta_text="n/a",
            tail_text="(tail line not found)",
        )

    try:
        size_bytes = out_path.stat().st_size
    except OSError:
        size_bytes = 0

    cycle: int | None = None
    energy_hartree: float | None = None
    if size_bytes <= _MAX_PROGRESS_FILE_BYTES:
        try:
            progress = parse_opt_progress(str(out_path))
        except Exception as exc:
            logger.debug("summary_progress_parse_failed: path=%s error=%s", out_path, exc)
        else:
            if progress.steps:
                best_step = max(progress.steps, key=lambda step: (step.cycle, step.energy_hartree or float("-inf")))
                cycle = best_step.cycle
                energy_hartree = best_step.energy_hartree

    tail_text = _read_tail_text(out_path, max_bytes=32768)
    energy_matches = _ENERGY_RE.findall(tail_text)
    if energy_matches and energy_hartree is None:
        energy_hartree = float(energy_matches[-1])

    updated_label = _updated_ago_text(out_path)

    return ProgressSnapshot(
        cycle=cycle,
        energy_hartree=energy_hartree,
        out_name=out_path.name,
        out_size_text=_human_bytes(size_bytes),
        updated_text=f"{updated_label} ago" if updated_label != "n/a" else "n/a ago",
        proc_count=process_counts.get(run.reaction_dir.resolve()),
        eta_text=_eta_summary(
            cycle=cycle,
            maxiter=_extract_geometry_maxiter(out_path),
            started_at=run.started_at,
        ),
        tail_text=_last_non_empty_line(out_path),
    )


def _matches_orca_process(proc_args: str, orca_executable: str) -> bool:
    stripped = proc_args.strip()
    if not stripped:
        return False

    exe_path = str(Path(orca_executable).expanduser())
    exe_name = Path(exe_path).name
    first_token = stripped.split(maxsplit=1)[0]
    return exe_path in stripped or first_token.endswith(f"/{exe_name}")


def _count_active_orca_processes(orca_executable: str) -> int:
    if not orca_executable.strip():
        return 0

    try:
        proc = subprocess.run(
            ["ps", "-eo", "args="],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        logger.warning("summary_process_count_failed: %s", exc)
        return 0

    count = 0
    for line in proc.stdout.splitlines():
        if _matches_orca_process(line, orca_executable):
            count += 1
    return count


def _format_overview_section(
    active: list[RunSnapshot],
    failed: list[RunSnapshot],
    other: list[RunSnapshot],
    active_simulations: int,
) -> str:
    running_count = sum(1 for snapshot in active if snapshot.status == "running")
    retrying_count = sum(1 for snapshot in active if snapshot.status == "retrying")
    parts: list[str] = []
    for status, count in [
        ("running", running_count),
        ("retrying", retrying_count),
        ("failed", len(failed)),
    ]:
        if count > 0:
            parts.append(f"{status_icon(status)} {status} {count}")
    if other:
        parts.append(f"\u2753 other {len(other)}")

    summary_line = " | ".join(parts) if parts else "No active or attention-needed runs"
    active_line = f"\U0001f517 Active simulations: {active_simulations}"
    return f"\U0001f4ca <b>Current State</b>\n{summary_line}\n{active_line}"


def _format_running_section(
    active: list[RunSnapshot],
    process_counts: dict[Path, int],
) -> str | None:
    if not active:
        return None

    shown = active[:_RUNNING_SHOW_LIMIT]
    lines: list[str] = []
    for run in shown:
        snapshot = _build_progress_snapshot(run, process_counts)
        elapsed = _elapsed_from_started(run.started_at)

        detail_lines = [
            f"   \U0001f4c4 {escape_html(run.selected_inp_name)}",
            f"   \u23f1 Elapsed: {escape_html(elapsed)}",
        ]

        cycle_text = str(snapshot.cycle) if snapshot.cycle is not None else None
        energy_text = f"{snapshot.energy_hartree:.6f} Eh" if snapshot.energy_hartree is not None else None
        if cycle_text or energy_text:
            progress_parts: list[str] = []
            if cycle_text:
                progress_parts.append(f"cycle={escape_html(cycle_text)}")
            if energy_text:
                progress_parts.append(f"E={escape_html(energy_text)}")
            detail_lines.append(f"   \U0001f52c {', '.join(progress_parts)}")

        if snapshot.eta_text != "n/a":
            detail_lines.append(f"   \u23f3 ETA\u2248{escape_html(snapshot.eta_text)}")

        if (run.reaction_dir / LOCK_FILE_NAME).exists():
            detail_lines.append("   \u26a0\ufe0f run.lock present")

        lines.append(
            f"{status_icon(run.status)} <b>{escape_html(run.name)}</b>\n"
            + "\n".join(detail_lines)
        )

    header = f"\u23f3 <b>Active Runs</b>  ({len(active)})"
    if len(active) > len(shown):
        header += f"  showing {len(shown)}/{len(active)}"
    return header + "\n\n" + "\n\n".join(lines)


def _format_attention_section(
    failed: list[RunSnapshot],
    other: list[RunSnapshot],
) -> str | None:
    attention: list[RunSnapshot] = list(failed)
    attention.extend(other)
    if not attention:
        return None

    shown = attention[:_ATTENTION_SHOW_LIMIT]
    lines: list[str] = []
    for run in shown:
        status_text = run.status or "unknown"
        detail = escape_html(status_text)
        if run.final_reason:
            detail += f" · {escape_html(run.final_reason)}"
        lines.append(
            f"{status_icon(run.status)} <b>{escape_html(run.name)}</b>\n"
            f"   \U0001f4cc {detail}"
        )

    header = f"\u26a0\ufe0f <b>Needs Attention</b>  ({len(attention)})"
    if len(attention) > len(shown):
        header += f"  showing {len(shown)}/{len(attention)}"
    return header + "\n\n" + "\n\n".join(lines)


def _build_summary_message(cfg: AppConfig) -> str:
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    snapshots = collect_run_snapshots(allowed_root)
    process_counts = _scan_cwd_process_counts(allowed_root)

    active = sort_snapshots_by_started(
        snapshot for snapshot in snapshots if snapshot.status in {"running", "retrying"}
    )
    failed = sort_snapshots_by_completed(
        snapshot for snapshot in snapshots if snapshot.status == "failed"
    )
    other = [
        snapshot
        for snapshot in snapshots
        if snapshot.status not in {"running", "retrying", "completed", "failed"}
    ]

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = f"\U0001f4ca <b>chemstack summary</b>  <code>{escape_html(now)}</code>"
    divider = "\u2500" * 28
    scope = (
        "\U0001f50e <b>Scope</b>\n"
        "Current-state digest only. Active runs and current blockers are shown; completed history is omitted."
    )

    sections: list[str] = [header, divider]
    sections.append(scope)
    sections.append(_format_overview_section(active, failed, other, len(active)))

    running = _format_running_section(active, process_counts)
    if running:
        sections.append(running)

    attention = _format_attention_section(failed, other)
    if attention:
        sections.append(attention)

    sections.append(divider)

    return "\n\n".join(sections)


def _html_to_plain_text(message: str) -> str:
    return unescape(_HTML_TAG_RE.sub("", message))


def _run_summary(cfg: AppConfig, *, send: bool = True) -> int:
    summary_message = _build_summary_message(cfg)
    print(_html_to_plain_text(summary_message))

    if not send:
        return 0

    if not cfg.telegram.enabled:
        logger.error("Telegram is not configured.")
        return 1

    if send_message(cfg.telegram, summary_message):
        logger.info("Telegram summary sent successfully")
        return 0

    logger.error("Failed to send Telegram summary")
    return 1


def cmd_summary(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_summary(cfg, send=not getattr(args, "no_send", False))
