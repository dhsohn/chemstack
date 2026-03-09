from __future__ import annotations

import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from ..config import AppConfig, load_config
from ..dft_discovery import _find_latest_out_in_dir
from ..orca_parser import parse_opt_progress
from ..pathing import is_subpath, resolve_artifact_path
from ..state_store import LOCK_FILE_NAME, load_state
from ..telegram_notifier import send_message

logger = logging.getLogger(__name__)

_ENERGY_RE = re.compile(r"FINAL SINGLE POINT ENERGY\s+([-\d.]+)")
_MAX_CYCLES_RE = re.compile(r"Max\.\s+no of cycles\s+MaxIter\s+\.\.\.\.\s+(\d+)", re.IGNORECASE)
_MAX_PROGRESS_FILE_BYTES = 128 * 1024 * 1024
_RUNNING_SHOW_LIMIT = 8
_FAILED_SHOW_LIMIT = 8
_COMPLETED_SHOW_LIMIT = 3


@dataclass
class SummaryRun:
    name: str
    reaction_dir: Path
    run_id: str
    status: str
    started_at: str
    updated_at: str
    completed_at: str
    selected_inp_name: str
    latest_out_path: Path | None


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


def _parse_iso_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_local_datetime(value: Any) -> str:
    dt = _parse_iso_utc(value)
    if dt is None:
        return "n/a"
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


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
    started = _parse_iso_utc(value)
    if started is None:
        return "n/a"
    return _human_duration((datetime.now(timezone.utc) - started).total_seconds())


def _updated_ago_text(path: Path) -> str:
    try:
        updated = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return "n/a"

    seconds = max(0, int((datetime.now(timezone.utc) - updated).total_seconds()))
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


def _latest_out_path(reaction_dir: Path, state: dict[str, Any]) -> Path | None:
    final_result = state.get("final_result")
    if isinstance(final_result, dict):
        last_out_path = final_result.get("last_out_path")
        if isinstance(last_out_path, str) and last_out_path.strip():
            resolved = resolve_artifact_path(last_out_path, reaction_dir)
            if resolved is not None:
                return resolved
    attempts = state.get("attempts")
    if isinstance(attempts, list):
        for attempt in reversed(attempts):
            if not isinstance(attempt, dict):
                continue
            out_path = attempt.get("out_path")
            if isinstance(out_path, str) and out_path.strip():
                resolved = resolve_artifact_path(out_path, reaction_dir)
                if resolved is not None:
                    return resolved
    return _find_latest_out_in_dir(reaction_dir)


def _load_runs(allowed_root: Path) -> list[SummaryRun]:
    runs: list[SummaryRun] = []
    if not allowed_root.is_dir():
        return runs

    for state_path in sorted(allowed_root.rglob("run_state.json")):
        reaction_dir = state_path.parent
        state = load_state(reaction_dir)
        if state is None:
            continue

        final_result = state.get("final_result")
        completed_at = ""
        if isinstance(final_result, dict):
            completed_at = str(final_result.get("completed_at", "")).strip()

        selected_inp = state.get("selected_inp", "")
        selected_inp_name = Path(selected_inp).name if isinstance(selected_inp, str) and selected_inp else "-"

        runs.append(SummaryRun(
            name=str(reaction_dir.relative_to(allowed_root)),
            reaction_dir=reaction_dir,
            run_id=str(state.get("run_id", "")),
            status=str(state.get("status", "")).strip().lower(),
            started_at=str(state.get("started_at", "")),
            updated_at=str(state.get("updated_at", "")),
            completed_at=completed_at,
            selected_inp_name=selected_inp_name,
            latest_out_path=_latest_out_path(reaction_dir, state),
        ))

    return runs


def _scan_cwd_process_counts(allowed_root: Path) -> dict[Path, int]:
    counts: dict[Path, int] = {}
    proc_root = Path("/proc")
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

    started = _parse_iso_utc(started_at)
    if started is None:
        return "n/a"

    elapsed_hours = (datetime.now(timezone.utc) - started).total_seconds() / 3600.0
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
    run: SummaryRun,
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


def _progress_line(snapshot: ProgressSnapshot) -> str:
    cycle_text = str(snapshot.cycle) if snapshot.cycle is not None else "n/a"
    energy_text = f"{snapshot.energy_hartree:.6f} Eh" if snapshot.energy_hartree is not None else "n/a"
    proc_text = str(snapshot.proc_count) if snapshot.proc_count is not None else "n/a"
    eta_text = f"ETA≈{snapshot.eta_text}" if snapshot.eta_text != "n/a" else "ETA=n/a"
    return (
        f"cycle={cycle_text}, "
        f"E={energy_text}, "
        f"out={snapshot.out_name} ({snapshot.out_size_text}), "
        f"updated={snapshot.updated_text}, "
        f"proc={proc_text}, "
        f"{eta_text}"
    )


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

    exe_path = str(Path(orca_executable).expanduser())
    exe_name = Path(exe_path).name
    count = 0
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if exe_path in stripped or stripped.split(maxsplit=1)[0].endswith(f"/{exe_name}"):
            count += 1
    return count


def _sorted_by_started(runs: Iterable[SummaryRun]) -> list[SummaryRun]:
    def key(run: SummaryRun) -> tuple[int, datetime]:
        parsed = _parse_iso_utc(run.started_at)
        if parsed is None:
            return (1, datetime.min.replace(tzinfo=timezone.utc))
        return (0, parsed)

    return sorted(runs, key=key)


def _sorted_by_completed(runs: Iterable[SummaryRun]) -> list[SummaryRun]:
    def key(run: SummaryRun) -> datetime:
        parsed = _parse_iso_utc(run.completed_at) or _parse_iso_utc(run.updated_at)
        return parsed or datetime.min.replace(tzinfo=timezone.utc)

    return sorted(runs, key=key, reverse=True)


def _build_summary_text(cfg: AppConfig) -> str:
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    runs = _load_runs(allowed_root)
    process_counts = _scan_cwd_process_counts(allowed_root)

    active = _sorted_by_started(r for r in runs if r.status in {"running", "retrying"})
    completed = _sorted_by_completed(r for r in runs if r.status == "completed")
    failed = _sorted_by_completed(r for r in runs if r.status == "failed")
    other = [r for r in runs if r.status not in {"running", "retrying", "completed", "failed"}]

    lines = [
        "[ORCA DFT 중간결과 요약]",
        f"generated: {datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"root: {allowed_root}",
        (
            "summary: "
            f"running={len(active)} completed={len(completed)} "
            f"failed={len(failed)} other={len(other)}"
        ),
        f"active_orca_processes: {_count_active_orca_processes(cfg.paths.orca_executable)}",
    ]

    if active:
        shown_active = active[:_RUNNING_SHOW_LIMIT]
        lines.extend(["", f"[running details] showing {len(shown_active)} / {len(active)}"])
        for run in shown_active:
            lines.append(
                f"- {run.name} | run_id={run.run_id or '-'} | "
                f"started={_format_local_datetime(run.started_at)} ({_elapsed_from_started(run.started_at)})"
            )
            snapshot = _build_progress_snapshot(run, process_counts)
            lines.append(f"  progress: {_progress_line(snapshot)}")
            lines.append(f"  tail: {snapshot.tail_text}")
            if (run.reaction_dir / LOCK_FILE_NAME).exists():
                lines.append("  note: run.lock present")
            lines.append("")
        if lines[-1] == "":
            lines.pop()

    if failed:
        lines.extend(["", "[failed suspects]"])
        for run in failed[:_FAILED_SHOW_LIMIT]:
            updated_value = run.updated_at or run.completed_at
            lines.append(
                f"- {run.name} | run_id={run.run_id} | status={run.status or '-'} | "
                f"updated={_format_local_datetime(updated_value)}"
            )

    if completed:
        shown = completed[:_COMPLETED_SHOW_LIMIT]
        lines.extend(["", f"[recent completed] showing {len(shown)} / {len(completed)}"])
        for run in shown:
            updated_value = run.completed_at or run.updated_at
            lines.append(
                f"- {run.name} | run_id={run.run_id} | status=completed | "
                f"updated={_format_local_datetime(updated_value)}"
            )

    return "\n".join(lines)


def _run_summary(cfg: AppConfig, *, send: bool = True) -> int:
    summary_text = _build_summary_text(cfg)
    print(summary_text)

    if not send:
        return 0

    if not cfg.telegram.enabled:
        logger.error("Telegram is not configured.")
        return 1

    if send_message(cfg.telegram, summary_text, parse_mode=None):
        logger.info("Telegram summary sent successfully")
        return 0

    logger.error("Failed to send Telegram summary")
    return 1


def cmd_summary(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_summary(cfg, send=not getattr(args, "no_send", False))
