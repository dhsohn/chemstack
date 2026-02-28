#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from core.commands._helpers import default_config_path
from core.config import AppConfig, load_config
from core.notifier import resolve_telegram_config
from core.telegram_client import send_with_retry

logger = logging.getLogger("dft_progress_report")

RUN_STATE_FILE = "run_state.json"
TAIL_BYTES = 96 * 1024
ENERGY_RE = re.compile(r"FINAL SINGLE POINT ENERGY\s+(-?\d+\.\d+(?:[EeDd][+-]?\d+)?)")
CYCLE_RE = re.compile(r"GEOMETRY OPTIMIZATION CYCLE\s+(\d+)", re.IGNORECASE)
MAXITER_RE = re.compile(r"\bmaxiter\s+(\d+)\b", re.IGNORECASE)
OUT_MAXITER_RE = re.compile(
    r"max\.\s*no\s*of\s*cycles\s+maxiter\s+\.\.\.\.\s+(\d+)",
    re.IGNORECASE,
)
FAILED_STATUSES = {"failed", "error", "aborted", "interrupted"}
ETA_STATE_FILE = ".dft_progress_eta_state.json"
ETA_MIN_DELTA_HOURS = 0.05
ETA_MAX_HOURS = 24.0 * 30.0
HEAD_BYTES = 64 * 1024
LARGE_TAIL_BYTES = 4 * 1024 * 1024


@dataclass
class CaseReport:
    name: str
    path: Path
    category: str
    status: str
    run_id: str
    selected_inp_name: str
    started_at: Optional[datetime]
    updated_at: Optional[datetime]
    out_path: Optional[Path]
    out_size_bytes: int
    out_mtime: Optional[datetime]
    cycle: Optional[int]
    energy: Optional[float]
    tail_line: str
    active_proc_count: int
    has_run_lock: bool
    terminated_normally: bool
    max_iter: Optional[int]
    cycle_rate_per_hour: Optional[float]
    eta_hours: Optional[float]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build and send ORCA DFT progress summary report.")
    parser.add_argument("--config", default=default_config_path(), help="Path to orca_auto.yaml")
    parser.add_argument("--print-only", action="store_true", help="Print report without sending to Telegram")
    parser.add_argument("--max-running", type=int, default=8, help="Maximum running cases to include")
    parser.add_argument("--max-completed", type=int, default=3, help="Maximum completed cases to include")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def _parse_iso(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _human_bytes(num_bytes: int) -> str:
    value = float(max(0, num_bytes))
    for unit in ("B", "KB", "MB", "GB"):
        if value < 1024.0:
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"


def _human_age(now: datetime, ts: Optional[datetime]) -> str:
    if ts is None:
        return "n/a"
    delta_sec = int((now - ts.astimezone(now.tzinfo)).total_seconds())
    if delta_sec < 0:
        delta_sec = 0
    days, rem = divmod(delta_sec, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours}h"
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _fmt_local(ts: Optional[datetime]) -> str:
    if ts is None:
        return "n/a"
    return ts.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _fmt_eta_duration(hours: float) -> str:
    if hours <= 0:
        return "<10m"
    total_minutes = int(round(hours * 60.0))
    if total_minutes <= 10:
        return "<10m"
    days, rem = divmod(total_minutes, 1440)
    h, m = divmod(rem, 60)
    if days > 0:
        return f"{days}d {h}h"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m"


def _parse_positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, float):
        ivalue = int(value)
        return ivalue if ivalue > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            ivalue = int(text)
            return ivalue if ivalue > 0 else None
    return None


def _parse_positive_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        fvalue = float(value)
        return fvalue if fvalue > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            fvalue = float(text)
        except ValueError:
            return None
        return fvalue if fvalue > 0 else None
    return None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return raw
    except Exception:
        pass
    return {}


def _decode_tail(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-16-le", "utf-16-be", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="ignore")


def _read_tail_text(path: Path, nbytes: int = TAIL_BYTES) -> str:
    try:
        size = path.stat().st_size
    except OSError:
        return ""
    try:
        with path.open("rb") as handle:
            if size > nbytes:
                handle.seek(size - nbytes)
            raw = handle.read()
    except OSError:
        return ""
    return _decode_tail(raw)


def _read_head_text(path: Path, nbytes: int = HEAD_BYTES) -> str:
    try:
        with path.open("rb") as handle:
            raw = handle.read(nbytes)
    except OSError:
        return ""
    return _decode_tail(raw)


def _parse_energy_value(raw: str) -> Optional[float]:
    token = raw.strip().replace("D", "E").replace("d", "e")
    try:
        return float(token)
    except ValueError:
        return None


def _extract_last_energy_from_text(text: str) -> Optional[float]:
    energy: Optional[float] = None
    for match in ENERGY_RE.finditer(text):
        parsed = _parse_energy_value(match.group(1))
        if parsed is not None:
            energy = parsed
    return energy


def _extract_last_energy_from_out(out_path: Path, tail_text: str) -> Optional[float]:
    energy = _extract_last_energy_from_text(tail_text)
    if energy is not None:
        return energy

    large_tail = _read_tail_text(out_path, nbytes=LARGE_TAIL_BYTES)
    energy = _extract_last_energy_from_text(large_tail)
    if energy is not None:
        return energy

    # Final fallback: stream the full file and keep the last seen energy.
    try:
        with out_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                match = ENERGY_RE.search(line)
                if not match:
                    continue
                parsed = _parse_energy_value(match.group(1))
                if parsed is not None:
                    energy = parsed
    except OSError:
        return None
    return energy


def _extract_progress(tail_text: str) -> tuple[Optional[int], str, bool]:
    cycle: Optional[int] = None
    for match in CYCLE_RE.finditer(tail_text):
        try:
            cycle = int(match.group(1))
        except ValueError:
            continue

    tail_line = ""
    for line in reversed(tail_text.splitlines()):
        cleaned = " ".join(line.strip().split())
        if cleaned:
            tail_line = cleaned
            break
    if len(tail_line) > 160:
        tail_line = tail_line[:157] + "..."

    terminated = "****ORCA TERMINATED NORMALLY****" in tail_text.upper()
    return cycle, tail_line, terminated


def _safe_sorted_out_files(case_dir: Path) -> list[Path]:
    out_files: list[Path] = []
    for path in case_dir.glob("*.out"):
        try:
            _ = path.stat().st_mtime_ns
        except OSError:
            continue
        out_files.append(path)
    out_files.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)
    return out_files


def _resolve_selected_inp_path(case_dir: Path, state: dict[str, Any]) -> Optional[Path]:
    selected_inp_raw = state.get("selected_inp")
    candidates: list[Path] = []
    if isinstance(selected_inp_raw, str) and selected_inp_raw.strip():
        selected = Path(selected_inp_raw)
        candidates.append(selected)
        candidates.append(case_dir / selected.name)
    for cand in candidates:
        if cand.exists() and cand.is_file():
            return cand

    inp_files = list(case_dir.glob("*.inp"))
    if not inp_files:
        return None
    inp_files.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)
    return inp_files[0]


def _parse_max_iter(inp_path: Optional[Path]) -> Optional[int]:
    if inp_path is None or not inp_path.exists():
        return None
    try:
        content = inp_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return None

    in_geom_block = False
    for raw_line in content.splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        lower = line.lower()

        if lower.startswith("%geom"):
            in_geom_block = True
            m_inline = MAXITER_RE.search(line)
            if m_inline:
                return _parse_positive_int(m_inline.group(1))
            continue

        if in_geom_block:
            m = MAXITER_RE.search(line)
            if m:
                return _parse_positive_int(m.group(1))
            if lower == "end":
                in_geom_block = False

    # Fallback: accept global line if %geom block format was unconventional.
    m_any = MAXITER_RE.search(content)
    if m_any:
        return _parse_positive_int(m_any.group(1))
    return None


def _parse_max_iter_from_out(out_path: Optional[Path]) -> Optional[int]:
    if out_path is None or not out_path.exists():
        return None
    head_text = _read_head_text(out_path)
    if not head_text:
        return None
    for line in head_text.splitlines():
        m = OUT_MAXITER_RE.search(line)
        if m:
            return _parse_positive_int(m.group(1))
    return None


def _guess_out_path(case_dir: Path, state: dict[str, Any]) -> Optional[Path]:
    selected_inp_raw = state.get("selected_inp")
    if isinstance(selected_inp_raw, str) and selected_inp_raw.strip():
        selected_inp = Path(selected_inp_raw)
        candidates = [
            selected_inp.with_suffix(".out"),
            case_dir / selected_inp.with_suffix(".out").name,
        ]
        for cand in candidates:
            if cand.exists():
                return cand
    outs = _safe_sorted_out_files(case_dir)
    return outs[0] if outs else None


def _scan_orca_process_cmdlines() -> list[str]:
    pattern = r"[/]home/daehyupsohn/opt/orca/orca|\borca\b"
    try:
        proc = subprocess.run(
            ["pgrep", "-af", pattern],
            check=False,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, OSError):
        return []

    cmdlines: list[str] = []
    self_pid = os.getpid()
    for raw_line in proc.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            continue
        pid_text, cmd = parts
        try:
            pid = int(pid_text)
        except ValueError:
            pid = -1
        if pid == self_pid:
            continue
        if "pgrep -af" in cmd:
            continue
        cmdlines.append(cmd)
    return cmdlines


def _count_active_processes(case_name: str, selected_inp_name: str, cmdlines: list[str]) -> int:
    tokens = {f"{case_name}.inp", f"{case_name}.gbw"}
    if selected_inp_name:
        tokens.add(selected_inp_name)
        stem = Path(selected_inp_name).stem
        if stem:
            tokens.add(f"{stem}.inp")
            tokens.add(f"{stem}.gbw")

    count = 0
    for cmd in cmdlines:
        if any(token in cmd for token in tokens):
            count += 1
    return count


def _eta_state_path() -> Path:
    return Path(__file__).resolve().parents[1] / ETA_STATE_FILE


def _load_eta_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"cases": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"cases": {}}
    if not isinstance(raw, dict):
        return {"cases": {}}
    cases = raw.get("cases")
    if not isinstance(cases, dict):
        return {"cases": {}}
    return {"cases": cases}


def _save_eta_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=True, indent=2), encoding="utf-8")
    tmp.replace(path)


def _apply_eta_estimates(reports: list[CaseReport], now_utc: datetime) -> None:
    state_path = _eta_state_path()
    old_state = _load_eta_state(state_path)
    old_cases = old_state.get("cases", {})
    if not isinstance(old_cases, dict):
        old_cases = {}

    new_cases: dict[str, Any] = {}
    for case in reports:
        case.cycle_rate_per_hour = None
        case.eta_hours = None

        if case.category != "running" or case.cycle is None:
            continue

        key = str(case.path)
        prev = old_cases.get(key, {})
        if not isinstance(prev, dict):
            prev = {}

        prev_run_id = str(prev.get("run_id", "")).strip()
        prev_cycle = _parse_positive_int(prev.get("last_cycle"))
        prev_seen = _parse_iso(prev.get("last_seen_at"))
        prev_rate = _parse_positive_float(prev.get("ema_rate_cph"))

        rate: Optional[float] = None
        if (
            prev_seen is not None
            and prev_cycle is not None
            and prev_run_id
            and prev_run_id == case.run_id
        ):
            delta_hours = (now_utc - prev_seen.astimezone(timezone.utc)).total_seconds() / 3600.0
            delta_cycles = case.cycle - prev_cycle
            if delta_hours >= ETA_MIN_DELTA_HOURS and delta_cycles > 0:
                inst_rate = delta_cycles / delta_hours
                if prev_rate is not None:
                    rate = (0.6 * inst_rate) + (0.4 * prev_rate)
                else:
                    rate = inst_rate
            elif prev_rate is not None:
                # No fresh delta yet; keep previous smoothed rate as hint.
                rate = prev_rate

        if rate is None and case.started_at is not None and case.cycle > 0:
            elapsed_hours = (
                now_utc - case.started_at.astimezone(timezone.utc)
            ).total_seconds() / 3600.0
            if elapsed_hours >= ETA_MIN_DELTA_HOURS:
                rate = case.cycle / elapsed_hours

        case.cycle_rate_per_hour = rate if rate and rate > 0 else None
        if (
            case.max_iter is not None
            and case.cycle is not None
            and case.max_iter > case.cycle
            and case.cycle_rate_per_hour is not None
        ):
            remaining_cycles = case.max_iter - case.cycle
            eta = remaining_cycles / case.cycle_rate_per_hour
            case.eta_hours = max(0.0, min(ETA_MAX_HOURS, eta))
        elif case.max_iter is not None and case.cycle is not None and case.max_iter <= case.cycle:
            case.eta_hours = 0.0

        new_cases[key] = {
            "run_id": case.run_id,
            "last_cycle": case.cycle,
            "last_seen_at": now_utc.isoformat(),
            "ema_rate_cph": case.cycle_rate_per_hour,
            "max_iter": case.max_iter,
        }

    _save_eta_state(state_path, {"cases": new_cases})


def _collect_case_reports(cfg: AppConfig) -> tuple[list[CaseReport], int]:
    root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        raise ValueError(f"allowed_root is not a directory: {root}")

    cmdlines = _scan_orca_process_cmdlines()
    reports: list[CaseReport] = []

    for case_dir in sorted((p for p in root.iterdir() if p.is_dir()), key=lambda p: p.name.lower()):
        run_state_path = case_dir / RUN_STATE_FILE
        has_inp = any(case_dir.glob("*.inp"))
        has_out = any(case_dir.glob("*.out"))
        if not run_state_path.exists() and not has_inp and not has_out:
            continue

        state = _load_json(run_state_path) if run_state_path.exists() else {}
        status = str(state.get("status", "unknown")).strip().lower() or "unknown"
        run_id = str(state.get("run_id", "")).strip()
        selected_inp_path = _resolve_selected_inp_path(case_dir, state)
        selected_inp_name = ""
        if selected_inp_path is not None:
            selected_inp_name = selected_inp_path.name

        out_path = _guess_out_path(case_dir, state)
        out_size_bytes = 0
        out_mtime: Optional[datetime] = None
        cycle: Optional[int] = None
        energy: Optional[float] = None
        tail_line = ""
        terminated_normally = False

        if out_path is not None and out_path.exists():
            try:
                st = out_path.stat()
                out_size_bytes = st.st_size
                out_mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
            except OSError:
                pass
            tail_text = _read_tail_text(out_path)
            cycle, tail_line, terminated_normally = _extract_progress(tail_text)
            energy = _extract_last_energy_from_out(out_path, tail_text)

        active_proc_count = _count_active_processes(case_dir.name, selected_inp_name, cmdlines)
        has_run_lock = (case_dir / "run.lock").exists()

        if active_proc_count > 0:
            category = "running"
        elif status == "running" and not terminated_normally:
            category = "running"
        elif status in FAILED_STATUSES:
            category = "failed"
        elif status == "completed" or terminated_normally:
            category = "completed"
        else:
            category = "other"

        reports.append(
            CaseReport(
                name=case_dir.name,
                path=case_dir,
                category=category,
                status=status,
                run_id=run_id,
                selected_inp_name=selected_inp_name,
                started_at=_parse_iso(state.get("started_at")),
                updated_at=_parse_iso(state.get("updated_at")),
                out_path=out_path,
                out_size_bytes=out_size_bytes,
                out_mtime=out_mtime,
                cycle=cycle,
                energy=energy,
                tail_line=tail_line,
                active_proc_count=active_proc_count,
                has_run_lock=has_run_lock,
                terminated_normally=terminated_normally,
                max_iter=_parse_max_iter(selected_inp_path) or _parse_max_iter_from_out(out_path),
                cycle_rate_per_hour=None,
                eta_hours=None,
            )
        )

    return reports, len(cmdlines)


def _format_running_case(case: CaseReport, now: datetime) -> list[str]:
    energy_text = f"{case.energy:.6f} Eh" if case.energy is not None else "n/a"
    cycle_text = str(case.cycle) if case.cycle is not None else "n/a"
    out_name = case.out_path.name if case.out_path is not None else "n/a"
    tail = case.tail_line if case.tail_line else "(tail line not found)"
    eta_hint = "ETA=n/a"
    if case.max_iter is not None and case.cycle is not None:
        if case.max_iter <= case.cycle:
            eta_hint = f"ETA≈soon (maxiter={case.max_iter})"
        elif case.eta_hours is not None and case.cycle_rate_per_hour is not None:
            eta_hint = (
                f"ETA≈{_fmt_eta_duration(case.eta_hours)} "
                f"(maxiter={case.max_iter}, rate={case.cycle_rate_per_hour:.2f} cyc/h)"
            )
        else:
            eta_hint = f"ETA=n/a (maxiter={case.max_iter}, 속도 데이터 부족)"

    lines = [
        f"- {case.name} | run_id={case.run_id or '-'} | started={_fmt_local(case.started_at)} ({_human_age(now, case.started_at)})",
        (
            f"  progress: cycle={cycle_text}, E={energy_text}, out={out_name} "
            f"({_human_bytes(case.out_size_bytes)}), updated={_human_age(now, case.out_mtime)} ago, "
            f"proc={case.active_proc_count}, {eta_hint}"
        ),
        f"  tail: {tail}",
    ]
    if case.status != "running":
        lines.append(f"  note: state.status={case.status}")
    elif case.has_run_lock:
        lines.append("  note: run.lock present")
    return lines


def _format_completed_case(case: CaseReport) -> str:
    when = _fmt_local(case.updated_at or case.out_mtime)
    return f"- {case.name} | run_id={case.run_id or '-'} | status={case.status} | updated={when}"


def _build_message(
    cfg: AppConfig,
    reports: list[CaseReport],
    proc_count: int,
    max_running: int,
    max_completed: int,
) -> str:
    now = datetime.now().astimezone()
    running = [r for r in reports if r.category == "running"]
    failed = [r for r in reports if r.category == "failed"]
    completed = [r for r in reports if r.category == "completed"]
    other = [r for r in reports if r.category == "other"]

    completed.sort(
        key=lambda r: (r.updated_at or datetime.min.replace(tzinfo=timezone.utc)),
        reverse=True,
    )

    lines: list[str] = []
    lines.append("[ORCA DFT 중간결과 요약]")
    lines.append(f"generated: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    lines.append(f"root: {cfg.runtime.allowed_root}")
    lines.append(
        f"summary: running={len(running)} completed={len(completed)} failed={len(failed)} other={len(other)}"
    )
    lines.append(f"active_orca_processes: {proc_count}")

    if running:
        lines.append("")
        lines.append(f"[running details] showing {min(len(running), max_running)} / {len(running)}")
        for case in running[: max(0, max_running)]:
            lines.extend(_format_running_case(case, now))
        hidden = len(running) - max(0, max_running)
        if hidden > 0:
            lines.append(f"... {hidden} more running cases omitted")

    if failed:
        lines.append("")
        lines.append("[failed suspects]")
        for case in failed:
            lines.append(
                f"- {case.name} | run_id={case.run_id or '-'} | status={case.status} | updated={_fmt_local(case.updated_at or case.out_mtime)}"
            )

    if completed:
        lines.append("")
        lines.append(f"[recent completed] showing {min(len(completed), max_completed)} / {len(completed)}")
        for case in completed[: max(0, max_completed)]:
            lines.append(_format_completed_case(case))

    if not running:
        lines.append("")
        lines.append("note: currently detected running cases = 0")

    text = "\n".join(lines)
    if len(text) > 3400:
        text = text[:3360] + "\n... [report truncated]"
    return text


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    try:
        cfg = load_config(args.config)
        reports, proc_count = _collect_case_reports(cfg)
        _apply_eta_estimates(reports, datetime.now(timezone.utc))
    except Exception as exc:
        logger.error("Failed to build report context: %s", exc)
        return 1

    text = _build_message(
        cfg,
        reports,
        proc_count,
        max_running=max(0, args.max_running),
        max_completed=max(0, args.max_completed),
    )
    print(text)

    if args.print_only:
        return 0

    tg_config = resolve_telegram_config(cfg.monitoring)
    if tg_config is None:
        logger.error(
            "Telegram config is unavailable. Check monitoring.enabled and env vars "
            "(%s, %s).",
            cfg.monitoring.telegram.bot_token_env,
            cfg.monitoring.telegram.chat_id_env,
        )
        return 1

    result = send_with_retry(tg_config, text)
    if not result.success:
        logger.error("Telegram send failed: status=%s error=%s", result.status_code, result.error)
        return 1
    logger.info("Telegram send succeeded: status=%s", result.status_code)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
