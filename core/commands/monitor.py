"""monitor 커맨드 — 시뮬레이션 상태를 스캔하고 텔레그램으로 요약 전송.

매시간 크론으로 실행되어 현재 running 시뮬레이션과
새로 감지된 DFT 계산 결과를 텔레그램 메시지로 보낸다.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import AppConfig, load_config
from ..dft_index import DFTIndex
from ..dft_monitor import DFTMonitor, ScanReport
from ..telegram_notifier import _escape_html, send_message
from ._helpers import _to_resolved_local
from .list_runs import _collect_runs

logger = logging.getLogger(__name__)

_STATE_FILE = ".dft_monitor_state.json"
_DFT_DB = "dft.db"

_ICON = {
    "completed": "\u2705",
    "running": "\u23f3",
    "failed": "\u274c",
    "retrying": "\U0001f504",
    "created": "\U0001f195",
}


def _status_icon(status: str) -> str:
    return _ICON.get(status, "\u2753")


def _format_running_section(runs: list[dict[str, Any]]) -> str | None:
    """running/retrying 시뮬레이션 상세 블록."""
    active = [r for r in runs if r["status"] in ("running", "retrying")]
    if not active:
        return None

    lines: list[str] = []
    for r in active:
        icon = _status_icon(r["status"])
        inp_name = r["inp"] or "-"
        attempt_info = f"(시도 #{r['attempts']})" if r["attempts"] > 1 else ""
        lines.append(
            f"{icon} <b>{_escape_html(r['dir'])}</b> {attempt_info}\n"
            f"   \U0001f4c4 {_escape_html(inp_name)}\n"
            f"   \u23f1 경과: {_escape_html(r['elapsed_text'])}"
        )

    header = f"\u23f3 <b>실행 중</b>  ({len(active)}건)"
    return header + "\n\n" + "\n\n".join(lines)


def _format_dft_section(report: ScanReport) -> str | None:
    """새로 감지된 DFT 계산 결과 블록."""
    if not report.new_results:
        return None

    lines: list[str] = []
    for r in report.new_results:
        icon = _status_icon(r.status)
        calc_label = r.calc_type.upper() if r.calc_type else "-"
        note = f"\n   \u26a0\ufe0f {_escape_html(r.note.strip('() '))}" if r.note else ""
        lines.append(
            f"{icon} <b>{_escape_html(r.formula)}</b>  [{_escape_html(calc_label)}]\n"
            f"   \U0001f9ec {_escape_html(r.method_basis)}\n"
            f"   \u26a1 {_escape_html(r.energy)}\n"
            f"   \U0001f4c2 <code>{_escape_html(r.path)}</code>"
            f"{note}"
        )

    header = f"\U0001f9ea <b>새 계산 감지</b>  ({len(report.new_results)}건)"
    return header + "\n\n" + "\n\n".join(lines)


def _format_overall_summary(runs: list[dict[str, Any]]) -> str:
    """전체 시뮬레이션 통계 한 줄 요약."""
    counts: dict[str, int] = {}
    for r in runs:
        counts[r["status"]] = counts.get(r["status"], 0) + 1

    parts: list[str] = []
    for status in ("running", "retrying", "completed", "failed", "created"):
        n = counts.get(status, 0)
        if n > 0:
            parts.append(f"{_status_icon(status)} {status} {n}")

    total = len(runs)
    summary = " | ".join(parts) if parts else "작업 없음"
    return f"\U0001f4ca <b>전체 현황</b>  (총 {total}건)\n{summary}"


def _build_message(
    runs: list[dict[str, Any]],
    report: ScanReport,
) -> str:
    """전체 텔레그램 메시지를 조합한다."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    header = f"\u2699\ufe0f <b>orca_auto monitor</b>  <code>{now}</code>"
    divider = "\u2500" * 28

    sections: list[str] = [header, divider]

    running = _format_running_section(runs)
    if running:
        sections.append(running)

    dft = _format_dft_section(report)
    if dft:
        sections.append(dft)

    sections.append(divider)
    sections.append(_format_overall_summary(runs))

    return "\n\n".join(sections)


def _run_monitor(cfg: AppConfig) -> int:
    """단일 스캔 실행 및 텔레그램 전송."""
    tg = cfg.telegram
    if not tg.enabled:
        logger.error("Telegram이 설정되지 않았습니다.")
        return 1

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    # 1) 현재 시뮬레이션 수집
    runs = _collect_runs(allowed_root)

    # 2) DFT Monitor 스캔 (새로 변경된 계산 감지)
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

    if report.baseline_seeded:
        logger.info("DFT Monitor baseline seeded (첫 실행). 다음 스캔부터 변경 감지.")

    # 3) 메시지 조합 및 전송
    message = _build_message(runs, report)
    success = send_message(tg, message)

    if success:
        logger.info("텔레그램 알림 전송 완료")
    else:
        logger.error("텔레그램 알림 전송 실패")
        return 1

    return 0


def cmd_monitor(args: Any) -> int:
    cfg = load_config(args.config)
    return _run_monitor(cfg)
