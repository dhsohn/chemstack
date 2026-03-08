"""DFT 계산 파일 변경 감지 및 자동 인덱싱.

kb_dirs를 주기적으로 스캔하여 새로 완료/변경된 ORCA 계산을 감지하고,
DFT 인덱스에 등록한다.

ollama_bot에서 이식됨 — Telegram/CREST 의존성 제거, 동기 전환.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

from core.dft_discovery import discover_orca_targets
from core.dft_index import DFTIndex
from core.orca_parser import parse_orca_output

logger = logging.getLogger(__name__)


@dataclass
class MonitorResult:
    """단일 스캔에서 감지된 계산 결과."""

    formula: str = ""
    method_basis: str = ""
    energy: str = ""
    status: str = ""
    calc_type: str = ""
    path: str = ""
    note: str = ""


@dataclass
class ScanReport:
    """스캔 결과 요약."""

    new_results: list[MonitorResult] = field(default_factory=list)
    scanned_files: int = 0
    baseline_seeded: bool = False


class DFTMonitor:
    """DFT 계산 파일 변경 감지 및 자동 인덱싱."""

    def __init__(
        self,
        dft_index: DFTIndex,
        kb_dirs: list[str],
        *,
        state_file: str | None = None,
    ) -> None:
        self._index = dft_index
        self._kb_dirs = kb_dirs
        self._state_file = state_file
        self._last_mtimes: dict[str, float] = (
            _load_state(state_file) if state_file else {}
        )
        self._baseline_seeded = bool(self._last_mtimes)

    def scan(
        self,
        *,
        max_file_size_mb: int = 64,
        recent_completed_window_minutes: int = 60,
    ) -> ScanReport:
        """kb_dirs에서 새로/변경된 ORCA 파일을 감지하여 인덱싱한다."""
        max_bytes = max_file_size_mb * 1024 * 1024
        new_results: list[MonitorResult] = []
        scanned_mtimes: dict[str, float] = {}
        processed_this_scan: set[str] = set()
        state_dirty = False

        for kb_dir in self._kb_dirs:
            kb_path = Path(kb_dir)
            if not kb_path.is_dir():
                logger.warning("dft_monitor_kb_dir_missing: %s", kb_dir)
                continue

            for target in discover_orca_targets(
                kb_path,
                max_bytes=max_bytes,
                recent_completed_window_minutes=recent_completed_window_minutes,
            ):
                spath = str(target.path)
                canonical = _canonical_path_key(spath)
                try:
                    current_mtime = os.path.getmtime(spath)
                except OSError:
                    continue
                scanned_mtimes[canonical] = current_mtime

                if not self._baseline_seeded:
                    continue

                last_mtime = self._last_mtimes.get(canonical)
                if last_mtime is not None and current_mtime <= last_mtime:
                    continue

                if canonical in processed_this_scan:
                    continue
                processed_this_scan.add(canonical)

                try:
                    result = parse_orca_output(spath)

                    is_running = (
                        result.status == "running"
                        or target.run_state_status == "running"
                    )

                    if is_running:
                        self._last_mtimes[canonical] = current_mtime
                        state_dirty = True
                    else:
                        success = self._index.upsert_single(spath)
                        if not success:
                            continue
                        self._last_mtimes[canonical] = current_mtime
                        state_dirty = True

                    energy_str = (
                        f"E = {result.energy_hartree:.6f} Eh"
                        if result.energy_hartree is not None
                        else "E = N/A"
                    )
                    method_basis = result.method
                    if result.basis_set:
                        method_basis += f"/{result.basis_set}"

                    effective_status = "running" if is_running else result.status

                    notes: list[str] = []
                    if result.opt_converged is False:
                        notes.append("NOT CONVERGED")
                    if result.has_imaginary_freq:
                        notes.append("imaginary freq")
                    note_str = f" ({', '.join(notes)})" if notes else ""

                    new_results.append(MonitorResult(
                        formula=result.formula or "unknown",
                        method_basis=method_basis or "unknown",
                        energy=energy_str,
                        status=effective_status,
                        calc_type=result.calc_type,
                        path=_short_path(canonical),
                        note=note_str,
                    ))

                    logger.info(
                        "dft_monitor_new_calc: path=%s formula=%s method=%s status=%s",
                        spath, result.formula, result.method, effective_status,
                    )
                except Exception as exc:
                    logger.warning(
                        "dft_monitor_parse_error: path=%s error=%s", spath, exc,
                    )

        # 첫 실행 baseline 저장
        if not self._baseline_seeded:
            self._last_mtimes.clear()
            self._last_mtimes.update(scanned_mtimes)
            self._baseline_seeded = True
            if self._state_file:
                _save_state(self._state_file, self._last_mtimes)
            logger.info("dft_monitor_baseline_seeded: file_count=%d", len(self._last_mtimes))
            return ScanReport(
                scanned_files=len(scanned_mtimes),
                baseline_seeded=True,
            )

        # 삭제된 파일 캐시 정리
        stale_paths = set(self._last_mtimes) - set(scanned_mtimes)
        if stale_paths:
            for stale in stale_paths:
                self._last_mtimes.pop(stale, None)
            state_dirty = True

        if state_dirty and self._state_file:
            _save_state(self._state_file, self._last_mtimes)

        logger.info(
            "dft_monitor_scan_complete: scanned=%d new=%d stale_removed=%d",
            len(scanned_mtimes), len(new_results), len(stale_paths),
        )

        return ScanReport(
            new_results=new_results,
            scanned_files=len(scanned_mtimes),
        )


def _short_path(path: str) -> str:
    """긴 경로를 마지막 3개 세그먼트로 축약한다."""
    parts = path.replace("\\", "/").split("/")
    if len(parts) <= 3:
        return path
    return "/".join(parts[-3:])


def _canonical_path_key(path: str | Path) -> str:
    """같은 파일의 경로 alias를 하나의 canonical key로 정규화한다."""
    try:
        return str(Path(path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError, TypeError, ValueError):
        return str(Path(path).expanduser().absolute())


def _load_state(state_file: str | None) -> dict[str, float]:
    """디스크에 저장된 dft_monitor 상태를 로드한다."""
    if not state_file:
        return {}
    try:
        with open(state_file, encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        state: dict[str, float] = {}
        for k, v in raw.items():
            if isinstance(k, str):
                try:
                    normalized_key = _canonical_path_key(k)
                    value = float(v)
                except (TypeError, ValueError):
                    continue
                previous = state.get(normalized_key)
                if previous is None or value > previous:
                    state[normalized_key] = value
        return state
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("dft_monitor_state_load_failed: path=%s error=%s", state_file, exc)
        return {}


def _save_state(state_file: str | None, mtimes: dict[str, float]) -> None:
    """dft_monitor 상태를 원자적으로 저장한다."""
    if not state_file:
        return
    try:
        path = Path(state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(mtimes, f, ensure_ascii=False)
        tmp_path.replace(path)
    except Exception as exc:
        logger.warning("dft_monitor_state_save_failed: path=%s error=%s", state_file, exc)
