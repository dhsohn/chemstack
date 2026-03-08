"""DFT 인덱싱 대상 파일 탐색 유틸."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ORCA_EXTENSIONS = {".out"}


@dataclass
class DiscoveredTarget:
    """탐색된 ORCA 출력 파일과 run_state 메타데이터."""

    path: Path
    run_state_status: str = ""


def discover_orca_targets(
    kb_path: Path,
    *,
    max_bytes: int,
    recent_completed_window_minutes: int | None = None,
) -> list[DiscoveredTarget]:
    """인덱싱 대상 ORCA 출력 파일 목록을 반환한다.

    규칙:
    1) 기본(`orca_runs` 포함): run_state.json 기준
       - status는 run_state.status만 신뢰
       - 출력 파일은 run_state.json 폴더의 최신 .out 추적
    2) `orca_outputs`:
       - run_state.json만 사용 (run_report는 무시)
       - status는 run_state.status만 신뢰
       - 출력 파일은 run_state.json 폴더의 최신 .out 추적
    """
    parts = {part.lower() for part in kb_path.parts}
    if "orca_outputs" in parts:
        return _discover_orca_outputs_targets(
            kb_path=kb_path,
            max_bytes=max_bytes,
            recent_completed_window_minutes=recent_completed_window_minutes,
        )
    return _discover_orca_runs_targets(
        kb_path=kb_path,
        max_bytes=max_bytes,
    )


def _discover_orca_outputs_targets(
    *,
    kb_path: Path,
    max_bytes: int,
    recent_completed_window_minutes: int | None,
) -> list[DiscoveredTarget]:
    targets: dict[str, DiscoveredTarget] = {}
    now_utc = datetime.now(timezone.utc)

    # run_state 전용 정책: run_report는 완전히 무시한다.
    for state_path in kb_path.rglob("run_state.json"):
        data = _load_report_json(state_path)
        if not isinstance(data, dict):
            continue
        status = str(data.get("status", "")).strip().lower()
        if status != "completed":
            continue
        resolved = _find_latest_out_in_dir(state_path.parent)
        if resolved is None:
            continue
        if not _is_recent_completed_output(
            data=data,
            output_path=resolved,
            now_utc=now_utc,
            recent_completed_window_minutes=recent_completed_window_minutes,
        ):
            continue
        _add_if_valid_target(
            resolved=resolved, max_bytes=max_bytes, targets=targets,
            run_state_status=status,
        )

    return sorted(targets.values(), key=lambda t: str(t.path))


def _discover_orca_runs_targets(
    *,
    kb_path: Path,
    max_bytes: int,
) -> list[DiscoveredTarget]:
    targets: dict[str, DiscoveredTarget] = {}

    for state_path in kb_path.rglob("run_state.json"):
        data = _load_report_json(state_path)
        if not isinstance(data, dict):
            continue

        status = str(data.get("status", "")).strip().lower()

        # 경로 정보(reaction_dir, last_out_path)는 실행 환경 차이로 오염될 수 있어
        # run_state.json이 위치한 폴더의 최신 .out만 신뢰한다.
        resolved = _find_latest_out_in_dir(state_path.parent)
        if resolved is None:
            continue
        _add_if_valid_target(
            resolved=resolved, max_bytes=max_bytes, targets=targets,
            run_state_status=status,
        )

    return sorted(targets.values(), key=lambda t: str(t.path))


def _find_latest_out_in_dir(directory: Path) -> Path | None:
    if not directory.is_dir():
        return None
    latest: tuple[float, Path] | None = None
    for candidate in directory.glob("*.out"):
        if not candidate.is_file():
            continue
        try:
            mtime = candidate.stat().st_mtime
        except OSError:
            continue
        if latest is None or mtime > latest[0]:
            latest = (mtime, candidate)
    return latest[1] if latest is not None else None


def _is_recent_completed_output(
    *,
    data: dict[str, Any],
    output_path: Path,
    now_utc: datetime,
    recent_completed_window_minutes: int | None,
) -> bool:
    if recent_completed_window_minutes is None:
        return True
    if recent_completed_window_minutes < 0:
        return True

    final_result = data.get("final_result")
    final_result_dict = final_result if isinstance(final_result, dict) else {}

    top_status = str(data.get("status", "")).strip().lower()
    final_status = str(final_result_dict.get("status", "")).strip().lower()
    if "completed" not in {top_status, final_status}:
        return False

    window = timedelta(minutes=recent_completed_window_minutes)
    allowed_future_skew = timedelta(minutes=5)
    completed_at_raw = final_result_dict.get("completed_at")
    completed_at = _parse_iso_datetime_utc(completed_at_raw)
    if completed_at is not None:
        age = now_utc - completed_at
        return (-allowed_future_skew) <= age <= window

    try:
        mtime_dt = datetime.fromtimestamp(output_path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return False
    age = now_utc - mtime_dt
    return (-allowed_future_skew) <= age <= window


def _parse_iso_datetime_utc(value: Any) -> datetime | None:
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


def _add_if_valid_target(
    *,
    resolved: Path,
    max_bytes: int,
    targets: dict[str, DiscoveredTarget],
    run_state_status: str = "",
) -> None:
    if resolved.suffix.lower() not in _ORCA_EXTENSIONS:
        return
    try:
        if resolved.stat().st_size > max_bytes:
            return
    except OSError:
        return
    targets[str(resolved)] = DiscoveredTarget(
        path=resolved, run_state_status=run_state_status,
    )


def _load_report_json(report_path: Path) -> dict[str, Any] | None:
    try:
        with open(report_path, encoding="utf-8") as f:
            loaded = json.load(f)
        return loaded if isinstance(loaded, dict) else None
    except Exception as exc:
        logger.warning(
            "dft_run_report_parse_failed: path=%s error=%s",
            report_path, exc,
        )
        return None
