"""DFT calculation file change detection and automatic indexing.

Periodically scans kb_dirs to detect newly completed/changed ORCA calculations
and registers them in the DFT index.

Ported from ollama_bot — removed Telegram/CREST dependencies, switched to synchronous.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .dft_discovery import discover_orca_targets
from .dft_index import DFTIndex
from .orca_parser import parse_orca_output

logger = logging.getLogger(__name__)

FileSignature = tuple[float, int | None, str]


@dataclass
class MonitorResult:
    """Calculation result detected in a single scan."""

    formula: str = ""
    method_basis: str = ""
    energy: str = ""
    status: str = ""
    calc_type: str = ""
    path: str = ""
    note: str = ""


@dataclass
class ParseFailure:
    """Record of a file that failed to parse."""

    path: str
    error: str
    error_type: str


@dataclass
class ScanReport:
    """Scan result summary."""

    new_results: list[MonitorResult] = field(default_factory=list)
    failures: list[ParseFailure] = field(default_factory=list)
    scanned_files: int = 0
    baseline_seeded: bool = False


class DFTMonitor:
    """DFT calculation file change detection and automatic indexing."""

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
        self._last_seen: dict[str, FileSignature] = (
            _load_state(state_file) if state_file else {}
        )
        self._baseline_seeded = bool(self._last_seen)

    def scan(
        self,
        *,
        max_file_size_mb: int = 64,
        recent_completed_window_minutes: int = 60,
    ) -> ScanReport:
        """Detect new/changed ORCA files in kb_dirs and index them."""
        max_bytes = max_file_size_mb * 1024 * 1024
        new_results: list[MonitorResult] = []
        failures: list[ParseFailure] = []
        scanned_signatures: dict[str, FileSignature] = {}
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
                status_override = _run_state_status_override(target.run_state_status)
                signature = _file_signature(spath, state_marker=status_override)
                if signature is None:
                    continue
                scanned_signatures[canonical] = signature

                if not self._baseline_seeded:
                    continue

                last_signature = self._last_seen.get(canonical)
                if last_signature is not None and _same_signature(last_signature, signature):
                    continue

                if canonical in processed_this_scan:
                    continue
                processed_this_scan.add(canonical)

                try:
                    result = parse_orca_output(spath)
                    effective_status = status_override or result.status
                    is_running = effective_status == "running"

                    if is_running:
                        self._last_seen[canonical] = signature
                        state_dirty = True
                    else:
                        success = self._index.upsert_single(
                            spath,
                            status_override=effective_status,
                        )
                        if not success:
                            continue
                        self._last_seen[canonical] = signature
                        state_dirty = True

                    energy_str = (
                        f"E = {result.energy_hartree:.6f} Eh"
                        if result.energy_hartree is not None
                        else "E = N/A"
                    )
                    method_basis = result.method
                    if result.basis_set:
                        method_basis += f"/{result.basis_set}"

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
                    failures.append(ParseFailure(
                        path=_short_path(canonical),
                        error=str(exc),
                        error_type=type(exc).__name__,
                    ))
                    logger.warning(
                        "dft_monitor_parse_error: path=%s error=%s", spath, exc,
                    )

        # Save baseline on first run
        if not self._baseline_seeded:
            self._last_seen.clear()
            self._last_seen.update(scanned_signatures)
            self._baseline_seeded = True
            if self._state_file:
                _save_state(self._state_file, self._last_seen)
            logger.info("dft_monitor_baseline_seeded: file_count=%d", len(self._last_seen))
            return ScanReport(
                failures=failures,
                scanned_files=len(scanned_signatures),
                baseline_seeded=True,
            )

        # Clean up cache for deleted files
        stale_paths = set(self._last_seen) - set(scanned_signatures)
        if stale_paths:
            for stale in stale_paths:
                self._last_seen.pop(stale, None)
            state_dirty = True

        if state_dirty and self._state_file:
            _save_state(self._state_file, self._last_seen)

        logger.info(
            "dft_monitor_scan_complete: scanned=%d new=%d stale_removed=%d",
            len(scanned_signatures), len(new_results), len(stale_paths),
        )

        return ScanReport(
            new_results=new_results,
            failures=failures,
            scanned_files=len(scanned_signatures),
        )


def _short_path(path: str) -> str:
    """Abbreviate a long path to its last 3 segments."""
    parts = path.replace("\\", "/").split("/")
    if len(parts) <= 3:
        return path
    return "/".join(parts[-3:])


def _canonical_path_key(path: str | Path) -> str:
    """Normalize path aliases for the same file into a single canonical key."""
    try:
        return str(Path(path).expanduser().resolve(strict=False))
    except (OSError, RuntimeError, TypeError, ValueError):
        return str(Path(path).expanduser().absolute())


def _run_state_status_override(status: str | None) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"created", "pending", "running", "retrying"}:
        return "running"
    if normalized in {"completed", "failed", "cancelled"}:
        return normalized
    return ""


def _file_signature(path: str | Path, *, state_marker: str = "") -> FileSignature | None:
    try:
        stat_result = Path(path).stat()
    except OSError:
        return None
    return (float(stat_result.st_mtime), int(stat_result.st_size), state_marker)


def _same_signature(previous: FileSignature, current: FileSignature) -> bool:
    prev_mtime, prev_size, prev_state = previous
    curr_mtime, curr_size, curr_state = current
    if prev_mtime != curr_mtime:
        return False
    if prev_state != curr_state:
        return False
    if prev_size is None:
        return True
    return prev_size == curr_size


def _signature_sort_key(signature: FileSignature) -> tuple[float, int, str]:
    mtime, size, state = signature
    return (mtime, -1 if size is None else size, state)


def _load_signature(value: Any) -> FileSignature | None:
    if isinstance(value, (int, float)):
        return (float(value), None, "")
    if not isinstance(value, dict):
        return None

    raw_mtime = value.get("mtime")
    if not isinstance(raw_mtime, (int, float, str)):
        return None
    try:
        mtime = float(raw_mtime)
    except (TypeError, ValueError):
        return None

    raw_size = value.get("size")
    raw_state = value.get("state")
    state = raw_state.strip().lower() if isinstance(raw_state, str) else ""
    if raw_size is None:
        return (mtime, None, state)
    try:
        return (mtime, int(raw_size), state)
    except (TypeError, ValueError):
        return (mtime, None, state)


def _load_state(state_file: str | None) -> dict[str, FileSignature]:
    """Load dft_monitor state from disk."""
    if not state_file:
        return {}
    try:
        with open(state_file, encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        state: dict[str, FileSignature] = {}
        for k, v in raw.items():
            if not isinstance(k, str):
                continue
            normalized_key = _canonical_path_key(k)
            signature = _load_signature(v)
            if signature is None:
                continue
            previous = state.get(normalized_key)
            if previous is None or _signature_sort_key(signature) > _signature_sort_key(previous):
                state[normalized_key] = signature
        return state
    except FileNotFoundError:
        return {}
    except Exception as exc:
        logger.warning("dft_monitor_state_load_failed: path=%s error=%s", state_file, exc)
        return {}


def _save_state(state_file: str | None, signatures: dict[str, FileSignature]) -> None:
    """Atomically save dft_monitor state."""
    if not state_file:
        return
    try:
        path = Path(state_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        payload = {
            key: {"mtime": mtime, "size": size, "state": state}
            for key, (mtime, size, state) in signatures.items()
        }
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        tmp_path.replace(path)
    except Exception as exc:
        logger.warning("dft_monitor_state_save_failed: path=%s error=%s", state_file, exc)
