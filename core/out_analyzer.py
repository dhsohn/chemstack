from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable

from .completion_rules import CompletionMode
from .statuses import AnalyzerStatus

logger = logging.getLogger(__name__)


NEG_FREQ_RE = re.compile(r"(^|\s)(-\d+\.\d+)\s*cm\*\*-1", re.IGNORECASE)
VIB_FREQ_HEADER = "VIBRATIONAL FREQUENCIES"

_DEFAULT_TAIL_BYTES = 64 * 1024
_TS_TAIL_BYTES = 256 * 1024
_HEAD_BYTES = 8 * 1024


@dataclass
class OutAnalysis:
    status: AnalyzerStatus
    reason: str
    markers: Dict[str, Any]

    @property
    def recoverable(self) -> bool:
        return self.status not in {
            AnalyzerStatus.COMPLETED,
            AnalyzerStatus.ERROR_MULTIPLICITY_IMPOSSIBLE,
        }


def _detect_encoding(out_path: Path) -> str:
    """Detect if the file uses UTF-16 encoding.

    Kept for backward compatibility with pre-migration .out files that were
    produced on Windows.  New Linux-only runs always produce UTF-8.
    """
    try:
        raw = out_path.read_bytes()[:4]
    except OSError:
        return "utf-8"
    if raw[:2] == b"\xff\xfe":
        return "utf-16-le"
    if raw[:2] == b"\xfe\xff":
        return "utf-16-be"
    # Heuristic: null bytes interleaved with ASCII suggest UTF-16 LE without BOM
    if len(raw) >= 4 and raw[1:2] == b"\x00" and raw[3:4] == b"\x00":
        return "utf-16-le"
    return "utf-8"


def _default_markers(out_path: Path) -> Dict[str, Any]:
    return {
        "out_path": str(out_path),
        "terminated_normally": False,
        "imaginary_frequency_count": 0,
        "irc_marker_found": False,
        "opt_converged": False,
        "scf_error": False,
        "scfgrad_abort": False,
        "multiplicity_impossible": False,
        "disk_io_error": False,
        "generic_error_termination": False,
        "ts_failure_marker": False,
        "total_run_time_seen": False,
    }


def _scan_line_for_markers(line: str, markers: Dict[str, Any]) -> None:
    upper = line.upper()
    if "****ORCA TERMINATED NORMALLY****" in upper:
        markers["terminated_normally"] = True
    if "TOTAL RUN TIME" in upper:
        markers["total_run_time_seen"] = True
    if "IRC PATH SUMMARY" in upper or "IRC-DRV" in upper:
        markers["irc_marker_found"] = True
    if "THE OPTIMIZATION HAS CONVERGED" in upper or "OPTIMIZATION RUN DONE" in upper:
        markers["opt_converged"] = True
    if "SCF NOT CONVERGED" in upper or "SCF CONVERGENCE FAILED" in upper:
        markers["scf_error"] = True
    if "ORCA FINISHED BY ERROR TERMINATION IN SCF GRADIENT" in upper:
        markers["scfgrad_abort"] = True
    if "MULTIPLICITY" in upper and "IMPOSSIBLE" in upper:
        markers["multiplicity_impossible"] = True
    if "COULD NOT WRITE TO DISK" in upper or "NO SPACE LEFT ON DEVICE" in upper:
        markers["disk_io_error"] = True
    if "ORCA FINISHED BY ERROR TERMINATION" in upper:
        markers["generic_error_termination"] = True
    if "NO ACCEPTABLE TS" in upper or "FAILED TO FIND TS" in upper:
        markers["ts_failure_marker"] = True


def _scan_text_for_markers(text: str, markers: Dict[str, Any]) -> None:
    for line in text.splitlines():
        _scan_line_for_markers(line, markers)


def _interpret_markers(markers: Dict[str, Any], mode: CompletionMode) -> OutAnalysis:
    if markers["multiplicity_impossible"]:
        return OutAnalysis(
            status=AnalyzerStatus.ERROR_MULTIPLICITY_IMPOSSIBLE,
            reason="multiplicity_parity_mismatch",
            markers=markers,
        )
    if markers["disk_io_error"]:
        return OutAnalysis(status=AnalyzerStatus.ERROR_DISK_IO, reason="disk_write_failed", markers=markers)
    if markers["scfgrad_abort"]:
        return OutAnalysis(status=AnalyzerStatus.ERROR_SCFGRAD_ABORT, reason="scf_gradient_abort", markers=markers)
    if markers["scf_error"]:
        return OutAnalysis(status=AnalyzerStatus.ERROR_SCF, reason="scf_not_converged", markers=markers)

    if markers["terminated_normally"]:
        if mode.kind == "ts":
            imag_ok = markers["imaginary_frequency_count"] == 1
            irc_ok = (not mode.require_irc) or bool(markers["irc_marker_found"])
            if imag_ok and irc_ok:
                return OutAnalysis(status=AnalyzerStatus.COMPLETED, reason="ts_criteria_met", markers=markers)
            return OutAnalysis(status=AnalyzerStatus.TS_NOT_FOUND, reason="ts_criteria_failed", markers=markers)
        return OutAnalysis(status=AnalyzerStatus.COMPLETED, reason="normal_termination", markers=markers)

    if mode.kind == "ts" and markers["ts_failure_marker"]:
        return OutAnalysis(status=AnalyzerStatus.TS_NOT_FOUND, reason="ts_failure_marker", markers=markers)

    if markers["generic_error_termination"]:
        return OutAnalysis(status=AnalyzerStatus.UNKNOWN_FAILURE, reason="error_termination", markers=markers)

    return OutAnalysis(status=AnalyzerStatus.INCOMPLETE, reason="run_incomplete", markers=markers)


def _read_tail(out_path: Path, encoding: str, nbytes: int) -> str:
    file_size = out_path.stat().st_size
    offset = max(0, file_size - nbytes)
    with out_path.open("rb") as fh:
        if offset > 0:
            fh.seek(offset)
        raw = fh.read()
    return raw.decode(encoding, errors="ignore")


def _read_head(out_path: Path, encoding: str, nbytes: int) -> str:
    with out_path.open("rb") as fh:
        raw = fh.read(nbytes)
    return raw.decode(encoding, errors="ignore")


def _scan_ts_lines_for_imag_count(lines: Iterable[str]) -> tuple[int, bool]:
    total_negative_count = 0
    last_vib_section_negative_count = 0
    saw_vib_section = False
    irc_found = False

    for line in lines:
        upper = line.upper()
        if "IRC PATH SUMMARY" in upper or "IRC-DRV" in upper:
            irc_found = True
        if VIB_FREQ_HEADER in upper:
            saw_vib_section = True
            last_vib_section_negative_count = 0
            continue

        neg_count = sum(1 for _ in NEG_FREQ_RE.finditer(line))
        total_negative_count += neg_count
        if saw_vib_section:
            last_vib_section_negative_count += neg_count

    if saw_vib_section:
        return last_vib_section_negative_count, irc_found
    return total_negative_count, irc_found


def _scan_ts_full_for_imag_count(out_path: Path, encoding: str) -> tuple[int, bool]:
    with out_path.open("r", encoding=encoding, errors="ignore") as handle:
        return _scan_ts_lines_for_imag_count(handle)


def _scan_ts_text_for_imag_count(text: str) -> tuple[int, bool]:
    return _scan_ts_lines_for_imag_count(text.splitlines())


def analyze_output(out_path: Path, mode: CompletionMode) -> OutAnalysis:
    markers = _default_markers(out_path)
    logger.debug("Analyzing output: %s (mode=%s)", out_path, mode.kind)
    if not out_path.exists():
        return OutAnalysis(status=AnalyzerStatus.INCOMPLETE, reason="output_missing", markers=markers)

    try:
        encoding = _detect_encoding(out_path)
        file_size = out_path.stat().st_size
        tail_bytes = _TS_TAIL_BYTES if mode.kind == "ts" else _DEFAULT_TAIL_BYTES
        full_text: str | None = None

        if file_size <= tail_bytes:
            # Small file: read entirely (identical to previous behaviour).
            with out_path.open("r", encoding=encoding, errors="ignore") as handle:
                full_text = handle.read()
            _scan_text_for_markers(full_text, markers)
        else:
            # Large file: tail-first strategy.
            tail_text = _read_tail(out_path, encoding, tail_bytes)
            _scan_text_for_markers(tail_text, markers)

            # Head scan: multiplicity_impossible typically appears near the top.
            if not markers["multiplicity_impossible"]:
                head_text = _read_head(out_path, encoding, _HEAD_BYTES)
                head_markers: Dict[str, Any] = _default_markers(out_path)
                _scan_text_for_markers(head_text, head_markers)
                if head_markers["multiplicity_impossible"]:
                    markers["multiplicity_impossible"] = True

        # TS mode needs exact imaginary frequency count from the final vibration block.
        if mode.kind == "ts" and markers["terminated_normally"]:
            if full_text is None:
                imag_count, irc_found = _scan_ts_full_for_imag_count(out_path, encoding)
            else:
                imag_count, irc_found = _scan_ts_text_for_imag_count(full_text)
            markers["imaginary_frequency_count"] = imag_count
            if irc_found:
                markers["irc_marker_found"] = True

    except OSError:
        return OutAnalysis(status=AnalyzerStatus.INCOMPLETE, reason="output_read_error", markers=markers)

    return _interpret_markers(markers, mode)
