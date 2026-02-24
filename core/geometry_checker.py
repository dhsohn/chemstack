from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .completion_rules import detect_completion_mode
from .out_analyzer import (
    NEG_FREQ_RE,
    _detect_encoding,
    _read_tail,
    analyze_output,
)
from .state_store import load_state

logger = logging.getLogger(__name__)

S2_RE = re.compile(
    r"Expectation value of <S\*\*2>\s*:\s*([-+]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
)
SCF_ENERGY_CHANGE_RE = re.compile(
    r"Last Energy change\s+\.\.\.\s+([-+]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)"
)
MULT_RE = re.compile(r"Multiplicity\s+Mult\s*\.\.\.\.\s*(\d+)")
INP_XYZ_MULT_RE = re.compile(r"^\s*\*\s*(?:xyz|xyzfile)\s+[-+]?\d+\s+(\d+)\b", re.IGNORECASE)
INP_COORDS_MULT_RE = re.compile(r"^\s*mult\s+(\d+)\b", re.IGNORECASE)
XYZ_ATOM_RE = re.compile(
    r"^\s*([A-Z][a-z]?)\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s+([-+]?\d+(?:\.\d+)?)\s*$"
)

_SHORT_CONTACT_THRESHOLD = 0.5
_FRAGMENTATION_NN_THRESHOLD = 3.5
_SCF_ENERGY_CHANGE_THRESHOLD = 1e-6
_SPIN_CONTAMINATION_THRESHOLD = 0.1
_TAIL_BYTES = 256 * 1024


@dataclass
class CheckItem:
    check_name: str
    severity: str  # "ok" | "warning" | "error"
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckResult:
    reaction_dir: str
    run_id: str
    job_type: str  # "opt" | "ts"
    overall: str  # "pass" | "warn" | "fail"
    checks: List[CheckItem] = field(default_factory=list)


@dataclass
class CheckSkipReason:
    reaction_dir: str
    reason: str


def _resolve_inp_path(
    state: Dict[str, Any], reaction_dir: Path, out_path: Path
) -> Optional[Path]:
    selected_inp = state.get("selected_inp")
    if isinstance(selected_inp, str) and selected_inp.strip():
        p = Path(selected_inp)
        if p.exists():
            return p

    stem = out_path.stem
    same_stem_inp = reaction_dir / f"{stem}.inp"
    if same_stem_inp.exists():
        return same_stem_inp

    inps = sorted(reaction_dir.glob("*.inp"), key=lambda x: x.stat().st_mtime, reverse=True)
    for inp in inps:
        return inp
    return None


def _find_out_file(
    reaction_dir: Path, state: Dict[str, Any]
) -> Optional[Path]:
    final_result = state.get("final_result")
    if isinstance(final_result, dict):
        last_out = final_result.get("last_out_path")
        if isinstance(last_out, str) and last_out.strip():
            p = Path(last_out)
            if p.exists():
                return p

    outs = sorted(reaction_dir.glob("*.out"), key=lambda x: x.stat().st_mtime, reverse=True)
    for out in outs:
        return out
    return None


def _find_xyz_file(reaction_dir: Path, out_path: Path) -> Optional[Path]:
    stem = out_path.stem
    same_stem = reaction_dir / f"{stem}.xyz"
    if same_stem.exists():
        return same_stem

    xyzs = sorted(reaction_dir.glob("*.xyz"), key=lambda x: x.stat().st_mtime, reverse=True)
    for xyz in xyzs:
        if not xyz.name.endswith("_trj.xyz"):
            return xyz
    return None


def _parse_xyz_atoms(xyz_path: Path) -> List[Tuple[str, float, float, float]]:
    atoms: List[Tuple[str, float, float, float]] = []
    lines = xyz_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    for line in lines:
        m = XYZ_ATOM_RE.match(line)
        if m:
            atoms.append((m.group(1), float(m.group(2)), float(m.group(3)), float(m.group(4))))
    return atoms


def _compute_pair_distances(
    atoms: List[Tuple[str, float, float, float]],
) -> List[Tuple[int, int, float]]:
    pairs: List[Tuple[int, int, float]] = []
    n = len(atoms)
    for i in range(n):
        for j in range(i + 1, n):
            dx = atoms[i][1] - atoms[j][1]
            dy = atoms[i][2] - atoms[j][2]
            dz = atoms[i][3] - atoms[j][3]
            d = math.sqrt(dx * dx + dy * dy + dz * dz)
            pairs.append((i, j, d))
    return pairs


def _nearest_neighbor_distances(
    atoms: List[Tuple[str, float, float, float]],
) -> List[float]:
    n = len(atoms)
    if n < 2:
        return [0.0] * n
    nn: List[float] = [float("inf")] * n
    for i in range(n):
        for j in range(i + 1, n):
            dx = atoms[i][1] - atoms[j][1]
            dy = atoms[i][2] - atoms[j][2]
            dz = atoms[i][3] - atoms[j][3]
            d = math.sqrt(dx * dx + dy * dy + dz * dz)
            if d < nn[i]:
                nn[i] = d
            if d < nn[j]:
                nn[j] = d
    return nn


def _check_imaginary_frequencies_opt(out_text: str) -> CheckItem:
    count = sum(1 for _ in NEG_FREQ_RE.finditer(out_text))
    if count > 0:
        return CheckItem(
            check_name="imaginary_frequencies_opt",
            severity="warning",
            message=f"Opt calculation has {count} imaginary frequency(ies)",
            details={"count": count},
        )
    return CheckItem(
        check_name="imaginary_frequencies_opt",
        severity="ok",
        message="No imaginary frequencies in opt",
    )


def _check_ts_frequency_count(imaginary_count: int) -> CheckItem:
    try:
        count = max(0, int(imaginary_count))
    except (TypeError, ValueError):
        count = 0
    if count == 1:
        return CheckItem(
            check_name="ts_frequency_count",
            severity="ok",
            message="TS has exactly 1 imaginary frequency",
            details={"count": count},
        )
    return CheckItem(
        check_name="ts_frequency_count",
        severity="error",
        message=f"TS expected 1 imaginary frequency, found {count}",
        details={"count": count},
    )


def _check_scf_convergence(out_text: str, analyzer_status: str) -> CheckItem:
    if analyzer_status == "error_scf":
        return CheckItem(
            check_name="scf_convergence",
            severity="error",
            message="SCF convergence failed (analyzer flagged error_scf)",
        )
    matches = list(SCF_ENERGY_CHANGE_RE.finditer(out_text))
    if not matches:
        return CheckItem(
            check_name="scf_convergence",
            severity="warning",
            message="Could not parse last SCF energy change from output",
        )
    delta_e = abs(float(matches[-1].group(1)))
    if delta_e > _SCF_ENERGY_CHANGE_THRESHOLD:
        return CheckItem(
            check_name="scf_convergence",
            severity="warning",
            message=f"Last SCF energy change |{delta_e:.2e}| > {_SCF_ENERGY_CHANGE_THRESHOLD:.0e}",
            details={"delta_e": delta_e},
        )
    return CheckItem(
        check_name="scf_convergence",
        severity="ok",
        message="SCF convergence looks fine",
        details={"delta_e": delta_e},
    )


def _check_short_contacts(
    atoms: List[Tuple[str, float, float, float]],
) -> CheckItem:
    pairs = _compute_pair_distances(atoms)
    short = [(i, j, d) for i, j, d in pairs if d < _SHORT_CONTACT_THRESHOLD]
    if short:
        details_list = [
            {"i": i, "j": j, "symbol_i": atoms[i][0], "symbol_j": atoms[j][0], "distance": round(d, 4)}
            for i, j, d in short
        ]
        return CheckItem(
            check_name="short_contacts",
            severity="error",
            message=f"{len(short)} atom pair(s) with distance < {_SHORT_CONTACT_THRESHOLD} A",
            details={"pairs": details_list},
        )
    return CheckItem(
        check_name="short_contacts",
        severity="ok",
        message="No short contacts detected",
    )


def _check_fragmentation_hint(
    atoms: List[Tuple[str, float, float, float]],
) -> CheckItem:
    if len(atoms) < 2:
        return CheckItem(
            check_name="fragmentation_hint",
            severity="ok",
            message="Too few atoms to check fragmentation",
        )
    nn_dists = _nearest_neighbor_distances(atoms)
    max_nn = max(nn_dists)
    max_idx = nn_dists.index(max_nn)
    if max_nn > _FRAGMENTATION_NN_THRESHOLD:
        return CheckItem(
            check_name="fragmentation_hint",
            severity="warning",
            message=f"Atom {max_idx} ({atoms[max_idx][0]}) has nearest neighbor at {max_nn:.2f} A (> {_FRAGMENTATION_NN_THRESHOLD} A)",
            details={"atom_index": max_idx, "symbol": atoms[max_idx][0], "nn_distance": round(max_nn, 4)},
        )
    return CheckItem(
        check_name="fragmentation_hint",
        severity="ok",
        message="No fragmentation hint",
    )


def _check_spin_contamination(out_text: str, multiplicity: int) -> CheckItem:
    matches = list(S2_RE.finditer(out_text))
    if not matches:
        return CheckItem(
            check_name="spin_contamination",
            severity="ok",
            message="No <S**2> value found (may be restricted calculation)",
        )
    s2_actual = float(matches[-1].group(1))
    s = (multiplicity - 1) / 2.0
    s2_expected = s * (s + 1)
    diff = abs(s2_actual - s2_expected)
    if diff > _SPIN_CONTAMINATION_THRESHOLD:
        return CheckItem(
            check_name="spin_contamination",
            severity="warning",
            message=f"<S**2> = {s2_actual:.4f}, expected {s2_expected:.4f} (diff={diff:.4f} > {_SPIN_CONTAMINATION_THRESHOLD})",
            details={"s2_actual": s2_actual, "s2_expected": s2_expected, "diff": round(diff, 4)},
        )
    return CheckItem(
        check_name="spin_contamination",
        severity="ok",
        message=f"<S**2> = {s2_actual:.4f}, expected {s2_expected:.4f}",
        details={"s2_actual": s2_actual, "s2_expected": s2_expected, "diff": round(diff, 4)},
    )


def _parse_multiplicity_from_out(out_text: str) -> int:
    matches = list(MULT_RE.finditer(out_text))
    if matches:
        return int(matches[-1].group(1))
    return 1


def _parse_multiplicity_from_inp(inp_path: Path) -> Optional[int]:
    in_coords_block = False
    try:
        with inp_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                xyz_match = INP_XYZ_MULT_RE.match(line)
                if xyz_match:
                    return int(xyz_match.group(1))

                stripped = line.strip()
                lowered = stripped.lower()
                if lowered.startswith("%coords"):
                    in_coords_block = True
                    continue

                if in_coords_block and lowered == "end":
                    in_coords_block = False
                    continue

                if in_coords_block:
                    coords_match = INP_COORDS_MULT_RE.match(stripped)
                    if coords_match:
                        return int(coords_match.group(1))
    except OSError:
        return None
    return None


def check_single(
    reaction_dir: Path,
) -> Tuple[Optional[CheckResult], Optional[CheckSkipReason]]:
    rd_str = str(reaction_dir)

    state = load_state(reaction_dir)
    if state is None or not isinstance(state, dict):
        return None, CheckSkipReason(reaction_dir=rd_str, reason="state_missing_or_invalid")

    if state.get("status") != "completed":
        return None, CheckSkipReason(reaction_dir=rd_str, reason="not_completed")

    out_path = _find_out_file(reaction_dir, state)
    if out_path is None:
        return None, CheckSkipReason(reaction_dir=rd_str, reason="output_missing")

    inp_path = _resolve_inp_path(state, reaction_dir, out_path)
    if inp_path is None:
        return None, CheckSkipReason(reaction_dir=rd_str, reason="inp_missing_for_mode_detection")

    mode = detect_completion_mode(inp_path)
    job_type = mode.kind

    run_id = state.get("run_id", "unknown")

    analysis = analyze_output(out_path, mode)
    encoding = _detect_encoding(out_path)
    out_text = _read_tail(out_path, encoding, _TAIL_BYTES)

    xyz_path = _find_xyz_file(reaction_dir, out_path)
    if xyz_path is None:
        return None, CheckSkipReason(reaction_dir=rd_str, reason="xyz_missing")

    atoms = _parse_xyz_atoms(xyz_path)
    if not atoms:
        return None, CheckSkipReason(reaction_dir=rd_str, reason="xyz_missing")

    # Analyzer status from state
    final_result = state.get("final_result")
    analyzer_status = ""
    if isinstance(final_result, dict):
        analyzer_status = str(final_result.get("analyzer_status", "") or "")
    if not analyzer_status:
        analyzer_status = analysis.status

    checks: List[CheckItem] = []

    # Frequency checks
    if job_type == "opt":
        checks.append(_check_imaginary_frequencies_opt(out_text))
    else:
        imag_count_raw = analysis.markers.get("imaginary_frequency_count", 0)
        checks.append(_check_ts_frequency_count(imag_count_raw))

    # SCF convergence
    checks.append(_check_scf_convergence(out_text, analyzer_status))

    # Geometry checks
    checks.append(_check_short_contacts(atoms))
    checks.append(_check_fragmentation_hint(atoms))

    # Spin contamination
    multiplicity = _parse_multiplicity_from_inp(inp_path)
    if multiplicity is None:
        multiplicity = _parse_multiplicity_from_out(out_text)
    checks.append(_check_spin_contamination(out_text, multiplicity))

    # Determine overall
    severities = [c.severity for c in checks]
    if "error" in severities:
        overall = "fail"
    elif "warning" in severities:
        overall = "warn"
    else:
        overall = "pass"

    return CheckResult(
        reaction_dir=rd_str,
        run_id=run_id,
        job_type=job_type,
        overall=overall,
        checks=checks,
    ), None


def check_root_scan(
    root: Path,
) -> Tuple[List[CheckResult], List[CheckSkipReason]]:
    results: List[CheckResult] = []
    skips: List[CheckSkipReason] = []

    if not root.is_dir():
        return results, skips

    for child in sorted(root.rglob("run_state.json")):
        reaction_dir = child.parent
        result, skip = check_single(reaction_dir)
        if result is not None:
            results.append(result)
        if skip is not None:
            skips.append(skip)

    return results, skips
