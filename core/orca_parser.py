"""ORCA quantum chemistry output file (.out) parser.

Extracts key metadata such as energy, method, basis set, convergence status,
and coordinates from ORCA calculation results.

Ported from ollama_bot — complements out_analyzer.py (status determination):
  - orca_parser: detailed data extraction (energy, thermodynamics, formula, coordinates, etc.)
  - out_analyzer: quick status determination (success/failure/retry)
"""

from __future__ import annotations

import hashlib
import os
import re
from collections import Counter
from dataclasses import dataclass, field

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HARTREE_TO_EV = 27.211386245988
HARTREE_TO_KCALMOL = 627.5094740631

# Element symbol -> atomic number order (for chemical formula sorting)
_ELEMENT_ORDER: dict[str, int] = {
    "H": 1, "He": 2, "Li": 3, "Be": 4, "B": 5, "C": 6, "N": 7, "O": 8,
    "F": 9, "Ne": 10, "Na": 11, "Mg": 12, "Al": 13, "Si": 14, "P": 15,
    "S": 16, "Cl": 17, "Ar": 18, "K": 19, "Ca": 20, "Sc": 21, "Ti": 22,
    "V": 23, "Cr": 24, "Mn": 25, "Fe": 26, "Co": 27, "Ni": 28, "Cu": 29,
    "Zn": 30, "Ga": 31, "Ge": 32, "As": 33, "Se": 34, "Br": 35, "Kr": 36,
    "Rb": 37, "Sr": 38, "Y": 39, "Zr": 40, "Nb": 41, "Mo": 42, "Tc": 43,
    "Ru": 44, "Rh": 45, "Pd": 46, "Ag": 47, "Cd": 48, "In": 49, "Sn": 50,
    "Sb": 51, "Te": 52, "I": 53, "Xe": 54, "Cs": 55, "Ba": 56,
    "La": 57, "Ce": 58, "Pr": 59, "Nd": 60, "Pm": 61, "Sm": 62, "Eu": 63,
    "Gd": 64, "Tb": 65, "Dy": 66, "Ho": 67, "Er": 68, "Tm": 69, "Yb": 70,
    "Lu": 71, "Hf": 72, "Ta": 73, "W": 74, "Re": 75, "Os": 76, "Ir": 77,
    "Pt": 78, "Au": 79, "Hg": 80, "Tl": 81, "Pb": 82, "Bi": 83, "Po": 84,
    "At": 85, "Rn": 86,
}

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Input line: "! B3LYP def2-TZVP Opt Freq ..." or "|  1> ! B3LYP ..."
_INPUT_LINE_RE = re.compile(r"^(?:\s*\|\s*\d+>\s*)?!\s*(.+)$", re.MULTILINE)

# Energy
_ENERGY_RE = re.compile(r"FINAL SINGLE POINT ENERGY\s+([-\d.]+)")

# Optimization convergence
_OPT_CONVERGED_RE = re.compile(r"THE OPTIMIZATION HAS CONVERGED")
_OPT_NOT_CONVERGED_RE = re.compile(
    r"ORCA GEOMETRY OPTIMIZATION.*(?:DID NOT CONVERGE|NOT CONVERGED)|"
    r"The optimization did not converge",
    re.IGNORECASE,
)

# Coordinate section (element + xyz)
_COORD_SECTION_RE = re.compile(
    r"CARTESIAN COORDINATES \(ANGSTROEM\)\s*\n"
    r"-+\s*\n"
    r"((?:\s*[A-Z][a-z]?\s+[-\d.]+\s+[-\d.]+\s+[-\d.]+\s*\n)+)",
)
_COORD_LINE_RE = re.compile(r"^\s*([A-Z][a-z]?)\s+[-\d.]+\s+[-\d.]+\s+[-\d.]+", re.MULTILINE)

# Vibrational frequencies
_FREQ_SECTION_RE = re.compile(
    r"VIBRATIONAL FREQUENCIES\s*\n"
    r"-+\s*\n"
    r"([\s\S]*?)(?:\n\s*\n|\n-{20,})",
)
_FREQ_VALUE_RE = re.compile(r"^\s*\d+:\s+([-\d.]+)\s+cm\*\*-1", re.MULTILINE)

# Thermodynamics
_ENTHALPY_RE = re.compile(r"Total (?:E|e)nthalpy\s*\.{3,}\s*([-\d.]+)\s*Eh")
_GIBBS_RE = re.compile(r"Final Gibbs free energy\s*\.{3,}\s*([-\d.]+)\s*Eh")

# Runtime
_RUNTIME_RE = re.compile(
    r"TOTAL RUN TIME:\s*(\d+)\s*days?\s+(\d+)\s*hours?\s+"
    r"(\d+)\s*minutes?\s+(\d+)\s*seconds?",
)

# charge / multiplicity: "* xyz 0 1" or "|  2> * xyz 0 1"
_CHARGE_MULT_RE = re.compile(r"(?:\|\s*\d+>\s*)?\*\s*xyz\s+([-\d]+)\s+(\d+)")

# Optimization cycle header
_OPT_CYCLE_RE = re.compile(r"Geometry Optimization Cycle\s+(\d+)")

# Convergence table items (Energy change, MAX gradient, RMS gradient, MAX step, RMS step)
_CONVERGENCE_ITEM_RE = re.compile(
    r"^\s*(Energy change|MAX gradient|RMS gradient|MAX step|RMS step)"
    r"\s+([-\d.eE+]+)\s+[-\d.eE+]+\s+(YES|NO)\s*$",
    re.MULTILINE,
)

# Normal termination marker
_NORMAL_TERMINATION_RE = re.compile(r"ORCA TERMINATED NORMALLY")
_ERROR_TERMINATION_RE = re.compile(
    r"ORCA\s+finished\s+by\s+error\s+termination|"
    r"aborting the run|"
    r"ended prematurely and may have crashed|"
    r"FATAL ERROR",
    re.IGNORECASE,
)

# Known calculation type keywords (searched in input line)
_CALC_TYPE_KEYWORDS: dict[str, str] = {
    "OPTTS": "ts",
    "TS": "ts",
    "OPT": "opt",
    "FREQ": "freq",
    "MD": "md",
    "COPT": "opt",
    "NEB": "neb",
    "NEB-TS": "neb",
    "NEB-CI": "neb",
    "ZOOM-NEB": "neb",
    "ZOOM-NEB-TS": "neb",
    "ZOOM-NEB-CI": "neb",
    "SCAN": "scan",
    "IRC": "irc",
}

# Known method keywords
_METHOD_KEYWORDS: list[str] = [
    "CCSD(T)", "CCSD", "MP2", "RI-MP2", "DLPNO-CCSD(T)",
    "B3LYP", "PBE0", "PBE", "BP86", "TPSS", "M06-2X", "M06",
    "ωB97X-D3", "wB97X-D3", "ωB97X-D", "wB97X-D", "ωB97X", "wB97X",
    "ωB97M-V", "wB97M-V", "ωB97M-D4", "wB97M-D4",
    "B2PLYP", "REVPBE", "BLYP",
    "CAM-B3LYP", "LC-BLYP", "BHandHLYP",
    "HF", "RHF", "UHF", "ROHF",
    "CASSCF", "NEVPT2", "MRCI",
    "B97-3c", "r2SCAN-3c", "PBEh-3c",
]

# Known basis set keywords
_BASIS_KEYWORDS: list[str] = [
    "def2-QZVPP", "def2-QZVP", "def2-TZVPP", "def2-TZVP",
    "def2-SVP", "def2-SV(P)",
    "ma-def2-TZVPP", "ma-def2-TZVP", "ma-def2-SVP",
    "cc-pVQZ", "cc-pVTZ", "cc-pVDZ",
    "aug-cc-pVQZ", "aug-cc-pVTZ", "aug-cc-pVDZ",
    "6-311++G(d,p)", "6-311+G(d,p)", "6-311G(d,p)",
    "6-311++G(d)", "6-311+G(d)", "6-311G(d)",
    "6-31++G(d,p)", "6-31+G(d,p)", "6-31G(d,p)",
    "6-31++G(d)", "6-31+G(d)", "6-31G(d)",
    "6-31G*", "6-31G**",
    "STO-3G",
]


# ---------------------------------------------------------------------------
# Result data class
# ---------------------------------------------------------------------------

@dataclass
class OrcaResult:
    """Calculation results extracted from an ORCA output file."""

    source_path: str
    calc_type: str = ""
    method: str = ""
    basis_set: str = ""
    charge: int = 0
    multiplicity: int = 1
    formula: str = ""
    n_atoms: int = 0
    energy_hartree: float | None = None
    energy_ev: float | None = None
    energy_kcalmol: float | None = None
    opt_converged: bool | None = None
    has_imaginary_freq: bool | None = None
    lowest_freq_cm1: float | None = None
    enthalpy: float | None = None
    gibbs_energy: float | None = None
    wall_time_seconds: int | None = None
    status: str = "completed"
    file_hash: str = ""
    mtime: float = 0.0
    input_line: str = ""
    elements: list[str] = field(default_factory=list)


@dataclass
class OptStep:
    """Convergence data for a single optimization cycle."""

    cycle: int
    energy_hartree: float
    energy_change: float | None = None
    max_gradient: float | None = None
    rms_gradient: float | None = None
    max_step: float | None = None
    rms_step: float | None = None
    converged_flags: dict[str, bool] = field(default_factory=dict)


@dataclass
class OptProgress:
    """Summary of optimization progress."""

    source_path: str
    formula: str = ""
    method: str = ""
    basis_set: str = ""
    calc_type: str = ""
    steps: list[OptStep] = field(default_factory=list)
    is_converged: bool = False
    is_running: bool = False


# ---------------------------------------------------------------------------
# Parser functions
# ---------------------------------------------------------------------------

def _build_formula(elements: list[str]) -> str:
    """Build a Hill system chemical formula from a list of element symbols."""
    counts = Counter(elements)
    if not counts:
        return ""

    # Hill system: C first, H next, then remaining in alphabetical order
    parts: list[str] = []
    for sym in ("C", "H"):
        if sym in counts:
            parts.append(sym if counts[sym] == 1 else f"{sym}{counts[sym]}")
            del counts[sym]

    for sym in sorted(counts, key=lambda s: _ELEMENT_ORDER.get(s, 999)):
        parts.append(sym if counts[sym] == 1 else f"{sym}{counts[sym]}")

    return "".join(parts)


def _parse_input_line(text: str) -> tuple[str, str, str, list[str]]:
    """Extract calc_type, method, and basis_set from the input line.

    Returns:
        (calc_type, method, basis_set, all_input_tokens)
    """
    matches = _INPUT_LINE_RE.findall(text)
    if not matches:
        return ("sp", "", "", [])

    # There may be multiple input lines — merge them
    all_tokens: list[str] = []
    for line in matches:
        all_tokens.extend(line.strip().split())

    tokens_upper = [t.upper() for t in all_tokens]

    # Determine calc_type
    calc_types: list[str] = []
    for token_upper in tokens_upper:
        for kw, ct in _CALC_TYPE_KEYWORDS.items():
            if token_upper == kw:
                calc_types.append(ct)
    if not calc_types:
        calc_type = "sp"
    elif "opt" in calc_types and "freq" in calc_types:
        calc_type = "opt+freq"
    elif "ts" in calc_types and "freq" in calc_types:
        calc_type = "ts+freq"
    else:
        calc_type = calc_types[0]

    # Determine method — preserve case
    method = ""
    for mk in _METHOD_KEYWORDS:
        for token in all_tokens:
            if token.upper() == mk.upper():
                method = mk
                break
        if method:
            break

    # Determine basis_set — preserve case
    basis_set = ""
    for bk in _BASIS_KEYWORDS:
        for token in all_tokens:
            if token.upper() == bk.upper():
                basis_set = bk
                break
        if basis_set:
            break

    return (calc_type, method, basis_set, all_tokens)


def _parse_coordinates(text: str) -> tuple[list[str], int]:
    """Extract element symbols from the coordinate section.

    Returns:
        (elements, n_atoms)
    """
    # Use the last coordinate section (final coordinates after optimization)
    sections = list(_COORD_SECTION_RE.finditer(text))
    if not sections:
        return ([], 0)

    last_section = sections[-1].group(1)
    elements = _COORD_LINE_RE.findall(last_section)
    return (elements, len(elements))


def _parse_frequencies(text: str) -> tuple[bool | None, float | None]:
    """Extract imaginary frequency status and lowest frequency from the vibrational frequency section.

    Returns:
        (has_imaginary_freq, lowest_freq_cm1)
    """
    section_match = _FREQ_SECTION_RE.search(text)
    if section_match is None:
        return (None, None)

    section = section_match.group(1)
    freq_values = [float(v) for v in _FREQ_VALUE_RE.findall(section)]

    if not freq_values:
        return (None, None)

    # Exclude translational/rotational modes near 0.0 cm^-1 (absolute value < 10 cm^-1)
    real_freqs = [f for f in freq_values if abs(f) > 10.0]
    if not real_freqs:
        return (False, None)

    lowest = min(real_freqs)
    has_imaginary = lowest < 0.0
    return (has_imaginary, lowest)


def _parse_wall_time(text: str) -> int | None:
    """Convert runtime to seconds."""
    m = _RUNTIME_RE.search(text)
    if m is None:
        return None
    days, hours, minutes, seconds = int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4))
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


def _compute_file_hash(file_path: str) -> str:
    """Return the first 16 characters of the file's SHA-256 hash."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _read_orca_text(file_path: str) -> str:
    """Read an ORCA output file with automatic encoding detection."""
    with open(file_path, "rb") as f:
        raw = f.read()

    if not raw:
        return ""

    # Use BOM if present
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")

    # UTF-16LE/BE (without BOM) heuristic: high null byte ratio
    nul_ratio = raw.count(0) / len(raw)
    if nul_ratio > 0.20:
        for enc in ("utf-16-le", "utf-16-be"):
            try:
                return raw.decode(enc)
            except UnicodeDecodeError:
                continue

    # Default UTF-8, fallback with replacement
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def parse_orca_output(file_path: str) -> OrcaResult:
    """Parse an ORCA .out file and return an OrcaResult.

    Args:
        file_path: Path to the ORCA output file

    Returns:
        Extracted calculation results

    Raises:
        FileNotFoundError: If the file does not exist
        UnicodeDecodeError: If there is a file encoding issue
    """
    text = _read_orca_text(file_path)

    result = OrcaResult(source_path=file_path)
    result.mtime = os.path.getmtime(file_path)
    result.file_hash = _compute_file_hash(file_path)

    # Parse input line
    calc_type, method, basis_set, input_tokens = _parse_input_line(text)
    result.calc_type = calc_type
    result.method = method
    result.basis_set = basis_set
    result.input_line = " ".join(input_tokens)

    # charge / multiplicity
    cm_match = _CHARGE_MULT_RE.search(text)
    if cm_match:
        result.charge = int(cm_match.group(1))
        result.multiplicity = int(cm_match.group(2))

    # Coordinates -> elements -> chemical formula
    elements, n_atoms = _parse_coordinates(text)
    result.elements = elements
    result.n_atoms = n_atoms
    result.formula = _build_formula(elements)

    # Energy (use last value — multiple values are printed during optimization)
    energy_matches = _ENERGY_RE.findall(text)
    if energy_matches:
        energy = float(energy_matches[-1])
        result.energy_hartree = energy
        result.energy_ev = energy * HARTREE_TO_EV
        result.energy_kcalmol = energy * HARTREE_TO_KCALMOL

    # Optimization convergence
    if _OPT_CONVERGED_RE.search(text):
        result.opt_converged = True
    elif _OPT_NOT_CONVERGED_RE.search(text):
        result.opt_converged = False

    # Vibrational frequencies
    has_imag, lowest = _parse_frequencies(text)
    result.has_imaginary_freq = has_imag
    result.lowest_freq_cm1 = lowest

    # Thermodynamics
    enthalpy_match = _ENTHALPY_RE.search(text)
    if enthalpy_match:
        result.enthalpy = float(enthalpy_match.group(1))

    gibbs_match = _GIBBS_RE.search(text)
    if gibbs_match:
        result.gibbs_energy = float(gibbs_match.group(1))

    # Runtime
    result.wall_time_seconds = _parse_wall_time(text)

    # Status determination
    if _NORMAL_TERMINATION_RE.search(text):
        if result.opt_converged is False:
            result.status = "failed"
        else:
            result.status = "completed"
    elif _ERROR_TERMINATION_RE.search(text):
        result.status = "failed"
    elif result.wall_time_seconds is not None:
        # TOTAL RUN TIME exists but TERMINATED NORMALLY is missing
        result.status = "failed"
    else:
        result.status = "running"

    return result


def parse_opt_progress(file_path: str) -> OptProgress:
    """Extract per-cycle energy/convergence data from an ORCA optimization output.

    Args:
        file_path: Path to the ORCA output file

    Returns:
        Optimization progress summary

    Raises:
        FileNotFoundError: If the file does not exist
    """
    text = _read_orca_text(file_path)

    # Basic metadata (reuse existing helpers)
    calc_type, method, basis_set, _ = _parse_input_line(text)
    elements, _ = _parse_coordinates(text)
    formula = _build_formula(elements)

    progress = OptProgress(
        source_path=file_path,
        formula=formula,
        method=method,
        basis_set=basis_set,
        calc_type=calc_type,
    )

    # Index cycle positions
    cycle_positions = [
        (m.start(), int(m.group(1)))
        for m in _OPT_CYCLE_RE.finditer(text)
    ]
    if not cycle_positions:
        return progress

    # Index energy positions
    energy_positions = [
        (m.start(), float(m.group(1)))
        for m in _ENERGY_RE.finditer(text)
    ]

    for i, (cycle_start, cycle_num) in enumerate(cycle_positions):
        # Determine text range for this cycle
        cycle_end = (
            cycle_positions[i + 1][0]
            if i + 1 < len(cycle_positions)
            else len(text)
        )
        cycle_text = text[cycle_start:cycle_end]

        # Find the last energy within this cycle's range
        energy: float | None = None
        for epos, eval_ in energy_positions:
            if cycle_start <= epos < cycle_end:
                energy = eval_

        if energy is None:
            continue

        step = OptStep(cycle=cycle_num, energy_hartree=energy)

        # Parse convergence table
        for item_match in _CONVERGENCE_ITEM_RE.finditer(cycle_text):
            name = item_match.group(1)
            value = float(item_match.group(2))
            converged = item_match.group(3) == "YES"

            step.converged_flags[name] = converged

            if name == "Energy change":
                step.energy_change = value
            elif name == "MAX gradient":
                step.max_gradient = value
            elif name == "RMS gradient":
                step.rms_gradient = value
            elif name == "MAX step":
                step.max_step = value
            elif name == "RMS step":
                step.rms_step = value

        progress.steps.append(step)

    # Status determination
    progress.is_converged = bool(_OPT_CONVERGED_RE.search(text))
    progress.is_running = (
        not _NORMAL_TERMINATION_RE.search(text)
        and not _ERROR_TERMINATION_RE.search(text)
        and _parse_wall_time(text) is None
    )

    return progress
