from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple


GEOM_HEADER_RE = re.compile(r"^\s*\*\s+(xyzfile|xyz)\s+(-?\d+)\s+(\d+)(?:\s+(.*))?$", re.IGNORECASE)
BLOCK_START_RE = re.compile(r"^\s*%([A-Za-z0-9_\-]+)")


def rewrite_for_retry(source_inp: Path, target_inp: Path, reaction_dir: Path, step: int) -> List[str]:
    lines = source_inp.read_text(encoding="utf-8", errors="ignore").splitlines()
    actions: List[str] = []

    if step == 1:
        if _ensure_route_keywords(lines, ["TightSCF", "SlowConv"]):
            actions.append("route_add_tightscf_slowconv")
        if _set_block_key_value(lines, "scf", "MaxIter", "300"):
            actions.append("scf_maxiter_300")
    elif step == 2:
        changed = False
        changed |= _set_block_key_value(lines, "geom", "Calc_Hess", "true")
        changed |= _set_block_key_value(lines, "geom", "Recalc_Hess", "5")
        changed |= _set_block_key_value(lines, "geom", "MaxIter", "300")
        if changed:
            actions.append("geom_hessian_and_maxiter")
    elif step == 3:
        # Increase memory and relax convergence for memory/geometry issues
        if _increase_maxcore(lines):
            actions.append("maxcore_increased")
        if _ensure_route_keywords(lines, ["LooseOpt"]):
            actions.append("route_add_looseopt")
    elif step == 4:
        # Combine all strategies: hessian + more memory + relaxed convergence
        changed = False
        changed |= _set_block_key_value(lines, "geom", "Calc_Hess", "true")
        changed |= _set_block_key_value(lines, "geom", "Recalc_Hess", "5")
        changed |= _set_block_key_value(lines, "geom", "MaxIter", "500")
        if changed:
            actions.append("geom_hessian_and_maxiter_500")
        if _increase_maxcore(lines):
            actions.append("maxcore_increased")
        if _ensure_route_keywords(lines, ["TightSCF", "SlowConv"]):
            actions.append("route_add_tightscf_slowconv")
    else:
        actions.append("no_recipe_applied")

    geometry_file = _previous_attempt_xyz(source_inp)
    if geometry_file is None:
        actions.append("no_previous_xyz_file_found")
        geometry_file = _latest_geometry_file(reaction_dir)

    if geometry_file is None:
        actions.append("no_geometry_file_found")
    else:
        if _replace_geometry_with_xyzfile(lines, geometry_file, target_inp.parent):
            actions.append(f"geometry_restart_from_{geometry_file.name}")
        else:
            actions.append("geometry_restart_not_applied")

    target_inp.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return actions


def _find_route_idx(lines: List[str]) -> Optional[int]:
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("!"):
            return idx
    return None


def _ensure_route_keywords(lines: List[str], keywords: List[str]) -> bool:
    idx = _find_route_idx(lines)
    if idx is None:
        lines.insert(0, "! " + " ".join(keywords))
        return True

    current = lines[idx].strip()
    token_set = {tok.upper() for tok in current[1:].split()}
    missing = [kw for kw in keywords if kw.upper() not in token_set]
    if not missing:
        return False
    lines[idx] = current + " " + " ".join(missing)
    return True


def _find_geometry_start(lines: List[str]) -> Optional[int]:
    for idx, line in enumerate(lines):
        if GEOM_HEADER_RE.match(line.strip()):
            return idx
    return None


def _find_block_range(lines: List[str], block_name: str) -> Optional[Tuple[int, int, bool]]:
    name = block_name.lower()
    for i, line in enumerate(lines):
        m = BLOCK_START_RE.match(line)
        if not m:
            continue
        if m.group(1).lower() != name:
            continue
        for j in range(i + 1, len(lines)):
            if lines[j].strip().lower() == "end":
                return i, j, False
        # Block opened but never closed: caller must insert "end".
        return i, len(lines), True
    return None


def _set_block_key_value(lines: List[str], block_name: str, key: str, value: str) -> bool:
    rng = _find_block_range(lines, block_name)
    key_lower = key.lower()

    if rng is None:
        insert_at = _find_geometry_start(lines)
        if insert_at is None:
            insert_at = len(lines)
        block = [f"%{block_name}", f"  {key} {value}", "end", ""]
        lines[insert_at:insert_at] = block
        return True

    start, end, needs_close = rng
    if needs_close:
        lines.insert(end, "end")
    changed = False
    replaced = False
    for i in range(start + 1, end):
        stripped = lines[i].strip()
        if not stripped:
            continue
        tokens = stripped.split()
        if tokens and tokens[0].lower() == key_lower:
            new_line = f"  {key} {value}"
            if lines[i] != new_line:
                lines[i] = new_line
                changed = True
            replaced = True

    if not replaced:
        lines.insert(end, f"  {key} {value}")
        changed = True
    return changed


_MAXCORE_RE = re.compile(r"^\s*%maxcore\s+(\d+)", re.IGNORECASE)
_DEFAULT_MAXCORE_MB = 4000
_MAXCORE_INCREASE_FACTOR = 1.5


def _read_maxcore(lines: List[str]) -> Optional[int]:
    """Read the %maxcore value (in MB) from the input file."""
    for line in lines:
        m = _MAXCORE_RE.match(line)
        if m:
            try:
                return int(m.group(1))
            except ValueError:
                return None
    return None


def _set_maxcore(lines: List[str], value_mb: int) -> bool:
    """Set or update the %maxcore directive."""
    for i, line in enumerate(lines):
        m = _MAXCORE_RE.match(line)
        if m:
            new_line = f"%maxcore {value_mb}"
            if lines[i].strip() == new_line:
                return False
            lines[i] = new_line
            return True
    # Insert before geometry block or at top
    insert_at = _find_route_idx(lines)
    if insert_at is not None:
        insert_at += 1
    else:
        insert_at = 0
    lines.insert(insert_at, f"%maxcore {value_mb}")
    return True


def _increase_maxcore(lines: List[str]) -> bool:
    """Increase %maxcore by 50%, or set a default if not present."""
    current = _read_maxcore(lines)
    if current is None:
        return _set_maxcore(lines, _DEFAULT_MAXCORE_MB)
    new_value = int(current * _MAXCORE_INCREASE_FACTOR)
    if new_value <= current:
        new_value = current + 1000
    return _set_maxcore(lines, new_value)


def _geometry_range(lines: List[str]) -> Optional[Tuple[int, int, int, int]]:
    for start, line in enumerate(lines):
        m = GEOM_HEADER_RE.match(line.strip())
        if not m:
            continue
        geom_type = m.group(1).lower()
        charge = int(m.group(2))
        mult = int(m.group(3))
        if geom_type == "xyzfile":
            return start, start + 1, charge, mult
        end = len(lines)
        for i in range(start + 1, len(lines)):
            if lines[i].strip() == "*":
                end = i + 1
                break
        return start, end, charge, mult
    return None


def _previous_attempt_xyz(source_inp: Path) -> Optional[Path]:
    candidate = source_inp.with_suffix(".xyz")
    if candidate.exists():
        return candidate
    return None


def _latest_geometry_file(reaction_dir: Path) -> Optional[Path]:
    candidates = {p.resolve(): p for p in reaction_dir.glob("*.xyz")}
    if not candidates:
        return None
    return max(candidates.values(), key=lambda p: p.stat().st_mtime_ns)


def _replace_geometry_with_xyzfile(lines: List[str], geom_file: Path, base_dir: Path) -> bool:
    geo = _geometry_range(lines)
    if geo is None:
        return False
    start, end, charge, mult = geo
    geom_resolved = geom_file.resolve()
    base_resolved = base_dir.resolve()
    try:
        rel = geom_resolved.relative_to(base_resolved)
    except ValueError:
        rel = geom_resolved
    ref = str(rel).replace("\\", "/")
    if " " in ref:
        ref = f'"{ref}"'
    lines[start:end] = [f"* xyzfile {charge} {mult} {ref}"]
    return True
