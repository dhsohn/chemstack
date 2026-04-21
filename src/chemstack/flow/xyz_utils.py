from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


_ENERGY_PATTERNS = (
    re.compile(r"energy:\s*([-+]?\d+(?:\.\d+)?)", re.IGNORECASE),
    re.compile(r"\bE\s+([-+]?\d+(?:\.\d+)?)", re.IGNORECASE),
)


@dataclass(frozen=True)
class XYZFrame:
    index: int
    natoms: int
    comment: str
    atom_lines: tuple[str, ...]
    energy: float | None

    def render(self) -> str:
        lines = [str(self.natoms), self.comment, *self.atom_lines]
        return "\n".join(lines) + "\n"


def _parse_energy(comment: str) -> float | None:
    for pattern in _ENERGY_PATTERNS:
        match = pattern.search(comment)
        if not match:
            continue
        try:
            return float(match.group(1))
        except ValueError:
            continue
    return None


def _line_has_xyz_tokens(line: str) -> bool:
    tokens = line.split()
    if len(tokens) < 4:
        return False
    try:
        float(tokens[1])
        float(tokens[2])
        float(tokens[3])
    except ValueError:
        return False
    return True


def load_xyz_frames(path: str | Path) -> tuple[XYZFrame, ...]:
    xyz_path = Path(path).expanduser().resolve()
    if not xyz_path.exists() or not xyz_path.is_file():
        return ()
    try:
        raw_lines = xyz_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return ()
    frames: list[XYZFrame] = []
    index = 0
    cursor = 0
    while cursor < len(raw_lines):
        while cursor < len(raw_lines) and not raw_lines[cursor].strip():
            cursor += 1
        if cursor >= len(raw_lines):
            break
        try:
            natoms = int(raw_lines[cursor].strip())
        except ValueError:
            return ()
        if natoms <= 0:
            return ()
        if cursor + 2 + natoms > len(raw_lines):
            return ()
        comment = raw_lines[cursor + 1]
        atom_lines = tuple(raw_lines[cursor + 2 : cursor + 2 + natoms])
        if len(atom_lines) != natoms or any(not _line_has_xyz_tokens(line) for line in atom_lines):
            return ()
        index += 1
        frames.append(
            XYZFrame(
                index=index,
                natoms=natoms,
                comment=comment,
                atom_lines=atom_lines,
                energy=_parse_energy(comment),
            )
        )
        cursor += 2 + natoms
    return tuple(frames)


def has_xyz_geometry(path: str | Path) -> bool:
    return bool(load_xyz_frames(path))


def load_xyz_atom_sequence(path: str | Path) -> tuple[str, ...]:
    xyz_path = Path(path).expanduser().resolve()
    frames = load_xyz_frames(xyz_path)
    if not frames:
        raise ValueError(f"Invalid or empty XYZ file: {xyz_path}")
    if len(frames) != 1:
        raise ValueError(f"Expected a single-geometry XYZ file: {xyz_path}")
    return tuple(line.split()[0] for line in frames[0].atom_lines)


def choose_orca_geometry_frame(path: str | Path, *, candidate_kind: str = "") -> tuple[XYZFrame | None, dict[str, object]]:
    xyz_path = Path(path).expanduser().resolve()
    frames = load_xyz_frames(xyz_path)
    metadata: dict[str, object] = {
        "source_artifact_path": str(xyz_path),
        "frame_count": len(frames),
        "candidate_kind": str(candidate_kind).strip(),
        "source_size_bytes": int(xyz_path.stat().st_size) if xyz_path.exists() and xyz_path.is_file() else 0,
    }
    if not frames:
        metadata["selection_reason"] = "invalid_or_empty_xyz"
        return None, metadata
    normalized_kind = str(candidate_kind).strip().lower()
    if normalized_kind == "ts_guess" and len(frames) != 1:
        metadata["selection_reason"] = "ts_guess_requires_single_frame"
        return None, metadata
    if len(frames) == 1:
        frame = frames[0]
        metadata["selected_frame_index"] = frame.index
        metadata["selection_reason"] = "single_frame"
        if frame.energy is not None:
            metadata["selected_frame_energy"] = frame.energy
        return frame, metadata

    if normalized_kind in {"ts_guess", "selected_path"}:
        energetic = [frame for frame in frames if frame.energy is not None]
        if energetic:
            frame = max(energetic, key=lambda item: item.energy if item.energy is not None else float("-inf"))
            metadata["selected_frame_index"] = frame.index
            metadata["selection_reason"] = "highest_energy_frame"
            metadata["selected_frame_energy"] = frame.energy
            return frame, metadata
        frame = frames[len(frames) // 2]
        metadata["selected_frame_index"] = frame.index
        metadata["selection_reason"] = "middle_frame_fallback"
        return frame, metadata

    frame = frames[0]
    metadata["selected_frame_index"] = frame.index
    metadata["selection_reason"] = "first_frame"
    if frame.energy is not None:
        metadata["selected_frame_energy"] = frame.energy
    return frame, metadata


def write_orca_ready_xyz(
    *,
    source_path: str | Path,
    target_path: str | Path,
    candidate_kind: str = "",
) -> dict[str, object]:
    frame, metadata = choose_orca_geometry_frame(source_path, candidate_kind=candidate_kind)
    if frame is None:
        raise ValueError(f"No ORCA-ready XYZ geometry found in source candidate: {source_path}")
    target = Path(target_path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(frame.render(), encoding="utf-8")
    metadata["materialized_xyz_path"] = str(target)
    return metadata


__all__ = [
    "XYZFrame",
    "choose_orca_geometry_frame",
    "has_xyz_geometry",
    "load_xyz_frames",
    "load_xyz_atom_sequence",
    "write_orca_ready_xyz",
]
