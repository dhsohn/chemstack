from __future__ import annotations

from pathlib import Path
from typing import Any


def _orca_module():
    from . import orca as o

    return o


def resolve_candidate_path_impl(path_text: Any) -> Path | None:
    o = _orca_module()
    raw = o._normalize_text(path_text)
    if not raw:
        return None
    try:
        candidate = o.Path(raw).expanduser()
    except OSError:
        return None
    try:
        return candidate.resolve()
    except OSError:
        return None


def direct_dir_target_impl(target: str) -> Path | None:
    o = _orca_module()
    raw = o._normalize_text(target)
    if not raw:
        return None
    try:
        candidate = o.Path(raw).expanduser().resolve()
    except OSError:
        return None
    if not candidate.exists():
        return None
    return candidate.parent if candidate.is_file() else candidate


def resolve_artifact_path_impl(path_value: Any, base_dir: Path | None) -> str:
    o = _orca_module()
    raw = o._normalize_text(path_value)
    if not raw:
        return ""
    try:
        candidate = o.Path(raw).expanduser()
    except OSError:
        return raw
    if candidate.is_absolute():
        try:
            return str(candidate.resolve())
        except OSError:
            return str(candidate)
    if base_dir is None:
        return raw
    try:
        return str((base_dir / candidate).resolve())
    except OSError:
        return str(base_dir / candidate)


def derive_selected_input_xyz_impl(selected_inp: str) -> str:
    o = _orca_module()
    inp_path = o._resolve_candidate_path(selected_inp)
    if inp_path is None:
        return ""
    try:
        text = inp_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return ""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or not stripped.startswith("*"):
            continue
        if "xyzfile" not in stripped.lower():
            continue
        parts = stripped.split()
        if len(parts) >= 5:
            return o._resolve_artifact_path(parts[-1], inp_path.parent)
    return ""


def iter_existing_dirs_impl(*candidates: Path | None) -> list[Path]:
    rows: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            resolved = candidate.expanduser().resolve()
        except OSError:
            continue
        if not resolved.exists() or not resolved.is_dir() or resolved in seen:
            continue
        seen.add(resolved)
        rows.append(resolved)
    return rows


def is_subpath_impl(candidate: Path, root: Path | None) -> bool:
    if root is None:
        return False
    try:
        candidate.resolve().relative_to(root.resolve())
    except (OSError, ValueError):
        return False
    return True


def prefer_orca_optimized_xyz_impl(
    *,
    selected_inp: str,
    selected_input_xyz: str,
    current_dir: Path | None,
    organized_dir: Path | None,
    latest_known_path: str,
    last_out_path: str,
) -> str:
    o = _orca_module()
    selected_inp_path = o._resolve_candidate_path(selected_inp)
    selected_input_xyz_path = o._resolve_candidate_path(selected_input_xyz)
    last_out = o._resolve_candidate_path(last_out_path)
    latest_known_dir = o._resolve_candidate_path(latest_known_path)
    if latest_known_dir is not None and not latest_known_dir.is_dir():
        latest_known_dir = latest_known_dir.parent

    search_dirs = o._iter_existing_dirs(
        selected_inp_path.parent if selected_inp_path is not None else None,
        current_dir,
        organized_dir,
        latest_known_dir,
        last_out.parent if last_out is not None else None,
    )
    preferred_names: list[str] = []
    if selected_inp_path is not None:
        preferred_names.append(f"{selected_inp_path.stem}.xyz")
    if last_out is not None:
        preferred_names.append(f"{last_out.stem}.xyz")

    for search_dir in search_dirs:
        for filename in preferred_names:
            candidate = search_dir / filename
            if candidate.exists():
                try:
                    return str(candidate.resolve())
                except OSError:
                    return str(candidate)

    source_input = None
    if selected_input_xyz_path is not None:
        try:
            source_input = selected_input_xyz_path.resolve()
        except OSError:
            source_input = selected_input_xyz_path

    xyz_candidates: list[Path] = []
    seen_files: set[Path] = set()
    for search_dir in search_dirs:
        try:
            files = sorted(
                (item for item in search_dir.glob("*.xyz") if item.is_file()),
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
        except OSError:
            continue
        for item in files:
            try:
                resolved = item.resolve()
            except OSError:
                resolved = item
            if source_input is not None and resolved == source_input:
                continue
            if resolved in seen_files:
                continue
            seen_files.add(resolved)
            xyz_candidates.append(item)

    if not xyz_candidates:
        return ""
    try:
        return str(xyz_candidates[0].resolve())
    except OSError:
        return str(xyz_candidates[0])


__all__ = [
    "derive_selected_input_xyz_impl",
    "direct_dir_target_impl",
    "is_subpath_impl",
    "iter_existing_dirs_impl",
    "prefer_orca_optimized_xyz_impl",
    "resolve_artifact_path_impl",
    "resolve_candidate_path_impl",
]
