from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional


_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:\\")
_WSL_WINDOWS_MOUNT_RE = re.compile(r"^/mnt/[a-zA-Z](/|$)")


def is_rejected_windows_path(path: str) -> bool:
    """Return True if *path* looks like a Windows or WSL-mount path (unsupported)."""
    return bool(_WINDOWS_DRIVE_RE.match(path) or _WSL_WINDOWS_MOUNT_RE.match(path))


def is_subpath(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def artifact_candidates(path_text: str, reaction_dir: Path) -> List[Path]:
    """Generate candidate file paths from a stored path string and a base directory."""
    raw = path_text.strip()
    if not raw:
        return []
    p = Path(raw)
    if p.is_absolute():
        return [p, reaction_dir / p.name]
    return [reaction_dir / p, reaction_dir / p.name]


def resolve_artifact_path(path_text: str, reaction_dir: Path) -> Optional[Path]:
    """Resolve a stored path string to an existing file, trying several candidates."""
    candidates = artifact_candidates(path_text, reaction_dir)
    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.exists():
            return resolved
    return None
