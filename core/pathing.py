from __future__ import annotations

import re
from pathlib import Path


WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:\\")
WSL_WINDOWS_MOUNT_RE = re.compile(r"^/mnt/[a-zA-Z](/|$)")


def is_windows_style_path(path: str) -> bool:
    return bool(WINDOWS_DRIVE_RE.match(path) or WSL_WINDOWS_MOUNT_RE.match(path))


def to_local_path(path: str) -> str:
    return path


def is_subpath(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False
