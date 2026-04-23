from __future__ import annotations

import re
from pathlib import Path

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:\\")
_WSL_WINDOWS_MOUNT_RE = re.compile(r"^/mnt/[a-zA-Z](/|$)")


def is_rejected_windows_path(path_text: str) -> bool:
    return bool(_WINDOWS_DRIVE_RE.match(path_text) or _WSL_WINDOWS_MOUNT_RE.match(path_text))


def resolve_local_path(path_text: str | Path) -> Path:
    text = str(path_text).strip()
    if not text:
        raise ValueError("Path must not be empty.")
    if is_rejected_windows_path(text):
        raise ValueError(f"Windows-style and /mnt/<drive> paths are not supported: {text}")
    return Path(text).expanduser().resolve()


def is_subpath(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_subpath(path: Path, root: Path, *, label: str = "Path") -> Path:
    resolved_path = path.expanduser().resolve()
    resolved_root = root.expanduser().resolve()
    if not is_subpath(resolved_path, resolved_root):
        raise ValueError(f"{label} must be under allowed root: {resolved_root}. got={resolved_path}")
    return resolved_path


def ensure_directory(path_text: str | Path, *, label: str = "Directory") -> Path:
    path = resolve_local_path(path_text)
    if not path.exists():
        raise ValueError(f"{label} not found: {path}")
    if not path.is_dir():
        raise ValueError(f"{label} is not a directory: {path}")
    return path


def validate_job_dir(job_dir_text: str, allowed_root_text: str, *, label: str = "Job directory") -> Path:
    job_dir = ensure_directory(job_dir_text, label=label)
    allowed_root = ensure_directory(allowed_root_text, label="Allowed root")
    return require_subpath(job_dir, allowed_root, label=label)


def resolve_artifact_path(path_text: str, base_dir: str | Path) -> Path | None:
    raw = str(path_text).strip()
    if not raw:
        return None
    base = Path(base_dir).expanduser().resolve()
    candidate = Path(raw)
    candidates = [candidate] if candidate.is_absolute() else [base / candidate, base / candidate.name]
    seen: set[Path] = set()
    for item in candidates:
        try:
            resolved = item.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.exists():
            return resolved
    return None
