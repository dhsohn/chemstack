from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class DirUsage:
    path: str
    size_bytes: int


@dataclass
class FilesystemInfo:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    usage_percent: float


@dataclass
class DiskReport:
    allowed_root: str
    allowed_root_bytes: int
    organized_root: str
    organized_root_bytes: int
    combined_bytes: int
    threshold_gb: float
    threshold_exceeded: bool
    top_dirs: List[DirUsage] = field(default_factory=list)
    filesystem: Optional[FilesystemInfo] = None
    timestamp: str = ""


def _dir_size(path: Path) -> int:
    total = 0
    try:
        for entry in os.scandir(path):
            try:
                if entry.is_symlink():
                    continue
                if entry.is_file(follow_symlinks=False):
                    total += entry.stat(follow_symlinks=False).st_size
                elif entry.is_dir(follow_symlinks=False):
                    total += _dir_size(Path(entry.path))
            except OSError:
                continue
    except OSError:
        pass
    return total


def _top_subdirs(root: Path, limit: int) -> List[DirUsage]:
    entries: List[DirUsage] = []
    subdirs: List[Path] = []
    try:
        for entry in os.scandir(root):
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(Path(entry.path))
            except OSError:
                continue
    except OSError:
        pass

    if not subdirs:
        return entries

    max_workers = min(8, len(subdirs))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_dir_size, path): path for path in subdirs}
        for future in as_completed(futures):
            path = futures[future]
            try:
                size = future.result()
            except Exception:
                continue
            entries.append(DirUsage(path=str(path), size_bytes=size))

    entries.sort(key=lambda e: e.size_bytes, reverse=True)
    return entries[:limit]


def _get_filesystem_info(path: Path) -> Optional[FilesystemInfo]:
    try:
        st = os.statvfs(str(path))
        total = st.f_frsize * st.f_blocks
        free = st.f_frsize * st.f_bavail
        used = total - free
        pct = (used / total * 100.0) if total > 0 else 0.0
        return FilesystemInfo(
            total_bytes=total,
            used_bytes=used,
            free_bytes=free,
            usage_percent=round(pct, 1),
        )
    except OSError:
        return None


def scan_disk_usage(
    allowed_root: str,
    organized_root: str,
    threshold_gb: float,
    top_n: int,
) -> DiskReport:
    ar = Path(allowed_root)
    org = Path(organized_root)

    ar_bytes = 0
    org_bytes = 0
    scan_targets: list[tuple[str, Path]] = []
    if ar.is_dir():
        scan_targets.append(("ar", ar))
    if org.is_dir():
        scan_targets.append(("org", org))

    if scan_targets:
        with ThreadPoolExecutor(max_workers=min(2, len(scan_targets))) as executor:
            futures = {executor.submit(_dir_size, path): label for label, path in scan_targets}
            for future in as_completed(futures):
                label = futures[future]
                try:
                    size = future.result()
                except Exception:
                    size = 0
                if label == "ar":
                    ar_bytes = size
                else:
                    org_bytes = size

    combined = ar_bytes + org_bytes
    combined_gb = combined / (1024 ** 3)
    exceeded = combined_gb >= threshold_gb

    # Top dirs from organized_root (most likely to have large data)
    top_dirs = _top_subdirs(org, top_n) if org.is_dir() else []

    # Filesystem info from organized_root (or allowed_root as fallback)
    fs_path = org if org.is_dir() else ar
    fs_info = _get_filesystem_info(fs_path) if fs_path.is_dir() else None

    return DiskReport(
        allowed_root=str(ar),
        allowed_root_bytes=ar_bytes,
        organized_root=str(org),
        organized_root_bytes=org_bytes,
        combined_bytes=combined,
        threshold_gb=threshold_gb,
        threshold_exceeded=exceeded,
        top_dirs=top_dirs,
        filesystem=fs_info,
        timestamp=datetime.now(timezone.utc).isoformat(),
    )
