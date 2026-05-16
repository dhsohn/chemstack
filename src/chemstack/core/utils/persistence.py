from __future__ import annotations

import errno
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex
from typing import Any


_DIR_FSYNC_UNSUPPORTED_ERRNOS = {
    code
    for code in (
        errno.EACCES,
        errno.EBADF,
        errno.EINVAL,
        errno.EISDIR,
        errno.EPERM,
        getattr(errno, "ENOSYS", None),
        getattr(errno, "ENOTSUP", None),
        getattr(errno, "ENOTTY", None),
        getattr(errno, "EOPNOTSUPP", None),
    )
    if code is not None
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def timestamped_token(prefix: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{stamp}_{token_hex(3)}"


def coerce_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def coerce_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def coerce_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"", "0", "false", "no", "n", "off"}:
            return False
    return default


def resolve_root_path(root: str | Path) -> Path:
    return Path(root).expanduser().resolve()


def _is_unsupported_dir_fsync_error(exc: OSError) -> bool:
    return exc.errno in _DIR_FSYNC_UNSUPPORTED_ERRNOS


def _fsync_parent_dir(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY

    try:
        dir_fd = os.open(str(path.parent), flags)
    except OSError as exc:
        if _is_unsupported_dir_fsync_error(exc):
            return
        raise

    try:
        try:
            os.fsync(dir_fd)
        except OSError as exc:
            if not _is_unsupported_dir_fsync_error(exc):
                raise
    finally:
        os.close(dir_fd)


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    ensure_ascii: bool = True,
    indent: int = 2,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=ensure_ascii, indent=indent, sort_keys=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_parent_dir(path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
