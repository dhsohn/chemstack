from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def timestamped_token(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"


def atomic_write_text(path: Path, payload: str) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid4().hex[:8]}")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(str(tmp_path), str(path))
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def atomic_write_json(
    path: Path,
    payload: Any,
    *,
    ensure_ascii: bool = True,
    indent: int | None = 2,
) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=ensure_ascii, indent=indent),
    )
