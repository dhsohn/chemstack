"""list command — display status of all simulations under allowed_root."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import load_config
from ..state_store import STATE_FILE_NAME
from ..types import RunInfo
from ._helpers import _to_resolved_local

logger = logging.getLogger(__name__)


def _elapsed_text(seconds: float) -> str:
    """Convert elapsed seconds to human-readable text."""
    if seconds < 0:
        return "-"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    if hours > 0:
        return f"{hours}h {minutes:02d}m"
    secs = int(seconds % 60)
    if minutes > 0:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _compute_elapsed(state: dict[str, Any]) -> float:
    """Compute elapsed time in seconds from run_state."""
    started = _parse_iso(state.get("started_at"))
    if started is None:
        return -1.0

    status = str(state.get("status", "")).lower()
    if status in ("completed", "failed"):
        # Finished run: use updated_at as end time
        ended = _parse_iso(state.get("updated_at"))
        if ended is not None:
            return (ended - started).total_seconds()

    # Still running or missing end time: use current time
    now = datetime.now(timezone.utc)
    return (now - started).total_seconds()


def _collect_runs(allowed_root: Path) -> list[RunInfo]:
    """Collect all run_state.json entries under allowed_root."""
    runs: list[RunInfo] = []

    if not allowed_root.is_dir():
        return runs

    for state_path in allowed_root.rglob(STATE_FILE_NAME):
        try:
            raw = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(raw, dict):
            continue

        reaction_dir = state_path.parent
        rel_dir = str(reaction_dir.relative_to(allowed_root))
        status = str(raw.get("status", "unknown"))
        elapsed = _compute_elapsed(raw)
        selected_inp = raw.get("selected_inp", "")
        if selected_inp:
            selected_inp = Path(selected_inp).name

        attempt_count = len(raw.get("attempts", []))

        runs.append(RunInfo(
            dir=rel_dir,
            status=status,
            elapsed=elapsed,
            elapsed_text=_elapsed_text(elapsed),
            inp=selected_inp,
            attempts=attempt_count,
            started_at=raw.get("started_at", ""),
        ))

    # Sort by most recent first
    runs.sort(key=lambda r: r["started_at"], reverse=True)
    return runs


def _print_table(runs: list[RunInfo], *, filter_status: str | None) -> None:
    """Print runs as a terminal table."""
    if filter_status:
        runs = [r for r in runs if r["status"] == filter_status]

    if not runs:
        print("No registered runs found.")
        return

    # Compute column widths
    headers = ["DIR", "STATUS", "ATTEMPTS", "ELAPSED", "INP"]
    keys = ["dir", "status", "attempts", "elapsed_text", "inp"]
    rows = [[str(r[k]) for k in keys] for r in runs]  # type: ignore[literal-required]

    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("\u2500" * (sum(widths) + 2 * (len(widths) - 1)))
    for row in rows:
        print(fmt.format(*row))

    print(f"\nTotal: {len(runs)}")


def cmd_list(args: Any) -> int:
    cfg = load_config(args.config)
    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)

    if not allowed_root.is_dir():
        logger.error("allowed_root not found: %s", allowed_root)
        return 1

    runs = _collect_runs(allowed_root)
    filter_status = getattr(args, "filter", None)

    if args.json:
        if filter_status:
            runs = [r for r in runs if r["status"] == filter_status]
        print(json.dumps(runs, ensure_ascii=False, indent=2))
    else:
        _print_table(runs, filter_status=filter_status)

    return 0
