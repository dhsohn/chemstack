"""list command — display status of all simulations under allowed_root."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from ..config import load_config
from ..run_snapshot import collect_run_snapshots, elapsed_text
from ..types import RunInfo
from ._helpers import _to_resolved_local

logger = logging.getLogger(__name__)


def _elapsed_text(seconds: float) -> str:
    return elapsed_text(seconds)


def _collect_runs(allowed_root: Path) -> list[RunInfo]:
    """Collect all run_state.json entries under allowed_root."""
    snapshots = collect_run_snapshots(allowed_root)
    runs: list[RunInfo] = []
    for snapshot in snapshots:
        runs.append(
            RunInfo(
                dir=snapshot.name,
                status=snapshot.status,
                elapsed=snapshot.elapsed,
                elapsed_text=snapshot.elapsed_text,
                inp=snapshot.selected_inp_name if snapshot.selected_inp_name != "-" else "",
                attempts=snapshot.attempts,
                started_at=snapshot.started_at,
            )
        )
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
