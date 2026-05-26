from __future__ import annotations

import logging
from pathlib import Path

from .queue_store import clear_terminal
from .run_snapshot import collect_run_snapshots
from .state import STATE_FILE_NAME
from .statuses import RunStatus

logger = logging.getLogger(__name__)

_TERMINAL_RUN_STATUSES = frozenset({RunStatus.COMPLETED.value, RunStatus.FAILED.value})


def _resolved_path_text(path_text: str) -> str:
    text = str(path_text).strip()
    if not text:
        return ""
    try:
        return str(Path(text).expanduser().resolve())
    except OSError:
        return text


def clear_terminal_run_states(allowed_root: Path) -> int:
    cleared_state_paths: set[str] = set()
    run_count = 0

    for snapshot in collect_run_snapshots(allowed_root):
        if snapshot.status not in _TERMINAL_RUN_STATUSES:
            continue

        state_path = snapshot.reaction_dir / STATE_FILE_NAME
        state_key = _resolved_path_text(str(state_path))
        if state_key in cleared_state_paths:
            continue
        cleared_state_paths.add(state_key)

        try:
            state_path.unlink()
            run_count += 1
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Failed to remove %s: %s", state_path, exc)

    return run_count


def clear_terminal_entries(allowed_root: Path) -> tuple[int, int]:
    return clear_terminal(allowed_root), clear_terminal_run_states(allowed_root)
