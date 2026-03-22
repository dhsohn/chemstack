"""Cancellation helpers for queued and direct-running simulations."""

from __future__ import annotations

import logging
import os
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .process_tracking import active_run_lock_pid
from .queue_store import cancel as queue_cancel, list_queue
from .state_store import STATE_FILE_NAME, load_state
from .statuses import QueueStatus
from .types import QueueEntry

logger = logging.getLogger(__name__)

_ACTIVE_QUEUE_STATUSES = frozenset({
    QueueStatus.PENDING.value,
    QueueStatus.RUNNING.value,
})


class CancelTargetError(ValueError):
    """Raised when a cancel target cannot be resolved safely."""


@dataclass(frozen=True)
class CancelResult:
    """Structured cancellation outcome for CLI and Telegram responses."""

    source: Literal["queue", "direct"]
    action: Literal["cancelled", "requested"]
    reaction_dir: str
    queue_id: str | None = None
    run_id: str | None = None
    pid: int | None = None


@dataclass(frozen=True)
class _CancelCandidate:
    source: Literal["queue", "direct"]
    reaction_dir: str
    queue_id: str | None = None
    run_id: str | None = None
    pid: int | None = None


def cancel_target(allowed_root: Path, target: str) -> CancelResult | None:
    """Cancel a queued or directly running simulation by several target forms."""
    normalized = target.strip()
    if not normalized:
        raise CancelTargetError("Cancel target is empty.")

    queue_candidates = _active_queue_candidates(allowed_root)

    exact_queue_id = next((c for c in queue_candidates if c.queue_id == normalized), None)
    if exact_queue_id is not None:
        entry = queue_cancel(allowed_root, exact_queue_id.queue_id or normalized)
        return _queue_result(entry)

    queue_matches = _match_candidates(allowed_root, queue_candidates, normalized)
    direct_candidates = _active_direct_candidates(
        allowed_root,
        excluded_reaction_dirs={c.reaction_dir for c in queue_candidates},
    )
    direct_matches = _match_candidates(allowed_root, direct_candidates, normalized)

    total_matches = len(queue_matches) + len(direct_matches)
    if total_matches == 0:
        return None
    if total_matches > 1:
        raise CancelTargetError(_format_ambiguous_target(allowed_root, normalized, queue_matches + direct_matches))

    if queue_matches:
        entry = queue_cancel(allowed_root, queue_matches[0].queue_id or normalized)
        return _queue_result(entry)

    candidate = direct_matches[0]
    return _cancel_direct_candidate(candidate)


def _active_queue_candidates(allowed_root: Path) -> list[_CancelCandidate]:
    candidates: list[_CancelCandidate] = []
    for entry in list_queue(allowed_root):
        if entry.get("status") not in _ACTIVE_QUEUE_STATUSES:
            continue
        reaction_dir = str(entry.get("reaction_dir", ""))
        if not reaction_dir:
            continue
        run_id = _run_id_for_reaction_dir(reaction_dir)
        candidates.append(
            _CancelCandidate(
                source="queue",
                reaction_dir=reaction_dir,
                queue_id=entry.get("queue_id"),
                run_id=run_id,
            )
        )
    return candidates


def _active_direct_candidates(
    allowed_root: Path,
    *,
    excluded_reaction_dirs: set[str],
) -> list[_CancelCandidate]:
    candidates: list[_CancelCandidate] = []
    if not allowed_root.is_dir():
        return candidates

    for state_path in allowed_root.rglob(STATE_FILE_NAME):
        reaction_dir = str(state_path.parent.resolve())
        if reaction_dir in excluded_reaction_dirs:
            continue
        pid = _active_lock_pid(state_path.parent)
        if pid is None:
            continue
        state = load_state(state_path.parent)
        run_id = state.get("run_id") if state else None
        candidates.append(
            _CancelCandidate(
                source="direct",
                reaction_dir=reaction_dir,
                run_id=run_id if isinstance(run_id, str) and run_id.strip() else None,
                pid=pid,
            )
        )
    return candidates


def _match_candidates(
    allowed_root: Path,
    candidates: list[_CancelCandidate],
    target: str,
) -> list[_CancelCandidate]:
    return [candidate for candidate in candidates if target in _candidate_aliases(allowed_root, candidate)]


def _candidate_aliases(allowed_root: Path, candidate: _CancelCandidate) -> set[str]:
    reaction_dir = Path(candidate.reaction_dir).expanduser().resolve()
    aliases = {str(reaction_dir), reaction_dir.name}

    try:
        relative = reaction_dir.relative_to(allowed_root)
    except ValueError:
        relative = None
    if relative is not None:
        aliases.add(str(relative))
        aliases.add(relative.as_posix())

    if candidate.run_id:
        aliases.add(candidate.run_id)

    return {alias for alias in aliases if alias}


def _active_lock_pid(reaction_dir: Path) -> int | None:
    return active_run_lock_pid(reaction_dir, logger=logger)


def _run_id_for_reaction_dir(reaction_dir: str) -> str | None:
    state = load_state(Path(reaction_dir))
    if not state:
        return None
    run_id = state.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        return run_id
    return None


def _queue_result(entry: QueueEntry | None) -> CancelResult | None:
    if entry is None:
        return None
    reaction_dir = str(entry.get("reaction_dir", ""))
    queue_id = entry.get("queue_id")
    action: Literal["cancelled", "requested"]
    if entry.get("status") == QueueStatus.CANCELLED.value:
        action = "cancelled"
    else:
        action = "requested"
    run_id = _run_id_for_reaction_dir(reaction_dir)
    return CancelResult(
        source="queue",
        action=action,
        reaction_dir=reaction_dir,
        queue_id=queue_id if isinstance(queue_id, str) and queue_id else None,
        run_id=run_id,
    )


def _cancel_direct_candidate(candidate: _CancelCandidate) -> CancelResult | None:
    pid = candidate.pid
    if pid is None:
        return None
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return None
    except PermissionError as exc:
        raise CancelTargetError(
            f"Permission denied cancelling running simulation: {candidate.reaction_dir} (pid={pid})"
        ) from exc

    logger.info("Cancel requested for direct running simulation: %s (pid=%d)", candidate.reaction_dir, pid)
    return CancelResult(
        source="direct",
        action="requested",
        reaction_dir=candidate.reaction_dir,
        run_id=candidate.run_id,
        pid=pid,
    )


def _format_ambiguous_target(allowed_root: Path, target: str, matches: list[_CancelCandidate]) -> str:
    formatted = ", ".join(_candidate_label(allowed_root, match) for match in matches)
    return f"Ambiguous cancel target: {target}. Matches: {formatted}. Use a full reaction_dir, run_id, or queue_id."


def _candidate_label(allowed_root: Path, candidate: _CancelCandidate) -> str:
    reaction_dir = Path(candidate.reaction_dir).expanduser().resolve()
    try:
        rel = str(reaction_dir.relative_to(allowed_root))
    except ValueError:
        rel = str(reaction_dir)

    if candidate.source == "queue":
        return f"queue:{candidate.queue_id} ({rel})"
    if candidate.run_id:
        return f"run:{candidate.run_id} ({rel})"
    return f"run:{rel}"
