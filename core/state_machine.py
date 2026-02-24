from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from .state_store import load_state, new_state, save_state
from .statuses import AnalyzerStatus, RunStatus
from .types import RunState


MAX_RETRY_RECIPES = 2
RESUMABLE_RUN_STATUSES = {RunStatus.RUNNING.value, RunStatus.RETRYING.value}


@dataclass(frozen=True)
class AttemptDecision:
    run_status: RunStatus
    reason: str
    exit_code: int


def parse_analyzer_status(status_text: AnalyzerStatus | str) -> AnalyzerStatus | None:
    if isinstance(status_text, AnalyzerStatus):
        return status_text
    try:
        return AnalyzerStatus(str(status_text))
    except ValueError:
        return None


def decide_attempt_outcome(
    *,
    analyzer_status: AnalyzerStatus | str,
    analyzer_reason: str,
    retries_used: int,
    max_retries: int,
) -> AttemptDecision | None:
    parsed = parse_analyzer_status(analyzer_status)
    if parsed == AnalyzerStatus.COMPLETED:
        return AttemptDecision(run_status=RunStatus.COMPLETED, reason=analyzer_reason, exit_code=0)
    if parsed == AnalyzerStatus.ERROR_MULTIPLICITY_IMPOSSIBLE:
        return AttemptDecision(run_status=RunStatus.FAILED, reason=analyzer_reason, exit_code=1)
    if retries_used >= max_retries:
        return AttemptDecision(run_status=RunStatus.FAILED, reason="retry_limit_reached", exit_code=1)
    return None


def state_matches_selected(
    state: RunState,
    selected_inp: Path,
    *,
    to_resolved_local: Callable[[str], Path],
) -> bool:
    selected = state.get("selected_inp")
    if not isinstance(selected, str) or not selected.strip():
        return False
    try:
        return to_resolved_local(selected) == selected_inp.resolve()
    except Exception:
        return False


def load_or_create_state(
    reaction_dir: Path,
    selected_inp: Path,
    *,
    max_retries: int,
    to_resolved_local: Callable[[str], Path],
) -> tuple[RunState, bool]:
    state = load_state(reaction_dir)
    resumed = False
    if not state or not state_matches_selected(state, selected_inp, to_resolved_local=to_resolved_local):
        state = new_state(reaction_dir, selected_inp, max_retries=max_retries)
    elif str(state.get("status")) in RESUMABLE_RUN_STATUSES:
        resumed = True
    else:
        state = new_state(reaction_dir, selected_inp, max_retries=max_retries)

    state["max_retries"] = max_retries
    if not isinstance(state.get("attempts"), list):
        state["attempts"] = []
    save_state(reaction_dir, state)
    return state, resumed
