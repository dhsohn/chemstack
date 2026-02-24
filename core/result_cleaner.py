from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .pathing import is_subpath
from .state_store import load_state
from .statuses import RunStatus

logger = logging.getLogger(__name__)


@dataclass
class CleanupFileEntry:
    path: Path
    size_bytes: int


@dataclass
class CleanupPlan:
    reaction_dir: Path
    run_id: str
    files_to_remove: List[CleanupFileEntry] = field(default_factory=list)
    keep_count: int = 0
    total_remove_bytes: int = 0


@dataclass
class CleanupSkipReason:
    reaction_dir: str
    reason: str


@dataclass
class CleanupResult:
    reaction_dir: str
    run_id: str
    files_removed: int = 0
    bytes_freed: int = 0
    errors: List[str] = field(default_factory=list)


def _should_keep(
    file_path: Path,
    keep_extensions: Set[str],
    keep_filenames: Set[str],
    remove_patterns: List[str],
) -> bool:
    name = file_path.name
    for pattern in remove_patterns:
        if fnmatch.fnmatch(name, pattern):
            return False
    if name in keep_filenames:
        return True
    if file_path.suffix.lower() in keep_extensions:
        return True
    return False


def _state_artifact_path_texts(state: Dict[str, Any]) -> Iterable[str]:
    selected_inp = state.get("selected_inp")
    if isinstance(selected_inp, str) and selected_inp.strip():
        yield selected_inp

    attempts = state.get("attempts")
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            inp_path = attempt.get("inp_path")
            if isinstance(inp_path, str) and inp_path.strip():
                yield inp_path
            out_path = attempt.get("out_path")
            if isinstance(out_path, str) and out_path.strip():
                yield out_path

    final_result = state.get("final_result")
    if isinstance(final_result, dict):
        last_out_path = final_result.get("last_out_path")
        if isinstance(last_out_path, str) and last_out_path.strip():
            yield last_out_path


def _artifact_candidates(path_text: str, reaction_dir: Path) -> list[Path]:
    raw = path_text.strip()
    if not raw:
        return []

    p = Path(raw)
    if p.is_absolute():
        return [p, reaction_dir / p.name]
    return [reaction_dir / p, reaction_dir / p.name]


def _collect_protected_artifacts(
    state: Dict[str, Any],
    reaction_dir: Path,
) -> tuple[Set[Path], Set[str]]:
    protected_paths: set[Path] = set()
    protected_names: set[str] = set()

    for path_text in _state_artifact_path_texts(state):
        raw = path_text.strip()
        name = Path(raw).name
        if name:
            protected_names.add(name)

        for candidate in _artifact_candidates(raw, reaction_dir):
            if not candidate.exists() or not candidate.is_file():
                continue
            try:
                resolved = candidate.resolve()
            except OSError:
                continue
            protected_paths.add(resolved)

    return protected_paths, protected_names


def check_cleanup_eligibility(
    reaction_dir: Path,
) -> Tuple[Optional[Dict[str, Any]], Optional[CleanupSkipReason]]:
    state = load_state(reaction_dir)
    if state is None:
        return None, CleanupSkipReason(str(reaction_dir), "state_missing_or_invalid")

    run_id = state.get("run_id")
    status = state.get("status")
    if not isinstance(run_id, str) or not run_id.strip():
        return None, CleanupSkipReason(str(reaction_dir), "state_schema_invalid")
    if not isinstance(status, str) or not status.strip():
        return None, CleanupSkipReason(str(reaction_dir), "state_schema_invalid")

    if status != RunStatus.COMPLETED.value:
        return None, CleanupSkipReason(str(reaction_dir), "not_completed")

    return state, None


def compute_cleanup_plan(
    reaction_dir: Path,
    state: Dict[str, Any],
    keep_extensions: Set[str],
    keep_filenames: Set[str],
    remove_patterns: List[str],
) -> CleanupPlan:
    run_id = state.get("run_id", "unknown")
    plan = CleanupPlan(reaction_dir=reaction_dir, run_id=run_id)
    protected_paths, protected_names = _collect_protected_artifacts(state, reaction_dir)

    for file_path in sorted(reaction_dir.iterdir()):
        if not file_path.is_file():
            continue
        try:
            resolved_file = file_path.resolve()
        except OSError:
            resolved_file = file_path
        if resolved_file in protected_paths or file_path.name in protected_names:
            plan.keep_count += 1
            continue
        if _should_keep(file_path, keep_extensions, keep_filenames, remove_patterns):
            plan.keep_count += 1
        else:
            try:
                size = file_path.stat().st_size
            except OSError:
                size = 0
            plan.files_to_remove.append(
                CleanupFileEntry(path=file_path, size_bytes=size)
            )
            plan.total_remove_bytes += size

    return plan


def plan_cleanup_single(
    reaction_dir: Path,
    keep_extensions: Set[str],
    keep_filenames: Set[str],
    remove_patterns: List[str],
) -> Tuple[Optional[CleanupPlan], Optional[CleanupSkipReason]]:
    state, skip = check_cleanup_eligibility(reaction_dir)
    if skip is not None:
        return None, skip
    assert state is not None
    plan = compute_cleanup_plan(
        reaction_dir, state, keep_extensions, keep_filenames, remove_patterns,
    )
    if not plan.files_to_remove:
        return None, CleanupSkipReason(str(reaction_dir), "nothing_to_clean")
    return plan, None


def plan_cleanup_root_scan(
    organized_root: Path,
    keep_extensions: Set[str],
    keep_filenames: Set[str],
    remove_patterns: List[str],
) -> Tuple[List[CleanupPlan], List[CleanupSkipReason]]:
    plans: List[CleanupPlan] = []
    skips: List[CleanupSkipReason] = []
    index_root = organized_root / "index"

    if not organized_root.exists():
        return plans, skips

    for state_file in sorted(organized_root.rglob("run_state.json")):
        if is_subpath(state_file, index_root):
            continue
        reaction_dir = state_file.parent
        plan, skip = plan_cleanup_single(
            reaction_dir, keep_extensions, keep_filenames, remove_patterns,
        )
        if plan is not None:
            plans.append(plan)
        if skip is not None:
            skips.append(skip)

    return plans, skips


def execute_cleanup(plan: CleanupPlan) -> CleanupResult:
    result = CleanupResult(
        reaction_dir=str(plan.reaction_dir),
        run_id=plan.run_id,
    )

    for entry in plan.files_to_remove:
        try:
            entry.path.unlink()
            result.files_removed += 1
            result.bytes_freed += entry.size_bytes
        except OSError as exc:
            result.errors.append(f"{entry.path.name}: {exc}")
            logger.error("Failed to remove %s: %s", entry.path, exc)

    return result
