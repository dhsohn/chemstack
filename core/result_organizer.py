from __future__ import annotations

import errno
import logging
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .completion_rules import TS_ROUTE_RE
from .molecule_key import extract_molecule_key
from .pathing import is_subpath
from .state_store import load_state, save_state, write_report_files
from .statuses import RunStatus

logger = logging.getLogger(__name__)

OPT_RE = re.compile(r"\bOpt\b", re.IGNORECASE)
SP_RE = re.compile(r"\b(SP|Energy)\b", re.IGNORECASE)
FREQ_RE = re.compile(r"\b(Freq|NumFreq|AnFreq)\b", re.IGNORECASE)

REQUIRED_FILES = ["run_state.json", "run_report.json", "run_report.md"]


@dataclass
class SkipReason:
    reaction_dir: str
    reason: str


@dataclass
class OrganizePlan:
    reaction_dir: Path
    run_id: str
    job_type: str
    molecule_key: str
    selected_inp: str
    last_out_path: str
    attempt_count: int
    status: str
    analyzer_status: str
    reason: str
    completed_at: str
    source_dir: Path
    target_rel_path: str
    target_abs_path: Path


def _resolve_existing_artifact(path_text: str, reaction_dir: Path) -> Optional[Path]:
    raw = path_text.strip()
    if not raw:
        return None

    p = Path(raw)
    candidates: List[Path] = []
    if p.is_absolute():
        candidates.append(p)
        candidates.append(reaction_dir / p.name)
    else:
        candidates.append(reaction_dir / p)
        candidates.append(reaction_dir / p.name)

    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        if candidate.exists():
            return resolved
    return None


def check_eligibility(reaction_dir: Path) -> Tuple[Optional[Dict[str, Any]], Optional[SkipReason]]:
    state = load_state(reaction_dir)
    if state is None:
        return None, SkipReason(str(reaction_dir), "state_missing_or_invalid")

    run_id = state.get("run_id")
    status = state.get("status")
    if not isinstance(run_id, str) or not run_id.strip():
        return None, SkipReason(str(reaction_dir), "state_schema_invalid")
    if not isinstance(status, str) or not status.strip():
        return None, SkipReason(str(reaction_dir), "state_schema_invalid")

    if status != RunStatus.COMPLETED.value:
        logger.debug("Skipping non-completed: %s (status=%s)", reaction_dir, status)
        return None, SkipReason(str(reaction_dir), "not_completed")

    final_result = state.get("final_result")
    if not isinstance(final_result, dict):
        return None, SkipReason(str(reaction_dir), "final_result_missing")

    selected_inp = state.get("selected_inp")
    if isinstance(selected_inp, str) and selected_inp.strip():
        inp_path = _resolve_existing_artifact(selected_inp, reaction_dir)
        if inp_path is None:
            logger.warning("Artifact missing: %s", selected_inp)
            return None, SkipReason(str(reaction_dir), "artifact_missing")
        state["selected_inp"] = str(inp_path)

    last_out = final_result.get("last_out_path")
    if isinstance(last_out, str) and last_out.strip():
        out_path = _resolve_existing_artifact(last_out, reaction_dir)
        if out_path is None:
            logger.warning("Artifact missing: %s", last_out)
            return None, SkipReason(str(reaction_dir), "state_output_mismatch")
        final_result["last_out_path"] = str(out_path)

    return state, None


def detect_job_type(inp_path: Path) -> str:
    route_line = _read_route_line(inp_path)
    if TS_ROUTE_RE.search(route_line):
        return "ts"
    if OPT_RE.search(route_line):
        return "opt"
    if SP_RE.search(route_line):
        return "sp"
    if FREQ_RE.search(route_line):
        return "freq"
    return "other"


def _read_route_line(inp_path: Path) -> str:
    try:
        with inp_path.open("r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith("#"):
                    continue
                if stripped.startswith("!"):
                    return stripped
    except OSError:
        pass
    return ""


def compute_organize_plan(
    reaction_dir: Path,
    state: Dict[str, Any],
    organized_root: Path,
) -> OrganizePlan:
    run_id = state["run_id"]
    selected_inp = state.get("selected_inp", "")
    inp_path = Path(selected_inp) if selected_inp else None

    job_type = detect_job_type(inp_path) if inp_path and inp_path.exists() else "other"
    molecule_key = extract_molecule_key(inp_path) if inp_path and inp_path.exists() else "unknown"

    final_result = state.get("final_result") or {}
    last_out_path = final_result.get("last_out_path", "")
    analyzer_status = final_result.get("analyzer_status", "")
    reason = final_result.get("reason", "")
    completed_at = final_result.get("completed_at", "")

    attempts = state.get("attempts")
    attempt_count = len(attempts) if isinstance(attempts, list) else 0

    target_rel_path = f"{job_type}/{molecule_key}/{run_id}"
    target_abs_path = organized_root / target_rel_path

    return OrganizePlan(
        reaction_dir=reaction_dir,
        run_id=run_id,
        job_type=job_type,
        molecule_key=molecule_key,
        selected_inp=selected_inp,
        last_out_path=last_out_path if isinstance(last_out_path, str) else "",
        attempt_count=attempt_count,
        status=state.get("status", ""),
        analyzer_status=analyzer_status if isinstance(analyzer_status, str) else "",
        reason=reason if isinstance(reason, str) else "",
        completed_at=completed_at if isinstance(completed_at, str) else "",
        source_dir=reaction_dir,
        target_rel_path=target_rel_path,
        target_abs_path=target_abs_path,
    )


def plan_single(
    reaction_dir: Path,
    organized_root: Path,
) -> Tuple[Optional[OrganizePlan], Optional[SkipReason]]:
    state, skip = check_eligibility(reaction_dir)
    if skip is not None:
        return None, skip
    assert state is not None
    plan = compute_organize_plan(reaction_dir, state, organized_root)
    return plan, None


def plan_root_scan(
    root: Path,
    organized_root: Path,
) -> Tuple[List[OrganizePlan], List[SkipReason]]:
    plans: List[OrganizePlan] = []
    skips: List[SkipReason] = []

    try:
        state_files = sorted(root.rglob("run_state.json"))
    except OSError as exc:
        logger.error("Cannot scan root: %s (%s)", root, exc)
        return plans, skips

    seen_dirs: set[Path] = set()
    for state_file in state_files:
        entry = state_file.parent
        if entry in seen_dirs:
            continue
        seen_dirs.add(entry)

        if entry.is_symlink():
            continue
        if is_subpath(entry, organized_root):
            continue
        plan, skip = plan_single(entry, organized_root)
        if plan is not None:
            plans.append(plan)
        if skip is not None:
            skips.append(skip)

    return plans, skips


# --- Phase 2: apply mode ---


def check_conflict(
    plan: OrganizePlan,
    index: Dict[str, Dict[str, Any]],
) -> Optional[str]:
    existing = index.get(plan.run_id)
    if existing is not None:
        if existing.get("organized_path") == plan.target_rel_path:
            return "already_organized"
        return "index_conflict"
    if plan.target_abs_path.exists():
        return "path_occupied"
    return None


def execute_move(plan: OrganizePlan) -> None:
    plan.target_abs_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(str(plan.source_dir), str(plan.target_abs_path))
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.copytree(str(plan.source_dir), str(plan.target_abs_path))
        _fsync_directory(plan.target_abs_path.parent)
        shutil.rmtree(str(plan.source_dir))


def rollback_move(plan: OrganizePlan) -> None:
    if not plan.target_abs_path.exists():
        return
    if plan.source_dir.exists():
        raise RuntimeError(f"Rollback blocked: source already exists: {plan.source_dir}")
    plan.source_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.rename(str(plan.target_abs_path), str(plan.source_dir))
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        shutil.copytree(str(plan.target_abs_path), str(plan.source_dir))
        _fsync_directory(plan.source_dir.parent)
        shutil.rmtree(str(plan.target_abs_path))


def _sync_state_after_relocation(
    *,
    state_dir: Path,
    source_dir: Path,
    target_dir: Path,
) -> Dict[str, Any]:
    state = load_state(state_dir)
    if state is None:
        raise RuntimeError(f"Relocated directory has invalid state: {state_dir}")

    state["reaction_dir"] = str(target_dir)

    selected_inp = state.get("selected_inp")
    if isinstance(selected_inp, str):
        state["selected_inp"] = _normalize_moved_artifact_path(
            selected_inp,
            source_dir=source_dir,
            target_dir=target_dir,
        )

    attempts = state.get("attempts")
    if isinstance(attempts, list):
        for attempt in attempts:
            if not isinstance(attempt, dict):
                continue
            inp_path = attempt.get("inp_path")
            if isinstance(inp_path, str):
                attempt["inp_path"] = _normalize_moved_artifact_path(
                    inp_path,
                    source_dir=source_dir,
                    target_dir=target_dir,
                )
            out_path = attempt.get("out_path")
            if isinstance(out_path, str):
                attempt["out_path"] = _normalize_moved_artifact_path(
                    out_path,
                    source_dir=source_dir,
                    target_dir=target_dir,
                )

    final_result = state.get("final_result")
    if isinstance(final_result, dict):
        last_out_path = final_result.get("last_out_path")
        if isinstance(last_out_path, str):
            final_result["last_out_path"] = _normalize_moved_artifact_path(
                last_out_path,
                source_dir=source_dir,
                target_dir=target_dir,
            )

    save_state(state_dir, state)
    write_report_files(state_dir, state)
    return state


def sync_state_after_move(plan: OrganizePlan) -> Dict[str, Any]:
    return _sync_state_after_relocation(
        state_dir=plan.target_abs_path,
        source_dir=plan.source_dir,
        target_dir=plan.target_abs_path,
    )


def sync_state_after_rollback(plan: OrganizePlan) -> Dict[str, Any]:
    return _sync_state_after_relocation(
        state_dir=plan.source_dir,
        source_dir=plan.target_abs_path,
        target_dir=plan.source_dir,
    )


def _remap_moved_path(path_text: str, source_dir: Path, target_dir: Path) -> str:
    path = Path(path_text)
    if not path.is_absolute():
        return path_text
    try:
        rel = path.relative_to(source_dir)
    except ValueError:
        return path_text
    return str(target_dir / rel)


def _normalize_moved_artifact_path(path_text: str, source_dir: Path, target_dir: Path) -> str:
    remapped = _remap_moved_path(path_text, source_dir, target_dir)
    resolved = _resolve_existing_artifact(remapped, target_dir)
    if resolved is None:
        return remapped
    return str(resolved)


def _fsync_directory(path: Path) -> None:
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
