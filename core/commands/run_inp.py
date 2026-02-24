from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Type

from ..attempt_engine import _exit_with_result, run_attempts
from ..completion_rules import detect_completion_mode
from ..config import load_config
from ..notifier import (
    EVT_RUN_COMPLETED,
    create_notifier,
    event_run_started,
    event_run_terminal,
    make_notify_callback,
)
from ..orca_runner import OrcaRunner
from ..out_analyzer import analyze_output
from ..state_machine import load_or_create_state
from ..state_store import acquire_run_lock, load_state, new_state, state_path
from ..statuses import AnalyzerStatus, RunStatus
from ._helpers import RETRY_INP_RE, _emit, _to_resolved_local, _validate_reaction_dir

logger = logging.getLogger(__name__)


def _select_latest_inp(reaction_dir: Path) -> Path:
    all_candidates = list(reaction_dir.glob("*.inp"))
    if not all_candidates:
        raise ValueError(f"No .inp file found in: {reaction_dir}")
    # Prefer user-authored base inputs over generated retry files.
    candidates = [p for p in all_candidates if not RETRY_INP_RE.search(p.stem)]
    if not candidates:
        candidates = all_candidates
    candidates.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)
    return candidates[0]


def _retry_inp_path(selected_inp: Path, retry_number: int) -> Path:
    base_stem = RETRY_INP_RE.sub("", selected_inp.stem)
    if not base_stem:
        base_stem = selected_inp.stem
    return selected_inp.with_name(f"{base_stem}.retry{retry_number:02d}.inp")


def _existing_completed_out(selected_inp: Path) -> Dict[str, Any] | None:
    base_stem = RETRY_INP_RE.sub("", selected_inp.stem)
    if not base_stem:
        base_stem = selected_inp.stem

    out_candidates = list(selected_inp.parent.glob(f"{base_stem}.out"))
    out_candidates.extend(selected_inp.parent.glob(f"{base_stem}.retry*.out"))
    out_candidates.sort(key=lambda p: (p.stat().st_mtime_ns, p.name.lower()), reverse=True)

    seen: set[Path] = set()
    for out_path in out_candidates:
        resolved = out_path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)

        mode_inp = out_path.with_suffix(".inp")
        if not mode_inp.exists():
            mode_inp = selected_inp
        mode = detect_completion_mode(mode_inp)
        analysis = analyze_output(out_path, mode)
        if analysis.status != AnalyzerStatus.COMPLETED.value:
            continue
        return {
            "out_path": str(out_path),
            "analysis": analysis,
        }
    return None


def cmd_status(args: Any) -> int:
    cfg = load_config(args.config)
    try:
        reaction_dir = _validate_reaction_dir(cfg, args.reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    state = load_state(reaction_dir)
    if not state:
        logger.error("State file not found: %s", state_path(reaction_dir))
        return 1

    payload = {
        "status": state.get("status"),
        "reaction_dir": str(reaction_dir),
        "selected_inp": state.get("selected_inp"),
        "attempt_count": len(state.get("attempts", [])),
        "run_state": str(state_path(reaction_dir)),
        "final_result": state.get("final_result"),
    }
    if args.json:
        print(json.dumps(state, ensure_ascii=True, indent=2))
    else:
        _emit(payload, as_json=False)
    return 0


def cmd_run_inp(args: Any, *, runner_cls: Type[OrcaRunner] = OrcaRunner) -> int:
    cfg = load_config(args.config)
    try:
        reaction_dir = _validate_reaction_dir(cfg, args.reaction_dir)
        selected_inp = _select_latest_inp(reaction_dir)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    logger.info("Selected input: %s", selected_inp)

    max_retries = int(args.max_retries if args.max_retries is not None else cfg.runtime.default_max_retries)
    max_retries = max(0, max_retries)

    if not args.force:
        done = _existing_completed_out(selected_inp)
        if done:
            state = new_state(reaction_dir, selected_inp, max_retries=max_retries)
            notifier = create_notifier(
                cfg.monitoring, reaction_dir,
                state["run_id"], str(selected_inp), state,
            )
            notify = make_notify_callback(notifier)
            if notify:
                notify(event_run_terminal(
                    EVT_RUN_COMPLETED,
                    state["run_id"], str(reaction_dir), str(selected_inp),
                    status="completed",
                    reason="existing_out_completed",
                    attempt_count=0,
                ))
            if notifier:
                notifier.shutdown()
            return _exit_with_result(
                reaction_dir, state, selected_inp,
                status=RunStatus.COMPLETED,
                analyzer_status=AnalyzerStatus.COMPLETED,
                reason="existing_out_completed",
                last_out_path=done["out_path"],
                resumed=None, as_json=args.json, exit_code=0, emit=_emit,
                extra={"skipped_execution": True},
            )

    try:
        with acquire_run_lock(reaction_dir):
            state, resumed = load_or_create_state(
                reaction_dir,
                selected_inp,
                max_retries=max_retries,
                to_resolved_local=_to_resolved_local,
            )
            notifier = create_notifier(
                cfg.monitoring, reaction_dir,
                state["run_id"], str(selected_inp), state,
            )
            notify = make_notify_callback(notifier)

            if notify:
                notify(event_run_started(
                    state["run_id"], str(reaction_dir), str(selected_inp),
                ))

            runner = runner_cls(cfg.paths.orca_executable)
            try:
                return run_attempts(
                    reaction_dir,
                    selected_inp,
                    state,
                    resumed=resumed,
                    runner=runner,
                    max_retries=max_retries,
                    as_json=args.json,
                    retry_inp_path=_retry_inp_path,
                    to_resolved_local=_to_resolved_local,
                    emit=_emit,
                    notify=notify,
                )
            finally:
                if notifier:
                    notifier.shutdown()
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        logger.exception("Unexpected error while running input: %s", exc)
        return 1
