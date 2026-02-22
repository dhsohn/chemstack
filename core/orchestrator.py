from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Type

from .attempt_engine import _exit_with_result, run_attempts

logger = logging.getLogger(__name__)
from .completion_rules import detect_completion_mode
from .config import AppConfig, load_config
from .orca_runner import OrcaRunner
from .out_analyzer import analyze_output
from .pathing import is_subpath, to_local_path
from .state_machine import load_or_create_state
from .state_store import acquire_run_lock, load_state, new_state, now_utc_iso, state_path
from .statuses import AnalyzerStatus, RunStatus
from .notifier import (
    create_notifier,
    event_run_started,
    event_run_terminal,
    make_notify_callback,
    EVT_RUN_COMPLETED,
)
from .organize_index import (
    append_record,
    acquire_index_lock,
    find_by_job_type,
    find_by_run_id,
    load_index,
    rebuild_index,
    to_reaction_relative_path,
)
from .result_organizer import (
    OrganizePlan,
    SkipReason,
    check_conflict,
    execute_move,
    rollback_move,
    plan_root_scan,
    plan_single,
    sync_state_after_rollback,
    sync_state_after_move,
)


RETRY_INP_RE = re.compile(r"\.retry\d+$", re.IGNORECASE)
CONFIG_ENV_VAR = "ORCA_AUTO_CONFIG"


def default_config_path() -> str:
    env_path = os.getenv(CONFIG_ENV_VAR, "").strip()
    if env_path:
        return env_path

    repo_default = Path(__file__).resolve().parents[1] / "config" / "orca_auto.yaml"
    if repo_default.exists():
        return str(repo_default)

    home_default = Path.home() / "orca_auto" / "config" / "orca_auto.yaml"
    if home_default.exists():
        return str(home_default)

    return str(repo_default)


def _to_resolved_local(path_text: str) -> Path:
    return Path(to_local_path(path_text)).expanduser().resolve()


def _validate_reaction_dir(cfg: AppConfig, reaction_dir_raw: str) -> Path:
    reaction_dir = _to_resolved_local(reaction_dir_raw)
    if not reaction_dir.exists() or not reaction_dir.is_dir():
        raise ValueError(f"Reaction directory not found: {reaction_dir}")

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if not is_subpath(reaction_dir, allowed_root):
        raise ValueError(
            f"Reaction directory must be under allowed root: {allowed_root}. got={reaction_dir}"
        )
    return reaction_dir


def _validate_root_scan_dir(cfg: AppConfig, root_raw: str) -> Path:
    root = _to_resolved_local(root_raw)
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Root directory not found: {root}")

    allowed_root = _to_resolved_local(cfg.runtime.allowed_root)
    if root != allowed_root:
        raise ValueError(
            f"--root must exactly match allowed_root: {allowed_root}. got={root}"
        )
    return root


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


def _emit(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in [
        "status",
        "reaction_dir",
        "selected_inp",
        "attempt_count",
        "reason",
        "run_state",
        "report_json",
        "report_md",
    ]:
        if key in payload:
            print(f"{key}: {payload[key]}")


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


# ---------------------------------------------------------------------------
# organize command
# ---------------------------------------------------------------------------


def _emit_organize(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in ["action", "to_organize", "skipped", "organized", "failed", "records_count"]:
        if key in payload:
            print(f"{key}: {payload[key]}")
    for p in payload.get("plans", []):
        print(f"  {p['source_dir']} -> {p['target_rel_path']}")
    for s in payload.get("skip_reasons", []):
        print(f"  SKIP {s['reaction_dir']}: {s['reason']}")
    for f in payload.get("failures", []):
        print(f"  FAIL {f['run_id']}: {f['reason']}")
    for key in ["run_id", "job_type", "molecule_key", "organized_path", "count"]:
        if key in payload:
            print(f"{key}: {payload[key]}")
    for r in payload.get("results", []):
        print(f"  {r.get('run_id', '?')}: {r.get('organized_path', '?')}")


def _plan_to_dict(plan: OrganizePlan) -> Dict[str, Any]:
    return {
        "run_id": plan.run_id,
        "source_dir": str(plan.source_dir),
        "target_rel_path": plan.target_rel_path,
        "target_abs_path": str(plan.target_abs_path),
        "job_type": plan.job_type,
        "molecule_key": plan.molecule_key,
    }


def _build_index_record(plan: OrganizePlan, state: Dict[str, Any]) -> Dict[str, Any]:
    final_result = state.get("final_result")
    if not isinstance(final_result, dict):
        final_result = {}

    attempts = state.get("attempts")
    attempt_count = len(attempts) if isinstance(attempts, list) else 0

    return {
        "run_id": plan.run_id,
        "reaction_dir": str(plan.target_abs_path),
        "status": state.get("status", ""),
        "analyzer_status": final_result.get("analyzer_status", ""),
        "reason": final_result.get("reason", ""),
        "job_type": plan.job_type,
        "molecule_key": plan.molecule_key,
        "selected_inp": to_reaction_relative_path(state.get("selected_inp", ""), plan.target_abs_path),
        "last_out_path": to_reaction_relative_path(final_result.get("last_out_path", ""), plan.target_abs_path),
        "attempt_count": attempt_count,
        "completed_at": final_result.get("completed_at", ""),
        "organized_at": now_utc_iso(),
        "organized_path": plan.target_rel_path,
    }


def _cmd_organize_find(args: Any, organized_root: Path, as_json: bool) -> int:
    run_id = getattr(args, "run_id", None)
    job_type = getattr(args, "job_type", None)

    if run_id:
        record = find_by_run_id(organized_root, run_id)
        if record is None:
            logger.error("run_id not found: %s", run_id)
            return 1
        _emit_organize(record, as_json=as_json)
        return 0

    if job_type:
        limit = getattr(args, "limit", 0) or 0
        records = find_by_job_type(organized_root, job_type, limit=limit)
        _emit_organize({"results": records, "count": len(records)}, as_json=as_json)
        return 0

    logger.error("--find requires --run-id or --job-type")
    return 1


def _cmd_organize_apply(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    as_json: bool,
) -> int:
    from .organize_index import append_failed_rollback

    results: list[Dict[str, Any]] = []
    failures: list[Dict[str, Any]] = []

    for plan in plans:
        moved = False
        try:
            with acquire_index_lock(organized_root):
                index = load_index(organized_root)
                conflict = check_conflict(plan, index)
                if conflict == "already_organized":
                    results.append({"run_id": plan.run_id, "action": "skipped", "reason": conflict})
                    continue
                if conflict:
                    failures.append({"run_id": plan.run_id, "reason": conflict})
                    continue

                execute_move(plan)
                moved = True
                state_after_move = sync_state_after_move(plan)
                record = _build_index_record(plan, state_after_move)
                append_record(organized_root, record)
                results.append({"run_id": plan.run_id, "action": "moved"})
        except Exception as exc:
            logger.error("Organize apply failed for %s: %s", plan.run_id, exc)
            failure_reason = f"apply_failed: {exc}"
            if moved:
                try:
                    rollback_move(plan)
                    sync_state_after_rollback(plan)
                    failure_reason = f"{failure_reason}; rolled_back=true"
                except Exception as rollback_exc:
                    logger.error("Rollback failed for %s: %s", plan.run_id, rollback_exc)
                    failure_reason = f"{failure_reason}; rollback_failed: {rollback_exc}"
                    append_failed_rollback(organized_root, {
                        "run_id": plan.run_id,
                        "target_path": str(plan.target_abs_path),
                        "error": str(rollback_exc),
                        "timestamp": now_utc_iso(),
                    })
            failures.append({"run_id": plan.run_id, "reason": failure_reason})

    organized_count = len([r for r in results if r.get("action") == "moved"])
    skipped_count = len(skips) + len([r for r in results if r.get("action") == "skipped"])

    summary = {
        "action": "apply",
        "organized": organized_count,
        "skipped": skipped_count,
        "failed": len(failures),
        "failures": failures,
    }
    _emit_organize(summary, as_json=as_json)
    return 1 if failures else 0


def cmd_organize(args: Any) -> int:
    cfg = load_config(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    if getattr(args, "rebuild_index", False):
        count = rebuild_index(organized_root)
        _emit_organize({"action": "rebuild_index", "records_count": count}, as_json=args.json)
        return 0

    if getattr(args, "find", False):
        return _cmd_organize_find(args, organized_root, as_json=args.json)

    reaction_dir_raw = getattr(args, "reaction_dir", None)
    root_raw = getattr(args, "root", None)

    if reaction_dir_raw and root_raw:
        logger.error("--reaction-dir and --root are mutually exclusive")
        return 1

    if not reaction_dir_raw and not root_raw:
        logger.error("Either --reaction-dir or --root is required")
        return 1

    apply_mode = getattr(args, "apply", False)

    if reaction_dir_raw:
        try:
            reaction_dir = _validate_reaction_dir(cfg, reaction_dir_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1
        plan, skip = plan_single(reaction_dir, organized_root)
        plans = [plan] if plan else []
        skips_list = [skip] if skip else []
    else:
        try:
            root = _validate_root_scan_dir(cfg, root_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1
        plans, skips_list = plan_root_scan(root, organized_root)

    if not apply_mode:
        summary = {
            "action": "dry_run",
            "to_organize": len(plans),
            "skipped": len(skips_list),
            "plans": [_plan_to_dict(p) for p in plans],
            "skip_reasons": [{"reaction_dir": s.reaction_dir, "reason": s.reason} for s in skips_list],
        }
        _emit_organize(summary, as_json=args.json)
        return 0

    return _cmd_organize_apply(plans, skips_list, organized_root, as_json=args.json)
