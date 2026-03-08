from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from ..config import load_config
from ..organize_index import (
    acquire_index_lock,
    append_record,
    load_index,
    rebuild_index,
    to_reaction_relative_path,
)
from ..result_organizer import (
    OrganizePlan,
    SkipReason,
    check_conflict,
    execute_move,
    plan_root_scan,
    plan_single,
    rollback_move,
    sync_state_after_move,
    sync_state_after_rollback,
)
from ..state_store import now_utc_iso
from ..types import RunState
from ._helpers import (
    _validate_reaction_dir,
    _validate_root_scan_dir,
    finalize_batch_apply,
)

logger = logging.getLogger(__name__)


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


def _build_index_record(plan: OrganizePlan, state: RunState) -> Dict[str, Any]:
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


def _cmd_organize_apply(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    cfg: Any,
    as_json: bool,
) -> int:
    from ..organize_index import append_failed_rollback

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
    return finalize_batch_apply(
        summary, _emit_organize, as_json, failures,
    )


def cmd_organize(args: Any) -> int:
    cfg = load_config(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    if getattr(args, "rebuild_index", False):
        count = rebuild_index(organized_root)
        _emit_organize({"action": "rebuild_index", "records_count": count}, as_json=args.json)
        return 0

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
            assert isinstance(root_raw, str)
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

    return _cmd_organize_apply(plans, skips_list, organized_root, cfg, as_json=args.json)
