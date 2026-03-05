from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

from ..config import load_config
from ..result_cleaner import (
    CleanupPlan,
    CleanupSkipReason,
    execute_cleanup,
    plan_cleanup_root_scan,
    plan_cleanup_single,
)
from ._helpers import (
    _MAX_SAMPLE_FILES,
    _human_bytes,
    _validate_cleanup_reaction_dir,
    _validate_organized_root_dir,
    finalize_batch_apply,
)

logger = logging.getLogger(__name__)


def _emit_cleanup(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in ["action", "to_clean", "skipped", "cleaned", "failed",
                 "total_files_removed", "total_bytes_freed_human"]:
        if key in payload:
            print(f"{key}: {payload[key]}")
    for p in payload.get("plans", []):
        print(f"  {p['reaction_dir']}: {p['remove_count']} files, {p['bytes_human']}")
    for s in payload.get("skip_reasons", []):
        print(f"  SKIP {s['reaction_dir']}: {s['reason']}")
    for f in payload.get("failures", []):
        print(f"  FAIL {f['run_id']}: {f['errors']}")


def _cleanup_plan_to_dict(plan: CleanupPlan) -> Dict[str, Any]:
    all_names = [e.path.name for e in plan.files_to_remove]
    return {
        "run_id": plan.run_id,
        "reaction_dir": str(plan.reaction_dir),
        "remove_count": len(plan.files_to_remove),
        "keep_count": plan.keep_count,
        "total_remove_bytes": plan.total_remove_bytes,
        "bytes_human": _human_bytes(plan.total_remove_bytes),
        "sample_files": all_names[:_MAX_SAMPLE_FILES],
    }


def _cmd_cleanup_apply(
    plans: list[CleanupPlan],
    skips: list[CleanupSkipReason],
    cfg: Any,
    as_json: bool,
) -> int:
    from ..result_cleaner import CleanupResult

    results: list[CleanupResult] = []
    failures: list[Dict[str, Any]] = []

    for plan in plans:
        try:
            result = execute_cleanup(plan)
            results.append(result)
            if result.errors:
                failures.append({"run_id": result.run_id, "errors": result.errors})
        except Exception as exc:
            logger.error("Cleanup failed for %s: %s", plan.run_id, exc)
            failures.append({"run_id": plan.run_id, "errors": [str(exc)]})

    total_files = sum(r.files_removed for r in results)
    total_bytes = sum(r.bytes_freed for r in results)

    cleaned_count = len([r for r in results if not r.errors])
    summary: Dict[str, Any] = {
        "action": "apply",
        "cleaned": cleaned_count,
        "skipped": len(skips),
        "failed": len(failures),
        "total_files_removed": total_files,
        "total_bytes_freed": total_bytes,
        "total_bytes_freed_human": _human_bytes(total_bytes),
        "failures": failures,
    }
    return finalize_batch_apply(
        summary, _emit_cleanup, as_json, failures,
    )


def cmd_cleanup(args: Any) -> int:
    cfg = load_config(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    reaction_dir_raw = getattr(args, "reaction_dir", None)
    root_raw = getattr(args, "root", None)

    if reaction_dir_raw and root_raw:
        logger.error("--reaction-dir and --root are mutually exclusive")
        return 1

    if not reaction_dir_raw and not root_raw:
        root_raw = cfg.runtime.organized_root

    apply_mode = getattr(args, "apply", False)
    keep_extensions = set(cfg.cleanup.keep_extensions)
    keep_filenames = set(cfg.cleanup.keep_filenames)
    remove_patterns = cfg.cleanup.remove_patterns
    remove_overrides_keep = cfg.cleanup.remove_overrides_keep

    if reaction_dir_raw:
        try:
            reaction_dir = _validate_cleanup_reaction_dir(cfg, reaction_dir_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1
        plan, skip = plan_cleanup_single(
            reaction_dir,
            keep_extensions,
            keep_filenames,
            remove_patterns,
            remove_overrides_keep=remove_overrides_keep,
        )
        plans = [plan] if plan else []
        skips_list = [skip] if skip else []
    else:
        try:
            root = _validate_organized_root_dir(cfg, root_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1
        plans, skips_list = plan_cleanup_root_scan(
            root,
            keep_extensions,
            keep_filenames,
            remove_patterns,
            remove_overrides_keep=remove_overrides_keep,
        )

    if not apply_mode:
        total_bytes = sum(p.total_remove_bytes for p in plans)
        summary = {
            "action": "dry_run",
            "to_clean": len(plans),
            "skipped": len(skips_list),
            "total_bytes_freed": total_bytes,
            "total_bytes_freed_human": _human_bytes(total_bytes),
            "plans": [_cleanup_plan_to_dict(p) for p in plans],
            "skip_reasons": [
                {"reaction_dir": s.reaction_dir, "reason": s.reason}
                for s in skips_list
            ],
        }
        _emit_cleanup(summary, as_json=args.json)
        return 0

    return _cmd_cleanup_apply(plans, skips_list, cfg, as_json=args.json)
