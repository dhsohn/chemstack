from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

from ..config import load_config
from ..geometry_checker import (
    CheckResult,
    CheckSkipReason,
    check_root_scan,
    check_single,
)
from ._helpers import _validate_check_reaction_dir, _validate_check_root_dir

logger = logging.getLogger(__name__)


def _check_item_to_dict(item: Any) -> Dict[str, Any]:
    return {
        "check_name": item.check_name,
        "severity": item.severity,
        "message": item.message,
        "details": item.details,
    }


def _result_to_dict(r: CheckResult) -> Dict[str, Any]:
    return {
        "reaction_dir": r.reaction_dir,
        "run_id": r.run_id,
        "job_type": r.job_type,
        "overall": r.overall,
        "checks": [_check_item_to_dict(c) for c in r.checks],
    }


def _skip_to_dict(s: CheckSkipReason) -> Dict[str, Any]:
    return {"reaction_dir": s.reaction_dir, "reason": s.reason}


def _emit_check(payload: Dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return
    for key in ["action", "checked", "skipped", "failed", "warned"]:
        if key in payload:
            print(f"{key}: {payload[key]}")
    for r in payload.get("results", []):
        overall = r.get("overall", "?")
        print(f"  [{overall.upper()}] {r.get('reaction_dir', '?')} (run_id={r.get('run_id', '?')}, type={r.get('job_type', '?')})")
        for c in r.get("checks", []):
            if c.get("severity") != "ok":
                print(f"    {c['severity']}: {c['message']}")
    for s in payload.get("skip_reasons", []):
        print(f"  SKIP {s['reaction_dir']}: {s['reason']}")


def cmd_check(args: Any) -> int:
    cfg = load_config(args.config)
    as_json = getattr(args, "json", False)

    reaction_dir_raw = getattr(args, "reaction_dir", None)
    root_raw = getattr(args, "root", None)

    if reaction_dir_raw and root_raw:
        logger.error("--reaction-dir and --root are mutually exclusive")
        return 1

    if reaction_dir_raw:
        try:
            reaction_dir = _validate_check_reaction_dir(cfg, reaction_dir_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return 1
        result, skip = check_single(reaction_dir)
        results = [result] if result else []
        skips: List[CheckSkipReason] = [skip] if skip else []
    else:
        if root_raw:
            try:
                root = _validate_check_root_dir(cfg, root_raw)
            except ValueError as exc:
                logger.error("%s", exc)
                return 1
        else:
            root = Path(cfg.runtime.organized_root).resolve()
            if not root.exists() or not root.is_dir():
                logger.error("Default organized_root is not a readable directory: %s", root)
                return 1
        results, skips = check_root_scan(root)

    failed_count = sum(1 for r in results if r.overall == "fail")
    warned_count = sum(1 for r in results if r.overall == "warn")

    payload: Dict[str, Any] = {
        "action": "scan",
        "checked": len(results),
        "skipped": len(skips),
        "failed": failed_count,
        "warned": warned_count,
        "results": [_result_to_dict(r) for r in results],
        "skip_reasons": [_skip_to_dict(s) for s in skips],
    }
    _emit_check(payload, as_json)

    return 1 if failed_count > 0 else 0
