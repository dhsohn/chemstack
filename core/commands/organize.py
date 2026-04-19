from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from ..config import AppConfig, load_config
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
from ..state_store import ORGANIZED_REF_NAME, now_utc_iso
from orca_auto.tracking import reindex_job_locations, resource_dict, upsert_job_record, write_organized_ref
from ..telegram_notifier import escape_html, send_message
from ..types import RunState
from ._helpers import (
    _validate_reaction_dir,
    _validate_root_scan_dir,
    finalize_batch_apply,
)

logger = logging.getLogger(__name__)


def _emit_organize(payload: Dict[str, Any]) -> None:
    for key in ["action", "to_organize", "skipped", "organized", "failed", "records_count", "job_locations_count"]:
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


def _tracking_resources(cfg: AppConfig) -> dict[str, int]:
    return resource_dict(
        cfg.resources.max_cores_per_task,
        cfg.resources.max_memory_gb_per_task,
    )


def _tracking_job_id(plan: OrganizePlan, state: RunState) -> str:
    return str(state.get("job_id") or state.get("run_id") or plan.run_id).strip()


def _write_tracking_after_move(
    cfg: AppConfig,
    *,
    plan: OrganizePlan,
    state_after_move: RunState,
) -> None:
    job_id = _tracking_job_id(plan, state_after_move)
    requested = _tracking_resources(cfg)
    selected_inp = str(state_after_move.get("selected_inp") or "").strip()

    plan.source_dir.mkdir(parents=True, exist_ok=True)
    write_organized_ref(
        plan.source_dir,
        {
            "job_id": job_id,
            "run_id": plan.run_id,
            "original_run_dir": str(plan.source_dir),
            "organized_output_dir": str(plan.target_abs_path),
            "organized_at": now_utc_iso(),
            "status": str(state_after_move.get("status") or "completed"),
            "job_type": plan.job_type,
            "selected_inp": selected_inp,
            "selected_input_xyz": selected_inp,
            "molecule_key": plan.molecule_key,
            "resource_request": requested,
            "resource_actual": requested,
        },
    )
    upsert_job_record(
        cfg,
        job_id=job_id,
        status=str(state_after_move.get("status") or "completed"),
        job_dir=plan.source_dir,
        job_type=plan.job_type,
        selected_input_xyz=selected_inp,
        organized_output_dir=plan.target_abs_path,
        molecule_key=plan.molecule_key,
        resource_request=requested,
        resource_actual=requested,
    )


def _cleanup_organized_ref_stub(plan: OrganizePlan) -> None:
    organized_ref_path = plan.source_dir / ORGANIZED_REF_NAME
    if organized_ref_path.exists():
        organized_ref_path.unlink()
    try:
        plan.source_dir.rmdir()
    except OSError:
        pass


def _restore_tracking_after_rollback(
    cfg: AppConfig,
    *,
    plan: OrganizePlan,
    state_after_rollback: RunState,
) -> None:
    job_id = _tracking_job_id(plan, state_after_rollback)
    requested = _tracking_resources(cfg)
    upsert_job_record(
        cfg,
        job_id=job_id,
        status=str(state_after_rollback.get("status") or "completed"),
        job_dir=plan.source_dir,
        job_type=plan.job_type,
        selected_input_xyz=str(state_after_rollback.get("selected_inp") or "").strip(),
        molecule_key=plan.molecule_key,
        resource_request=requested,
        resource_actual=requested,
    )


_ORGANIZE_RESULT_LIMIT = 10


def _build_organize_message(
    organized: list[Dict[str, Any]],
    skipped: list[Dict[str, Any]],
    failures: list[Dict[str, Any]],
    skips: list[SkipReason],
) -> str | None:
    """Compose a Telegram HTML message for organize results.

    Returns None if there is nothing to report.
    """
    organized_count = len(organized)
    skipped_count = len(skipped) + len(skips)
    failed_count = len(failures)

    if organized_count == 0 and skipped_count == 0 and failed_count == 0:
        return None

    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    header = f"\U0001f4c1 <b>orca_auto organize</b>  <code>{escape_html(now)}</code>"
    divider = "\u2500" * 28

    sections: list[str] = [header, divider]

    # Summary line
    summary_parts: list[str] = []
    if organized_count > 0:
        summary_parts.append(f"\u2705 Organized: {organized_count}")
    if skipped_count > 0:
        summary_parts.append(f"\u23ed Skipped: {skipped_count}")
    if failed_count > 0:
        summary_parts.append(f"\u274c Failed: {failed_count}")
    sections.append(f"\U0001f4ca <b>Summary</b>\n{' | '.join(summary_parts)}")

    # Organized details
    if organized:
        lines: list[str] = []
        for item in organized[:_ORGANIZE_RESULT_LIMIT]:
            plan = item.get("_plan")
            if plan is not None:
                job_label = plan.job_type.upper() if plan.job_type else "-"
                mol_label = plan.molecule_key or "-"
                lines.append(
                    f"\u2705 <b>{escape_html(plan.run_id[:12])}</b>\n"
                    f"   \U0001f4c2 {escape_html(str(plan.source_dir.name))} \u2192 {escape_html(plan.target_rel_path)}\n"
                    f"   \U0001f3f7 {escape_html(job_label)} | {escape_html(mol_label)}"
                )
            else:
                lines.append(f"\u2705 <b>{escape_html(item.get('run_id', '?'))}</b>")
        detail_header = f"\u2705 <b>Organized</b>  ({organized_count})"
        if organized_count > _ORGANIZE_RESULT_LIMIT:
            detail_header += f"  showing {_ORGANIZE_RESULT_LIMIT}/{organized_count}"
        sections.append(detail_header + "\n\n" + "\n\n".join(lines))

    # Failed details
    if failures:
        lines = []
        for item in failures[:5]:
            run_id = escape_html(item.get("run_id", "?"))
            reason = escape_html(item.get("reason", "unknown"))
            lines.append(f"\u274c <b>{run_id}</b>\n   \U0001f4ac {reason}")
        fail_header = f"\u274c <b>Failed</b>  ({failed_count})"
        sections.append(fail_header + "\n\n" + "\n\n".join(lines))

    # Skipped details (abbreviated)
    if skips:
        skip_lines: list[str] = []
        for s in skips[:5]:
            skip_lines.append(
                f"\u23ed {escape_html(s.reaction_dir)}\n"
                f"   \U0001f4ac {escape_html(s.reason)}"
            )
        skip_header = f"\u23ed <b>Skipped</b>  ({skipped_count})"
        if skipped_count > 5:
            skip_header += f"  showing 5/{skipped_count}"
        sections.append(skip_header + "\n\n" + "\n\n".join(skip_lines))

    sections.append(divider)

    return "\n\n".join(sections)


def _send_organize_notification(
    cfg: AppConfig,
    *,
    organized: list[Dict[str, Any]],
    skipped_results: list[Dict[str, Any]],
    failures: list[Dict[str, Any]],
    skips: list[SkipReason],
) -> None:
    if not cfg.telegram.enabled:
        return

    message = _build_organize_message(organized, skipped_results, failures, skips)
    if message is None:
        return
    if send_message(cfg.telegram, message):
        logger.info("Telegram organize notification sent successfully")
    else:
        logger.warning("Failed to send Telegram organize notification")


def _resolve_organize_scope(
    cfg: AppConfig,
    *,
    organized_root: Path,
    reaction_dir_raw: str | None,
    root_raw: str | None,
) -> tuple[list[OrganizePlan], list[SkipReason]] | None:
    if reaction_dir_raw:
        try:
            reaction_dir = _validate_reaction_dir(cfg, reaction_dir_raw)
        except ValueError as exc:
            logger.error("%s", exc)
            return None
        plan, skip = plan_single(reaction_dir, organized_root)
        return ([plan] if plan else []), ([skip] if skip else [])

    try:
        assert isinstance(root_raw, str)
        root = _validate_root_scan_dir(cfg, root_raw)
    except ValueError as exc:
        logger.error("%s", exc)
        return None
    return plan_root_scan(root, organized_root)


def _build_dry_run_summary(
    plans: list[OrganizePlan],
    skips_list: list[SkipReason],
) -> Dict[str, Any]:
    return {
        "action": "dry_run",
        "to_organize": len(plans),
        "skipped": len(skips_list),
        "plans": [_plan_to_dict(p) for p in plans],
        "skip_reasons": [{"reaction_dir": s.reaction_dir, "reason": s.reason} for s in skips_list],
    }


def _cmd_organize_apply(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    cfg: AppConfig,
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
                _write_tracking_after_move(cfg, plan=plan, state_after_move=state_after_move)
                append_record(organized_root, _build_index_record(plan, state_after_move))
                results.append({"run_id": plan.run_id, "action": "moved", "_plan": plan})
        except Exception as exc:
            logger.error("Organize apply failed for %s: %s", plan.run_id, exc)
            failure_reason = f"apply_failed: {exc}"
            if moved:
                try:
                    _cleanup_organized_ref_stub(plan)
                    rollback_move(plan)
                    state_after_rollback = sync_state_after_rollback(plan)
                    _restore_tracking_after_rollback(cfg, plan=plan, state_after_rollback=state_after_rollback)
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

    organized = [r for r in results if r.get("action") == "moved"]
    skipped_results = [r for r in results if r.get("action") == "skipped"]
    organized_count = len(organized)
    skipped_count = len(skips) + len(skipped_results)

    summary = {
        "action": "apply",
        "organized": organized_count,
        "skipped": skipped_count,
        "failed": len(failures),
        "failures": failures,
    }

    _send_organize_notification(
        cfg,
        organized=organized,
        skipped_results=skipped_results,
        failures=failures,
        skips=skips,
    )

    return finalize_batch_apply(
        summary, _emit_organize, failures,
    )


def cmd_organize(args: Any) -> int:
    cfg = load_config(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    if getattr(args, "rebuild_index", False):
        count = rebuild_index(organized_root)
        tracking_count = reindex_job_locations(cfg)
        _emit_organize({"action": "rebuild_index", "records_count": count, "job_locations_count": tracking_count})
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

    scope = _resolve_organize_scope(
        cfg,
        organized_root=organized_root,
        reaction_dir_raw=reaction_dir_raw,
        root_raw=root_raw,
    )
    if scope is None:
        return 1
    plans, skips_list = scope

    if not apply_mode:
        _emit_organize(_build_dry_run_summary(plans, skips_list))
        return 0

    return _cmd_organize_apply(plans, skips_list, organized_root, cfg)
