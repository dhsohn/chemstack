from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable, Dict

from chemstack.core.paths.workflow import workflow_workspace_internal_engine_paths_from_path

from ..config import AppConfig
from ..result_organizer import OrganizePlan, SkipReason
from . import organize_apply as _organize_apply

logger = logging.getLogger(__name__)


def workflow_runtime_paths(cfg: AppConfig, path: str | Path) -> dict[str, Path] | None:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return None
    return workflow_workspace_internal_engine_paths_from_path(
        path,
        workflow_root=workflow_root,
        engine="orca",
    )


def resolved_organized_root(cfg: AppConfig, reaction_dir: str | Path) -> Path:
    runtime_paths = workflow_runtime_paths(cfg, reaction_dir)
    if runtime_paths is not None:
        return runtime_paths["organized_root"].expanduser().resolve()
    return Path(cfg.runtime.organized_root).resolve()


def resolve_organize_scope(
    cfg: AppConfig,
    *,
    organized_root: Path,
    reaction_dir_raw: str | None,
    root_raw: str | None,
    validate_reaction_dir_fn: Callable[[AppConfig, str], Path],
    validate_root_scan_dir_fn: Callable[[AppConfig, str], Path],
    plan_single_fn: Callable[[Path, Path], tuple[OrganizePlan | None, SkipReason | None]],
    plan_root_scan_fn: Callable[[Path, Path], tuple[list[OrganizePlan], list[SkipReason]]],
    log: logging.Logger = logger,
) -> tuple[list[OrganizePlan], list[SkipReason]] | None:
    if reaction_dir_raw:
        try:
            reaction_dir = validate_reaction_dir_fn(cfg, reaction_dir_raw)
        except ValueError as exc:
            log.error("%s", exc)
            return None
        plan, skip = plan_single_fn(reaction_dir, organized_root)
        return ([plan] if plan else []), ([skip] if skip else [])

    try:
        assert isinstance(root_raw, str)
        root = validate_root_scan_dir_fn(cfg, root_raw)
    except ValueError as exc:
        log.error("%s", exc)
        return None
    return plan_root_scan_fn(root, organized_root)


def build_apply_dependencies(
    *,
    acquire_index_lock_fn: Any,
    append_failed_rollback_fn: Any,
    append_record_fn: Any,
    build_index_record_fn: Any,
    check_conflict_fn: Any,
    cleanup_organized_ref_stub_fn: Any,
    execute_move_fn: Any,
    load_index_fn: Any,
    now_utc_iso_fn: Any,
    rollback_move_fn: Any,
    send_organize_notification_fn: Any,
    sync_state_after_move_fn: Any,
    sync_state_after_rollback_fn: Any,
    write_tracking_after_move_fn: Any,
    restore_tracking_after_rollback_fn: Any,
    log: logging.Logger = logger,
    plan_conflict_result_fn: Any = None,
    bookkeep_successful_move_fn: Any = None,
    bookkeep_rollback_failure_fn: Any = None,
    rollback_after_apply_failure_fn: Any = None,
    apply_one_organize_plan_fn: Any = None,
) -> _organize_apply.OrganizeApplyDependencies:
    return _organize_apply.OrganizeApplyDependencies(
        acquire_index_lock=acquire_index_lock_fn,
        append_failed_rollback=append_failed_rollback_fn,
        append_record=append_record_fn,
        build_index_record=build_index_record_fn,
        check_conflict=check_conflict_fn,
        cleanup_organized_ref_stub=cleanup_organized_ref_stub_fn,
        execute_move=execute_move_fn,
        load_index=load_index_fn,
        now_utc_iso=now_utc_iso_fn,
        rollback_move=rollback_move_fn,
        send_organize_notification=send_organize_notification_fn,
        sync_state_after_move=sync_state_after_move_fn,
        sync_state_after_rollback=sync_state_after_rollback_fn,
        write_tracking_after_move=write_tracking_after_move_fn,
        restore_tracking_after_rollback=restore_tracking_after_rollback_fn,
        log=log,
        plan_conflict_result=plan_conflict_result_fn,
        bookkeep_successful_move=bookkeep_successful_move_fn,
        bookkeep_rollback_failure=bookkeep_rollback_failure_fn,
        rollback_after_apply_failure=rollback_after_apply_failure_fn,
        apply_one_organize_plan=apply_one_organize_plan_fn,
    )


def cmd_organize_apply(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    cfg: AppConfig,
    *,
    apply_plans_fn: Callable[..., Dict[str, Any]],
    finalize_batch_apply_fn: Callable[[Dict[str, Any], Any, list[Dict[str, Any]]], int],
    emit_organize_fn: Callable[[Dict[str, Any]], None],
) -> int:
    summary = apply_plans_fn(
        plans,
        skips,
        organized_root,
        cfg,
        notify_summary=True,
    )
    return finalize_batch_apply_fn(
        summary,
        emit_organize_fn,
        summary["failures"],
    )


def organize_no_plan_result(reaction_dir: Path, skips: list[SkipReason]) -> Dict[str, Any]:
    if skips:
        first_skip = skips[0]
        return {
            "action": "skipped",
            "reaction_dir": first_skip.reaction_dir,
            "reason": first_skip.reason,
        }
    return {
        "action": "skipped",
        "reaction_dir": str(reaction_dir),
        "reason": "nothing_to_organize",
    }


def organize_failure_result(reaction_dir: Path, summary: Dict[str, Any]) -> Dict[str, Any]:
    failure = next(
        (item for item in summary["failures"] if isinstance(item, dict)),
        {},
    )
    return {
        "action": "failed",
        "reaction_dir": str(reaction_dir),
        "reason": str(failure.get("reason") or "organize_failed"),
        "run_id": str(failure.get("run_id") or ""),
    }


def organize_success_result(organized: list[Any]) -> Dict[str, Any] | None:
    if not organized:
        return None
    plan = organized[0].get("_plan") if isinstance(organized[0], dict) else None
    if not isinstance(plan, OrganizePlan):
        return None
    return {
        "action": "organized",
        "reaction_dir": str(plan.source_dir),
        "run_id": plan.run_id,
        "target_dir": str(plan.target_abs_path),
        "job_type": plan.job_type,
        "molecule_key": plan.molecule_key,
    }


def organize_skipped_result(
    reaction_dir: Path,
    *,
    skipped_results: list[Any],
    skips: list[SkipReason],
) -> Dict[str, Any]:
    if skipped_results:
        skipped_item = skipped_results[0]
        return {
            "action": "skipped",
            "reaction_dir": str(reaction_dir),
            "reason": str(skipped_item.get("reason") or "skipped"),
            "run_id": str(skipped_item.get("run_id") or ""),
        }
    return organize_no_plan_result(reaction_dir, skips)


def organize_reaction_dir(
    cfg: AppConfig,
    reaction_dir: Path,
    *,
    notify_summary: bool,
    resolved_organized_root_fn: Callable[[AppConfig, str | Path], Path],
    resolve_scope_fn: Callable[..., tuple[list[OrganizePlan], list[SkipReason]] | None],
    apply_plans_fn: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    organized_root = resolved_organized_root_fn(cfg, reaction_dir)
    scope = resolve_scope_fn(
        cfg,
        organized_root=organized_root,
        reaction_dir_raw=str(reaction_dir),
        root_raw=None,
    )
    if scope is None:
        return {
            "action": "failed",
            "reaction_dir": str(reaction_dir),
            "reason": "invalid_reaction_dir",
        }

    plans, skips = scope
    if not plans:
        return organize_no_plan_result(reaction_dir, skips)

    summary = apply_plans_fn(
        plans,
        skips,
        organized_root,
        cfg,
        notify_summary=notify_summary,
    )
    organized = summary.get("_organized_results", [])
    skipped_results = summary.get("_skipped_results", [])

    if summary.get("failed"):
        return organize_failure_result(reaction_dir, summary)

    organized_result = organize_success_result(organized)
    if organized_result is not None:
        return organized_result
    return organize_skipped_result(
        reaction_dir,
        skipped_results=skipped_results,
        skips=skips,
    )


def cmd_organize(
    args: Any,
    *,
    load_config_fn: Callable[[Any], AppConfig],
    rebuild_index_fn: Callable[[Path], int],
    reindex_job_locations_fn: Callable[[AppConfig], int],
    emit_organize_fn: Callable[[Dict[str, Any]], None],
    resolved_organized_root_fn: Callable[[AppConfig, str | Path], Path],
    resolve_scope_fn: Callable[..., tuple[list[OrganizePlan], list[SkipReason]] | None],
    build_dry_run_summary_fn: Callable[[list[OrganizePlan], list[SkipReason]], Dict[str, Any]],
    cmd_apply_fn: Callable[[list[OrganizePlan], list[SkipReason], Path, AppConfig], int],
    log: logging.Logger = logger,
) -> int:
    cfg = load_config_fn(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    if getattr(args, "rebuild_index", False):
        count = rebuild_index_fn(organized_root)
        tracking_count = reindex_job_locations_fn(cfg)
        emit_organize_fn(
            {
                "action": "rebuild_index",
                "records_count": count,
                "job_locations_count": tracking_count,
            }
        )
        return 0

    reaction_dir_raw = getattr(args, "reaction_dir", None)
    root_raw = getattr(args, "root", None)

    if reaction_dir_raw and root_raw:
        log.error("--reaction-dir and --root are mutually exclusive")
        return 1

    if not reaction_dir_raw and not root_raw:
        log.error("Either --reaction-dir or --root is required")
        return 1

    if reaction_dir_raw:
        organized_root = resolved_organized_root_fn(cfg, str(reaction_dir_raw))

    scope = resolve_scope_fn(
        cfg,
        organized_root=organized_root,
        reaction_dir_raw=reaction_dir_raw,
        root_raw=root_raw,
    )
    if scope is None:
        return 1
    plans, skips_list = scope

    if not getattr(args, "apply", False):
        emit_organize_fn(build_dry_run_summary_fn(plans, skips_list))
        return 0

    return cmd_apply_fn(plans, skips_list, organized_root, cfg)
