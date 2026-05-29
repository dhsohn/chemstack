from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Mapping

from chemstack.core.paths.workflow import workflow_workspace_internal_engine_paths_from_path

from ..config import AppConfig, load_config
from ..organize_index import (
    acquire_index_lock,
    append_failed_rollback,
    append_record,
    load_index,
    rebuild_index,
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
from ..state import now_utc_iso
from ..job_locations import reindex_job_locations
from ..telegram_notifier import escape_html, send_message
from ..types import RunState
from ._helpers import (
    _validate_reaction_dir,
    _validate_root_scan_dir,
    finalize_batch_apply,
)
from . import organize_apply as _organize_apply
from . import organize_notifications as _organize_notifications
from . import organize_output as _organize_output
from . import organize_tracking as _organize_tracking

logger = logging.getLogger(__name__)


def _workflow_runtime_paths(cfg: AppConfig, path: str | Path) -> dict[str, Path] | None:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return None
    return workflow_workspace_internal_engine_paths_from_path(
        path,
        workflow_root=workflow_root,
        engine="orca",
    )


def _resolved_organized_root(cfg: AppConfig, reaction_dir: str | Path) -> Path:
    runtime_paths = _workflow_runtime_paths(cfg, reaction_dir)
    if runtime_paths is not None:
        return runtime_paths["organized_root"].expanduser().resolve()
    return Path(cfg.runtime.organized_root).resolve()


def _emit_organize(payload: Dict[str, Any]) -> None:
    _organize_output.emit_organize(payload)


def _plan_to_dict(plan: OrganizePlan) -> Dict[str, Any]:
    return _organize_output.plan_to_dict(plan)


def _build_index_record(plan: OrganizePlan, state: Mapping[str, Any]) -> Dict[str, Any]:
    return _organize_tracking.build_index_record(plan, state)


def _tracking_resources(cfg: AppConfig) -> dict[str, int]:
    return _organize_tracking.tracking_resources(cfg)


def _tracking_job_id(plan: OrganizePlan, state: RunState) -> str:
    return _organize_tracking.tracking_job_id(plan, state)


def _write_tracking_after_move(
    cfg: AppConfig,
    *,
    plan: OrganizePlan,
    state_after_move: RunState,
) -> None:
    _organize_tracking.write_tracking_after_move(
        cfg,
        plan=plan,
        state_after_move=state_after_move,
    )


def _cleanup_organized_ref_stub(plan: OrganizePlan) -> None:
    _organize_tracking.cleanup_organized_ref_stub(plan)


def _restore_tracking_after_rollback(
    cfg: AppConfig,
    *,
    plan: OrganizePlan,
    state_after_rollback: RunState,
) -> None:
    _organize_tracking.restore_tracking_after_rollback(
        cfg,
        plan=plan,
        state_after_rollback=state_after_rollback,
    )


def _organize_summary_parts(
    organized_count: int, skipped_count: int, failed_count: int
) -> list[str]:
    return _organize_notifications._organize_summary_parts(
        organized_count,
        skipped_count,
        failed_count,
    )


def _format_organized_line(item: Dict[str, Any]) -> str:
    return _organize_notifications._format_organized_line(
        item,
        escape_html_fn=escape_html,
    )


def _organized_section(organized: list[Dict[str, Any]]) -> str | None:
    return _organize_notifications._organized_section(
        organized,
        escape_html_fn=escape_html,
    )


def _failure_section(failures: list[Dict[str, Any]]) -> str | None:
    return _organize_notifications._failure_section(
        failures,
        escape_html_fn=escape_html,
    )


def _skip_section(skips: list[SkipReason], skipped_count: int) -> str | None:
    return _organize_notifications._skip_section(
        skips,
        skipped_count,
        escape_html_fn=escape_html,
    )


def _build_organize_message(
    organized: list[Dict[str, Any]],
    skipped: list[Dict[str, Any]],
    failures: list[Dict[str, Any]],
    skips: list[SkipReason],
) -> str | None:
    return _organize_notifications._build_organize_message(
        organized,
        skipped,
        failures,
        skips,
        escape_html_fn=escape_html,
    )


def _send_organize_notification(
    cfg: AppConfig,
    *,
    organized: list[Dict[str, Any]],
    skipped_results: list[Dict[str, Any]],
    failures: list[Dict[str, Any]],
    skips: list[SkipReason],
) -> None:
    return _organize_notifications._send_organize_notification(
        cfg,
        organized=organized,
        skipped_results=skipped_results,
        failures=failures,
        skips=skips,
        build_message_fn=_build_organize_message,
        send_message_fn=send_message,
        log=logger,
    )


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
    return _organize_output.build_dry_run_summary(plans, skips_list)


def _cmd_organize_apply(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    cfg: AppConfig,
) -> int:
    summary = _apply_organize_plans(
        plans,
        skips,
        organized_root,
        cfg,
        notify_summary=True,
    )
    return finalize_batch_apply(
        summary,
        _emit_organize,
        summary["failures"],
    )


def _apply_dependencies() -> _organize_apply.OrganizeApplyDependencies:
    return _organize_apply.OrganizeApplyDependencies(
        acquire_index_lock=acquire_index_lock,
        append_failed_rollback=append_failed_rollback,
        append_record=append_record,
        build_index_record=_build_index_record,
        check_conflict=check_conflict,
        cleanup_organized_ref_stub=_cleanup_organized_ref_stub,
        execute_move=execute_move,
        load_index=load_index,
        now_utc_iso=now_utc_iso,
        rollback_move=rollback_move,
        send_organize_notification=_send_organize_notification,
        sync_state_after_move=sync_state_after_move,
        sync_state_after_rollback=sync_state_after_rollback,
        write_tracking_after_move=_write_tracking_after_move,
        restore_tracking_after_rollback=_restore_tracking_after_rollback,
        log=logger,
        plan_conflict_result=_plan_conflict_result,
        bookkeep_successful_move=_bookkeep_successful_move,
        bookkeep_rollback_failure=_bookkeep_rollback_failure,
        rollback_after_apply_failure=_rollback_after_apply_failure,
        apply_one_organize_plan=_apply_one_organize_plan,
    )


def _plan_conflict_result(
    plan: OrganizePlan,
    index: Dict[str, Dict[str, Any]],
) -> _organize_apply._PlanApplyResult | None:
    return _organize_apply._plan_conflict_result(
        plan,
        index,
        deps=_apply_dependencies(),
    )


def _bookkeep_successful_move(
    cfg: AppConfig,
    *,
    organized_root: Path,
    plan: OrganizePlan,
) -> _organize_apply._PlanApplyResult:
    return _organize_apply._bookkeep_successful_move(
        cfg,
        organized_root=organized_root,
        plan=plan,
        deps=_apply_dependencies(),
    )


def _bookkeep_rollback_failure(
    organized_root: Path,
    *,
    plan: OrganizePlan,
    rollback_exc: Exception,
) -> None:
    return _organize_apply._bookkeep_rollback_failure(
        organized_root,
        plan=plan,
        rollback_exc=rollback_exc,
        deps=_apply_dependencies(),
    )


def _rollback_after_apply_failure(
    cfg: AppConfig,
    *,
    organized_root: Path,
    plan: OrganizePlan,
    failure_reason: str,
) -> str:
    return _organize_apply._rollback_after_apply_failure(
        cfg,
        organized_root=organized_root,
        plan=plan,
        failure_reason=failure_reason,
        deps=_apply_dependencies(),
    )


def _apply_one_organize_plan(
    cfg: AppConfig,
    *,
    organized_root: Path,
    plan: OrganizePlan,
) -> _organize_apply._PlanApplyResult:
    deps = _apply_dependencies()
    return _organize_apply._apply_one_organize_plan(
        cfg,
        organized_root=organized_root,
        plan=plan,
        deps=deps,
    )


def _build_apply_summary(
    *,
    results: list[Dict[str, Any]],
    failures: list[Dict[str, Any]],
    skips: list[SkipReason],
) -> Dict[str, Any]:
    return _organize_apply._build_apply_summary(
        results=results,
        failures=failures,
        skips=skips,
    )


def _apply_organize_plans(
    plans: list[OrganizePlan],
    skips: list[SkipReason],
    organized_root: Path,
    cfg: AppConfig,
    *,
    notify_summary: bool,
) -> Dict[str, Any]:
    return _organize_apply._apply_organize_plans(
        plans,
        skips,
        organized_root,
        cfg,
        notify_summary=notify_summary,
        deps=_apply_dependencies(),
    )


def _organize_no_plan_result(reaction_dir: Path, skips: list[SkipReason]) -> Dict[str, Any]:
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


def _organize_failure_result(reaction_dir: Path, summary: Dict[str, Any]) -> Dict[str, Any]:
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


def _organize_success_result(organized: list[Any]) -> Dict[str, Any] | None:
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


def _organize_skipped_result(
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
    return _organize_no_plan_result(reaction_dir, skips)


def organize_reaction_dir(
    cfg: AppConfig,
    reaction_dir: Path,
    *,
    notify_summary: bool = True,
) -> Dict[str, Any]:
    organized_root = _resolved_organized_root(cfg, reaction_dir)
    scope = _resolve_organize_scope(
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
        return _organize_no_plan_result(reaction_dir, skips)

    summary = _apply_organize_plans(
        plans,
        skips,
        organized_root,
        cfg,
        notify_summary=notify_summary,
    )
    organized = summary.get("_organized_results", [])
    skipped_results = summary.get("_skipped_results", [])

    if summary.get("failed"):
        return _organize_failure_result(reaction_dir, summary)

    organized_result = _organize_success_result(organized)
    if organized_result is not None:
        return organized_result
    return _organize_skipped_result(
        reaction_dir,
        skipped_results=skipped_results,
        skips=skips,
    )


def cmd_organize(args: Any) -> int:
    cfg = load_config(args.config)
    organized_root = Path(cfg.runtime.organized_root).resolve()

    if getattr(args, "rebuild_index", False):
        count = rebuild_index(organized_root)
        tracking_count = reindex_job_locations(cfg)
        _emit_organize(
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
        logger.error("--reaction-dir and --root are mutually exclusive")
        return 1

    if not reaction_dir_raw and not root_raw:
        logger.error("Either --reaction-dir or --root is required")
        return 1

    if reaction_dir_raw:
        organized_root = _resolved_organized_root(cfg, str(reaction_dir_raw))

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
