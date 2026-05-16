from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from chemstack.core.commands import organize as _shared_organize
from chemstack.core.paths import is_subpath
from chemstack.core.utils import now_utc_iso

from ..config import load_config
from ..notifications import notify_organize_summary
from ..state import (
    REPORT_MD_FILE_NAME,
    load_report_json,
    load_state,
    write_organized_ref,
    write_report_json,
    write_report_md_lines,
    write_state,
)
from ..tracking import is_terminal_status, reaction_key_from_job_dir, upsert_job_record
from ._helpers import resolve_job_dir


def _workflow_runtime_paths(cfg: Any, path: Path) -> dict[str, Path] | None:
    return _shared_organize.workflow_runtime_paths(cfg, path, engine="xtb")


def _resolved_organized_root(cfg: Any, job_dir: Path) -> Path:
    return _shared_organize.resolved_organized_root(cfg, job_dir, engine="xtb")


def _default_scan_roots(cfg: Any) -> list[Path]:
    return _shared_organize.default_scan_roots(cfg, engine="xtb")


def _is_supported_scan_root(cfg: Any, root: Path) -> bool:
    return _shared_organize.is_supported_scan_root(cfg, root, engine="xtb")


def _resolve_scope(cfg: Any, args: Any) -> tuple[Path | None, Path | None]:
    return _shared_organize.resolve_scope(
        cfg,
        args,
        engine="xtb",
        resolve_job_dir_fn=resolve_job_dir,
    )


def _iter_candidate_job_dirs(root: Path) -> list[Path]:
    return _shared_organize.iter_candidate_job_dirs(root)


def _planned_target_for_job_dir(cfg: Any, *, job_dir: Path, job_id: str, job_type: str, reaction_key: str) -> Path:
    organized_root = _resolved_organized_root(cfg, job_dir)
    return organized_root / job_type / reaction_key / job_id


def _rewrite_artifact_path(original_run_dir: Path, target_dir: Path, path_text: str) -> str:
    raw = str(path_text).strip()
    if not raw:
        return raw
    try:
        original = Path(raw).expanduser().resolve()
    except OSError:
        return raw
    try:
        relative = original.relative_to(original_run_dir)
    except ValueError:
        candidate = target_dir / original.name
        return str(candidate.resolve()) if candidate.exists() else raw
    relocated = (target_dir / relative).resolve()
    return str(relocated) if relocated.exists() else raw


def _is_path_like_key(key: str) -> bool:
    normalized = str(key).strip().lower()
    return normalized == "path" or normalized.endswith(("_path", "_paths", "_dir", "_dirs", "_xyz"))


def _rewrite_path_like_value(original_run_dir: Path, target_dir: Path, key: str, value: Any) -> Any:
    if isinstance(value, str) and _is_path_like_key(key):
        return _rewrite_artifact_path(original_run_dir, target_dir, value)
    if isinstance(value, list) and _is_path_like_key(key):
        return [
            _rewrite_artifact_path(original_run_dir, target_dir, item) if isinstance(item, str) else item
            for item in value
        ]
    return value


def _rewrite_path_like_mapping(original_run_dir: Path, target_dir: Path, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    updated: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            updated[key] = _rewrite_path_like_mapping(original_run_dir, target_dir, value)
            continue
        if isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            updated[key] = [
                _rewrite_path_like_mapping(original_run_dir, target_dir, item)
                for item in value
                if isinstance(item, dict)
            ]
            continue
        updated[key] = _rewrite_path_like_value(original_run_dir, target_dir, key, value)
    return updated


def _rewrite_candidate_details(original_run_dir: Path, target_dir: Path, details: Any) -> list[dict[str, Any]]:
    if not isinstance(details, list):
        return []
    rewritten: list[dict[str, Any]] = []
    for item in details:
        if not isinstance(item, dict):
            continue
        rewritten.append(_rewrite_path_like_mapping(original_run_dir, target_dir, item))
    return rewritten


def _rewrite_analysis_summary(original_run_dir: Path, target_dir: Path, summary: Any) -> dict[str, Any]:
    return _rewrite_path_like_mapping(original_run_dir, target_dir, summary)


def _refresh_organized_artifacts(target_dir: Path, *, original_run_dir: Path, status: str) -> None:
    state = load_state(target_dir)
    if state is not None:
        updated_state = dict(state)
        updated_state["job_dir"] = str(target_dir)
        updated_state["original_run_dir"] = str(original_run_dir)
        updated_state["organized_output_dir"] = str(target_dir)
        updated_state["latest_known_path"] = str(target_dir)
        updated_state["status"] = status
        updated_state["updated_at"] = now_utc_iso()
        updated_state["selected_input_xyz"] = _rewrite_artifact_path(
            original_run_dir,
            target_dir,
            str(updated_state.get("selected_input_xyz", "")),
        )
        updated_state["input_summary"] = _rewrite_path_like_mapping(
            original_run_dir,
            target_dir,
            updated_state.get("input_summary", {}),
        )
        updated_state["selected_candidate_paths"] = [
            _rewrite_artifact_path(original_run_dir, target_dir, item)
            for item in updated_state.get("selected_candidate_paths", [])
        ]
        updated_state["candidate_details"] = _rewrite_candidate_details(
            original_run_dir,
            target_dir,
            updated_state.get("candidate_details", []),
        )
        updated_state["analysis_summary"] = _rewrite_analysis_summary(
            original_run_dir,
            target_dir,
            updated_state.get("analysis_summary", {}),
        )
        write_state(target_dir, updated_state)

    report = load_report_json(target_dir)
    if report is not None:
        updated_report = dict(report)
        updated_report["job_dir"] = str(target_dir)
        updated_report["original_run_dir"] = str(original_run_dir)
        updated_report["organized_output_dir"] = str(target_dir)
        updated_report["latest_known_path"] = str(target_dir)
        updated_report["status"] = status
        updated_report["selected_input_xyz"] = _rewrite_artifact_path(
            original_run_dir,
            target_dir,
            str(updated_report.get("selected_input_xyz", "")),
        )
        updated_report["input_summary"] = _rewrite_path_like_mapping(
            original_run_dir,
            target_dir,
            updated_report.get("input_summary", {}),
        )
        updated_report["selected_candidate_paths"] = [
            _rewrite_artifact_path(original_run_dir, target_dir, item)
            for item in updated_report.get("selected_candidate_paths", [])
        ]
        updated_report["candidate_details"] = _rewrite_candidate_details(
            original_run_dir,
            target_dir,
            updated_report.get("candidate_details", []),
        )
        updated_report["analysis_summary"] = _rewrite_analysis_summary(
            original_run_dir,
            target_dir,
            updated_report.get("analysis_summary", {}),
        )
        write_report_json(target_dir, updated_report)

    report_md = target_dir / REPORT_MD_FILE_NAME
    if report_md.exists():
        lines = report_md.read_text(encoding="utf-8").splitlines()
        lines.extend(
            [
                "",
                "## Organization",
                f"- Original Run Dir: `{original_run_dir}`",
                f"- Organized Output Dir: `{target_dir}`",
            ]
        )
        write_report_md_lines(target_dir, lines)


def _collect_plan_for_dir(cfg: Any, job_dir: Path) -> dict[str, Any]:
    state = load_state(job_dir)
    if state is None:
        return {"action": "skip", "job_dir": str(job_dir), "reason": "missing_state"}

    status = str(state.get("status", "")).strip().lower()
    if not is_terminal_status(status):
        return {"action": "skip", "job_dir": str(job_dir), "reason": f"non_terminal:{status or 'unknown'}"}

    job_id = str(state.get("job_id", "")).strip()
    if not job_id:
        return {"action": "skip", "job_dir": str(job_dir), "reason": "missing_job_id"}

    selected_input_xyz = str(state.get("selected_input_xyz", "")).strip()
    resource_request = state.get("resource_request", {})
    resource_actual = state.get("resource_actual", {})
    job_type = str(state.get("job_type", "path_search")).strip().lower() or "path_search"
    reaction_key = str(state.get("reaction_key", "")).strip() or reaction_key_from_job_dir(job_dir)
    input_summary = state.get("input_summary", {})
    selected_candidate_paths = state.get("selected_candidate_paths", [])
    candidate_details = state.get("candidate_details", [])
    analysis_summary = state.get("analysis_summary", {})
    candidate_count = int(state.get("candidate_count", 0) or 0)
    target_dir = _planned_target_for_job_dir(
        cfg,
        job_dir=job_dir,
        job_id=job_id,
        job_type=job_type,
        reaction_key=reaction_key,
    )

    organized_root = _resolved_organized_root(cfg, job_dir)
    if is_subpath(job_dir, organized_root):
        return {"action": "skip", "job_dir": str(job_dir), "reason": "already_under_organized_root"}

    if target_dir.exists():
        return {
            "action": "skip",
            "job_dir": str(job_dir),
            "job_id": job_id,
            "reason": "target_exists",
            "target_dir": str(target_dir),
        }

    return {
        "action": "organize",
        "job_dir": str(job_dir),
        "job_id": job_id,
        "status": status,
        "job_type": job_type,
        "selected_input_xyz": selected_input_xyz,
        "resource_request": resource_request,
        "resource_actual": resource_actual,
        "reaction_key": reaction_key,
        "input_summary": input_summary,
        "selected_candidate_paths": list(selected_candidate_paths),
        "candidate_details": candidate_details,
        "analysis_summary": analysis_summary,
        "candidate_count": candidate_count,
        "target_dir": str(target_dir),
    }


def _apply_plan(cfg: Any, plan: dict[str, Any]) -> dict[str, str]:
    job_dir = Path(plan["job_dir"]).expanduser().resolve()
    target_dir = Path(plan["target_dir"]).expanduser().resolve()
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    shutil.move(str(job_dir), str(target_dir))
    job_dir.mkdir(parents=True, exist_ok=True)
    rewritten_selected_input_xyz = _rewrite_artifact_path(job_dir, target_dir, plan["selected_input_xyz"])
    rewritten_input_summary = _rewrite_path_like_mapping(job_dir, target_dir, plan.get("input_summary", {}))
    rewritten_selected_candidate_paths = [
        _rewrite_artifact_path(job_dir, target_dir, item)
        for item in plan.get("selected_candidate_paths", [])
    ]
    rewritten_candidate_details = _rewrite_candidate_details(job_dir, target_dir, plan.get("candidate_details", []))
    rewritten_analysis_summary = _rewrite_analysis_summary(job_dir, target_dir, plan.get("analysis_summary", {}))
    write_organized_ref(
        job_dir,
        {
            "job_id": plan["job_id"],
            "original_run_dir": str(job_dir),
            "organized_output_dir": str(target_dir),
            "organized_at": now_utc_iso(),
            "status": plan["status"],
            "job_type": plan["job_type"],
            "selected_input_xyz": rewritten_selected_input_xyz,
            "reaction_key": plan["reaction_key"],
            "input_summary": rewritten_input_summary,
            "candidate_count": plan.get("candidate_count", 0),
            "selected_candidate_paths": rewritten_selected_candidate_paths,
            "candidate_details": rewritten_candidate_details,
            "analysis_summary": rewritten_analysis_summary,
            "resource_request": plan.get("resource_request", {}),
            "resource_actual": plan.get("resource_actual", {}),
        },
    )
    _refresh_organized_artifacts(target_dir, original_run_dir=job_dir, status=plan["status"])
    upsert_job_record(
        cfg,
        job_id=plan["job_id"],
        status=plan["status"],
        job_dir=job_dir,
        job_type=plan["job_type"],
        selected_input_xyz=rewritten_selected_input_xyz,
        organized_output_dir=target_dir,
        reaction_key=plan["reaction_key"],
        resource_request=plan.get("resource_request", {}),
        resource_actual=plan.get("resource_actual", {}),
    )
    return {
        "action": "organized",
        "job_id": plan["job_id"],
        "status": plan["status"],
        "job_dir": str(job_dir),
        "target_dir": str(target_dir),
        "job_type": plan["job_type"],
        "reaction_key": plan["reaction_key"],
    }


def organize_job_dir(cfg: Any, job_dir: Path, *, notify_summary: bool = False) -> dict[str, str]:
    return _shared_organize.organize_job_dir(
        cfg,
        job_dir,
        notify_summary=notify_summary,
        collect_plan_for_dir_fn=_collect_plan_for_dir,
        apply_plan_fn=_apply_plan,
        notify_organize_summary_fn=notify_organize_summary,
    )


def cmd_organize(args: Any) -> int:
    return _shared_organize.run_organize_command(
        args,
        load_config_fn=load_config,
        resolve_scope_fn=_resolve_scope,
        default_scan_roots_fn=_default_scan_roots,
        iter_candidate_job_dirs_fn=_iter_candidate_job_dirs,
        collect_plan_for_dir_fn=_collect_plan_for_dir,
        organize_job_dir_fn=organize_job_dir,
        notify_organize_summary_fn=notify_organize_summary,
    )
