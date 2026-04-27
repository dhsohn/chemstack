from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, is_subpath
from chemstack.core.utils import now_utc_iso
from chemstack.flow.state import (
    iter_workflow_runtime_workspaces,
    workflow_workspace_internal_engine_paths,
    workflow_workspace_internal_engine_paths_from_path,
)

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
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return None
    return workflow_workspace_internal_engine_paths_from_path(
        path,
        workflow_root=workflow_root,
        engine="xtb",
    )


def _resolved_organized_root(cfg: Any, job_dir: Path) -> Path:
    runtime_paths = _workflow_runtime_paths(cfg, job_dir)
    if runtime_paths is not None:
        return runtime_paths["organized_root"].expanduser().resolve()
    return Path(cfg.runtime.organized_root).expanduser().resolve()


def _default_scan_roots(cfg: Any) -> list[Path]:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return [Path(cfg.runtime.allowed_root).expanduser().resolve()]

    roots: list[Path] = []
    for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine="xtb"):
        runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine="xtb")
        candidate = runtime_paths["allowed_root"].expanduser().resolve()
        if candidate.exists() and candidate not in roots:
            roots.append(candidate)
    return roots


def _is_supported_scan_root(cfg: Any, root: Path) -> bool:
    runtime_paths = _workflow_runtime_paths(cfg, root)
    if runtime_paths is not None:
        return is_subpath(root, runtime_paths["allowed_root"])
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    return is_subpath(root, allowed_root)


def _resolve_scope(cfg: Any, args: Any) -> tuple[Path | None, Path | None]:
    raw_job_dir = str(getattr(args, "job_dir", "") or "").strip()
    raw_root = str(getattr(args, "root", "") or "").strip()

    if raw_job_dir and raw_root:
        raise ValueError("job directory target and --root are mutually exclusive")

    if raw_job_dir:
        return resolve_job_dir(cfg, raw_job_dir), None

    if raw_root:
        root = ensure_directory(raw_root, label="Scan root")
        if not _is_supported_scan_root(cfg, root):
            workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
            if workflow_root:
                allowed_root = Path(workflow_root).expanduser().resolve()
            else:
                allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
            raise ValueError(f"Scan root must be under allowed_root: {allowed_root}")
        return None, root

    return None, None


def _iter_candidate_job_dirs(root: Path) -> list[Path]:
    state_files = sorted(root.rglob("job_state.json"))
    return [path.parent.resolve() for path in state_files]


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
    plan = _collect_plan_for_dir(cfg, job_dir.expanduser().resolve())
    if plan["action"] != "organize":
        return plan

    organized = _apply_plan(cfg, plan)
    if notify_summary:
        notify_organize_summary(cfg, organized_count=1, skipped_count=0, root=job_dir)
    return organized


def cmd_organize(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    job_dir, root = _resolve_scope(cfg, args)
    apply_changes = bool(getattr(args, "apply", False))

    if job_dir is not None:
        candidates = [job_dir]
    else:
        scan_roots = [root] if root is not None else _default_scan_roots(cfg)
        candidates = sorted(
            {
                candidate
                for scan_root in scan_roots
                for candidate in _iter_candidate_job_dirs(scan_root)
            },
            key=lambda path: str(path).lower(),
        )
    plans = [_collect_plan_for_dir(cfg, candidate) for candidate in candidates]

    to_organize = [item for item in plans if item["action"] == "organize"]
    skipped = [item for item in plans if item["action"] == "skip"]

    if not apply_changes:
        print("action: dry_run")
        print(f"to_organize: {len(to_organize)}")
        print(f"skipped: {len(skipped)}")
        for item in to_organize:
            print(f"{item['job_id']}: {item['job_dir']} -> {item['target_dir']}")
        return 0

    organized: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    for plan in to_organize:
        try:
            organized.append(organize_job_dir(cfg, Path(plan["job_dir"]), notify_summary=False))
        except Exception as exc:
            failures.append(
                {
                    "job_id": plan.get("job_id", ""),
                    "job_dir": plan["job_dir"],
                    "reason": str(exc),
                }
            )

    print("action: apply")
    print(f"organized: {len(organized)}")
    print(f"skipped: {len(skipped)}")
    print(f"failed: {len(failures)}")
    for item in organized:
        print(f"{item['job_id']}: {item['target_dir']}")
    for item in failures:
        print(f"failed: {item['job_id'] or item['job_dir']} ({item['reason']})")

    notify_organize_summary(
        cfg,
        organized_count=len(organized),
        skipped_count=len(skipped) + len(failures),
        root=root or job_dir or Path(str(getattr(cfg, "workflow_root", "")).strip() or cfg.runtime.allowed_root).expanduser().resolve(),
    )
    return 0 if not failures else 1
