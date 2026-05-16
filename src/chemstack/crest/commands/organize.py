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
    write_report_json,
    write_report_md_lines,
    write_organized_ref,
    write_state,
)
from ..tracking import is_terminal_status, molecule_key_from_selected_xyz, upsert_job_record
from ._helpers import resolve_job_dir


def _workflow_runtime_paths(cfg: Any, path: Path) -> dict[str, Path] | None:
    return _shared_organize.workflow_runtime_paths(cfg, path, engine="crest")


def _resolved_organized_root(cfg: Any, job_dir: Path) -> Path:
    return _shared_organize.resolved_organized_root(cfg, job_dir, engine="crest")


def _default_scan_roots(cfg: Any) -> list[Path]:
    return _shared_organize.default_scan_roots(cfg, engine="crest")


def _is_supported_scan_root(cfg: Any, root: Path) -> bool:
    return _shared_organize.is_supported_scan_root(cfg, root, engine="crest")


def _resolve_scope(cfg: Any, args: Any) -> tuple[Path | None, Path | None]:
    return _shared_organize.resolve_scope(
        cfg,
        args,
        engine="crest",
        resolve_job_dir_fn=resolve_job_dir,
    )


def _iter_candidate_job_dirs(root: Path) -> list[Path]:
    return _shared_organize.iter_candidate_job_dirs(root)


def _planned_target_for_job_dir(cfg: Any, *, job_dir: Path, job_id: str, mode: str, molecule_key: str) -> Path:
    organized_root = _resolved_organized_root(cfg, job_dir)
    return organized_root / mode / molecule_key / job_id


def _refresh_organized_artifacts(
    target_dir: Path,
    *,
    original_run_dir: Path,
    status: str,
) -> None:
    state = load_state(target_dir)
    if state is not None:
        updated_state = dict(state)
        updated_state["job_dir"] = str(target_dir)
        updated_state["original_run_dir"] = str(original_run_dir)
        updated_state["organized_output_dir"] = str(target_dir)
        updated_state["latest_known_path"] = str(target_dir)
        updated_state["status"] = status
        updated_state["updated_at"] = now_utc_iso()
        write_state(target_dir, updated_state)

    report = load_report_json(target_dir)
    if report is not None:
        updated_report = dict(report)
        updated_report["job_dir"] = str(target_dir)
        updated_report["original_run_dir"] = str(original_run_dir)
        updated_report["organized_output_dir"] = str(target_dir)
        updated_report["latest_known_path"] = str(target_dir)
        updated_report["status"] = status
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
    mode = str(state.get("mode", "standard")).strip().lower() or "standard"
    molecule_key = molecule_key_from_selected_xyz(selected_input_xyz, job_dir)
    target_dir = _planned_target_for_job_dir(
        cfg,
        job_dir=job_dir,
        job_id=job_id,
        mode=mode,
        molecule_key=molecule_key,
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
        "mode": mode,
        "selected_input_xyz": selected_input_xyz,
        "resource_request": resource_request,
        "resource_actual": resource_actual,
        "molecule_key": molecule_key,
        "target_dir": str(target_dir),
    }


def _apply_plan(cfg: Any, plan: dict[str, str]) -> dict[str, str]:
    job_dir = Path(plan["job_dir"]).expanduser().resolve()
    target_dir = Path(plan["target_dir"]).expanduser().resolve()
    resource_request = plan.get("resource_request")
    resource_actual = plan.get("resource_actual")
    resource_request_dict: dict[str, int] = dict(resource_request) if isinstance(resource_request, dict) else {}
    resource_actual_dict: dict[str, int] = dict(resource_actual) if isinstance(resource_actual, dict) else {}
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    shutil.move(str(job_dir), str(target_dir))
    job_dir.mkdir(parents=True, exist_ok=True)
    write_organized_ref(
        job_dir,
        {
            "job_id": plan["job_id"],
            "original_run_dir": str(job_dir),
            "organized_output_dir": str(target_dir),
            "organized_at": now_utc_iso(),
            "status": plan["status"],
            "mode": plan["mode"],
            "selected_input_xyz": plan["selected_input_xyz"],
            "molecule_key": plan["molecule_key"],
            "resource_request": resource_request_dict,
            "resource_actual": resource_actual_dict,
        },
    )
    _refresh_organized_artifacts(
        target_dir,
        original_run_dir=job_dir,
        status=plan["status"],
    )
    upsert_job_record(
        cfg,
        job_id=plan["job_id"],
        status=plan["status"],
        job_dir=job_dir,
        mode=plan["mode"],
        selected_input_xyz=plan["selected_input_xyz"],
        organized_output_dir=target_dir,
        molecule_key=plan["molecule_key"],
        resource_request=resource_request_dict,
        resource_actual=resource_actual_dict,
    )
    return {
        "action": "organized",
        "job_id": plan["job_id"],
        "status": plan["status"],
        "job_dir": str(job_dir),
        "target_dir": str(target_dir),
        "mode": plan["mode"],
        "molecule_key": plan["molecule_key"],
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
