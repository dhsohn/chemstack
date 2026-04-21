from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, is_subpath, validate_job_dir
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


def _resolve_scope(cfg: Any, args: Any) -> tuple[Path | None, Path | None]:
    raw_job_dir = str(getattr(args, "job_dir", "") or "").strip()
    raw_root = str(getattr(args, "root", "") or "").strip()

    if raw_job_dir and raw_root:
        raise ValueError("job directory target and --root are mutually exclusive")

    if raw_job_dir:
        return validate_job_dir(raw_job_dir, cfg.runtime.allowed_root, label="Job directory"), None

    if raw_root:
        root = ensure_directory(raw_root, label="Scan root")
        allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
        if not is_subpath(root, allowed_root):
            raise ValueError(f"Scan root must be under allowed_root: {allowed_root}")
        return None, root

    return None, Path(cfg.runtime.allowed_root).expanduser().resolve()


def _iter_candidate_job_dirs(root: Path) -> list[Path]:
    state_files = sorted(root.rglob("job_state.json"))
    return [path.parent.resolve() for path in state_files]


def _planned_target(cfg: Any, *, job_id: str, mode: str, molecule_key: str) -> Path:
    organized_root = Path(cfg.runtime.organized_root).expanduser().resolve()
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
    target_dir = _planned_target(cfg, job_id=job_id, mode=mode, molecule_key=molecule_key)

    organized_root = Path(cfg.runtime.organized_root).expanduser().resolve()
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
    plan = _collect_plan_for_dir(cfg, job_dir.expanduser().resolve())
    if plan["action"] != "organize":
        return plan

    organized = _apply_plan(cfg, plan)
    if notify_summary:
        notify_organize_summary(
            cfg,
            organized_count=1,
            skipped_count=0,
            root=job_dir,
        )
    return organized


def cmd_organize(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    job_dir, root = _resolve_scope(cfg, args)
    apply_changes = bool(getattr(args, "apply", False))

    if job_dir is not None:
        candidates = [job_dir]
    else:
        if root is None:
            raise ValueError("Scan root could not be resolved.")
        candidates = _iter_candidate_job_dirs(root)
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
        root=root or job_dir or Path(cfg.runtime.allowed_root).expanduser().resolve(),
    )
    return 0 if not failures else 1
