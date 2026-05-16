from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from chemstack.core.paths import ensure_directory, is_subpath
from chemstack.flow.state import (
    iter_workflow_runtime_workspaces,
    workflow_workspace_internal_engine_paths,
    workflow_workspace_internal_engine_paths_from_path,
)


def workflow_runtime_paths(cfg: Any, path: Path, *, engine: str) -> dict[str, Path] | None:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return None
    return workflow_workspace_internal_engine_paths_from_path(
        path,
        workflow_root=workflow_root,
        engine=engine,
    )


def resolved_organized_root(cfg: Any, job_dir: Path, *, engine: str) -> Path:
    runtime_paths = workflow_runtime_paths(cfg, job_dir, engine=engine)
    if runtime_paths is not None:
        return runtime_paths["organized_root"].expanduser().resolve()
    return Path(cfg.runtime.organized_root).expanduser().resolve()


def default_scan_roots(cfg: Any, *, engine: str) -> list[Path]:
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if not workflow_root:
        return [Path(cfg.runtime.allowed_root).expanduser().resolve()]

    roots: list[Path] = []
    for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine=engine):
        runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)
        candidate = runtime_paths["allowed_root"].expanduser().resolve()
        if candidate.exists() and candidate not in roots:
            roots.append(candidate)
    return roots


def is_supported_scan_root(cfg: Any, root: Path, *, engine: str) -> bool:
    runtime_paths = workflow_runtime_paths(cfg, root, engine=engine)
    if runtime_paths is not None:
        return is_subpath(root, runtime_paths["allowed_root"])
    allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
    return is_subpath(root, allowed_root)


def resolve_scope(
    cfg: Any,
    args: Any,
    *,
    engine: str,
    resolve_job_dir_fn: Callable[[Any, str], Path],
) -> tuple[Path | None, Path | None]:
    raw_job_dir = str(getattr(args, "job_dir", "") or "").strip()
    raw_root = str(getattr(args, "root", "") or "").strip()

    if raw_job_dir and raw_root:
        raise ValueError("job directory target and --root are mutually exclusive")

    if raw_job_dir:
        return resolve_job_dir_fn(cfg, raw_job_dir), None

    if raw_root:
        root = ensure_directory(raw_root, label="Scan root")
        if not is_supported_scan_root(cfg, root, engine=engine):
            workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
            if workflow_root:
                allowed_root = Path(workflow_root).expanduser().resolve()
            else:
                allowed_root = Path(cfg.runtime.allowed_root).expanduser().resolve()
            raise ValueError(f"Scan root must be under allowed_root: {allowed_root}")
        return None, root

    return None, None


def iter_candidate_job_dirs(root: Path) -> list[Path]:
    state_files = sorted(root.rglob("job_state.json"))
    return [path.parent.resolve() for path in state_files]


def organize_job_dir(
    cfg: Any,
    job_dir: Path,
    *,
    notify_summary: bool = False,
    collect_plan_for_dir_fn: Callable[[Any, Path], dict[str, Any]],
    apply_plan_fn: Callable[[Any, dict[str, Any]], dict[str, str]],
    notify_organize_summary_fn: Callable[..., Any],
) -> dict[str, str]:
    plan = collect_plan_for_dir_fn(cfg, job_dir.expanduser().resolve())
    if plan["action"] != "organize":
        return plan

    organized = apply_plan_fn(cfg, plan)
    if notify_summary:
        notify_organize_summary_fn(
            cfg,
            organized_count=1,
            skipped_count=0,
            root=job_dir,
        )
    return organized


def run_organize_command(
    args: Any,
    *,
    load_config_fn: Callable[[Any], Any],
    resolve_scope_fn: Callable[[Any, Any], tuple[Path | None, Path | None]],
    default_scan_roots_fn: Callable[[Any], list[Path]],
    iter_candidate_job_dirs_fn: Callable[[Path], list[Path]],
    collect_plan_for_dir_fn: Callable[[Any, Path], dict[str, Any]],
    organize_job_dir_fn: Callable[..., dict[str, str]],
    notify_organize_summary_fn: Callable[..., Any],
) -> int:
    cfg = load_config_fn(getattr(args, "config", None))
    job_dir, root = resolve_scope_fn(cfg, args)
    apply_changes = bool(getattr(args, "apply", False))

    if job_dir is not None:
        candidates = [job_dir]
    else:
        scan_roots = [root] if root is not None else default_scan_roots_fn(cfg)
        candidates = sorted(
            {
                candidate
                for scan_root in scan_roots
                for candidate in iter_candidate_job_dirs_fn(scan_root)
            },
            key=lambda path: str(path).lower(),
        )
    plans = [collect_plan_for_dir_fn(cfg, candidate) for candidate in candidates]

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
            organized.append(
                organize_job_dir_fn(cfg, Path(plan["job_dir"]), notify_summary=False)
            )
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

    notify_organize_summary_fn(
        cfg,
        organized_count=len(organized),
        skipped_count=len(skipped) + len(failures),
        root=root
        or job_dir
        or Path(
            str(getattr(cfg, "workflow_root", "")).strip() or cfg.runtime.allowed_root
        ).expanduser().resolve(),
    )
    return 0 if not failures else 1

