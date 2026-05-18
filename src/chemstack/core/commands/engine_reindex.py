from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import get_job_location, upsert_job_location
from chemstack.core.paths import ensure_directory
from chemstack.core.paths.workflow import iter_workflow_runtime_workspaces, workflow_workspace_internal_engine_paths


def scan_roots(cfg: Any, raw_root: str | None, *, engine: str) -> list[Path]:
    if raw_root:
        return [ensure_directory(raw_root, label="Reindex root")]

    roots: list[Path] = []
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if workflow_root:
        for workspace_dir in iter_workflow_runtime_workspaces(workflow_root, engine=engine):
            runtime_paths = workflow_workspace_internal_engine_paths(workspace_dir, engine=engine)
            for key in ("allowed_root", "organized_root"):
                candidate = runtime_paths[key]
                try:
                    root = ensure_directory(candidate, label="Reindex root")
                except ValueError:
                    continue
                if root not in roots:
                    roots.append(root)
        return roots

    for candidate in (cfg.runtime.allowed_root, cfg.runtime.organized_root):
        try:
            root = ensure_directory(candidate, label="Reindex root")
        except ValueError:
            continue
        if root not in roots:
            roots.append(root)
    return roots


def iter_candidate_dirs(root: Path) -> set[Path]:
    candidates: set[Path] = set()
    for pattern in ("job_state.json", "job_report.json", "organized_ref.json"):
        for path in root.rglob(pattern):
            if path.is_file():
                candidates.add(path.parent.resolve())
    return candidates


def cmd_reindex(
    args: Any,
    *,
    engine: str,
    deps: Any,
    default_payload_kind_name: str | None = None,
    default_payload_kind: str | None = None,
) -> int:
    cfg = deps.load_config(getattr(args, "config", None))
    raw_root = getattr(args, "root", None)
    raw_root_text = str(raw_root).strip() if isinstance(raw_root, str) else ""
    roots = deps._scan_roots(cfg, raw_root_text or None)
    if not roots:
        print("error: no reindex roots available")
        return 1

    discovered: set[Path] = set()
    for root in roots:
        discovered.update(deps._iter_candidate_dirs(root))

    indexed = 0
    skipped = 0
    index_roots_used: set[Path] = set()
    for job_dir in sorted(discovered, key=lambda path: str(path).lower()):
        index_root = deps.index_root_for_path(cfg, job_dir)
        index_roots_used.add(index_root)
        state = deps.load_state(job_dir)
        report = deps.load_report_json(job_dir)
        organized_ref = deps.load_organized_ref(job_dir)
        candidate_job_id = str(
            (report or {}).get("job_id")
            or (state or {}).get("job_id")
            or (organized_ref or {}).get("job_id")
            or job_dir.name
        ).strip()
        existing = get_job_location(index_root, candidate_job_id) if candidate_job_id else None
        record_kwargs: dict[str, Any] = {
            "job_dir": job_dir,
            "state": state,
            "report": report,
            "organized_ref": organized_ref,
            "existing": existing,
        }
        if default_payload_kind_name is not None and default_payload_kind is not None:
            record_kwargs[default_payload_kind_name] = default_payload_kind
        record = deps.record_from_artifacts(**record_kwargs)
        if record is None or not record.job_id:
            skipped += 1
            continue
        upsert_job_location(index_root, record)
        indexed += 1

    print(f"index_roots: {len(index_roots_used)}")
    print(f"scan_roots: {len(roots)}")
    print(f"candidate_dirs: {len(discovered)}")
    print(f"indexed: {indexed}")
    print(f"skipped: {skipped}")
    return 0
