from __future__ import annotations

from pathlib import Path
from typing import Any

from chemstack.core.indexing import get_job_location, upsert_job_location
from chemstack.core.paths import ensure_directory

from ..config import load_config
from ..state import load_organized_ref, load_report_json, load_state
from ..tracking import index_root_for_cfg, record_from_artifacts


def _scan_roots(cfg: Any, raw_root: str | None) -> list[Path]:
    if raw_root:
        return [ensure_directory(raw_root, label="Reindex root")]

    roots: list[Path] = []
    for candidate in (cfg.runtime.allowed_root, cfg.runtime.organized_root):
        try:
            root = ensure_directory(candidate, label="Reindex root")
        except ValueError:
            continue
        if root not in roots:
            roots.append(root)
    return roots


def _iter_candidate_dirs(root: Path) -> set[Path]:
    candidates: set[Path] = set()
    for pattern in ("job_state.json", "job_report.json", "organized_ref.json"):
        for path in root.rglob(pattern):
            if path.is_file():
                candidates.add(path.parent.resolve())
    return candidates


def cmd_reindex(args: Any) -> int:
    cfg = load_config(getattr(args, "config", None))
    raw_root = getattr(args, "root", None)
    raw_root_text = str(raw_root).strip() if isinstance(raw_root, str) else ""
    roots = _scan_roots(cfg, raw_root_text or None)
    if not roots:
        print("error: no reindex roots available")
        return 1

    index_root = index_root_for_cfg(cfg)
    discovered: set[Path] = set()
    for root in roots:
        discovered.update(_iter_candidate_dirs(root))

    indexed = 0
    skipped = 0
    for job_dir in sorted(discovered, key=lambda path: str(path).lower()):
        state = load_state(job_dir)
        report = load_report_json(job_dir)
        organized_ref = load_organized_ref(job_dir)
        candidate_job_id = str(
            (report or {}).get("job_id")
            or (state or {}).get("job_id")
            or (organized_ref or {}).get("job_id")
            or job_dir.name
        ).strip()
        existing = get_job_location(index_root, candidate_job_id) if candidate_job_id else None
        record = record_from_artifacts(
            job_dir=job_dir,
            state=state,
            report=report,
            organized_ref=organized_ref,
            existing=existing,
            default_job_type="path_search",
        )
        if record is None or not record.job_id:
            skipped += 1
            continue
        upsert_job_location(index_root, record)
        indexed += 1

    print(f"index_root: {index_root}")
    print(f"scan_roots: {len(roots)}")
    print(f"candidate_dirs: {len(discovered)}")
    print(f"indexed: {indexed}")
    print(f"skipped: {skipped}")
    return 0
