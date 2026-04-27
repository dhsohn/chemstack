from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.paths import validate_job_dir
from chemstack.core.utils import now_utc_iso, timestamped_token
from chemstack.flow.state import workflow_workspace_internal_engine_paths_from_path

from ..config import AppConfig

_PREFERRED_EXCLUDE_RE = re.compile(r"(?:^crest_|^struc|^coord)", re.IGNORECASE)
MANIFEST_FILE_NAME = "crest_job.yaml"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _positive_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def load_job_manifest(job_dir: Path) -> dict[str, Any]:
    path = job_dir / MANIFEST_FILE_NAME
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle) or {}
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid CREST job manifest: {path}")
    return parsed


def job_mode(manifest: dict[str, Any]) -> str:
    mode = str(manifest.get("mode", "standard")).strip().lower()
    return "nci" if mode == "nci" else "standard"


def select_latest_xyz(job_dir: Path) -> Path:
    candidates = list(job_dir.glob("*.xyz"))
    if not candidates:
        raise ValueError(f"No .xyz file found in: {job_dir}")

    preferred = [path for path in candidates if not _PREFERRED_EXCLUDE_RE.search(path.name)]
    if preferred:
        candidates = preferred

    candidates.sort(key=lambda path: (path.stat().st_mtime_ns, path.name.lower()), reverse=True)
    return candidates[0]


def select_input_xyz(job_dir: Path, manifest: dict[str, Any]) -> Path:
    input_xyz = str(manifest.get("input_xyz", "")).strip()
    if input_xyz:
        candidate = (job_dir / input_xyz).resolve()
        if not candidate.exists() or not candidate.is_file():
            raise ValueError(f"Manifest input_xyz not found: {candidate}")
        if candidate.suffix.lower() != ".xyz":
            raise ValueError(f"Manifest input_xyz must point to a .xyz file: {candidate}")
        return candidate
    return select_latest_xyz(job_dir)


def resolve_job_dir(cfg: AppConfig, raw_job_dir: str) -> Path:
    candidate = Path(raw_job_dir).expanduser().resolve()
    workflow_root = str(getattr(cfg, "workflow_root", "")).strip()
    if workflow_root:
        runtime_paths = workflow_workspace_internal_engine_paths_from_path(
            candidate,
            workflow_root=workflow_root,
            engine="crest",
        )
        if runtime_paths is None:
            raise ValueError(
                "Job directory must be under a workflow-local CREST root: "
                "<workflow.root>/<workflow_id>/01_crest/..."
            )
        return validate_job_dir(raw_job_dir, str(runtime_paths["allowed_root"]), label="Job directory")
    return validate_job_dir(raw_job_dir, cfg.runtime.allowed_root, label="Job directory")


def resource_request_from_manifest(cfg: AppConfig, manifest: dict[str, Any]) -> dict[str, int]:
    resources = _mapping(manifest.get("resources"))
    default_cores = max(1, int(cfg.resources.max_cores_per_task))
    default_memory = max(1, int(cfg.resources.max_memory_gb_per_task))
    max_cores = (
        _positive_int(resources.get("max_cores"))
        or _positive_int(resources.get("max_cores_per_task"))
        or _positive_int(manifest.get("max_cores"))
        or _positive_int(manifest.get("max_cores_per_task"))
        or default_cores
    )
    max_memory_gb = (
        _positive_int(resources.get("max_memory_gb"))
        or _positive_int(resources.get("max_memory_gb_per_task"))
        or _positive_int(manifest.get("max_memory_gb"))
        or _positive_int(manifest.get("max_memory_gb_per_task"))
        or default_memory
    )
    return {
        "max_cores": max_cores,
        "max_memory_gb": max_memory_gb,
    }


def new_job_id() -> str:
    return timestamped_token("crest")


def queued_state_payload(
    *,
    job_id: str,
    job_dir: Path,
    selected_xyz: Path,
    mode: str,
    molecule_key: str = "",
    resource_request: dict[str, int] | None = None,
) -> dict[str, Any]:
    now = now_utc_iso()
    return {
        "job_id": job_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": str(selected_xyz),
        "molecule_key": molecule_key,
        "mode": mode,
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "resource_request": dict(resource_request or {}),
        "resource_actual": dict(resource_request or {}),
    }
