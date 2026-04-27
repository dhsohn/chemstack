from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from chemstack.core.paths import validate_job_dir
from chemstack.core.utils import now_utc_iso, timestamped_token
from chemstack.flow.state import workflow_workspace_internal_engine_paths_from_path

from ..config import AppConfig

MANIFEST_FILE_NAME = "xtb_job.yaml"
SUPPORTED_JOB_TYPES = {"path_search", "opt", "sp", "ranking"}
_EXCLUDE_RE = re.compile(r"(?:^xtb_|^struc|^coord)", re.IGNORECASE)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_key(value: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "_", _normalize_text(value)).strip("._-")
    return collapsed.lower() or "unknown_key"


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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
        raise ValueError(f"Missing xTB job manifest: {path}")
    with path.open("r", encoding="utf-8") as handle:
        parsed = yaml.safe_load(handle) or {}
    if not isinstance(parsed, dict):
        raise ValueError(f"Invalid xTB job manifest: {path}")
    return parsed


def job_type(manifest: dict[str, Any]) -> str:
    value = _normalize_text(manifest.get("job_type", "path_search")).lower() or "path_search"
    if value not in SUPPORTED_JOB_TYPES:
        raise ValueError(f"Unsupported xtb job_type: {value}. supported={sorted(SUPPORTED_JOB_TYPES)}")
    return value


def _xyz_files(root: Path) -> list[Path]:
    return sorted([path.resolve() for path in root.glob("*.xyz") if path.is_file()], key=lambda path: path.name.lower())


def _choose_xyz(root: Path, explicit_name: str, *, label: str) -> Path:
    files = _xyz_files(root)
    if explicit_name:
        candidate = (root / explicit_name).resolve()
        if not candidate.exists() or not candidate.is_file():
            raise ValueError(f"{label} file not found: {candidate}")
        if candidate.suffix.lower() != ".xyz":
            raise ValueError(f"{label} file must be .xyz: {candidate}")
        return candidate
    if not files:
        raise ValueError(f"No .xyz files found in {label} directory: {root}")
    preferred = [path for path in files if not _EXCLUDE_RE.search(path.name)]
    return preferred[0] if preferred else files[0]


def _choose_root_xyz(job_dir: Path, explicit_name: str) -> Path:
    return _choose_xyz(job_dir, explicit_name, label="input")


def resolve_job_inputs(job_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    resolved_job_dir = job_dir.expanduser().resolve()
    resolved_type = job_type(manifest)

    if resolved_type == "path_search":
        reaction_key = _normalize_key(_normalize_text(manifest.get("reaction_key")) or resolved_job_dir.name)

        reactants_dir = resolved_job_dir / "reactants"
        products_dir = resolved_job_dir / "products"
        if not reactants_dir.exists() or not reactants_dir.is_dir():
            raise ValueError(f"Missing reactants directory: {reactants_dir}")
        if not products_dir.exists() or not products_dir.is_dir():
            raise ValueError(f"Missing products directory: {products_dir}")

        reactant_xyz = _choose_xyz(reactants_dir, _normalize_text(manifest.get("reactant_xyz")), label="reactant")
        product_xyz = _choose_xyz(products_dir, _normalize_text(manifest.get("product_xyz")), label="product")

        input_summary = {
            "reactant_xyz": str(reactant_xyz),
            "product_xyz": str(product_xyz),
            "reactant_count": len(_xyz_files(reactants_dir)),
            "product_count": len(_xyz_files(products_dir)),
        }
        return {
            "job_type": resolved_type,
            "reaction_key": reaction_key,
            "selected_input_xyz": reactant_xyz,
            "secondary_input_xyz": product_xyz,
            "input_summary": input_summary,
        }

    if resolved_type == "ranking":
        candidates_dir_name = _normalize_text(manifest.get("candidates_dir", "candidates")) or "candidates"
        candidates_dir = (resolved_job_dir / candidates_dir_name).resolve()
        if not candidates_dir.exists() or not candidates_dir.is_dir():
            raise ValueError(f"Missing ranking candidates directory: {candidates_dir}")
        candidate_paths = _xyz_files(candidates_dir)
        if not candidate_paths:
            raise ValueError(f"No .xyz candidates found in ranking directory: {candidates_dir}")
        molecule_key = _normalize_key(
            _normalize_text(manifest.get("molecule_key"))
            or _normalize_text(manifest.get("reaction_key"))
            or resolved_job_dir.name
        )
        top_n = max(1, _as_int(manifest.get("top_n", 3), 3))
        input_summary = {
            "candidates_dir": str(candidates_dir),
            "candidate_count": len(candidate_paths),
            "candidate_paths": [str(path) for path in candidate_paths],
            "top_n": top_n,
        }
        return {
            "job_type": resolved_type,
            "reaction_key": molecule_key,
            "selected_input_xyz": candidate_paths[0],
            "secondary_input_xyz": None,
            "input_summary": input_summary,
        }

    input_xyz = _choose_root_xyz(resolved_job_dir, _normalize_text(manifest.get("input_xyz")))
    molecule_key = _normalize_key(
        _normalize_text(manifest.get("molecule_key"))
        or _normalize_text(manifest.get("reaction_key"))
        or input_xyz.stem
        or resolved_job_dir.name
    )
    return {
        "job_type": resolved_type,
        "reaction_key": molecule_key,
        "selected_input_xyz": input_xyz,
        "secondary_input_xyz": None,
        "input_summary": {
            "input_xyz": str(input_xyz),
            "input_count": 1,
        },
    }


def resolve_job_dir(cfg: AppConfig, raw_job_dir: str) -> Path:
    candidate = Path(raw_job_dir).expanduser().resolve()
    workflow_root = _normalize_text(getattr(cfg, "workflow_root", ""))
    if workflow_root:
        runtime_paths = workflow_workspace_internal_engine_paths_from_path(
            candidate,
            workflow_root=workflow_root,
            engine="xtb",
        )
        if runtime_paths is None:
            raise ValueError(
                "Job directory must be under a workflow-local xTB root: "
                "<workflow.root>/<workflow_id>/02_xtb/..."
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
    return timestamped_token("xtb")


def queued_state_payload(
    *,
    job_id: str,
    job_dir: Path,
    selected_input_xyz: Path,
    job_type: str,
    reaction_key: str,
    input_summary: dict[str, Any],
    resource_request: dict[str, int] | None = None,
) -> dict[str, Any]:
    now = now_utc_iso()
    candidate_count = int(input_summary.get("candidate_count", 0) or 0)
    return {
        "job_id": job_id,
        "job_dir": str(job_dir),
        "selected_input_xyz": str(selected_input_xyz),
        "job_type": job_type,
        "reaction_key": reaction_key,
        "input_summary": dict(input_summary),
        "status": "queued",
        "created_at": now,
        "updated_at": now,
        "candidate_count": candidate_count,
        "candidate_paths": list(input_summary.get("candidate_paths", [])),
        "selected_candidate_paths": [],
        "resource_request": dict(resource_request or {}),
        "resource_actual": dict(resource_request or {}),
    }
